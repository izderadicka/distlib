//! Files only the owner may read.
//!
//! The node's secret key and the local API token are both of this kind: a file
//! that is worthless if anyone else on the machine can read it. The rules are
//! the same for both, so they live here rather than being written twice and
//! drifting apart.

use std::{fs, path::Path};

use crate::error::{CoreError, Result};

/// Writes `bytes` to `path`, readable by the owner alone.
#[cfg(unix)]
pub fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};

    // Created 0600 in one step: opening first and chmod-ing after would leave a
    // window in which the file is world-readable.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(CoreError::io("create private file", path))?;
    file.write_all(bytes)
        .map_err(CoreError::io("write private file", path))?;
    file.sync_all()
        .map_err(CoreError::io("flush private file", path))
}

#[cfg(not(unix))]
pub fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    warn_unenforced(path);
    fs::write(path, bytes).map_err(CoreError::io("write private file", path))
}

/// Makes `path` readable by its owner alone, creating it empty if absent.
///
/// For a file this process does not write itself — a database another crate
/// opens, say — where refusing is the wrong answer: a node that has been
/// running since before the file held anything secret should tighten it and
/// carry on, not fail to start. Whoever writes the bytes still decides what
/// goes in; this only decides who may read them.
#[cfg(unix)]
pub fn ensure_private(path: &Path) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    let Ok(metadata) = fs::metadata(path) else {
        // Created here rather than by the caller so that it exists at 0600
        // from the start: creating it first and tightening after would leave a
        // window in which it is world-readable, and it is the caller's very
        // next act to write to it.
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .map_err(CoreError::io("create private file", path))?;
        return Ok(());
    };

    let mode = metadata.permissions().mode();
    if mode & 0o077 == 0 {
        return Ok(());
    }
    // Loud, because it means the file has been readable by somebody else for
    // as long as it has existed, and tightening it now does not undo that.
    tracing::warn!(
        path = %path.display(),
        mode = format!("{:o}", mode & 0o777),
        "tightening the permissions on a file that others could read"
    );
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(CoreError::io("restrict private file", path))
}

#[cfg(not(unix))]
pub fn ensure_private(path: &Path) -> Result<()> {
    warn_unenforced(path);
    Ok(())
}

/// Refuses a file that anyone but the owner can read or write.
#[cfg(unix)]
pub fn check_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path)
        .map_err(CoreError::io("inspect private file", path))?
        .permissions()
        .mode();
    // Only the owner may read or write. Group and other bits must be clear.
    if mode & 0o077 != 0 {
        return Err(CoreError::NotPrivate {
            path: path.to_path_buf(),
            mode: mode & 0o777,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn check_permissions(path: &Path) -> Result<()> {
    warn_unenforced(path);
    Ok(())
}

#[cfg(not(unix))]
fn warn_unenforced(path: &Path) {
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        tracing::warn!(
            path = %path.display(),
            "file permissions are not enforced on this platform; \
             ensure the data directory is not shared"
        );
    });
}
