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
