//! Loading and creating the node's ed25519 secret key.
//!
//! The key file holds 32 raw bytes and nothing else — no encoding, no header.
//! It is the node's whole identity: losing it means losing membership, and
//! leaking it means someone else can be this member.

use std::{fs, path::Path};

use iroh::SecretKey;

use crate::{error::CoreError, id::MemberId};

/// Length of a raw ed25519 secret key.
const KEY_LEN: usize = 32;

/// Loads the secret key at `path`, generating and storing one if absent.
pub fn load_or_create_secret_key(path: &Path) -> Result<SecretKey, CoreError> {
    match fs::read(path) {
        Ok(bytes) => {
            check_permissions(path)?;
            decode(path, &bytes)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => create_secret_key(path, false),
        Err(err) => Err(CoreError::io("read secret key", path)(err)),
    }
}

/// Generates a new secret key and writes it to `path`.
///
/// Refuses to replace an existing key unless `force` is set: overwriting is
/// indistinguishable from losing group membership.
pub fn create_secret_key(path: &Path, force: bool) -> Result<SecretKey, CoreError> {
    if !force && path.exists() {
        return Err(CoreError::KeyExists {
            path: path.to_path_buf(),
        });
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(CoreError::io("create key directory", parent))?;
    }

    let secret = SecretKey::generate();
    write_private(path, &secret.to_bytes())?;
    Ok(secret)
}

/// The member identity derived from a secret key.
pub fn member_id(secret: &SecretKey) -> MemberId {
    MemberId::from(secret.public())
}

fn decode(path: &Path, bytes: &[u8]) -> Result<SecretKey, CoreError> {
    let bytes: [u8; KEY_LEN] = bytes.try_into().map_err(|_| CoreError::MalformedKey {
        path: path.to_path_buf(),
        len: bytes.len(),
    })?;
    Ok(SecretKey::from_bytes(&bytes))
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8; KEY_LEN]) -> Result<(), CoreError> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};

    // Created 0600 in one step: opening first and chmod-ing after would leave a
    // window in which the key is world-readable.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(CoreError::io("create secret key", path))?;
    file.write_all(bytes)
        .map_err(CoreError::io("write secret key", path))?;
    file.sync_all()
        .map_err(CoreError::io("flush secret key", path))
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8; KEY_LEN]) -> Result<(), CoreError> {
    warn_permissions_unenforced(path);
    fs::write(path, bytes).map_err(CoreError::io("write secret key", path))
}

#[cfg(unix)]
fn check_permissions(path: &Path) -> Result<(), CoreError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path)
        .map_err(CoreError::io("inspect secret key", path))?
        .permissions()
        .mode();
    // Only the owner may read or write. Group and other bits must be clear.
    if mode & 0o077 != 0 {
        return Err(CoreError::KeyPermissions {
            path: path.to_path_buf(),
            mode: mode & 0o777,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_permissions(path: &Path) -> Result<(), CoreError> {
    warn_permissions_unenforced(path);
    Ok(())
}

#[cfg(not(unix))]
fn warn_permissions_unenforced(path: &Path) {
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        tracing::warn!(
            path = %path.display(),
            "file permissions on the secret key are not enforced on this platform; \
             ensure the data directory is not shared"
        );
    });
}
