//! Loading and creating the node's ed25519 secret key.
//!
//! The key file holds 32 raw bytes and nothing else — no encoding, no header.
//! It is the node's whole identity: losing it means losing membership, and
//! leaking it means someone else can be this member.

use std::{fs, path::Path};

use iroh::SecretKey;

use crate::{
    error::{CoreError, Result},
    id::MemberId,
    private_file,
};

/// Length of a raw ed25519 secret key.
const KEY_LEN: usize = 32;

/// Loads the secret key at `path`, generating and storing one if absent.
pub fn load_or_create_secret_key(path: &Path) -> Result<SecretKey> {
    match fs::read(path) {
        Ok(bytes) => {
            private_file::check_permissions(path)?;
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
pub fn create_secret_key(path: &Path, force: bool) -> Result<SecretKey> {
    if !force && path.exists() {
        return Err(CoreError::KeyExists {
            path: path.to_path_buf(),
        });
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(CoreError::io("create key directory", parent))?;
    }

    let secret = SecretKey::generate();
    private_file::write(path, &secret.to_bytes())?;
    Ok(secret)
}

/// The member identity derived from a secret key.
pub fn member_id(secret: &SecretKey) -> MemberId {
    MemberId::from(secret.public())
}

fn decode(path: &Path, bytes: &[u8]) -> Result<SecretKey> {
    let bytes: [u8; KEY_LEN] = bytes.try_into().map_err(|_| CoreError::MalformedKey {
        path: path.to_path_buf(),
        len: bytes.len(),
    })?;
    Ok(SecretKey::from_bytes(&bytes))
}
