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

/// Loads the secret key at `path`, refusing to create one.
///
/// The right call for anything that is *using* an identity rather than
/// establishing one. `distlib run` is the case that matters: pointed at an
/// empty directory it would otherwise mint a key and start a node that is
/// nobody — in no group, listening on a port nobody was told about — and the
/// only symptom is a member id the operator has never seen before. The
/// directory being wrong is the fact worth reporting, and this is the call
/// that can report it.
pub fn load_secret_key(path: &Path) -> Result<SecretKey> {
    match fs::read(path) {
        Ok(bytes) => {
            private_file::check_permissions(path)?;
            decode(path, &bytes)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(CoreError::NoIdentity {
            path: path.to_path_buf(),
        }),
        Err(err) => Err(CoreError::io("read secret key", path)(err)),
    }
}

/// Loads the secret key at `path`, generating and storing one if absent.
///
/// For the commands whose job is to establish an identity — `init`, `whoami`.
pub fn load_or_create_secret_key(path: &Path) -> Result<SecretKey> {
    match load_secret_key(path) {
        Err(CoreError::NoIdentity { .. }) => create_secret_key(path, false),
        loaded => loaded,
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
