//! The bearer token for this node's local API.
//!
//! Whoever holds it can make this node propose membership changes as itself, so
//! it is a secret in the same sense the node's key is — kept in the data
//! directory at mode 0600, never logged, never printed. What it is *not* is a
//! second identity: it authenticates a caller to the local API and means
//! nothing to any other node.

use std::path::Path;

use secrecy::{ExposeSecret as _, SecretString};

use crate::{
    error::{CoreError, Result},
    private_file,
};

/// Bytes of entropy behind a token. 256 bits, rendered as 64 hex characters.
const TOKEN_BYTES: usize = 32;

/// Loads the token at `path`, generating and storing one if absent.
pub fn load_or_create(path: &Path) -> Result<SecretString> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            private_file::check_permissions(path)?;
            let token = text.trim();
            if token.is_empty() {
                return Err(CoreError::EmptyToken {
                    path: path.to_path_buf(),
                });
            }
            Ok(SecretString::from(token))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => create(path),
        Err(err) => Err(CoreError::io("read api token", path)(err)),
    }
}

/// Generates a token and writes it to `path`.
pub fn create(path: &Path) -> Result<SecretString> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(CoreError::io("create api token directory", parent))?;
    }

    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|error| CoreError::Random {
        message: error.to_string(),
    })?;

    let token = hex(&bytes);
    private_file::write(path, token.expose_secret().as_bytes())?;
    Ok(token)
}

fn hex(bytes: &[u8; TOKEN_BYTES]) -> SecretString {
    let mut out = String::with_capacity(TOKEN_BYTES * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        // Writing to a String cannot fail; the Result is `fmt`'s signature.
        let _ = write!(out, "{byte:02x}");
    }
    SecretString::from(out)
}
