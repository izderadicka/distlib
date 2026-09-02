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

/// Whether `offered` is this token.
///
/// Compares in constant time rather than with `==`, which stops at the first
/// differing byte and so takes measurably longer the more of a guess is right —
/// enough to recover a token one byte at a time from a few thousand calls.
///
/// That is worth four lines here because the people who can call this API are
/// not the people who can read the token file. A different local user reaches
/// `127.0.0.1` but cannot read mode 0600, and `[api] bind_addr` may be set to a
/// non-loopback address for a node on a server or in a container. Neither can
/// see the file; both can time a reply.
///
/// No test guards this and none can: `==` is functionally identical and passes
/// everything below. Constant time is a property of *how* it answers rather
/// than what it answers, so it is kept by reading the code.
pub fn matches(token: &SecretString, offered: &str) -> bool {
    let (token, offered) = (token.expose_secret().as_bytes(), offered.as_bytes());
    if token.len() != offered.len() {
        return false;
    }
    token
        .iter()
        .zip(offered)
        .fold(0u8, |differences, (a, b)| differences | (a ^ b))
        == 0
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
