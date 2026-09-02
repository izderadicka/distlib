//! The local API's bearer token.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{CoreError, token};
use secrecy::ExposeSecret as _;
use tempfile::TempDir;

#[test]
fn a_token_is_created_private_and_reused() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("api.token");

    let first = token::load_or_create(&path).unwrap();
    assert_eq!(first.expose_secret().len(), 64, "256 bits, hex encoded");

    let again = token::load_or_create(&path).unwrap();
    assert_eq!(
        first.expose_secret(),
        again.expose_secret(),
        "a restart must not invalidate every client's token"
    );
}

#[cfg(unix)]
#[test]
fn a_token_readable_by_others_is_refused() {
    use std::os::unix::fs::PermissionsExt;

    // Whoever holds it can make the node propose as itself, so a token the rest
    // of the machine can read is not a token.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("api.token");
    token::load_or_create(&path).unwrap();

    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "created private, not chmod-ed after");

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let error = token::load_or_create(&path).unwrap_err();
    assert!(
        matches!(error, CoreError::NotPrivate { mode: 0o644, .. }),
        "got {error}"
    );
}

#[test]
fn two_tokens_differ() {
    let dir = TempDir::new().unwrap();
    let one = token::create(&dir.path().join("one")).unwrap();
    let other = token::create(&dir.path().join("other")).unwrap();

    assert_ne!(one.expose_secret(), other.expose_secret());
}

#[test]
fn an_empty_token_file_is_refused() {
    // Rather than authenticating everyone who sends an empty bearer token.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("api.token");
    // Written privately, so this reaches the emptiness check rather than
    // tripping the permission one first.
    distlib_core::private_file::write(&path, b"   \n").unwrap();

    let error = token::load_or_create(&path).unwrap_err();
    assert!(matches!(error, CoreError::EmptyToken { .. }), "got {error}");
}
