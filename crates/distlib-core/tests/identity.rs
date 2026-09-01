//! Secret key creation, reload and the permission guard.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{
    CoreError,
    identity::{create_secret_key, load_or_create_secret_key, member_id},
};
use tempfile::TempDir;

fn key_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("keys").join("node.key")
}

#[test]
fn a_key_is_created_once_and_reloaded_thereafter() {
    let dir = TempDir::new().unwrap();
    let path = key_path(&dir);

    let created = load_or_create_secret_key(&path).unwrap();
    let reloaded = load_or_create_secret_key(&path).unwrap();

    assert_eq!(
        member_id(&created),
        member_id(&reloaded),
        "identity must survive a restart — it is the group membership"
    );
    assert!(path.exists());
}

#[test]
fn the_key_file_holds_exactly_the_raw_key() {
    let dir = TempDir::new().unwrap();
    let path = key_path(&dir);

    let secret = load_or_create_secret_key(&path).unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), secret.to_bytes());
}

#[test]
fn a_truncated_key_file_is_reported_not_guessed_at() {
    let dir = TempDir::new().unwrap();
    let path = key_path(&dir);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, [0u8; 16]).unwrap();
    set_owner_only(&path);

    let error = load_or_create_secret_key(&path).unwrap_err();

    assert!(
        matches!(error, CoreError::MalformedKey { len: 16, .. }),
        "expected MalformedKey, got {error:?}"
    );
}

#[test]
fn an_existing_key_is_not_replaced_by_accident() {
    let dir = TempDir::new().unwrap();
    let path = key_path(&dir);
    let original = load_or_create_secret_key(&path).unwrap();

    let error = create_secret_key(&path, false).unwrap_err();
    assert!(
        matches!(error, CoreError::KeyExists { .. }),
        "expected KeyExists, got {error:?}"
    );

    let replaced = create_secret_key(&path, true).unwrap();
    assert_ne!(
        member_id(&original),
        member_id(&replaced),
        "--force must actually generate a new identity"
    );
}

#[cfg(unix)]
fn set_owner_only(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[cfg(not(unix))]
fn set_owner_only(_path: &std::path::Path) {}

#[cfg(unix)]
mod unix {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn a_new_key_is_owner_only() {
        let dir = TempDir::new().unwrap();
        let path = key_path(&dir);

        load_or_create_secret_key(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "key was created with mode {mode:04o}");
    }

    #[test]
    fn a_readable_key_is_refused() {
        let dir = TempDir::new().unwrap();
        let path = key_path(&dir);
        load_or_create_secret_key(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let error = load_or_create_secret_key(&path).unwrap_err();

        assert!(
            matches!(error, CoreError::NotPrivate { mode: 0o644, .. }),
            "a key other users can read must not be loaded silently; got {error:?}"
        );
    }
}
