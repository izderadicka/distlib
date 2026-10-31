//! The data directory's format (5d-6): stamped when new, refused when newer.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use distlib_core::{CoreError, DATA_FORMAT, DataDir};
use tempfile::TempDir;

#[test]
fn a_new_directory_is_stamped_with_this_builds_format_and_opens_again() {
    let root = TempDir::new().unwrap();
    let data_dir = DataDir::new(root.path().join("node"));

    data_dir.create().unwrap();
    data_dir.create().unwrap();

    assert_eq!(
        std::fs::read_to_string(data_dir.format_file()).unwrap(),
        format!("{DATA_FORMAT}\n")
    );
}

#[test]
fn a_directory_from_before_the_format_file_is_stamped_too() {
    let root = TempDir::new().unwrap();
    let data_dir = DataDir::new(root.path());
    std::fs::write(data_dir.raft_db(), b"").unwrap();

    data_dir.check_format().unwrap();

    assert_eq!(
        std::fs::read_to_string(data_dir.format_file()).unwrap(),
        format!("{DATA_FORMAT}\n")
    );
}

#[test]
fn a_newer_format_is_refused_and_says_to_run_the_newer_build() {
    let root = TempDir::new().unwrap();
    let data_dir = DataDir::new(root.path());
    std::fs::write(data_dir.format_file(), format!("{}\n", DATA_FORMAT + 1)).unwrap();

    let refused = data_dir.create().unwrap_err();

    assert!(
        matches!(refused, CoreError::DataDirTooNew { found, supported, .. }
            if found == DATA_FORMAT + 1 && supported == DATA_FORMAT),
        "{refused:?}"
    );
    assert!(
        refused.to_string().contains("run the newer build"),
        "{refused}"
    );
}

#[test]
fn a_format_file_holding_no_format_is_refused() {
    for text in ["", "two", "0"] {
        let root = TempDir::new().unwrap();
        let data_dir = DataDir::new(root.path());
        std::fs::write(data_dir.format_file(), text).unwrap();

        let refused = data_dir.check_format().unwrap_err();

        assert!(
            matches!(refused, CoreError::BadDataFormat { .. }),
            "{text:?}: {refused:?}"
        );
    }
}
