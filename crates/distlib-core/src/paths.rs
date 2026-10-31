//! The data directory layout.
//!
//! Everything a node owns lives under one root, so a container mounts a single
//! volume and a backup is one directory:
//!
//! ```text
//! <root>/
//! ├── format           the data format, one number (see [`DATA_FORMAT`])
//! ├── config.toml      configuration (see [`crate::config`])
//! ├── keys/node.key    ed25519 secret key, 0600
//! ├── api.token        local api bearer token, 0600
//! ├── raft.redb        membership log + state machine   (phase 1)
//! ├── docs/            iroh-docs replicas               (phase 2)
//! ├── blobs/           iroh-blobs content store         (phase 2)
//! ├── db/              SQLite read model                (phase 2)
//! └── index/           tantivy full-text index          (phase 2)
//! ```
//!
//! Accessors exist for the paths that are used today; the rest are listed above
//! so each phase adds its accessor here rather than inventing a path locally.
//!
//! `raft.redb` is one file rather than the `raft/` directory this doc used to
//! promise, and the doc was the thing that was wrong: openraft's log and state
//! machine share a single redb database, which is what makes a node's whole
//! consensus state one thing to copy or delete.

use std::{
    cmp::Ordering,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use crate::error::{CoreError, Result};

/// How this build lays out and encodes what a data directory holds (5d-6).
///
/// Raised by any change an older build would misread, with the step up from
/// the old number added to [`DataDir::check_format`].
pub const DATA_FORMAT: u32 = 1;

/// The root of everything a node stores.
#[derive(Debug, Clone, PartialEq)]
pub struct DataDir(PathBuf);

impl DataDir {
    /// Uses `root` as given.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self(root.into())
    }

    /// Resolves the data directory: an explicit override wins, otherwise the
    /// platform default (`~/.local/share/distlib` and its equivalents).
    ///
    /// The config file lives *inside* the data directory, so this deliberately
    /// consults no configuration — see [`CoreError::DataDirInConfig`].
    pub fn resolve(override_root: Option<PathBuf>) -> Result<Self> {
        if let Some(root) = override_root {
            return Ok(Self(root));
        }
        directories::ProjectDirs::from("", "", "distlib")
            .map(|dirs| Self(dirs.data_dir().to_path_buf()))
            .ok_or(CoreError::NoDataDir)
    }

    /// The root directory itself.
    pub fn root(&self) -> &Path {
        &self.0
    }

    /// `<root>/config.toml`.
    pub fn config_file(&self) -> PathBuf {
        self.0.join("config.toml")
    }

    /// `<root>/keys/node.key`.
    pub fn secret_key_file(&self) -> PathBuf {
        self.0.join("keys").join("node.key")
    }

    /// `<root>/api.token`.
    pub fn api_token_file(&self) -> PathBuf {
        self.0.join("api.token")
    }

    /// `<root>/raft.redb` — the membership log and the state folded from it.
    pub fn raft_db(&self) -> PathBuf {
        self.0.join("raft.redb")
    }

    /// `<root>/docs` — the catalogue's replicas.
    pub fn docs_dir(&self) -> PathBuf {
        self.0.join("docs")
    }

    /// `<root>/blobs` — content, from catalogue entry values to media files.
    pub fn blobs_dir(&self) -> PathBuf {
        self.0.join("blobs")
    }

    /// `<root>/db` — the SQLite read model projected from the catalogue.
    ///
    /// Deletable, unlike everything else under the root: it holds nothing that
    /// is not derived from `docs/`, and a node that starts without it rebuilds
    /// it from the document. That is a property worth keeping rather than an
    /// accident — it is what makes a schema change a deletion rather than a
    /// migration.
    pub fn db_dir(&self) -> PathBuf {
        self.0.join("db")
    }

    /// `<root>/index` — the tantivy full-text index over the read model.
    ///
    /// Deletable for the same reason `db_dir` is: it holds nothing that is not
    /// derived from `docs/`, rebuilt by the same replay that rebuilds `db/`.
    pub fn index_dir(&self) -> PathBuf {
        self.0.join("index")
    }

    /// `<root>/format` — [`DATA_FORMAT`] as the directory was last written.
    pub fn format_file(&self) -> PathBuf {
        self.0.join("format")
    }

    /// Creates the root directory and any parents, and checks its format.
    /// Existing directories are fine.
    pub fn create(&self) -> Result<()> {
        std::fs::create_dir_all(&self.0)
            .map_err(CoreError::io("create data directory", &self.0))?;
        self.check_format()
    }

    /// Refuses a directory written in a format this build does not know.
    ///
    /// One without a format file is new, or was written before 5d-6, which
    /// no release was; either way it is stamped with this build's. Every
    /// directory a release opens is stamped from then on, so a missing file
    /// never has to mean an older format. A newer format is refused, since
    /// downgrades are not supported (`plan-upgrades.md`, Q5).
    pub fn check_format(&self) -> Result<()> {
        let path = self.format_file();
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return std::fs::write(&path, format!("{DATA_FORMAT}\n"))
                    .map_err(CoreError::io("write", &path));
            }
            Err(error) => return Err(CoreError::io("read", &path)(error)),
        };
        let bad_format = || CoreError::BadDataFormat {
            path: path.clone(),
            text: text.trim().to_owned(),
        };
        let found: u32 = text.trim().parse().map_err(|_| bad_format())?;
        match found.cmp(&DATA_FORMAT) {
            Ordering::Equal => Ok(()),
            Ordering::Greater => Err(CoreError::DataDirTooNew {
                path: self.0.clone(),
                found,
                supported: DATA_FORMAT,
            }),
            // Where a migration goes: one step per format, each keeping a
            // backup of what it changes, then the new number written. Format
            // 1 is the first, so nothing is older.
            Ordering::Less => Err(bad_format()),
        }
    }
}
