//! The data directory layout.
//!
//! Everything a node owns lives under one root, so a container mounts a single
//! volume and a backup is one directory:
//!
//! ```text
//! <root>/
//! ├── config.toml      configuration (see [`crate::config`])
//! ├── keys/node.key    ed25519 secret key, 0600
//! ├── raft/            membership log + state machine   (phase 1)
//! ├── docs/            iroh-docs replicas               (phase 2)
//! ├── blobs/           iroh-blobs content store         (phase 2)
//! ├── db/              SQLite read model                (phase 2)
//! └── index/           tantivy full-text index          (phase 2)
//! ```
//!
//! Accessors exist for the paths that are used today; the rest are listed above
//! so each phase adds its accessor here rather than inventing a path locally.

use std::path::{Path, PathBuf};

use crate::error::{CoreError, Result};

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

    /// Creates the root directory and any parents. Existing directories are fine.
    pub fn create(&self) -> Result<()> {
        std::fs::create_dir_all(&self.0).map_err(CoreError::io("create data directory", &self.0))
    }
}
