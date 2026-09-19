//! The error type for everything in `distlib-store`.

use std::path::PathBuf;

use thiserror::Error;

/// The result of any fallible operation in this crate.
pub type Result<T> = std::result::Result<T, StoreError>;

/// Why the read model could not be opened, read or written.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The database file would not open, or its directory could not be made.
    #[error("could not open the read model at {path}")]
    Open {
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// SQLite refused a statement.
    #[error("the read model could not be {doing}")]
    Sql {
        doing: &'static str,
        #[source]
        source: rusqlite::Error,
    },

    /// The blocking pool the database runs on went away, which is shutdown.
    #[error("the read model's database task did not finish")]
    Stopped(#[source] tokio::task::JoinError),
}

impl StoreError {
    /// Wraps a SQLite failure, saying what was being attempted.
    pub(crate) fn sql(doing: &'static str) -> impl Fn(rusqlite::Error) -> Self {
        move |source| Self::Sql { doing, source }
    }
}
