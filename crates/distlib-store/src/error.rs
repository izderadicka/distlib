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

    /// tantivy refused to open, write or read the search index.
    #[error("the search index could not be {doing}")]
    Index {
        doing: &'static str,
        #[source]
        source: tantivy::TantivyError,
    },

    /// A search query was not well-formed.
    #[error("could not parse the search query {query:?}")]
    Query {
        query: String,
        #[source]
        source: tantivy::query::QueryParserError,
    },

    /// The blocking pool the database or index runs on went away, which is shutdown.
    #[error("the read model's background task did not finish")]
    Stopped(#[source] tokio::task::JoinError),

    /// `Projection::reindex` was called after the projection task had already
    /// stopped — there is nobody left to ask.
    #[error("the read model's projection task is no longer running")]
    ProjectionGone,

    /// A write was attempted after [`crate::index::SearchIndex::close`] took
    /// the writer away.
    #[error("the search index has been closed")]
    IndexClosed,

    /// A search hit's id field did not hold a readable [`distlib_core::ItemId`].
    ///
    /// Every document the index holds was written from one, so this is a bug
    /// in the encoding rather than an index that went wrong — the same
    /// argument `store.rs`'s `unreadable` makes for a SQLite column.
    #[error("a search hit's id field could not be read back: {0}")]
    Corrupt(String),
}

impl StoreError {
    /// Wraps a SQLite failure, saying what was being attempted.
    pub(crate) fn sql(doing: &'static str) -> impl Fn(rusqlite::Error) -> Self {
        move |source| Self::Sql { doing, source }
    }

    /// Wraps a tantivy failure, saying what was being attempted.
    pub(crate) fn index(doing: &'static str) -> impl Fn(tantivy::TantivyError) -> Self {
        move |source| Self::Index { doing, source }
    }
}
