//! The error type for everything in `distlib-sync`.

use std::path::PathBuf;

use thiserror::Error;

/// The result of any fallible operation in this crate.
pub type Result<T> = std::result::Result<T, SyncError>;

/// Why the catalogue could not be opened, read or written.
///
/// The two crates underneath report failures in two different idioms —
/// iroh-docs in `anyhow`, iroh-blobs in `n0-error` — and neither belongs in
/// the rest of the codebase (CLAUDE.md: no `anyhow` in library code). Both are
/// boxed behind these variants at the crate edge, which is the same seam
/// §5.1's "wrap, don't expose" rule draws for the types.
#[derive(Debug, Error)]
pub enum SyncError {
    /// The replica store would not open.
    #[error("could not open the document store at {path}")]
    DocumentStore {
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// iroh-docs itself failed: starting it, opening a document, or an entry.
    #[error("the catalogue could not be {doing}")]
    Docs {
        doing: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The entry is here and its content is not — usually because it has just
    /// arrived from another member and the download has not finished.
    ///
    /// **A state, not a failure**, which is why it carries no source: it is
    /// read from [`iroh_blobs::api::proto::BlobStatus`] rather than inferred
    /// from something going wrong. A moment on a node that is catching up; a
    /// fault only if it persists. See [`crate::Catalogue::get`].
    #[error("the content of catalogue entry {hash} has not arrived on this node")]
    MissingContent { hash: String },

    /// The content store itself failed.
    ///
    /// Distinct from [`Self::MissingContent`] on purpose: a caller polling for
    /// an entry to arrive treats *that* as "not yet", and a store that is
    /// broken, closed or out of disk must not look like one that is merely
    /// behind — it would be waited on for ever.
    #[error("the catalogue's content store failed while it was {doing}")]
    Content {
        doing: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Asked for the catalogue before there is a group to have one.
    ///
    /// Not a failure so much as a "not yet": the document's identity comes
    /// from the group id, so a node that has not fetched the log has nothing
    /// to open. [`crate::Catalogue::ready`] is how a caller waits instead.
    #[error("this node is in no group yet, so it has no catalogue")]
    NoGroupYet,

    /// A local file offered to [`crate::Catalogue::add_file`] could not be
    /// read.
    ///
    /// Distinct from [`Self::Content`]: the store this crate wraps is not
    /// involved yet, and reporting a filesystem failure through it — "the
    /// catalogue's content store failed" — would send whoever reads the
    /// message looking in the wrong place.
    #[error("could not read {path} to add it as a blob")]
    LocalFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl SyncError {
    /// Wraps a content-store failure, saying what was being attempted.
    pub(crate) fn content<E>(doing: &'static str) -> impl Fn(E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        move |source| Self::Content {
            doing,
            source: Box::new(source),
        }
    }

    /// Wraps an iroh-docs failure, saying what was being attempted.
    pub(crate) fn docs(doing: &'static str) -> impl Fn(anyhow::Error) -> Self {
        move |source| Self::Docs {
            doing,
            source: source.into(),
        }
    }
}
