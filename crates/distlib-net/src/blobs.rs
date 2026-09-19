//! Fetching a media blob from a set of providers we choose.
//!
//! **Not iroh-docs' own downloader.** `distlib-sync::Catalogue` builds and
//! drives one of those already, for the reasons its own module doc gives: an
//! entry's value is a blob, and the engine fetches it as part of
//! reconciliation, with `distlib-sync`'s own repair sweep driving the same
//! downloader for entries the engine gave up on. This is the independent path
//! §5.6 and `library.download` want instead, for a blob that was never a
//! document entry's value in the first place — a media file `library.add`
//! hashed and stored directly, fetched here by the exact hash the catalogue's
//! item record names for it.
//!
//! **`BlobsProtocol`'s registration on the router stays where 2a-2 put it** —
//! `distlib_sync::Catalogue::protocols()`. The two need the very same `Store`
//! a document entry's content might already be sitting in, and a router
//! keeps only the last handler registered for one ALPN, silently — so moving
//! that registration here as well, to serve the *same* store, would be two
//! owners of one thing rather than one. What was missing was only the fetch
//! half: asking a chosen set of members for a hash, independent of whatever
//! iroh-docs itself is doing.

use iroh::{Endpoint, EndpointId};
use iroh_blobs::{
    Hash, HashAndFormat,
    api::{
        Store,
        downloader::{Downloader, Shuffled},
    },
};

use crate::error::{NetError, Result};

/// A handle for fetching blobs from chosen providers, against a local store.
///
/// Cheap to clone: [`Downloader`] is itself a handle to an actor spawned once.
/// Built once and kept, per [`Store::downloader`]'s own advice — a fresh one
/// per fetch would be a fresh pool of connections every time.
#[derive(Debug, Clone)]
pub struct Blobs {
    downloader: Downloader,
}

impl Blobs {
    /// Wraps `store`'s downloader for fetching over `endpoint`.
    pub fn new(store: &Store, endpoint: &Endpoint) -> Self {
        Self {
            downloader: store.downloader(endpoint),
        }
    }

    /// Fetches `hash` into the store this was built from, trying `providers`
    /// in a random order.
    ///
    /// **No address is supplied here** — only ids. `providers` is a set of
    /// [`EndpointId`]s this crate chose (the item's custodians, say), and
    /// resolving one into a socket is left to whatever the endpoint's own
    /// address lookup already does — [`crate::AddressBook`] or
    /// [`crate::Directory`] in production, so the same knowledge that lets
    /// every other protocol dial a bare id lets this one too.
    ///
    /// No deadline of its own: how long is worth waiting for a media file is
    /// a question for the caller, not for a fetch primitive with no idea how
    /// big the file is or how urgent the request behind it. Wrap the call in
    /// `tokio::time::timeout` for one, the way `distlib-sync`'s repair sweep
    /// wraps the same downloader for its own.
    pub async fn fetch(&self, hash: Hash, providers: Vec<EndpointId>) -> Result<()> {
        self.downloader
            .download(HashAndFormat::raw(hash), Shuffled::new(providers))
            .await
            .map_err(|source| NetError::Fetch {
                hash,
                source: Box::new(source),
            })
    }
}
