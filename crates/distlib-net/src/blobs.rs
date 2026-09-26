//! Fetching a media blob from a set of providers we choose, and writing one
//! back out to a file.
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
//!
//! **2b-3 widened this from fetching to fetching and exporting**, which is a
//! charter worth stating rather than leaving to be inferred from the method
//! list. `library.download` is fetch-then-write-to-a-file, and splitting the
//! two halves across two crates — the fetch here, the export on
//! `distlib_sync::Catalogue` beside its `add_file` — would put two crates on
//! iroh-blobs' file-transfer API for one operation. The division that holds
//! instead is by *what the store is being used as*: `distlib-sync` owns it as
//! the thing iroh-docs keeps entry values in, and imports a local file into
//! it for that reason; this crate owns it as the thing media moves in and out
//! of, in either direction.

use std::path::Path;

use distlib_core::{ContentHash, MemberId};
use futures_lite::StreamExt as _;
use iroh::Endpoint;
use iroh_blobs::{
    Hash, HashAndFormat,
    api::{
        Store,
        blobs::BlobStatus,
        downloader::{DownloadProgressItem, Downloader, Shuffled},
    },
};

use crate::error::{NetError, Result};

/// A handle for moving media in and out of a local blob store: fetching what
/// is not here from members who have it, and writing what is here to a file.
///
/// Cheap to clone: both halves are handles to an actor spawned once. Built
/// once and kept, per [`Store::downloader`]'s own advice — a fresh one per
/// fetch would be a fresh pool of connections every time.
#[derive(Debug, Clone)]
pub struct Blobs {
    store: Store,
    downloader: Downloader,
}

impl Blobs {
    /// Wraps `store`, fetching over `endpoint`.
    pub fn new(store: &Store, endpoint: &Endpoint) -> Self {
        Self {
            store: store.clone(),
            downloader: store.downloader(endpoint),
        }
    }

    /// Whether this store already holds all of `hash`.
    ///
    /// **`Complete`, not merely present.** A half-downloaded blob is in the
    /// store's own listing, and treating it as held is the exact bug the
    /// phase doc records against collapsing `distlib-sync`'s repair sweep
    /// onto `blobs.list()`: a caller that believed it would never ask for the
    /// rest of the bytes.
    ///
    /// Still the store's own bookkeeping rather than a stat of anything: a
    /// blob the store holds only as a reference to a file outside it reads as
    /// `Complete` after that file is deleted. Nothing in this workspace can
    /// produce one — [`Self::export`] copies and `Catalogue::add_file`
    /// imports by copying — and
    /// `an_exported_blob_is_a_copy_and_the_store_still_holds_it` is where that
    /// is pinned.
    pub async fn has(&self, hash: ContentHash) -> Result<bool> {
        let status = self
            .store
            .blobs()
            .status(to_blobs(hash))
            .await
            .map_err(|source| NetError::Blob {
                hash,
                source: Box::new(source),
            })?;
        Ok(matches!(status, BlobStatus::Complete { .. }))
    }

    /// Fetches `hash` into the store this was built from, trying `providers`
    /// in a random order.
    ///
    /// **No address is supplied here** — only member ids. `providers` is a
    /// set this crate's caller chose (the item's custodians, say), and
    /// resolving one into a socket is left to whatever the endpoint's own
    /// address lookup already does — [`crate::AddressBook`] or
    /// [`crate::Directory`] in production, so the same knowledge that lets
    /// every other protocol dial a bare id lets this one too.
    ///
    /// **A provider that does not hold the blob is skipped, not fatal**, and
    /// so is one that cannot be reached at all —
    /// `a_provider_that_does_not_hold_the_blob_is_skipped_rather_than_fatal`
    /// in this crate's own tests. That is what lets a caller with no
    /// availability index offer the whole membership and let the download
    /// find the copy, which is exactly what `library.download` does until
    /// §5.6's index exists.
    ///
    /// No deadline of its own: how long is worth waiting for a media file is
    /// a question for the caller, not for a fetch primitive with no idea how
    /// big the file is or how urgent the request behind it. Wrap the call in
    /// `tokio::time::timeout` for one, the way `distlib-sync`'s repair sweep
    /// wraps the same downloader for its own.
    pub async fn fetch(&self, hash: ContentHash, providers: Vec<MemberId>) -> Result<()> {
        self.fetch_with_progress(hash, providers, |_| {}).await
    }

    /// [`Self::fetch`], telling `progress` how far it has got as it goes.
    ///
    /// **What `progress` is told is a byte offset, and only that.** It has no
    /// total — the blob's size is the catalogue's `FileRecord::size`, which
    /// the caller has and this does not. It is **not monotonic**: when one
    /// provider fails and the next is tried, the offset starts again, and a
    /// caller drawing a bar keeps its own high-water mark rather than this
    /// pretending the bytes were not re-read. And a blob this store already
    /// holds is not fetched at all, so `progress` may never be called.
    ///
    /// **The outcome is decided here, by three rules.** iroh-blobs' own
    /// `.await` on a download applies them and throws the progress away, and
    /// its version is private, so this is a reimplementation — a download
    /// fails if the stream reports an error or a download error, and **the
    /// stream ending with neither is the only thing that means success**.
    /// Getting that wrong would report a failed fetch as done, which is worse
    /// than reporting no progress at all.
    pub async fn fetch_with_progress(
        &self,
        hash: ContentHash,
        providers: Vec<MemberId>,
        mut progress: impl FnMut(u64),
    ) -> Result<()> {
        let failed =
            |source: Box<dyn std::error::Error + Send + Sync>| NetError::Fetch { hash, source };
        let providers = providers
            .into_iter()
            .map(|member| member.endpoint_id())
            .collect();
        let mut items = self
            .downloader
            .download(HashAndFormat::raw(to_blobs(hash)), Shuffled::new(providers))
            .stream()
            .await
            .map_err(|source| failed(Box::new(source)))?;
        while let Some(item) = items.next().await {
            match item {
                DownloadProgressItem::Progress(offset) => progress(offset),
                DownloadProgressItem::Error(source) => return Err(failed(source.into())),
                // Only sent when a request is split across providers, and
                // `download` never splits one: for a single raw blob this arm
                // cannot be reached, and no test here reaches it. Kept because
                // it is the downloader's own contract, and the day this
                // fetches a collection it is the failure that will arrive.
                DownloadProgressItem::DownloadError => {
                    return Err(failed(
                        "every provider was tried, and none of them served it".into(),
                    ));
                }
                // Which provider is being asked, and which one gave up, are
                // the downloader's business; what a caller sees is the offset.
                DownloadProgressItem::TryProvider { .. }
                | DownloadProgressItem::ProviderFailed { .. }
                | DownloadProgressItem::PartComplete { .. } => {}
            }
        }
        Ok(())
    }

    /// Writes `hash` out of the store to `target`, as a copy.
    ///
    /// **A copy, not a reference** — iroh-blobs' own default for
    /// [`iroh_blobs::api::blobs::Blobs::export`], and the one that matches
    /// what a download means: the file an operator asked for is theirs to
    /// move, rename or delete, and the store goes on holding its own copy so
    /// that this node keeps serving the blob to the group afterwards. The
    /// alternative mode hands out a path into the store's own data, where
    /// either of those would be the node quietly ceasing to be a holder.
    ///
    /// Fails if the blob is not here in full; [`Self::has`] is how a caller
    /// asks first.
    pub async fn export(&self, hash: ContentHash, target: &Path) -> Result<()> {
        self.store
            .blobs()
            .export(to_blobs(hash), target)
            .await
            .map_err(|source| NetError::Export {
                hash,
                target: target.to_owned(),
                source: Box::new(source),
            })
            // The byte count it answers with is the size of a file this
            // caller is about to be told the path of, so it is a fact they
            // can read off the filesystem rather than one worth a return
            // type — and `FileRecord::size` already carries the claim it
            // would be checked against.
            .map(|_written| ())
    }
}

/// The seam [`ContentHash`]'s own doc comment names: both spell a blake3
/// hash, and the conversion is the raw bytes.
fn to_blobs(hash: ContentHash) -> Hash {
    Hash::from_bytes(*hash.as_bytes())
}
