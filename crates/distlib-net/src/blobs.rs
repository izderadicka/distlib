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
//! **[`serving_events`] is the one piece of serving here** (5b-2c, 5b-3b): the
//! provider events `Catalogue` hands the `BlobsProtocol` it registers, which
//! tell which blobs are served and pace them the way [`BytePacer`] paces its
//! fetches.
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

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use distlib_core::{ContentHash, ItemId, KeepReason, KeptBlobs, MemberId};
use futures_lite::StreamExt as _;
use iroh::Endpoint;
use iroh_blobs::{
    Hash, HashAndFormat,
    api::{
        Store,
        blobs::BlobStatus,
        downloader::{DownloadProgressItem, Downloader},
    },
    provider::events::{
        AbortReason, ConnectMode, EventMask, EventResult, EventSender, ProviderMessage,
        RequestMode, ThrottleMode,
    },
};
use tokio::sync::mpsc;

use crate::{
    error::{NetError, Result},
    pacer::BytePacer,
};

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
    /// Every fetch through any clone of this paces here, so the limit is
    /// this node's, not each fetch's.
    download_pacer: Arc<BytePacer>,
}

impl Blobs {
    /// Wraps `store`, fetching over `endpoint`.
    pub fn new(store: &Store, endpoint: &Endpoint) -> Self {
        Self {
            store: store.clone(),
            downloader: store.downloader(endpoint),
            download_pacer: Arc::new(BytePacer::new(None)),
        }
    }

    /// What every fetch paces through, all of them together: its rate is
    /// this node's download limit.
    pub fn download_pacer(&self) -> &Arc<BytePacer> {
        &self.download_pacer
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
        Ok(matches!(
            self.status(hash).await?,
            BlobStatus::Complete { .. }
        ))
    }

    async fn status(&self, hash: ContentHash) -> Result<BlobStatus> {
        self.store
            .blobs()
            .status(to_blobs(hash))
            .await
            .map_err(|source| NetError::Blob {
                hash,
                source: Box::new(source),
            })
    }

    /// Every blob kept for any reason, with its size and the tags that keep
    /// it.
    ///
    /// **A blob being fetched counts at its whole size** once the store knows
    /// it, the space it is about to take; one tagged before any of it arrived
    /// counts nothing yet.
    pub async fn kept_blobs(&self) -> Result<KeptBlobs> {
        let mut kept = KeptBlobs::default();
        for reason in KeepReason::ALL {
            for (item, blob) in self.kept(reason).await? {
                let bytes = match self.status(blob).await? {
                    BlobStatus::Complete { size } | BlobStatus::Partial { size: Some(size) } => {
                        size
                    }
                    BlobStatus::Partial { size: None } | BlobStatus::NotFound => 0,
                };
                kept.add(reason, item, blob, bytes);
            }
        }
        Ok(kept)
    }

    /// Keeps `hash`, one of `item`'s blobs, for `reason`, until that reason
    /// is released: garbage collection deletes only what no tag keeps.
    ///
    /// The blob need not be here yet. Tagging a hash before fetching it keeps
    /// the fetch's bytes from the moment they land.
    pub async fn keep(&self, reason: KeepReason, item: ItemId, hash: ContentHash) -> Result<()> {
        let tag = reason.tag(item, hash);
        self.store
            .tags()
            .set(&tag, to_blobs(hash))
            .await
            .map_err(|source| NetError::Keep {
                hash,
                tag,
                source: Box::new(source),
            })
    }

    /// Stops keeping `hash`, one of `item`'s blobs, for `reason`. The blob
    /// stays while another tag keeps it, and goes at the next garbage
    /// collection when none does.
    pub async fn release(&self, reason: KeepReason, item: ItemId, hash: ContentHash) -> Result<()> {
        let tag = reason.tag(item, hash);
        self.store
            .tags()
            .delete(&tag)
            .await
            .map(|_deleted| ())
            .map_err(|source| NetError::Release {
                tag,
                source: Box::new(source),
            })
    }

    /// Every item and blob kept for `reason`, whether its bytes are here yet
    /// or not.
    pub async fn kept(&self, reason: KeepReason) -> Result<Vec<(ItemId, ContentHash)>> {
        self.kept_under(reason, reason.prefix()).await
    }

    /// The blobs of `item` kept for `reason`.
    pub async fn kept_for(&self, reason: KeepReason, item: ItemId) -> Result<Vec<ContentHash>> {
        let kept = self.kept_under(reason, &reason.item_prefix(item)).await?;
        Ok(kept.into_iter().map(|(_, blob)| blob).collect())
    }

    async fn kept_under(
        &self,
        reason: KeepReason,
        prefix: &str,
    ) -> Result<Vec<(ItemId, ContentHash)>> {
        let failed = |source: Box<dyn std::error::Error + Send + Sync>| NetError::ListKept {
            prefix: prefix.to_owned(),
            source,
        };
        let mut tags = self
            .store
            .tags()
            .list_prefix(prefix)
            .await
            .map_err(|source| failed(Box::new(source)))?;
        let mut kept = Vec::new();
        while let Some(tag) = tags.next().await {
            let tag = tag.map_err(|source| failed(Box::new(source)))?;
            match std::str::from_utf8(tag.name.as_ref())
                .ok()
                .and_then(|name| reason.parse(name))
            {
                Some(item_and_blob) => kept.push(item_and_blob),
                None => tracing::warn!(tag = ?tag.name, "a tag that names no item and blob"),
            }
        }
        Ok(kept)
    }

    /// Fetches `hash` into the store this was built from, trying `providers`
    /// one at a time, **in the order given**. The caller knows who is online
    /// and who holds what; this does not, so it does not reorder them.
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
    /// in this crate's own tests. That is what lets a caller offer the whole
    /// membership and let the download find the copy, which is what
    /// `library.download` does — the members known to hold it first.
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
        let providers: Vec<_> = providers
            .into_iter()
            .map(|member| member.endpoint_id())
            .collect();
        let mut items = self
            .downloader
            .download(HashAndFormat::raw(to_blobs(hash)), providers)
            .stream()
            .await
            .map_err(|source| failed(Box::new(source)))?;
        let mut last_offset = 0;
        while let Some(item) = items.next().await {
            match item {
                // Paced by not reading on: the downloader waits for this
                // stream before it reads the next chunk off the connection.
                DownloadProgressItem::Progress(offset) => {
                    progress(offset);
                    // An offset that went back is the next provider starting
                    // again, with all of its bytes new.
                    let arrived = offset.checked_sub(last_offset).unwrap_or(offset);
                    last_offset = offset;
                    self.download_pacer.pace(arrived).await;
                }
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
    /// **All or nothing at `target`.** iroh-blobs writes straight into the
    /// path it is given, so an export that fails partway — a full disk, the
    /// node stopping — would leave a truncated file there, and a truncated
    /// file at a download's destination is indistinguishable from one the
    /// operator put there: the next download would refuse to touch it. So
    /// the copy is written to a hidden sibling and renamed into place once
    /// complete; the same directory keeps the rename on one filesystem, where
    /// it is atomic. A sibling left by a node that died mid-copy is simply
    /// overwritten by the next export of the same file.
    ///
    /// Fails if the blob is not here in full; [`Self::has`] is how a caller
    /// asks first.
    pub async fn export(&self, hash: ContentHash, target: &Path) -> Result<()> {
        let failed = |source: Box<dyn std::error::Error + Send + Sync>| NetError::Export {
            hash,
            target: target.to_owned(),
            source,
        };
        let partial = partial_of(target);
        let copied = self
            .store
            .blobs()
            .export(to_blobs(hash), &partial)
            .await
            .map_err(|error| failed(Box::new(error)))
            .and_then(|_written| {
                // The byte count it answers with is the size of a file this
                // caller is about to be told the path of, so it is a fact
                // they can read off the filesystem rather than one worth a
                // return type.
                std::fs::rename(&partial, target).map_err(|error| failed(Box::new(error)))
            });
        if copied.is_err() {
            // Best effort: the error being returned is the one worth reading.
            let _ = std::fs::remove_file(&partial);
        }
        copied
    }
}

/// How many asks about chunks and requests the serving side queues before
/// iroh-blobs waits to make the next one.
const ASKS_WAITING: usize = 64;

/// How many served blobs wait to be recorded before more are dropped.
const SERVED_WAITING: usize = 1024;

/// Whether a member may be sent a blob, asked of every request this node
/// serves.
pub trait ServingGate: Send + Sync + 'static {
    fn may_serve(
        &self,
        requester: MemberId,
        hash: ContentHash,
    ) -> impl Future<Output = bool> + Send;
}

/// Provider events for `BlobsProtocol`: every request judged by `gate`, every
/// blob served told on the receiver, and every blob served paced through
/// `pacer`, if there is one, so everyone served at once shares the one rate.
/// The task answering ends once the protocol, and with it these events, is
/// dropped.
///
/// **A request is judged by who sent it**: the member each connection is
/// from, as it connected, and refused whole unless `gate` allows each of its
/// blobs.
///
/// **What is served is told, not waited on.** A blob served while the
/// receiver is full is dropped from it rather than holding up serving. One
/// task answers all of it, so a request can wait behind the gate's answer to
/// the one before, and with a limit, behind the chunks queued before it.
///
/// **The pace is a sustained rate, not a hard cap.** iroh-blobs asks about a
/// chunk (about 16 KiB of payload) after it has sent it, and waits for the
/// answer before sending the next. So:
/// - each request's first chunk goes out before the pacer is asked, and
///   requests at once make a burst that grows with how many there are;
/// - a stream of small requests, a blob of one chunk each, can go over the
///   rate;
/// - only payload is counted: the bao framing (a size and 64-byte parent
///   hashes) and QUIC's own overhead are not.
///
/// For media, a large blob per request, the excess is under one chunk in
/// each, so the rate holds within a few percent.
pub fn serving_events(
    pacer: Option<Arc<BytePacer>>,
    gate: impl ServingGate,
) -> (EventSender, mpsc::Receiver<ContentHash>) {
    let mask = EventMask {
        connected: ConnectMode::Notify,
        get: RequestMode::Intercept,
        get_many: RequestMode::Intercept,
        throttle: if pacer.is_some() {
            ThrottleMode::Intercept
        } else {
            ThrottleMode::None
        },
        ..EventMask::DEFAULT
    };
    let (events, mut asked) = EventSender::channel(ASKS_WAITING, mask);
    let (served, served_blobs) = mpsc::channel(SERVED_WAITING);
    tokio::spawn(async move {
        let mut requesters: HashMap<u64, MemberId> = HashMap::new();
        while let Some(message) = asked.recv().await {
            match message {
                ProviderMessage::ClientConnectedNotify(connected) => {
                    if let Some(endpoint) = connected.inner.endpoint_id {
                        requesters.insert(connected.inner.connection_id, endpoint.into());
                    }
                }
                ProviderMessage::ConnectionClosed(closed) => {
                    requesters.remove(&closed.inner.connection_id);
                }
                ProviderMessage::GetRequestReceived(request) => {
                    let requester = requesters.get(&request.inner.connection_id).copied();
                    let hashes = [from_blobs(request.inner.request.hash)];
                    let judged = judge(&gate, requester, &hashes, &served).await;
                    // A requester gone meanwhile has nothing to be told.
                    let _ = request.tx.send(judged).await;
                }
                ProviderMessage::GetManyRequestReceived(request) => {
                    let requester = requesters.get(&request.inner.connection_id).copied();
                    let hashes: Vec<ContentHash> = request
                        .inner
                        .request
                        .hashes
                        .iter()
                        .copied()
                        .map(from_blobs)
                        .collect();
                    let judged = judge(&gate, requester, &hashes, &served).await;
                    let _ = request.tx.send(judged).await;
                }
                ProviderMessage::Throttle(chunk) => {
                    if let Some(pacer) = &pacer {
                        pacer.pace(chunk.inner.size).await;
                    }
                    let _ = chunk.tx.send(Ok(())).await;
                }
                // The mask asks for no others.
                _ => {}
            }
        }
    });
    (events, served_blobs)
}

/// Allows a request for `hashes` only if `gate` allows `requester` each of
/// them, telling `served` of each one then. Refused when the requester is
/// not known, which a connection always tells first.
async fn judge(
    gate: &impl ServingGate,
    requester: Option<MemberId>,
    hashes: &[ContentHash],
    served: &mpsc::Sender<ContentHash>,
) -> EventResult {
    let Some(requester) = requester else {
        return Err(AbortReason::Permission);
    };
    for hash in hashes {
        if !gate.may_serve(requester, *hash).await {
            tracing::debug!(requester = %requester.fmt_short(), %hash, "refused a blob to a member who may not have it");
            return Err(AbortReason::Permission);
        }
    }
    for hash in hashes {
        // Full or closed: a use not recorded is harmless.
        let _ = served.try_send(*hash);
    }
    Ok(())
}

/// Where [`Blobs::export`] writes `target` before it is complete: beside it,
/// hidden, and named after it so an operator who finds one knows what it was.
pub fn partial_of(target: &Path) -> PathBuf {
    let name = target
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_default();
    target.with_file_name(format!(".{name}.partial"))
}

/// The seam [`ContentHash`]'s own doc comment names: both spell a blake3
/// hash, and the conversion is the raw bytes.
fn to_blobs(hash: ContentHash) -> Hash {
    Hash::from_bytes(*hash.as_bytes())
}

fn from_blobs(hash: Hash) -> ContentHash {
    ContentHash::from_bytes(*hash.as_bytes())
}
