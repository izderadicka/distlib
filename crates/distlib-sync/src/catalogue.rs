//! The group's catalogue: one iroh-docs document, replicated between members.
//!
//! One document per group, and its identity is *derived* rather than
//! distributed. The alternative — generating a random namespace secret and
//! carrying it in the membership log — was built and then thrown away: a
//! secret every member can read out of the log is not a secret from any
//! member, and a non-member never gets far enough to use it, because the
//! allowlist refuses the connection (§5.1 says as much: the namespace secret
//! is a bearer token of convenience, not access control). What it bought was
//! an event, a stored-format break and a repair path for groups founded
//! before it existed. So the key is `blake3(domain || group_id)`, which every
//! member can compute from what it already has, and the log goes back to
//! carrying membership alone.
//!
//! Everything lives in this one document, under key prefixes (`item/...`, and
//! later `rating/...`). Separate documents only buy separate *audiences* —
//! syncing one without the other, or writing one without the other — and
//! nothing asks for that yet. A later phase that wants it derives a second
//! key beside this one; nothing here has to move.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use distlib_consensus::MembershipState;
use distlib_core::{Absorbed, GroupId, Item, ItemId, Key, MemberId};
use distlib_net::{Protocols, Transport};
use futures_lite::stream::StreamExt as _;
use iroh::{EndpointAddr, SecretKey};
use iroh_blobs::{BlobsProtocol, api::Store as BlobStore, api::proto::BlobStatus};
use iroh_docs::{
    Author, AuthorId, Capability, Entry, NamespaceSecret,
    api::Doc,
    engine::{DefaultAuthorStorage, Engine},
    protocol::Docs,
    store::{Query, Store as DocumentStore},
};
use tokio::{sync::watch, task::JoinHandle};

use crate::error::{Result, SyncError};

/// Domain tag for deriving the catalogue's document key, so it cannot collide
/// with a group id, an item id or any other BLAKE3 output in the system.
const CATALOGUE_TAG: &[u8] = b"distlib.catalogue.v1";

/// The replica database, inside the directory the caller names.
const REPLICAS: &str = "docs.redb";

/// What this subsystem needs advertised on the endpoint.
///
/// **Gossip is not in here, and the catalogue does not work without it.** A
/// document's live updates travel over the process's gossip swarm, so a node
/// that does not serve `iroh-gossip` gets whatever the first reconciliation
/// brought and then never hears another word — quietly, with no error on
/// either side. One ALPN has one owner (a router keeps the last handler
/// registered for one, silently), and gossip's owner is the membership node,
/// which needs it for the log. Anything assembling a process therefore serves
/// both lists, which is what `distlib::alpns` does.
///
/// Declared apart from the handlers for the same reason
/// [`distlib_consensus::alpns`] is: the endpoint is bound before any handler
/// exists, so the two lists are written separately and kept in step by being
/// compared — see the agreement test over the composed set.
pub fn alpns() -> Vec<Vec<u8>> {
    vec![iroh_docs::ALPN.to_vec(), iroh_blobs::ALPN.to_vec()]
}

/// The document key every member of `group` computes for its catalogue.
///
/// **A wire fact, like the group id derivation it is built on.** Two nodes
/// that compute this differently do not fail: they open different documents,
/// sync nothing, and each sees an empty catalogue with no error anywhere. So
/// it is domain-separated, fixed, and pinned by a golden value in the tests.
pub fn catalogue_key(group: GroupId) -> NamespaceSecret {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CATALOGUE_TAG);
    hasher.update(group.as_bytes());
    NamespaceSecret::from_bytes(hasher.finalize().as_bytes())
}

/// The catalogue subsystem: the document, the stores under it, and the task
/// that opens it once this node knows which group it is in.
#[derive(Debug, Clone)]
pub struct Catalogue {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    docs: Docs,
    blobs: BlobsProtocol,
    /// This node's key, as an iroh-docs author.
    ///
    /// The node's own key, so `AuthorId == MemberId` by construction and "who
    /// wrote this entry" needs nothing stored beside the entry. The cost,
    /// stated rather than buried: the same ed25519 key signs iroh's TLS
    /// handshakes and docs entries. The two are domain-separated by protocol
    /// and neither is an oracle for the other.
    author: AuthorId,
    /// The document, once there is a group to derive it from.
    open: watch::Receiver<Option<Doc>>,
    /// Waiting for the group, then opening and syncing. Aborted on shutdown.
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Catalogue {
    /// Starts the catalogue on `transport`, storing documents under `store`.
    ///
    /// Returns as soon as the engines are up, which is before there is a
    /// document: the catalogue's identity comes from the group id, and a node
    /// that has just joined has not fetched the log yet. The task this spawns
    /// waits for the group the way the gossip task does, then opens the
    /// document and starts syncing with the core group.
    pub async fn start(
        transport: Transport,
        blobs: BlobStore,
        documents: Option<PathBuf>,
        key: &SecretKey,
        membership: watch::Receiver<MembershipState>,
    ) -> Result<Self> {
        // A directory rather than a file, because iroh-docs keeps its author
        // storage beside the replicas and later phases may keep more; the
        // caller names the directory and this owns what is in it.
        let replicas = match &documents {
            Some(dir) => {
                let failed = |source: std::io::Error| SyncError::DocumentStore {
                    path: dir.clone(),
                    source: source.into(),
                };
                std::fs::create_dir_all(dir).map_err(failed)?;
                DocumentStore::persistent(dir.join(REPLICAS)).map_err(|source| {
                    SyncError::DocumentStore {
                        path: dir.clone(),
                        source: source.into(),
                    }
                })?
            }
            None => DocumentStore::memory(),
        };

        // `Docs::persistent` would be the obvious call and is deliberately not
        // used: it pairs the replica store with
        // `DefaultAuthorStorage::Persistent`, a `default-author` file that is
        // a second copy of this node's identity and hard-errors when the two
        // disagree. The author is the node key, derived every start, so there
        // is nothing to persist and nothing to fall out of step.
        let downloader = blobs.downloader(&transport.endpoint);
        let engine = Engine::spawn(
            transport.endpoint.clone(),
            transport.gossip.clone(),
            replicas,
            blobs.clone(),
            downloader,
            DefaultAuthorStorage::Mem,
            None,
        )
        .await
        .map_err(SyncError::docs("started"))?;
        let docs = Docs::new(engine);

        let author = Author::from(key.clone());
        let author_id = author.id();
        docs.author_import(author)
            .await
            .map_err(SyncError::docs("given this node's author key"))?;

        let (opened, open) = watch::channel(None);
        let task = tokio::spawn(open_when_founded(
            docs.clone(),
            membership,
            MemberId::from(key.public()),
            opened,
        ));

        Ok(Self {
            inner: Arc::new(Inner {
                blobs: BlobsProtocol::new(&blobs, None),
                docs,
                author: author_id,
                open,
                task: Mutex::new(Some(task)),
            }),
        })
    }

    /// What this subsystem needs the process's router to serve.
    ///
    /// Both handlers, because docs cannot work without blobs: an entry's value
    /// *is* a blob, so a node that served documents and not content would
    /// replicate the keys and none of the values.
    pub fn protocols(&self) -> Protocols {
        vec![
            (iroh_docs::ALPN.to_vec(), Box::new(self.inner.docs.clone())),
            (
                iroh_blobs::ALPN.to_vec(),
                Box::new(self.inner.blobs.clone()),
            ),
        ]
    }

    /// Waits until this node has a catalogue to read and write.
    ///
    /// Which is to say: until it knows what group it is in. A founder reaches
    /// this immediately; a node that has just joined waits for its first
    /// fetch of the membership log.
    pub async fn ready(&self) {
        let mut open = self.inner.open.clone();
        // The borrow is dropped before the await, and a value that arrived
        // before this call is seen by the first check rather than waited for.
        while open.borrow_and_update().is_none() {
            if open.changed().await.is_err() {
                tracing::error!("the catalogue task ended before the catalogue opened");
                return;
            }
        }
    }

    /// Writes one entry, replacing whatever this node wrote there before.
    pub async fn put(&self, key: impl AsRef<[u8]>, value: impl Into<bytes::Bytes>) -> Result<()> {
        let doc = self.document()?;
        doc.set_bytes(self.inner.author, key.as_ref().to_vec(), value.into())
            .await
            .map_err(SyncError::docs("written to"))?;
        Ok(())
    }

    /// Reads one entry: the latest value any member wrote at `key`.
    ///
    /// `Ok(None)` means nobody has written that key.
    /// [`SyncError::MissingContent`] means somebody has and the bytes have not
    /// landed here yet — see `content` below for why those are two answers
    /// and not one. A caller polling for an entry to appear treats the second
    /// as "not yet" rather than as a failure.
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<bytes::Bytes>> {
        let doc = self.document()?;
        let query = Query::single_latest_per_key().key_exact(key.as_ref());
        let Some(entry) = doc
            .get_one(query)
            .await
            .map_err(SyncError::docs("read from"))?
        else {
            return Ok(None);
        };
        self.content(&entry)
            .await?
            .ok_or_else(|| SyncError::MissingContent {
                hash: entry.content_hash().to_string(),
            })
            .map(Some)
    }

    /// The content of one entry, or `None` if it has not arrived here yet.
    ///
    /// **An entry and its content arrive separately.** Set reconciliation
    /// brings the key and the hash of its value, and the engine's downloader
    /// fetches the value afterwards — which is why `LiveEvent` has both
    /// `InsertRemote { content_status }` and a later `ContentReady`.
    ///
    /// So the store is *asked*, rather than the answer inferred from a failed
    /// read: `get_bytes` reports seven kinds of trouble and only one of them is
    /// "we do not have this yet", so a caller polling for content to arrive
    /// would wait for ever on the other six. `None` is that one state. Anything
    /// that goes wrong on the way stays an error, here as much as in
    /// [`Self::get`] — the two differ only in how they say "not yet", because
    /// reading a whole item treats a field still on its way as a field the item
    /// does not have, while reading one key by name has a caller waiting on it.
    async fn content(&self, entry: &Entry) -> Result<Option<bytes::Bytes>> {
        let blobs = self.inner.blobs.store().blobs();
        let hash = entry.content_hash();
        let status = blobs.status(hash).await.map_err(SyncError::content(
            "asked whether an entry's content is here",
        ))?;
        if !matches!(status, BlobStatus::Complete { .. }) {
            return Ok(None);
        }
        blobs
            .get_bytes(hash)
            .await
            .map(Some)
            .map_err(SyncError::content("reading an entry's content"))
    }

    /// Writes what `item` says, and only that.
    ///
    /// One entry per field the item has, so two members improving the same
    /// item at once do not clobber each other: iroh-docs resolves
    /// last-writer-wins per key, and a field nobody touched is a key nobody
    /// wrote. An `Item` with one field set is a one-key update — see
    /// [`Item::entries`].
    pub async fn write(&self, item: &Item) -> Result<()> {
        for (key, value) in item.entries() {
            self.put(key, value).await?;
        }
        Ok(())
    }

    /// Reads one item back out of the entries that make it up.
    ///
    /// `None` when this node holds no entry for it at all. Otherwise the item
    /// as far as this node knows it: **an item is its entries**, so one that
    /// is still arriving reads as a partial item rather than as an error, and
    /// a field whose content has not landed yet is left out the same way. A
    /// caller that needs to know the difference watches for it to fill in.
    pub async fn item(&self, id: ItemId) -> Result<Option<Item>> {
        let doc = self.document()?;
        let query = Query::single_latest_per_key().key_prefix(Key::prefix_of(id));
        let entries = doc
            .get_many(query)
            .await
            .map_err(SyncError::docs("read from"))?;
        tokio::pin!(entries);

        let mut item = Item::new(id);
        let mut found = false;
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(SyncError::docs("read from"))?;
            found = true;
            let Some(value) = self.content(&entry).await? else {
                tracing::debug!(
                    key = %String::from_utf8_lossy(entry.key()),
                    "the content of this entry has not arrived yet; leaving the field out"
                );
                continue;
            };
            match item.absorb(entry.key(), &value) {
                Absorbed::Took | Absorbed::Unknown => {}
                // Both worth saying out loud and neither worth failing for:
                // the first means somebody wrote something this build cannot
                // read, the second means the prefix query answered with a key
                // about another item, which would be a bug in either the
                // query or the key encoding.
                Absorbed::Unreadable => tracing::warn!(
                    key = %String::from_utf8_lossy(entry.key()),
                    "a catalogue entry holds a value this build cannot read"
                ),
                Absorbed::NotThisItem => tracing::warn!(
                    key = %String::from_utf8_lossy(entry.key()),
                    %id,
                    "a prefix search for one item answered with another"
                ),
            }
        }
        Ok(found.then_some(item))
    }

    /// This node's author id, which is its member id.
    pub fn author(&self) -> AuthorId {
        self.inner.author
    }

    /// Stops the subsystem's own task. The engines stop with the router.
    pub fn shutdown(&self) {
        if let Some(task) = self
            .inner
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }

    fn document(&self) -> Result<Doc> {
        self.inner
            .open
            .borrow()
            .clone()
            .ok_or(SyncError::NoGroupYet)
    }
}

/// Opens the catalogue once the log says which group this node is in, and
/// starts syncing it with the core group.
///
/// The same shape as the gossip task next door: the identity of the thing to
/// join comes from the log, so the task waits for one rather than every
/// caller having to order startup around it.
async fn open_when_founded(
    docs: Docs,
    mut membership: watch::Receiver<MembershipState>,
    me: MemberId,
    opened: watch::Sender<Option<Doc>>,
) {
    let (group, peers) = loop {
        let seen = membership.borrow_and_update().clone();
        if let Some(group) = seen.group_id() {
            break (group, sync_with(&seen, me));
        }
        if membership.changed().await.is_err() {
            return;
        }
    };

    let doc = match docs
        .import_namespace(Capability::Write(catalogue_key(group)))
        .await
    {
        Ok(doc) => doc,
        Err(error) => {
            tracing::error!(%error, %group, "could not open the catalogue");
            return;
        }
    };
    tracing::info!(%group, document = %doc.id(), "opened the catalogue");

    // The core group, because those are the members whose addresses the log
    // carries. Everyone else joins through the same gossip swarm iroh-docs
    // runs for the document, which is how a follower reaches another
    // follower without either being told where it is.
    if let Err(error) = doc.start_sync(peers).await {
        tracing::error!(%error, "could not start syncing the catalogue");
        return;
    }

    let _ = opened.send(Some(doc));
    // Held so the receivers stay open for the life of the node; dropping the
    // sender would make `ready()` return on a catalogue that never opened.
    opened.closed().await;
}

/// The members to start syncing with: the core group, minus this node.
fn sync_with(membership: &MembershipState, me: MemberId) -> Vec<EndpointAddr> {
    membership
        .core()
        .iter()
        .filter(|(member, _)| **member != me)
        .filter_map(|(member, addr)| addr.to_endpoint_addr(*member).ok())
        .collect()
}
