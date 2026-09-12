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

use std::{path::PathBuf, sync::Arc};

use distlib_consensus::MembershipState;
use distlib_core::{GroupId, MemberId};
use distlib_net::{Protocols, Transport};
use iroh::{EndpointAddr, SecretKey};
use iroh_blobs::{BlobsProtocol, api::Store as BlobStore};
use iroh_docs::{
    Author, AuthorId, Capability, NamespaceSecret,
    api::Doc,
    engine::{DefaultAuthorStorage, Engine},
    protocol::Docs,
    store::Store as DocumentStore,
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

use std::sync::Mutex;

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
    /// **An entry and its content arrive separately**, and the distinction is
    /// visible here: set reconciliation brings the key and the hash of its
    /// value, and the engine's downloader fetches the value afterwards — which
    /// is why `LiveEvent` has both `InsertRemote { content_status }` and a
    /// later `ContentReady`. So `Ok(None)` means nobody has written that key,
    /// while [`SyncError::MissingContent`] means somebody has and the bytes
    /// have not landed on this node yet. A caller that is polling should treat
    /// the second as "not yet" rather than as a failure.
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<bytes::Bytes>> {
        let doc = self.document()?;
        let query = iroh_docs::store::Query::single_latest_per_key().key_exact(key.as_ref());
        let Some(entry) = doc
            .get_one(query)
            .await
            .map_err(SyncError::docs("read from"))?
        else {
            return Ok(None);
        };
        let content = self
            .inner
            .blobs
            .store()
            .get_bytes(entry.content_hash())
            .await
            .map_err(|source| SyncError::MissingContent {
                source: source.into(),
            })?;
        Ok(Some(content))
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
