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
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use distlib_consensus::MembershipState;
use distlib_core::{Absorbed, GroupId, Item, ItemId, Key, MemberId};
use distlib_net::{Directory, Protocols, Transport};
use futures_lite::stream::StreamExt as _;
use iroh::{EndpointAddr, EndpointId, SecretKey};
use iroh_blobs::{
    BlobsProtocol, Hash, HashAndFormat,
    api::Store as BlobStore,
    api::downloader::{Downloader, Shuffled},
    api::proto::BlobStatus,
};
use iroh_docs::{
    Author, AuthorId, Capability, Entry, NamespaceSecret,
    api::Doc,
    engine::{DefaultAuthorStorage, Engine},
    protocol::Docs,
    store::{Query, Store as DocumentStore},
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

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
        // Cloned rather than built twice: `downloader` spawns an actor, and a
        // second one would be a second pool of connections to the same peers.
        // The engine drives it for the content it knows to ask for; the task
        // below drives the same one for the content it never asked for.
        let downloader = blobs.downloader(&transport.endpoint);
        let engine = Engine::spawn(
            transport.endpoint.clone(),
            transport.gossip.clone(),
            replicas,
            blobs.clone(),
            downloader.clone(),
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
        let task = tokio::spawn(open_when_founded(Opening {
            docs: docs.clone(),
            blobs: blobs.clone(),
            downloader,
            membership,
            me: MemberId::from(key.public()),
            opened,
            directory: transport.directory.clone(),
        }));

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
/// What opening the catalogue needs, gathered rather than passed one by one.
///
/// Seven values, and the two loops the task ends in want overlapping subsets of
/// them — which is the case the house rule on parameter counts names.
struct Opening {
    docs: Docs,
    blobs: BlobStore,
    downloader: Downloader,
    membership: watch::Receiver<MembershipState>,
    me: MemberId,
    opened: watch::Sender<Option<Doc>>,
    directory: Directory,
}

async fn open_when_founded(opening: Opening) {
    let Opening {
        docs,
        blobs,
        downloader,
        mut membership,
        me,
        opened,
        directory,
    } = opening;
    let learned = directory.learned();
    let (group, peers) = loop {
        let seen = membership.borrow_and_update().clone();
        if let Some(group) = seen.group_id() {
            break (group, sync_with(&seen, me, &directory));
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

    let _ = opened.send(Some(doc.clone()));

    // The peer set is not a one-off, and finding that out cost a day. A
    // document's peers are handed to gossip once, and **a peer that could not
    // be resolved at that moment is never retried** — iroh-docs has no timer
    // for it. At the moment a node opens its catalogue it knows where the core
    // group is and nowhere else, because a follower's address arrives later, by
    // announcement. So the followers named above were named and dropped.
    //
    // Offering the peer again once its address is learned is what closes that,
    // and `start_sync` is the only way to say it: it skips the open if the
    // document is already syncing and takes whatever peers it is given. What it
    // does *not* do is take only the new ones — it dials every peer in the list,
    // which is why what it is handed is kept narrow.
    // Both until shutdown, which only the first can see: it holds `opened`, so
    // when its receivers go it returns and the other is dropped with it.
    tokio::select! {
        () = offer_peers_as_they_are_learned(
            doc.clone(),
            membership.clone(),
            me,
            learned,
            opened,
            directory.clone(),
        ) => {}
        () = fetch_content_nobody_offered(doc, blobs, downloader, membership, me, directory) => {}
    }
}

/// How often the document's peers are offered again regardless.
///
/// The same shape as the follow loop's idle poll, and for the same reason
/// (§4.2): the prompt path is an event, and a timer behind it is what makes the
/// guarantee. Learning an address is the event, but it is not the only thing
/// that strands a document — the node that introduced two members can go away
/// afterwards, and gossip does not repair a swarm it has lost its last
/// neighbour in. Nothing is broadcast here; it is a local call that re-offers
/// peers to this node's own engine, so the cost of it being wrong is a
/// connection attempt.
///
/// The longest the whole set can go un-offered, rather than the longest the
/// loop can sit idle — which are the same thing only while every wake-up offers
/// everything, and one of them no longer does.
const OFFER_AGAIN: Duration = Duration::from_secs(15);

/// The least time between two offers of the document's peers.
///
/// The learned-address signal fires once per member learned, and a node joining
/// a group learns several within a moment of each other. An offer makes
/// iroh-docs dial each peer it is handed, and when two nodes sync at each other
/// one aborts the other's incoming attempt — `AlreadySyncing`, handled there as
/// "do nothing, our outgoing sync is in progress". So an unthrottled burst
/// spends the group's time on handshakes that abort each other, which is worst
/// exactly where it is least affordable: a small, busy machine.
///
/// A floor coalesces the burst without losing it. The signal is a watch, which
/// keeps only its latest value, so what is waiting afterwards is "there was
/// news" — however much news there was.
///
/// Still needed now that a reaction offers only what it learned. It bounds the
/// *arrival* rate, not the size of one offer, and arrivals come in bursts by
/// their nature: a node joining a group of twenty hears about twenty members
/// inside a second.
const LEAST_BETWEEN_OFFERS: Duration = Duration::from_secs(2);

/// Re-offers the document's peers, promptly when there is news and slowly
/// regardless.
///
/// **Two kinds of round, and they want different things.** Hearing where one
/// member is says nothing about the others, so it offers that member alone. A
/// membership change or the idle timer says the set itself may be wrong — the
/// node that introduced two members may have gone — so those re-offer
/// everything, which is what repairs a stranded document.
///
/// Ends when the node shuts down, which is when `opened`'s receivers go — the
/// same condition that used to hold this task open.
async fn offer_peers_as_they_are_learned(
    doc: Doc,
    mut membership: watch::Receiver<MembershipState>,
    me: MemberId,
    mut learned: watch::Receiver<u64>,
    opened: watch::Sender<Option<Doc>>,
    directory: Directory,
) {
    // Cleared if the directory goes away, which disables that arm rather than
    // stopping: a receiver whose sender is gone reports so immediately and for
    // ever, and selecting on it again would spin.
    let mut still_learning = true;

    // What has already been handed over, so that learning where *one* member is
    // does not re-offer the rest. `start_sync` is not an add: it dials every
    // peer in the list it is given, new or not, and appends the document's own
    // remembered peers on top — so the cost of an offer follows the size of the
    // group rather than the size of the news.
    let mut offered: HashMap<EndpointId, EndpointAddr> = HashMap::new();

    // When everything was last offered. The timer arm below is restarted by
    // every other arm, so on its own it measures an *idle* gap — and now that a
    // learned address offers only that address, a group whose members keep
    // moving would push the sweep out for ever and never repair anything. This
    // makes `OFFER_AGAIN` what its own docs say it is: the longest the whole set
    // can go un-offered, whatever else happens in between.
    let mut last_full = Instant::now();

    loop {
        // Whether this round is a repair or a reaction. The two arms that mean
        // "the set itself may be wrong" — a membership change, and the timer —
        // re-offer everything; learning one address offers that one.
        let mut repair = tokio::select! {
            // Held so the receivers stay open for the life of the node;
            // dropping the sender would make `ready()` return on a catalogue
            // that never opened.
            () = opened.closed() => return,
            heard = learned.changed(), if still_learning => {
                if heard.is_err() {
                    // Nothing fills the directory any more. The timer below
                    // still stands, so this is not a reason to stop.
                    still_learning = false;
                    tracing::debug!("no longer learning addresses; offering on the timer alone");
                }
                false
            }
            changed = membership.changed() => {
                if changed.is_err() {
                    return;
                }
                true
            }
            // Deadline rather than delay: a plain `sleep` is built afresh on
            // every iteration, so any other arm firing at T+14 would push the
            // sweep out to T+29 and the bound below would be twice what it
            // says.
            () = tokio::time::sleep_until(last_full + OFFER_AGAIN) => true,
        };
        // The deadline above can come due at the same moment as a learned
        // address, and `select!` picks between two ready arms at random. This
        // makes the sweep win that race rather than losing it half the time.
        repair |= last_full.elapsed() >= OFFER_AGAIN;

        let reachable = sync_with(&membership.borrow_and_update().clone(), me, &directory);
        let peers = if repair {
            // Rebuilt rather than extended, so a member that left and came back
            // is offered again without anything here knowing that it did.
            last_full = Instant::now();
            offered = reachable
                .iter()
                .map(|peer| (peer.id, peer.clone()))
                .collect();
            reachable
        } else {
            let news: Vec<EndpointAddr> = reachable
                .into_iter()
                .filter(|peer| offered.get(&peer.id) != Some(peer))
                .collect();
            offered.extend(news.iter().map(|peer| (peer.id, peer.clone())));
            news
        };

        if peers.is_empty() {
            // Nothing was learned that this document did not already have —
            // somebody else's address changed, or a core node moved and the log
            // is the authority on where those are. An empty offer is not a free
            // one: `start_sync` would still read the remembered peers out of the
            // store and dial them.
            tracing::trace!("nothing new to offer the catalogue");
        } else if let Err(error) = doc.start_sync(peers).await {
            // Not fatal: whatever was already syncing goes on, and the sweep
            // above offers these again within `OFFER_AGAIN`. Not the next
            // address learned — that is a reaction, and these peers are already
            // written down as offered.
            tracing::debug!(%error, "could not offer the catalogue's peers again");
        }
        // Unconditional, including after a round that offered nothing. The floor
        // is there to coalesce a burst of arrivals, and a burst is exactly when
        // some of the rounds in it have nothing in them.
        tokio::time::sleep(LEAST_BETWEEN_OFFERS).await;
    }
}

/// How often this node asks for content that nobody offered it.
///
/// A repair path rather than the way content normally arrives, so it is paced
/// against the cost of being wrong. On a node that has everything a round is a
/// document scan and nothing on the wire: a hash whose bytes are here is never
/// asked for. What it is paced *against* is the opposite case — content nobody
/// reachable will ever serve, which an expelled member can be left holding the
/// key to. There is no backoff, so that node asks again on every round for as
/// long as it runs; slower than the burst floor next door for that reason, and
/// still prompt against the tens of seconds a stranded entry used to cost.
const FETCH_AGAIN: Duration = Duration::from_secs(5);

/// The longest one download is waited on before the sweep moves past it.
///
/// Not a limit on how long a transfer may take — it is a limit on how long one
/// unreachable hash may hold up every other. Without it a single peer that
/// accepts a connection and then says nothing would stop this loop for good,
/// which would be a worse fault than the one it is here to repair.
const FETCH_DEADLINE: Duration = Duration::from_secs(20);

/// Asks for the content of entries this node holds and was never sent.
///
/// **This repairs a one-shot in iroh-docs, and the gap is structural.** An
/// entry and its content travel separately, and when an entry arrives over
/// gossip the sender's content is assumed present *only if the message came
/// straight from its publisher*: `engine/gossip.rs` reads
/// `msg.scope.is_direct()` and records `ContentStatus::Missing` for anything
/// relayed. A relayed entry therefore starts no download and remembers no
/// provider — the hash goes into the engine's `missing_hashes` and stays there.
///
/// What is supposed to rescue it is `Op::ContentReady`, which a node broadcasts
/// when its own download finishes. That message is sent **once**, to **direct
/// neighbours only**, and never by the author — a local write emits `Op::Put`
/// and nothing else. So a node that is not a neighbour of whoever completes
/// first, at the moment they complete, holds the key for ever and never the
/// value. Nothing retries: a failed download is put back in `missing_hashes`
/// with no timer, and the engine's provider list is a snapshot taken when the
/// download started, so a peer learned afterwards is not tried either.
///
/// Measured, both before this existed and on the branch before this PR: the
/// group converges, then `catalogue.get` answers `MissingContent` until the
/// test gives up — *the entry is here, its content has not arrived*.
///
/// **It can only work because of the other half of this PR.** The downloader is
/// given bare [`EndpointId`]s and has to resolve them, and a follower's address
/// is in no log. Before members announced where they are, this retry would have
/// failed exactly the way the download it repairs did.
async fn fetch_content_nobody_offered(
    doc: Doc,
    blobs: BlobStore,
    downloader: Downloader,
    mut membership: watch::Receiver<MembershipState>,
    me: MemberId,
    directory: Directory,
) {
    loop {
        // At the top, so the first sweep does not race the opening sync that
        // will usually make it unnecessary.
        tokio::time::sleep(FETCH_AGAIN).await;

        // The same set the document is offered as peers, which is the right
        // one: a member this node cannot reach cannot serve it content either.
        let providers: Vec<EndpointId> =
            sync_with(&membership.borrow_and_update().clone(), me, &directory)
                .into_iter()
                .map(|peer| peer.id)
                .collect();
        if providers.is_empty() {
            continue;
        }

        let wanted = match content_not_here(&doc, &blobs).await {
            Ok(wanted) => wanted,
            Err(error) => {
                tracing::debug!(%error, "could not look for content that has not arrived");
                continue;
            }
        };

        for hash in wanted {
            // One at a time, which is what keeps this from asking twice for the
            // same hash: the next sweep reads the store again, and anything
            // that landed in the meantime is no longer wanted. Shuffled so that
            // a group of readers repairing at once does not all ask one member.
            let asked = tokio::time::timeout(
                FETCH_DEADLINE,
                downloader.download(HashAndFormat::raw(hash), Shuffled::new(providers.clone())),
            )
            .await;
            match asked {
                Ok(Ok(())) => tracing::debug!(%hash, "fetched content nobody offered"),
                // Ordinary rather than alarming: nobody reachable has it yet.
                // The next sweep asks again, which is the whole point.
                Ok(Err(error)) => tracing::debug!(%hash, %error, "no member had this content"),
                Err(_) => tracing::debug!(%hash, "gave up waiting for this content for now"),
            }
        }
    }
}

/// The hashes of entries this node holds whose bytes are not in its store.
///
/// Read from the document every time rather than remembered, so nothing has to
/// stay in step with it: an entry that arrived while this was not looking is
/// found on the next sweep, and one whose content landed drops out by itself.
///
/// Entries of zero length are skipped. Their hash is the hash of no bytes,
/// which every store can answer for without asking anybody — and a deletion
/// looks exactly like one, so asking for it would dial the group on behalf of
/// an entry that has no content by design.
async fn content_not_here(doc: &Doc, blobs: &BlobStore) -> Result<Vec<Hash>> {
    let entries = doc
        .get_many(Query::all())
        .await
        .map_err(SyncError::docs("read from"))?;
    tokio::pin!(entries);

    let mut wanted = HashSet::new();
    while let Some(entry) = entries.next().await {
        let entry = entry.map_err(SyncError::docs("read from"))?;
        let hash = entry.content_hash();
        if entry.content_len() == 0 || wanted.contains(&hash) {
            continue;
        }
        let status = blobs
            .blobs()
            .status(hash)
            .await
            .map_err(SyncError::content(
                "asked whether an entry's content is here",
            ))?;
        if !matches!(status, BlobStatus::Complete { .. }) {
            wanted.insert(hash);
        }
    }
    Ok(wanted.into_iter().collect())
}

/// The members to start syncing with: everyone this node can actually reach.
///
/// The whole membership rather than the core group, because the core group is
/// not the shape of the catalogue: what one follower writes has to reach
/// another, and it used to do so only by passing through a core node. Handing
/// the document every member is what lets two followers find each other, and
/// what keeps them in touch when the node that introduced them goes away.
///
/// **But only members there is an address for**, which is the part that had to
/// be learned. iroh-docs will accept a peer named by id alone — it does that
/// with its own remembered peers — and passing one costs a dial that cannot
/// succeed, a gossip join that cannot complete, and the retries under both. At
/// the moment a node opens its catalogue that is *every* follower, since
/// addresses arrive by announcement afterwards. Handing them over anyway made
/// the group slower to converge at exactly the moment it had the most to do,
/// which showed up as a test that passed alone and failed on a loaded
/// two-core machine.
///
/// So the set grows instead: the core group at first, because the log carries
/// their addresses, and every other member as its address is heard. That is
/// what [`offer_peers_as_they_are_learned`] is for.
fn sync_with(
    membership: &MembershipState,
    me: MemberId,
    directory: &Directory,
) -> Vec<EndpointAddr> {
    let core = membership.core();
    membership
        .allowlist()
        .filter(|member| *member != me)
        .filter_map(|member| {
            let addr = core
                .get(&member)
                .cloned()
                .or_else(|| directory.address_of(member))?;
            addr.to_endpoint_addr(member).ok()
        })
        .collect()
}
