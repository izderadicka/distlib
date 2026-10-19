//! Everything a running node is made of, and the order it comes apart in.
//!
//! Until phase 2 the consensus node built its own router, because the only
//! other protocol in the process was ping and P1-11 had established that
//! `distlib-net` could not serve `distlib/raft/0`. That stops working here.
//! `iroh-docs` must be handed the same [`Gossip`] the memberlog announces on,
//! and docs, blobs and gossip must all be accepted on one router — so one
//! thing above consensus has to own the endpoint, the gossip and the router,
//! and hand each subsystem what it needs.
//!
//! This is that thing. It lives in the binary's crate rather than a new one
//! because everything it assembles already depends on the crates below it, and
//! a crate above `distlib-consensus` that consensus's own tests could not use
//! would have bought nothing.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, Result};
use distlib_api::{tasks::Tasks, uploads::Uploads};
use distlib_consensus::{MembershipNode, MembershipState};
use distlib_core::{
    Config, ContentHash, DataDir, Event, MemberId, NodeAddr, TransferConfig, identity::member_id,
};
use distlib_custody::{Custody, CustodySources, PinManager, PinSources};
use distlib_net::{
    AllowlistHooks, Blobs, BytePacer, Transport, allowlist, blobs::serving_events, build_endpoint,
};
use distlib_store::{Projection, SearchIndex, Store};
use distlib_sync::{Availability, Catalogue, CatalogueStorage, ProtectCallbackHandler, Sources};
use iroh::{Endpoint, SecretKey, protocol::Router};
use iroh_blobs::store::{
    GcConfig, ProtectCb,
    fs::{FsStore, options::Options},
};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};

/// A node and the transport it is served on.
///
/// Holds the router so that dropping the runtime does not leave iroh
/// complaining about an endpoint nobody closed — see [`Self::shutdown`], which
/// is the way to stop one.
pub struct Runtime {
    node: Arc<MembershipNode>,
    catalogue: Catalogue,
    /// The heartbeat, and who else is online.
    availability: Availability,
    /// Media in and out of the same store the catalogue hands iroh-docs and
    /// `BlobsProtocol` serves from — so what `library.download` fetches is
    /// held, and served, by this node from the moment the fetch returns.
    ///
    /// Built once and kept because it owns a connection pool; see
    /// [`Blobs`]'s own doc comment.
    blobs: Blobs,
    store: Store,
    search: SearchIndex,
    /// Held rather than detached: dropping it stops the task, so a runtime
    /// that goes away does not leave one writing to a database nobody reads.
    projection: Projection,
    /// Which items this node keeps for the group.
    custody: Custody,
    pins: PinManager,
    /// Sets the transfer limits once the group is known, each raised to the
    /// group's minimum; none without a limit.
    transfer_limits: Option<JoinHandle<()>>,
    /// When the last garbage collection pass began; `None` before the first.
    collections: watch::Receiver<Option<SystemTime>>,
    upkeep: StorageUpkeep,
    /// The downloads this node runs, and the bus that it and every other
    /// producer publish to — what this node tells whoever is watching it.
    /// Made here, by the thing that assembles the producers, so that each is
    /// handed a clone rather than reaching the bus through another's API.
    tasks: Tasks,
    /// `[library] download_dir`, resolved against the data directory.
    downloads: PathBuf,
    /// What the web UI uploads, held for `library.add`.
    uploads: Uploads,
    /// `[storage] limit_bytes`: the most the blobs this node keeps may take.
    storage_limit: Option<NonZeroU64>,
    router: Router,
}

impl Runtime {
    /// Binds the endpoint, starts consensus on it, and serves what it asks for.
    ///
    /// The order is forced and worth naming, because three of the four steps
    /// cannot move. The endpoint comes first because everything else is built
    /// on it; gossip before the node, because the node announces on it; the
    /// router last, because it needs the handlers and the node is what says
    /// which. Only the ALPNs the endpoint advertises are declared out of
    /// order — they have to be, since the endpoint exists before any handler
    /// does. That is the gap [`MembershipNode::protocols`] is tested against.
    pub async fn start(secret: &SecretKey, config: &Config, data_dir: &DataDir) -> Result<Self> {
        // `init` has usually done this, and `run` refuses a directory with no
        // identity in it — but a caller that starts a node from a key it holds
        // (a test, an embedder) has no reason to have created it first.
        data_dir.create()?;
        let (transport, node) = start_consensus(secret, config, data_dir).await?;
        let endpoint = transport.endpoint.clone();

        // Content, and the catalogue that refers to it. The store comes first
        // because iroh-docs cannot be built without one: an entry's value is a
        // blob. Both are handed the transport the node already has — one
        // endpoint, one gossip, whatever the process grows next.
        let (protect, protected_by_documents) = ProtectCallbackHandler::new();
        let (blobs, collections) = open_blobs(config, data_dir, protected_by_documents).await?;
        // One store, two users of it, and the sharing is the point rather
        // than an economy: `library.download` fetching a media blob has to
        // land it in the very store `Catalogue::protocols`' `BlobsProtocol`
        // answers from, or a node would download a file and still not be a
        // provider of it.
        let media = Blobs::new(&blobs, &endpoint);
        let (upload_pacer, transfer_limits) =
            limit_transfers(&config.transfer, &media, node.subscribe());
        let (serving_events, served) = serving_events(upload_pacer);
        let catalogue = Catalogue::start(
            transport.clone(),
            CatalogueStorage {
                blobs: (*blobs).clone(),
                documents: Some(data_dir.docs_dir()),
                protect: Some(protect),
                serving_events: Some(serving_events),
            },
            secret,
            node.subscribe(),
        )
        .await
        .context("could not start the catalogue")?;
        // The event bus, before anything that publishes into it.
        let tasks = Tasks::new(distlib_api::events::bus());
        // On the same swarm, one topic the whole group shares, apart from the
        // membership and catalogue topics. It beats once this node knows its
        // group and where it is, and both arrive from the node.
        let availability = Availability::start(
            &transport,
            secret,
            Sources {
                membership: node.subscribe(),
                own_address: node.own_address(),
                holdings: catalogue.holdings().clone(),
            },
            Duration::from_secs(u64::from(config.availability.beat_interval_secs.get())),
            tasks.events().clone(),
        )
        .context("could not start the heartbeat")?;

        // The read model, and the task that fills it. After the catalogue
        // because it is derived from it, and before the router because the
        // projection should be watching for changes before any peer can start
        // sending them — it replays the document either way, but a node that
        // subscribes late does more work than one that does not.
        let (store, search) = open_read_model(data_dir).await?;
        let upkeep = StorageUpkeep::start(&store, served, &collections, gc_interval(config));
        let uploads = open_uploads(config, data_dir).await?;
        let projection = Projection::start(
            catalogue.clone(),
            store.clone(),
            search.clone(),
            node.subscribe(),
            tasks.events().clone(),
        );
        // After the projection, whose read model and events custody follows.
        let (custody, pins) = start_custody(
            member_id(secret),
            &CustodyInputs {
                node: &node,
                store: &store,
                projection: &projection,
                catalogue: &catalogue,
                availability: &availability,
                blobs: &media,
                events: tasks.events(),
                max_fetches: config.transfer.max_fetches,
            },
        );

        // Nothing is answered until here: the endpoint has been advertising
        // these ALPNs since it bound, and a peer arriving in the window
        // between gets no handler. That window is as short as the node's own
        // startup and is why `alpns` exists at all rather than being derived
        // from the handlers.
        let router = serve_protocols(endpoint, &node, &catalogue);

        Ok(Self {
            node,
            catalogue,
            availability,
            blobs: media,
            store,
            search,
            projection,
            custody,
            pins,
            transfer_limits,
            collections,
            upkeep,
            tasks,
            // `join` keeps an absolute path as it is.
            downloads: data_dir.root().join(&config.library.download_dir),
            uploads,
            storage_limit: config.storage.limit_bytes,
            router,
        })
    }

    /// The consensus node, for the API and the commands that drive it.
    pub fn node(&self) -> &Arc<MembershipNode> {
        &self.node
    }

    /// The group's catalogue.
    pub fn catalogue(&self) -> &Catalogue {
        &self.catalogue
    }

    /// The heartbeat, and who else is online.
    pub fn availability(&self) -> &Availability {
        &self.availability
    }

    /// Media transfer: what `library.download` fetches and exports with.
    pub fn blobs(&self) -> &Blobs {
        &self.blobs
    }

    /// The read model: what every query in §7.1 is answered from.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The search index: what `library.search` ranks against.
    pub fn search(&self) -> &SearchIndex {
        &self.search
    }

    /// The task keeping the read model in step — `admin.reindex` drives it.
    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    /// Which items this node keeps for the group.
    pub fn custody(&self) -> &Custody {
        &self.custody
    }

    /// What keeps this node's custody in its blob store.
    pub fn pins(&self) -> &PinManager {
        &self.pins
    }

    /// When the last garbage collection pass began; `None` before the first.
    pub fn collections(&self) -> watch::Receiver<Option<SystemTime>> {
        self.collections.clone()
    }

    /// The most the blobs this node keeps may take; no limit when `None`.
    pub fn storage_limit(&self) -> Option<NonZeroU64> {
        self.storage_limit
    }

    /// The event bus: what the projection publishes into, and what the local
    /// API streams to its watchers.
    pub fn events(&self) -> &broadcast::Sender<Event> {
        self.tasks.events()
    }

    /// The downloads this node is running or has run, for the API.
    pub fn tasks(&self) -> &Tasks {
        &self.tasks
    }

    /// Where the web UI's uploads wait for `library.add`.
    pub fn uploads(&self) -> &Uploads {
        &self.uploads
    }

    /// Where a download asked for without a destination is written.
    pub fn downloads(&self) -> &Path {
        &self.downloads
    }

    /// The endpoint everything in this process is served on.
    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    /// Says goodbye, closes the connections, then stops everything else.
    ///
    /// In this order: the heartbeat's goodbye, while there is an endpoint to
    /// carry it; the endpoint's connections (P4-2); then the node, the
    /// projection, the catalogue's task and the search index; and last the
    /// router, which shuts down its protocol handlers. Each step says below
    /// why it comes where it does.
    pub async fn shutdown(&self) {
        // **The heartbeat's goodbye before anything else**, while there is
        // still an endpoint to carry it: the group then counts this node
        // offline at once, not a TTL later (D4). It is a message on one topic,
        // not the connection-level goodbye below.
        self.availability.leave().await;
        // **Then connections, so peers hear one goodbye that reaches every
        // gossip topic** (P4-2). Left to the subsystems, the membership topic
        // says goodbye on its own when `node.shutdown` drops it — and a peer
        // hearing that drops this node from *all* its topics' bookkeeping
        // without telling the others, so the catalogue's topic keeps a
        // neighbour that is gone until it next tries to send to it, about a
        // minute later. A closed connection, by contrast, is reported to every
        // topic at once. Nothing below needs the network to stop.
        self.router.endpoint().close().await;
        self.node.shutdown().await;
        // Before the catalogue, because it reads from it: a projection left
        // running against a closing document logs failures about a shutdown.
        self.projection.shutdown();
        self.pins.shutdown();
        self.custody.shutdown();
        if let Some(transfer_limits) = &self.transfer_limits {
            transfer_limits.abort();
        }
        // Before the router closes the blob store, which stops collection.
        self.upkeep.stop();
        // Only this subsystem's own task. The engines under it are protocol
        // handlers, and the router shuts those down itself — including the
        // blob store, which `BlobsProtocol::shutdown` closes.
        self.catalogue.shutdown();
        // Explicit, and after the projection: tantivy's `IndexWriter` holds a
        // lockfile for as long as it is alive, and its own `Drop` does not
        // wait for its merge thread to actually exit — only
        // `wait_merging_threads` does that, and `close` is what calls it. A
        // restart in this same process, which reopens `index_dir()` right
        // after this returns, is exactly what needs that lock gone rather
        // than merely on its way out.
        if let Err(error) = self.search.close().await {
            tracing::warn!(%error, "search index did not close cleanly");
        }
        if let Err(error) = self.router.shutdown().await {
            tracing::warn!(%error, "router did not shut down cleanly");
        }
    }
}

/// Every ALPN this process serves.
///
/// One declaration, used to bind the endpoint, because the endpoint exists
/// before any handler does and must already offer what the router will later
/// accept: an endpoint that negotiates a protocol nothing handles accepts the
/// connection and then refuses every stream (P1-11).
///
/// A subsystem therefore says what it serves twice — here, and through its own
/// `protocols()` — and the two are kept in step by being compared. **Adding a
/// subsystem means three places**: this list, the router below, and the
/// agreement test that would otherwise not know to look.
pub fn alpns() -> Vec<Vec<u8>> {
    distlib_consensus::alpns()
        .into_iter()
        .chain(distlib_sync::alpns())
        .collect()
}

/// Binds the endpoint and starts consensus on it, with the gossip both share.
async fn start_consensus(
    secret: &SecretKey,
    config: &Config,
    data_dir: &DataDir,
) -> Result<(Transport, Arc<MembershipNode>)> {
    let me = member_id(secret);
    // The bootstrap seed, and the last time configuration has anything to say
    // about who this node talks to. Once `GroupFounded` is applied the node
    // replaces it with the log's membership and never reads it again.
    let (writer, allowed) = allowlist(me, config.consensus.core.iter().map(|core| core.member));
    let hooks = AllowlistHooks::new(allowed);

    // What this node serves is the same on every node since 2.3-2: whether it
    // *answers* consensus is decided per connection by whether it has a Raft,
    // because a router's protocols are fixed when it spawns and a follower that
    // may be promoted has to be listening first.
    let endpoint = build_endpoint(secret.clone(), &config.net, hooks.clone(), alpns()).await?;

    tracing::info!(member = %me, "node started");
    for addr in endpoint.bound_sockets() {
        tracing::info!(%addr, "listening");
    }

    // One gossip for the process, shared by the memberlog and iroh-docs: two
    // would mean two swarms over one endpoint, and a topic joined on one is
    // invisible to the other.
    let swarm = distlib_net::spawn_gossip(&endpoint);

    // `Transport::new` installs the directory on the endpoint, so the thing
    // consensus fills and the thing iroh resolves against cannot be two
    // different objects.
    let transport =
        Transport::new(endpoint, swarm).context("could not install the address directory")?;
    let node = MembershipNode::start(
        transport.clone(),
        hooks,
        writer,
        data_dir.root(),
        core_group(config),
    )
    .await
    .context("could not start consensus")?;
    Ok((transport, Arc::new(node)))
}

/// The blob store, collecting every `[storage] gc_interval_secs` what neither
/// a tag nor `protected` keeps.
///
/// `protected` is the documents' half of the collection: every blob a
/// catalogue entry references. A pass whose callback cannot answer is
/// skipped, not run without it.
/// What custody, and the pin manager keeping it, are computed from.
struct CustodyInputs<'a> {
    node: &'a MembershipNode,
    store: &'a Store,
    projection: &'a Projection,
    catalogue: &'a Catalogue,
    availability: &'a Availability,
    blobs: &'a Blobs,
    events: &'a broadcast::Sender<Event>,
    max_fetches: NonZeroUsize,
}

/// Answers every protocol this node speaks: consensus's and the catalogue's.
fn serve_protocols(endpoint: Endpoint, node: &MembershipNode, catalogue: &Catalogue) -> Router {
    let protocols = node
        .protocols()
        .into_iter()
        .chain(catalogue.protocols())
        .collect();
    distlib_net::serve(endpoint, protocols)
}

/// The read model and its search index, from where `data_dir` keeps them.
async fn open_read_model(data_dir: &DataDir) -> Result<(Store, SearchIndex)> {
    let store = Store::open(Some(data_dir.db_dir()))
        .await
        .with_context(|| format!("could not open {}", data_dir.db_dir().display()))?;
    let search = SearchIndex::open(Some(data_dir.index_dir()))
        .await
        .with_context(|| format!("could not open {}", data_dir.index_dir().display()))?;
    Ok((store, search))
}

/// One of `[transfer]`'s limits, and the pacer that holds this node to it.
struct TransferLimit {
    key: &'static str,
    limit: NonZeroU64,
    pacer: Arc<BytePacer>,
}

/// Paces this node's downloads and uploads to `transfer`'s limits. Returns
/// the pacer for what the catalogue serves, and the task that sets the
/// limits.
fn limit_transfers(
    transfer: &TransferConfig,
    media: &Blobs,
    membership: watch::Receiver<MembershipState>,
) -> (Option<Arc<BytePacer>>, Option<JoinHandle<()>>) {
    let download = transfer.max_download_rate.map(|limit| TransferLimit {
        key: "max_download_rate",
        limit,
        pacer: Arc::clone(media.download_pacer()),
    });
    let upload = transfer.max_upload_rate.map(|limit| TransferLimit {
        key: "max_upload_rate",
        limit,
        pacer: Arc::new(BytePacer::new(None)),
    });
    let upload_pacer = upload.as_ref().map(|upload| Arc::clone(&upload.pacer));
    let limits = [download, upload].into_iter().flatten().collect();
    (
        upload_pacer,
        set_once_the_group_is_known(limits, membership),
    )
}

/// Sets each of `limits`, raised to the group's `min_transfer_rate`, once the
/// group is known: until then its settings are only the defaults, and a node
/// moves no media before it knows its group. They never change after
/// founding, so the task ends there. None without a limit.
fn set_once_the_group_is_known(
    limits: Vec<TransferLimit>,
    mut membership: watch::Receiver<MembershipState>,
) -> Option<JoinHandle<()>> {
    if limits.is_empty() {
        return None;
    }
    Some(tokio::spawn(async move {
        let Ok(state) = membership
            .wait_for(|state| state.group_id().is_some())
            .await
        else {
            return;
        };
        for TransferLimit { key, limit, pacer } in limits {
            let raised = state.settings().at_least_the_minimum(limit);
            if raised != limit {
                tracing::warn!(key, %limit, %raised, "a transfer limit is below the group's min_transfer_rate; raised to it");
            }
            pacer.set_rate(Some(raised));
        }
    }))
}

/// Starts following `me`'s custody, and keeping it in the blob store.
fn start_custody(me: MemberId, inputs: &CustodyInputs<'_>) -> (Custody, PinManager) {
    let custody = Custody::start(
        me,
        CustodySources {
            membership: inputs.node.subscribe(),
            store: inputs.store.clone(),
            events: inputs.events.subscribe(),
            replays: inputs.projection.replays(),
        },
    );
    let pins = PinManager::start(
        me,
        &custody,
        PinSources {
            membership: inputs.node.subscribe(),
            store: inputs.store.clone(),
            blobs: inputs.blobs.clone(),
            availability: inputs.availability.clone(),
            holdings: inputs.catalogue.holdings().clone(),
            events: inputs.events.subscribe(),
        },
        inputs.max_fetches,
    );
    (custody, pins)
}

async fn open_blobs(
    config: &Config,
    data_dir: &DataDir,
    protected: ProtectCb,
) -> Result<(FsStore, watch::Receiver<Option<SystemTime>>)> {
    let root = data_dir.blobs_dir();
    let (protected, collections) = stamp_each_collection(protected);
    // What `FsStore::load` does, with collection switched on.
    let mut options = Options::new(&root);
    options.gc = Some(GcConfig {
        interval: gc_interval(config),
        add_protected: Some(protected),
    });
    let store = FsStore::load_with_opts(root.join("blobs.db"), options)
        .await
        .with_context(|| format!("could not open {}", root.display()))?;
    Ok((store, collections))
}

/// `[storage] gc_interval_secs`.
fn gc_interval(config: &Config) -> Duration {
    Duration::from_secs(u64::from(config.storage.gc_interval_secs.get()))
}

/// `protected`, stamping the time on the receiver each time a collection pass
/// asks it what to keep, at the start of the pass.
fn stamp_each_collection(protected: ProtectCb) -> (ProtectCb, watch::Receiver<Option<SystemTime>>) {
    let (stamp, collections) = watch::channel(None);
    let stamped: ProtectCb = Arc::new(move |live| {
        stamp.send_replace(Some(SystemTime::now()));
        protected(live)
    });
    (stamped, collections)
}

/// The tasks that look after the blob store while the node runs.
struct StorageUpkeep {
    /// Records when this node serves an item, as a use of it.
    served_recorder: JoinHandle<()>,
    /// Reports garbage collection once it stops (C42).
    collection_watch: JoinHandle<()>,
}

impl StorageUpkeep {
    fn start(
        store: &Store,
        served: mpsc::Receiver<ContentHash>,
        collections: &watch::Receiver<Option<SystemTime>>,
        gc_interval: Duration,
    ) -> Self {
        Self {
            served_recorder: tokio::spawn(record_served(store.clone(), served)),
            collection_watch: tokio::spawn(report_stopped_collection(
                collections.clone(),
                gc_interval,
            )),
        }
    }

    fn stop(&self) {
        self.served_recorder.abort();
        self.collection_watch.abort();
    }
}

/// Logs an error, once, when no collection pass begins for twice `interval`.
/// iroh-blobs stops collecting after a pass fails, and only a restart starts
/// it again (C42).
async fn report_stopped_collection(
    mut collections: watch::Receiver<Option<SystemTime>>,
    interval: Duration,
) {
    let overdue_after = interval * 2;
    while let Ok(began) = tokio::time::timeout(overdue_after, collections.changed()).await {
        if began.is_err() {
            return;
        }
    }
    tracing::error!(
        ?overdue_after,
        "garbage collection has stopped, so nothing released is deleted; restart the node to start it again"
    );
}

/// Records each blob in `served` as a use of the items it is a file of.
async fn record_served(store: Store, mut served: mpsc::Receiver<ContentHash>) {
    while let Some(blob) = served.recv().await {
        if let Err(error) = store.mark_used_by_blob(blob, SystemTime::now()).await {
            tracing::warn!(%blob, %error, "could not record that a blob was served");
        }
    }
}

/// Where the web UI's uploads wait for `library.add`.
///
/// Opening clears what a previous run was sent and never added — which can be
/// a large file's worth of removal, so off the async threads.
async fn open_uploads(config: &Config, data_dir: &DataDir) -> Result<Uploads> {
    let dir = data_dir.root().join("uploads");
    let max = config.api.max_upload_bytes;
    tokio::task::spawn_blocking(move || Uploads::open(dir, max))
        .await
        .context("the upload directory's clean-up did not finish")?
        .context("could not clear the web UI's upload directory")
}

/// The configured core group, with an address for each.
///
/// Decides two things, and only until the log can decide them instead:
/// whether this node votes, and who it speaks to before it has a log.
fn core_group(config: &Config) -> Vec<(MemberId, NodeAddr)> {
    config
        .consensus
        .core
        .iter()
        .map(|member| (member.member, member.addr()))
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use std::collections::HashSet;

    use iroh_blobs::{Hash, store::ProtectOutcome};

    use super::*;

    /// A pass's start is stamped, and what the documents protect still is.
    #[tokio::test]
    async fn each_collection_pass_is_stamped_and_still_asks_the_documents() {
        let referenced = Hash::from_bytes([7; 32]);
        let documents: ProtectCb = Arc::new(move |live| {
            live.insert(referenced);
            Box::pin(async { ProtectOutcome::Continue })
        });
        let (protected, collections) = stamp_each_collection(documents);
        assert_eq!(*collections.borrow(), None);

        let mut live = HashSet::new();
        let outcome = protected(&mut live).await;

        assert!(matches!(outcome, ProtectOutcome::Continue));
        assert_eq!(live, HashSet::from([referenced]));
        assert!(collections.borrow().is_some());
    }

    /// Passes on time are quiet; none for two intervals is reported.
    #[tokio::test(start_paused = true)]
    async fn collection_is_reported_stopped_after_two_intervals_without_a_pass() {
        let interval = Duration::from_secs(60);
        let (stamp, collections) = watch::channel(None);
        let watching = tokio::spawn(report_stopped_collection(collections, interval));
        for _ in 0..5 {
            tokio::time::sleep(interval).await;
            stamp.send_replace(Some(SystemTime::now()));
        }
        tokio::time::sleep(interval * 2 - Duration::from_secs(1)).await;
        assert!(
            !watching.is_finished(),
            "reported while passes were on time"
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(watching.is_finished(), "never reported");
    }
}
