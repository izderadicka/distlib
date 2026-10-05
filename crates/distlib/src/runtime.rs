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
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result};
use distlib_api::{tasks::Tasks, uploads::Uploads};
use distlib_consensus::MembershipNode;
use distlib_core::{Config, DataDir, Event, MemberId, NodeAddr, identity::member_id};
use distlib_net::{AllowlistHooks, Blobs, Transport, allowlist, build_endpoint};
use distlib_store::{Projection, SearchIndex, Store};
use distlib_sync::{Availability, Catalogue};
use iroh::{Endpoint, SecretKey, protocol::Router};
use iroh_blobs::store::fs::FsStore;
use tokio::sync::broadcast;

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
    /// The downloads this node runs, and the bus that it and every other
    /// producer publish to — what this node tells whoever is watching it.
    /// Made here, by the thing that assembles the producers, so that each is
    /// handed a clone rather than reaching the bus through another's API.
    tasks: Tasks,
    /// `[library] download_dir`, resolved against the data directory.
    downloads: PathBuf,
    /// What the web UI uploads, held for `library.add`.
    uploads: Uploads,
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
        let me = member_id(secret);
        // `init` has usually done this, and `run` refuses a directory with no
        // identity in it — but a caller that starts a node from a key it holds
        // (a test, an embedder) has no reason to have created it first.
        data_dir.create()?;

        // The bootstrap seed, and the last time configuration has anything to
        // say about who this node talks to. Once `GroupFounded` is applied the
        // node replaces it with the log's membership and never reads it again.
        let (writer, allowed) = allowlist(me, config.consensus.core.iter().map(|core| core.member));
        let hooks = AllowlistHooks::new(allowed);

        // What this node serves is the same on every node since 2.3-2:
        // whether it *answers* consensus is decided per connection by whether
        // it has a Raft, because a router's protocols are fixed when it spawns
        // and a follower that may be promoted has to be listening first.
        let endpoint = build_endpoint(secret.clone(), &config.net, hooks.clone(), alpns()).await?;

        tracing::info!(member = %me, "node started");
        for addr in endpoint.bound_sockets() {
            tracing::info!(%addr, "listening");
        }

        // One gossip for the process. The memberlog announces on it today and
        // iroh-docs will be handed this same instance in 2a-2; two would mean
        // two swarms over one endpoint, and a topic joined on one is invisible
        // to the other.
        let swarm = distlib_net::spawn_gossip(&endpoint);

        // `Transport::new` installs the directory on the endpoint, so the
        // thing consensus fills and the thing iroh resolves against cannot be
        // two different objects.
        let transport = Transport::new(endpoint.clone(), swarm)
            .context("could not install the address directory")?;
        let node = Arc::new(
            MembershipNode::start(
                transport.clone(),
                hooks,
                writer,
                data_dir.root(),
                core_group(config),
            )
            .await
            .context("could not start consensus")?,
        );

        // Content, and the catalogue that refers to it. The store comes first
        // because iroh-docs cannot be built without one: an entry's value is a
        // blob. Both are handed the transport the node already has — one
        // endpoint, one gossip, whatever the process grows next.
        let blobs = FsStore::load(data_dir.blobs_dir())
            .await
            .with_context(|| format!("could not open {}", data_dir.blobs_dir().display()))?;
        // One store, two users of it, and the sharing is the point rather
        // than an economy: `library.download` fetching a media blob has to
        // land it in the very store `Catalogue::protocols`' `BlobsProtocol`
        // answers from, or a node would download a file and still not be a
        // provider of it.
        let media = Blobs::new(&blobs, &endpoint);
        let catalogue = Catalogue::start(
            transport.clone(),
            (*blobs).clone(),
            Some(data_dir.docs_dir()),
            secret,
            node.subscribe(),
        )
        .await
        .context("could not start the catalogue")?;
        // On the same swarm, a topic of its own. It beats once this node knows
        // its group and where it is, and both arrive from the node.
        let availability = Availability::start(
            &transport,
            secret,
            node.subscribe(),
            node.own_address(),
            Duration::from_secs(u64::from(config.availability.beat_interval_secs.get())),
        )
        .context("could not start the heartbeat")?;

        // The read model, and the task that fills it. After the catalogue
        // because it is derived from it, and before the router because the
        // projection should be watching for changes before any peer can start
        // sending them — it replays the document either way, but a node that
        // subscribes late does more work than one that does not.
        let store = Store::open(Some(data_dir.db_dir()))
            .await
            .with_context(|| format!("could not open {}", data_dir.db_dir().display()))?;
        let search = SearchIndex::open(Some(data_dir.index_dir()))
            .await
            .with_context(|| format!("could not open {}", data_dir.index_dir().display()))?;
        let tasks = Tasks::new(distlib_api::events::bus());
        // Clears what a previous run was sent and never added — which can be
        // a large file's worth of removal, so off the async threads.
        let uploads = {
            let dir = data_dir.root().join("uploads");
            let max = config.api.max_upload_bytes;
            tokio::task::spawn_blocking(move || Uploads::open(dir, max))
                .await
                .context("the upload directory's clean-up did not finish")?
                .context("could not clear the web UI's upload directory")?
        };
        let projection = Projection::start(
            catalogue.clone(),
            store.clone(),
            search.clone(),
            node.subscribe(),
            tasks.events().clone(),
        );

        // Nothing is answered until here: the endpoint has been advertising
        // these ALPNs since it bound, and a peer arriving in the window
        // between gets no handler. That window is as short as the node's own
        // startup and is why `alpns` exists at all rather than being derived
        // from the handlers.
        let protocols = node
            .protocols()
            .into_iter()
            .chain(catalogue.protocols())
            .collect();
        let router = distlib_net::serve(endpoint, protocols);

        Ok(Self {
            node,
            catalogue,
            availability,
            blobs: media,
            store,
            search,
            projection,
            tasks,
            // `join` keeps an absolute path as it is.
            downloads: data_dir.root().join(&config.library.download_dir),
            uploads,
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
