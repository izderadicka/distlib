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

use std::sync::Arc;

use anyhow::{Context as _, Result};
use distlib_consensus::MembershipNode;
use distlib_core::{Config, DataDir, MemberId, NodeAddr, identity::member_id};
use distlib_net::{AllowlistHooks, Transport, allowlist, build_endpoint};
use distlib_sync::Catalogue;
use iroh::{Endpoint, SecretKey, protocol::Router};
use iroh_blobs::store::fs::FsStore;
use iroh_gossip::net::Gossip;

/// A node and the transport it is served on.
///
/// Holds the router so that dropping the runtime does not leave iroh
/// complaining about an endpoint nobody closed — see [`Self::shutdown`], which
/// is the way to stop one.
pub struct Runtime {
    node: Arc<MembershipNode>,
    catalogue: Catalogue,
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
        let swarm = Gossip::builder().spawn(endpoint.clone());

        let transport = Transport {
            endpoint: endpoint.clone(),
            gossip: swarm,
        };
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
        let catalogue = Catalogue::start(
            transport,
            (*blobs).clone(),
            Some(data_dir.docs_dir()),
            secret,
            node.subscribe(),
        )
        .await
        .context("could not start the catalogue")?;

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

    /// The endpoint everything in this process is served on.
    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    /// Stops the node, then the transport under it.
    ///
    /// The order is the one `MembershipNode::shutdown` used to have on its
    /// own, and it is kept deliberately: the node's tasks stop, then Raft,
    /// then the gossip task, and only then does the router stop accepting and
    /// close the endpoint. Shutting the router first is defensible — it stops
    /// new connections arriving mid-teardown — but it is a different choice
    /// with different failure modes, and moving ownership is not the change
    /// that should make it.
    pub async fn shutdown(&self) {
        self.node.shutdown().await;
        // Only this subsystem's own task. The engines under it are protocol
        // handlers, and the router shuts those down itself — including the
        // blob store, which `BlobsProtocol::shutdown` closes.
        self.catalogue.shutdown();
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
