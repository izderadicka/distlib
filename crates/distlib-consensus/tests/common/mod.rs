//! The pieces every multi-node test needs.
//!
//! Shared rather than copied because these tests are about what a *group* does,
//! and a harness that drifted between files would have two ideas of what a node
//! is — which is exactly the thing under test.

#![allow(dead_code)] // each test file uses a different part of this

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib_consensus::{MemberRecord, MembershipNode, MembershipState, Transport};
use distlib_core::{MemberId, NodeAddr};
use distlib_net::{AllowlistHooks, allowlist, endpoint::configure};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_gossip::net::Gossip;
use tempfile::TempDir;

/// A member, its endpoint and its running consensus.
pub struct Peer {
    pub secret: SecretKey,
    pub id: MemberId,
    pub node: MembershipNode,
    pub addr: NodeAddr,
    /// Kept so a test can ask what this node would actually admit, which is the
    /// thing being enforced — not just what the log says.
    pub hooks: AllowlistHooks,
    /// What serves this node's protocols, which the node no longer owns.
    ///
    /// Held for the same reason production holds it: nothing answers without
    /// it, and closing the endpoint is its job — see [`Peer::shutdown`].
    router: Router,
    _dir: TempDir,
}

impl Peer {
    /// Starts a node whose allowlist is seeded with `bootstrap`.
    /// Starts a node seeded with `bootstrap` that will found with `bootstrap`.
    ///
    /// The common case: everyone this node talks to before there is a log is
    /// somebody it is founding with.
    pub async fn start(secret: SecretKey, bootstrap: Vec<MemberId>) -> Self {
        let id = MemberId::from(secret.public());
        // Addresses are empty because a core node never dials from this list —
        // Raft carries its own addressing, and only a follower fetches from it.
        let core = bootstrap
            .iter()
            .copied()
            .chain([id])
            .map(|member| (member, NodeAddr::default()))
            .collect();
        Self::start_with(secret, bootstrap, core).await
    }

    /// Starts a node whose founding core group is stated separately.
    ///
    /// The two sets are not the same thing, and a node that conflates them
    /// serves consensus to anyone it would talk to. A member with an empty
    /// founding core is one that is not founding anything — it should speak
    /// `distlib/raft/0` with nobody.
    pub async fn start_with(
        secret: SecretKey,
        bootstrap: Vec<MemberId>,
        core: Vec<(MemberId, NodeAddr)>,
    ) -> Self {
        let id = MemberId::from(secret.public());
        let dir = TempDir::new().unwrap();

        // The bootstrap seed and the hooks share one channel; the node gets the
        // write half. Exactly the arrangement production uses.
        let (writer, reader) = allowlist(id, bootstrap);
        let hooks = AllowlistHooks::new(reader);

        let endpoint = configure(
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
            secret.clone(),
            hooks.clone(),
            distlib_consensus::alpns(),
        )
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .unwrap()
        .bind()
        .await
        .unwrap();

        let addr = NodeAddr {
            relay: None,
            direct: endpoint.bound_sockets().into_iter().collect(),
        };
        // The same assembly production does, in the same order: gossip before
        // the node that announces on it, the router after the node that says
        // what it serves. See `distlib::Runtime`.
        let swarm = Gossip::builder().spawn(endpoint.clone());
        let node = MembershipNode::start(
            Transport {
                endpoint: endpoint.clone(),
                gossip: swarm,
            },
            hooks.clone(),
            writer,
            dir.path(),
            core,
        )
        .await
        .unwrap();
        let router = distlib_net::serve(endpoint, node.protocols());

        Self {
            secret,
            id,
            node,
            addr,
            hooks,
            router,
            _dir: dir,
        }
    }

    /// Stops this node and the transport under it, in production's order.
    pub async fn shutdown(&self) {
        self.node.shutdown().await;
        let _ = self.router.shutdown().await;
    }

    pub fn record(&self, name: &str) -> MemberRecord {
        MemberRecord {
            member_id: self.id,
            display_name: name.to_owned(),
            pledge_bytes: 0,
        }
    }
}

/// The default bound: long enough for anything gossip or Raft does promptly.
///
/// Deliberately *shorter* than [`PATIENTLY`], so a test that waits on something
/// prompt fails quickly when it breaks.
const SOON: Duration = Duration::from_secs(15);

/// For waiting on something whose guarantee is the follower's own timer.
///
/// Gossip announcements are best-effort (P1-33) — one made before a follower's
/// subscription is live is simply lost — and the guarantee behind them is
/// `follower::IDLE_POLL`, which is thirty seconds. Anything a follower has to
/// learn *without being told directly* must therefore be bounded above that, or
/// the test is asserting a promptness the design does not offer. That is not
/// hypothetical: it is why the acceptance test failed under a parallel runner.
pub const PATIENTLY: Duration = Duration::from_secs(45);

/// Waits for a node's derived membership to satisfy `predicate`.
pub async fn wait_for(peer: &Peer, what: &str, predicate: impl Fn(&MembershipState) -> bool) {
    wait_for_upto(peer, SOON, what, predicate).await;
}

/// [`wait_for`] with the bound named, for waits that outlast [`SOON`].
pub async fn wait_for_upto(
    peer: &Peer,
    bound: Duration,
    what: &str,
    predicate: impl Fn(&MembershipState) -> bool,
) {
    tokio::time::timeout(bound, async {
        loop {
            if predicate(&peer.node.membership()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    // Naming the node matters: every failure of this so far has been one node
    // out of five, and a message that does not say which one costs a rerun.
    .unwrap_or_else(|_| {
        panic!(
            "timed out after {bound:?} waiting for {what}, on {}",
            peer.id.fmt_short()
        )
    });
}

/// Waits for a proposal to be pending on `peer`, and answers with the log index
/// [`distlib_consensus::MembershipEvent::Approved`] names it by.
///
/// Waited for on the node that is about to approve it, not read from the one
/// that proposed it: an approval names an index its own node has to have applied
/// before it can sign against the membership that index produced.
pub async fn pending_on(peer: &Peer, what: &str) -> u64 {
    wait_for(peer, what, |membership| {
        membership.pending().next().is_some()
    })
    .await;
    let (proposal, _) = peer
        .node
        .membership()
        .pending()
        .next()
        .expect("just waited for one");
    proposal
}

/// Waits until `condition` holds, or gives up and says what it was waiting for.
///
/// For the things a test cannot see through the membership: each node derives
/// what it will talk to in a task of its own, subscribed to the same watch a
/// test waits on, so "node A applied the entry" does not yet mean "node B will
/// accept a connection". Waiting on the thing itself is the difference between
/// a test that is deterministic and one that passes when the machine is idle.
///
/// Retrying the *assertion* would be the other way to write this, and it is
/// worse: it lets a second mechanism satisfy the test while the one under test
/// does nothing — which is exactly what happened here, gossip supplying an
/// address the address book had failed to learn.
pub async fn until(what: &str, condition: impl Fn() -> bool) {
    until_upto(SOON, what, condition).await;
}

/// [`until`] with the bound named, for waits that outlast [`SOON`].
pub async fn until_upto(bound: Duration, what: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(bound, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}
