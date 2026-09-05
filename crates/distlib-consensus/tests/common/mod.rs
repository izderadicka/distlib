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

use distlib_consensus::{MemberRecord, MembershipNode, MembershipState};
use distlib_core::{MemberId, NodeAddr};
use distlib_net::{AllowlistHooks, allowlist, endpoint::configure};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
};
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
            distlib_consensus::alpns(true),
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
        let node = MembershipNode::start(endpoint, hooks.clone(), writer, dir.path(), core)
            .await
            .unwrap();

        Self {
            secret,
            id,
            node,
            addr,
            hooks,
            _dir: dir,
        }
    }

    pub fn record(&self, name: &str) -> MemberRecord {
        MemberRecord {
            member_id: self.id,
            display_name: name.to_owned(),
            pledge_bytes: 0,
        }
    }
}

/// Waits for a node's derived membership to satisfy `predicate`.
pub async fn wait_for(peer: &Peer, what: &str, predicate: impl Fn(&MembershipState) -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if predicate(&peer.node.membership()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}
