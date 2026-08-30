//! Raft RPC over iroh.
//!
//! These drive real `openraft::Raft` instances over real iroh endpoints, rather
//! than exercising the codec against canned responses. The whole question this
//! layer has to answer is whether two Rafts can actually reach each other, and
//! only a real pair can answer it: a leader is elected, or it is not.
//!
//! No test here touches n0's relay or DNS infrastructure — endpoints are built
//! from `presets::Minimal` with relays disabled and given explicit loopback
//! addresses, so the suite stays deterministic and works offline.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::result_large_err)] // openraft's error types, in its own signatures

use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use distlib_consensus::{
    LogStore, NodeAddr, RaftNetworkFactoryImpl, RaftProtocol, StateMachineStore, TypeConfig,
};
use distlib_core::{MemberId, RawMemberId};
use distlib_net::{AllowlistHooks, allowlist, endpoint::configure};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use openraft::{Config, Raft};
use redb::Database;
use tempfile::TempDir;

/// One node: its Raft, the router serving it, and the files it lives in.
struct Node {
    id: RawMemberId,
    raft: Raft<TypeConfig>,
    addr: NodeAddr,
    router: Router,
    _dir: TempDir,
}

impl Node {
    /// Builds a node that admits `peers` and listens on loopback.
    async fn start(secret: SecretKey, peers: Vec<MemberId>) -> Self {
        let me = MemberId::from(secret.public());
        let dir = TempDir::new().unwrap();

        // Same endpoint wiring as production, including the allowlist hooks:
        // testing a configuration nothing ships would prove little.
        let (_writer, allowed) = allowlist(me, peers);
        let builder = Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled);
        // Ping plus Raft: this router serves both, so the endpoint advertises
        // exactly that and no more.
        let mut alpns = distlib_net::alpn::registered();
        alpns.push(distlib_net::alpn::RAFT.to_vec());
        let endpoint = configure(builder, secret, AllowlistHooks::new(allowed), alpns)
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();

        let addr = NodeAddr {
            relay: None,
            direct: endpoint.bound_sockets().into_iter().collect(),
        };

        let db = Arc::new(Database::create(dir.path().join("raft.redb")).unwrap());
        let log = LogStore::from_database(Arc::clone(&db)).unwrap();
        let state_machine = StateMachineStore::from_database(db).unwrap();

        // Short timers: these tests wait for real elections, and the defaults
        // are tuned for real networks rather than loopback.
        let config = Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let raft = Raft::new(
            RawMemberId::from(me),
            config,
            RaftNetworkFactoryImpl::new(endpoint.clone(), distlib_net::Connections::new()),
            log,
            state_machine,
        )
        .await
        .unwrap();

        // The router serves peers; the factory dials them. Both need the
        // endpoint, and the Raft has to exist before it can be served.
        let router = Router::builder(endpoint)
            .accept(distlib_net::alpn::RAFT, RaftProtocol::new(raft.clone()))
            .spawn();

        Self {
            id: RawMemberId::from(me),
            raft,
            addr,
            router,
            _dir: dir,
        }
    }

    async fn shutdown(self) {
        let _ = self.raft.shutdown().await;
        let _ = self.router.shutdown().await;
    }
}

/// Waits for `predicate` to hold of the node's metrics, or gives up.
async fn wait_for(
    raft: &Raft<TypeConfig>,
    what: &str,
    predicate: impl Fn(&openraft::RaftMetrics<RawMemberId, NodeAddr>) -> bool,
) {
    let mut metrics = raft.metrics();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if predicate(&metrics.borrow_and_update()) {
                return;
            }
            metrics.changed().await.unwrap();
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// Two members who admit each other.
async fn pair() -> (Node, Node) {
    let one = SecretKey::generate();
    let two = SecretKey::generate();
    let (one_id, two_id) = (MemberId::from(one.public()), MemberId::from(two.public()));

    (
        Node::start(one, vec![two_id]).await,
        Node::start(two, vec![one_id]).await,
    )
}

#[tokio::test]
async fn two_nodes_elect_a_leader_over_iroh() {
    // The question this layer exists to answer. An election needs votes to
    // cross the wire and be counted, so a leader emerging means append_entries
    // and vote both work end to end.
    let (first, second) = pair().await;

    let members: BTreeMap<_, _> = [
        (first.id, first.addr.clone()),
        (second.id, second.addr.clone()),
    ]
    .into_iter()
    .collect();
    first.raft.initialize(members).await.unwrap();

    wait_for(&first.raft, "a leader", |metrics| {
        metrics.current_leader.is_some()
    })
    .await;

    let leader = first.raft.metrics().borrow().current_leader.unwrap();
    assert!(
        leader == first.id || leader == second.id,
        "the leader must be one of the two members"
    );

    // And the follower agrees who it is, which needs heartbeats flowing.
    wait_for(
        &second.raft,
        "the follower to learn the leader",
        move |metrics| metrics.current_leader == Some(leader),
    )
    .await;

    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn a_committed_entry_replicates_to_the_follower() {
    // Election alone only proves votes cross. This proves entries do, which is
    // what the membership log actually needs.
    let (first, second) = pair().await;
    let members: BTreeMap<_, _> = [
        (first.id, first.addr.clone()),
        (second.id, second.addr.clone()),
    ]
    .into_iter()
    .collect();
    first.raft.initialize(members).await.unwrap();
    wait_for(&first.raft, "a leader", |metrics| {
        metrics.current_leader.is_some()
    })
    .await;

    // Whichever won, ask the leader to commit something.
    let leader = if first.raft.metrics().borrow().current_leader == Some(first.id) {
        &first
    } else {
        &second
    };
    let before = leader.raft.metrics().borrow().last_applied;

    leader.raft.client_write(sample_event()).await.unwrap();

    let follower = if std::ptr::eq(leader, &first) {
        &second
    } else {
        &first
    };
    wait_for(&follower.raft, "the entry to replicate", move |metrics| {
        metrics.last_applied > before
    })
    .await;

    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn an_unreachable_peer_does_not_stall_startup() {
    // `new_client` must not connect: openraft documents it as building a client
    // even for a node that cannot be reached, and blocking there would stall
    // Raft's startup behind an offline member.
    let secret = SecretKey::generate();
    let absent = MemberId::from(SecretKey::generate().public());
    let node = Node::start(secret, vec![absent]).await;

    let members: BTreeMap<_, _> = [
        (node.id, node.addr.clone()),
        // Addressed but never started.
        (RawMemberId::from(absent), NodeAddr::lookup_only()),
    ]
    .into_iter()
    .collect();

    // Returns promptly even though one member is not there.
    tokio::time::timeout(Duration::from_secs(5), node.raft.initialize(members))
        .await
        .expect("initialize must not block on an unreachable member")
        .unwrap();

    node.shutdown().await;
}

fn sample_event() -> distlib_consensus::SignedEvent {
    use distlib_consensus::{MemberRecord, MembershipEvent, SignedEvent, Timestamp};

    let secret = SecretKey::generate();
    let record = MemberRecord {
        member_id: MemberId::from(secret.public()),
        display_name: "replicated".to_owned(),
        pledge_bytes: 0,
    };
    SignedEvent::sign(
        &secret,
        MembershipEvent::found(vec![record], Timestamp::from_millis(1)).unwrap(),
        Timestamp::from_millis(1),
    )
    .unwrap()
}
