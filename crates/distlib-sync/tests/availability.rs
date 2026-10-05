//! The heartbeat between nodes: who is online, for how long, and what a beat
//! teaches about where its sender is (phase 4's 4b-3).
//!
//! Without a running `MembershipNode`, as in `converge.rs`: the membership is
//! folded by hand, and each node's address statement is handed to it the way
//! the node's own announcer would.

// Each test waits out real heartbeats and TTLs.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipState, SignedEvent, Timestamp};
use distlib_core::{MemberId, NodeAddr, SignedAddress};
use distlib_net::Transport;
use distlib_sync::Availability;
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_gossip::net::GOSSIP_ALPN;
use tokio::sync::watch;

fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// Long enough for a node to join a topic and hear a first beat.
const SOON: Duration = Duration::from_secs(10);

/// A member with a heartbeat, and the transport under it.
struct Node {
    id: MemberId,
    addr: NodeAddr,
    availability: Availability,
    directory: distlib_net::Directory,
    secret: SecretKey,
    /// What the node's own announcer would hand the heartbeat. Kept for as
    /// long as the node runs: dropping it stops the beat.
    says: watch::Sender<Option<SignedAddress>>,
    router: Router,
}

impl Node {
    async fn start(
        secret: SecretKey,
        membership: watch::Receiver<MembershipState>,
        interval: Duration,
    ) -> Self {
        init_logging();
        let endpoint = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .secret_key(secret.clone())
            .alpns(vec![GOSSIP_ALPN.to_vec()])
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let addr = NodeAddr {
            relay: None,
            direct: endpoint.bound_sockets().into_iter().collect(),
        };
        let gossip = distlib_net::spawn_gossip(&endpoint);
        let transport = Transport::new(endpoint.clone(), gossip.clone()).unwrap();
        let says = watch::Sender::new(None);
        let availability =
            Availability::start(&transport, &secret, membership, says.subscribe(), interval)
                .unwrap();
        let router = Router::builder(endpoint)
            .accept(GOSSIP_ALPN, gossip)
            .spawn();
        let node = Self {
            id: MemberId::from(secret.public()),
            addr,
            availability,
            directory: transport.directory,
            secret,
            says,
            router,
        };
        node.says_where_it_is(true);
        node
    }

    /// Hands the heartbeat this node's address statement, or takes it away.
    fn says_where_it_is(&self, saying: bool) {
        self.says.send_replace(
            saying.then(|| SignedAddress::sign(&self.secret, self.addr.clone(), 1).unwrap()),
        );
    }

    /// Tells this node where `other` is, the way the log's core addresses
    /// or an announcement would.
    fn knows(&self, other: &Node, key: &SecretKey) {
        self.directory
            .learn(&SignedAddress::sign(key, other.addr.clone(), 1).unwrap())
            .unwrap();
    }

    async fn sees_online(&self, members: &[MemberId]) {
        let expected: BTreeSet<MemberId> = members.iter().copied().collect();
        tokio::time::timeout(
            SOON,
            self.availability
                .online()
                .wait_for(|online| *online == expected),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "expected {expected:?} online, saw {:?}",
                *self.availability.online().borrow()
            )
        })
        .unwrap();
    }
}

fn record(id: MemberId) -> MemberRecord {
    MemberRecord {
        member_id: id,
        display_name: String::new(),
        pledge_bytes: 0,
    }
}

/// A group founded by `core`, each at its address, with `followers` admitted
/// afterwards — whose addresses the log does not hold.
fn group(core: &[(&SecretKey, &Node)], followers: &[MemberId]) -> MembershipState {
    let founding = MembershipEvent::found(
        core.iter()
            .map(|(_, node)| (record(node.id), node.addr.clone()))
            .collect(),
        Timestamp::from_millis(1),
    )
    .unwrap();
    let by = core[0].0;
    let mut state = MembershipState::new();
    state
        .apply(
            1,
            &SignedEvent::sign(by, founding, Timestamp::from_millis(1), 0).unwrap(),
        )
        .unwrap();
    for (n, follower) in (2..).zip(followers) {
        let event = MembershipEvent::MemberAdded {
            member: record(*follower),
        };
        let signed = SignedEvent::sign(by, event, Timestamp::from_millis(n), n - 1).unwrap();
        state.apply(n, &signed).unwrap();
    }
    state
}

/// Two core members, beating every `interval`, each told the other's address.
async fn a_pair(interval: Duration) -> (Node, Node, Vec<watch::Sender<MembershipState>>) {
    let (alice_key, bob_key) = (SecretKey::generate(), SecretKey::generate());
    let (to_alice, alice_sees) = watch::channel(MembershipState::new());
    let (to_bob, bob_sees) = watch::channel(MembershipState::new());
    let alice = Node::start(alice_key.clone(), alice_sees, interval).await;
    let bob = Node::start(bob_key.clone(), bob_sees, interval).await;
    alice.knows(&bob, &bob_key);
    bob.knows(&alice, &alice_key);

    let founded = group(&[(&alice_key, &alice), (&bob_key, &bob)], &[]);
    to_alice.send(founded.clone()).unwrap();
    to_bob.send(founded).unwrap();
    (alice, bob, vec![to_alice, to_bob])
}

/// **Online, then gone at once when it says goodbye** — well inside the 30 s
/// its 10 s beat would otherwise be believed for.
#[tokio::test]
async fn a_member_that_leaves_is_offline_at_once() {
    let (alice, bob, _membership) = a_pair(Duration::from_secs(10)).await;
    alice.sees_online(&[bob.id]).await;
    bob.sees_online(&[alice.id]).await;

    bob.availability.leave().await;
    alice.sees_online(&[]).await;

    alice.router.shutdown().await.unwrap();
    bob.router.shutdown().await.unwrap();
}

/// A node that never found its group has nothing to say goodbye on, and must
/// not hold its shutdown up looking for it.
#[tokio::test]
async fn leaving_before_the_group_is_known_returns_at_once() {
    let (_membership, nobody) = watch::channel(MembershipState::new());
    let node = Node::start(SecretKey::generate(), nobody, Duration::from_secs(1)).await;

    tokio::time::timeout(Duration::from_secs(1), node.availability.leave())
        .await
        .expect("a node in no group left at once");
    node.router.shutdown().await.unwrap();
}

/// **A sender's interval sets its receivers' TTL.** Bob beats every second
/// and carol every ten; both stop without a word. Alice takes bob offline
/// after his 3 s — while carol, whose beats are believed for 30, is still
/// online. The bound is loose, for a busy runner; carol staying online is
/// what tells the two TTLs apart.
#[tokio::test]
async fn a_member_that_stops_is_offline_after_its_own_ttl() {
    let keys: [SecretKey; 3] = std::array::from_fn(|_| SecretKey::generate());
    let [alice_key, bob_key, carol_key] = &keys;
    let (to_alice, alice_sees) = watch::channel(MembershipState::new());
    let (to_bob, bob_sees) = watch::channel(MembershipState::new());
    let (to_carol, carol_sees) = watch::channel(MembershipState::new());
    let alice = Node::start(alice_key.clone(), alice_sees, Duration::from_secs(10)).await;
    let bob = Node::start(bob_key.clone(), bob_sees, Duration::from_secs(1)).await;
    let carol = Node::start(carol_key.clone(), carol_sees, Duration::from_secs(10)).await;
    for (node, others) in [
        (&alice, [(&bob, bob_key), (&carol, carol_key)]),
        (&bob, [(&alice, alice_key), (&carol, carol_key)]),
        (&carol, [(&alice, alice_key), (&bob, bob_key)]),
    ] {
        for (other, key) in others {
            node.knows(other, key);
        }
    }
    let founded = group(
        &[(alice_key, &alice), (bob_key, &bob), (carol_key, &carol)],
        &[],
    );
    for to in [&to_alice, &to_bob, &to_carol] {
        to.send(founded.clone()).unwrap();
    }
    alice.sees_online(&[bob.id, carol.id]).await;

    bob.availability.shutdown();
    carol.availability.shutdown();
    let stopped = tokio::time::Instant::now();
    alice.sees_online(&[carol.id]).await;
    assert!(
        stopped.elapsed() < Duration::from_secs(10),
        "bob's beats are believed for 3 s, and he was online for {:?} more",
        stopped.elapsed()
    );

    for node in [alice, bob, carol] {
        node.router.shutdown().await.unwrap();
    }
}

/// **C5: a follower learns where another follower is from its heartbeat.**
///
/// Alice is the one core member and the only address either follower is
/// given; the log names bob and carol and says nothing of where they are, and
/// nobody here announces addresses. So the only way carol can come to know
/// where bob is — and bob where carol is — is the address each beat carries,
/// relayed by alice.
#[tokio::test]
async fn a_follower_learns_where_another_follower_is_from_a_heartbeat() {
    let keys: [SecretKey; 3] = std::array::from_fn(|_| SecretKey::generate());
    let [alice_key, bob_key, carol_key] = &keys;
    let (to_alice, alice_sees) = watch::channel(MembershipState::new());
    let (to_bob, bob_sees) = watch::channel(MembershipState::new());
    let (to_carol, carol_sees) = watch::channel(MembershipState::new());
    let interval = Duration::from_secs(1);
    let alice = Node::start(alice_key.clone(), alice_sees, interval).await;
    let bob = Node::start(bob_key.clone(), bob_sees, interval).await;
    let carol = Node::start(carol_key.clone(), carol_sees, interval).await;
    bob.knows(&alice, alice_key);
    carol.knows(&alice, alice_key);

    let founded = group(&[(alice_key, &alice)], &[bob.id, carol.id]);
    for to in [&to_alice, &to_bob, &to_carol] {
        to.send(founded.clone()).unwrap();
    }

    for (who, node, other) in [("carol", &carol, &bob), ("bob", &bob, &carol)] {
        tokio::time::timeout(SOON, async {
            while node.directory.address_of(other.id).is_none() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{who} never learned where the other follower is"));
        assert_eq!(
            node.directory.address_of(other.id),
            Some(other.addr.clone())
        );
    }

    for node in [alice, bob, carol] {
        node.router.shutdown().await.unwrap();
    }
}

/// **A node beats as soon as it knows where it is**, not an interval later.
///
/// Bob has no address statement yet when the two connect, so the beat a new
/// neighbour prompts has nothing to say; the statement arriving is what must
/// prompt the next.
#[tokio::test]
async fn a_node_beats_as_soon_as_it_knows_where_it_is() {
    let (alice, bob, _membership) = a_pair(Duration::from_secs(10)).await;
    bob.says_where_it_is(false);
    // Bob hearing alice is the two being neighbours.
    bob.sees_online(&[alice.id]).await;

    bob.says_where_it_is(true);
    tokio::time::timeout(
        Duration::from_secs(3),
        alice
            .availability
            .online()
            .wait_for(|online| online.contains(&bob.id)),
    )
    .await
    .expect("alice heard bob only at his next interval")
    .unwrap();

    alice.router.shutdown().await.unwrap();
    bob.router.shutdown().await.unwrap();
}

/// **Who counts follows the group.** Bob beats before alice's log admits him,
/// and is refused; once it does, his next beat counts.
#[tokio::test]
async fn a_member_counts_once_the_group_admits_it() {
    let (alice_key, bob_key) = (SecretKey::generate(), SecretKey::generate());
    let (to_alice, alice_sees) = watch::channel(MembershipState::new());
    let (to_bob, bob_sees) = watch::channel(MembershipState::new());
    let interval = Duration::from_secs(1);
    let alice = Node::start(alice_key.clone(), alice_sees, interval).await;
    let bob = Node::start(bob_key.clone(), bob_sees, interval).await;
    bob.knows(&alice, &alice_key);

    let before = group(&[(&alice_key, &alice)], &[]);
    let admitted = group(&[(&alice_key, &alice)], &[bob.id]);
    to_alice.send(before).unwrap();
    to_bob.send(admitted.clone()).unwrap();
    // Bob hearing alice is the two being neighbours, and bob beating.
    bob.sees_online(&[alice.id]).await;
    assert!(alice.availability.online().borrow().is_empty());

    to_alice.send(admitted).unwrap();
    alice.sees_online(&[bob.id]).await;

    alice.router.shutdown().await.unwrap();
    bob.router.shutdown().await.unwrap();
}
