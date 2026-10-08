//! The heartbeat between nodes: who is online, for how long, what a beat
//! teaches about where its sender is (phase 4's 4b-3), and what it says its
//! sender holds (4b-4).
//!
//! Without a running `MembershipNode`, as in `converge.rs`: the membership is
//! folded by hand, and each node's address statement is handed to it the way
//! the node's own announcer would.

// Each test waits out real heartbeats and TTLs.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    collections::{BTreeSet, HashSet},
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipState, SignedEvent, Timestamp};
use distlib_core::{
    ContentHash, FileRecord, FileRole, GroupId, Heartbeat, Item, ItemId, MemberId, NodeAddr,
    SignedAddress, SignedHeartbeat,
    availability::{DELTA_MAX, decode_base},
};
use distlib_net::Transport;
use distlib_sync::{Availability, Holdings, Sources, availability::topic_for};
use futures_lite::StreamExt as _;
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_blobs::{
    BlobsProtocol,
    provider::events::{ConnectMode, EventMask, EventSender, ProviderMessage},
    store::mem::MemStore,
};
use iroh_gossip::{
    api::{Event, GossipReceiver},
    net::{GOSSIP_ALPN, Gossip},
};
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
    /// Where this node's content and published lists are.
    blobs: MemStore,
    /// How many connections the store has served — one per base list a
    /// member fetched from here.
    served: Arc<AtomicUsize>,
    holdings: Holdings,
    /// For listening to the topic as it is, beside the node's own heartbeat.
    gossip: Gossip,
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
            .alpns(vec![GOSSIP_ALPN.to_vec(), iroh_blobs::ALPN.to_vec()])
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
        let blobs = MemStore::new();
        let holdings = Holdings::new((*blobs).clone());
        let availability = Availability::start(
            &transport,
            &secret,
            Sources {
                membership,
                own_address: says.subscribe(),
                holdings: holdings.clone(),
            },
            interval,
        )
        .unwrap();
        let (events, mut served_events) = EventSender::channel(
            16,
            EventMask {
                connected: ConnectMode::Notify,
                ..EventMask::DEFAULT
            },
        );
        let served = Arc::new(AtomicUsize::new(0));
        tokio::spawn({
            let served = served.clone();
            async move {
                while let Some(event) = served_events.recv().await {
                    if let ProviderMessage::ClientConnectedNotify(_) = event {
                        served.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });
        let router = Router::builder(endpoint)
            .accept(GOSSIP_ALPN, gossip.clone())
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&blobs, Some(events)))
            .spawn();
        let node = Self {
            id: MemberId::from(secret.public()),
            addr,
            availability,
            directory: transport.directory,
            secret,
            says,
            blobs,
            served,
            holdings,
            gossip,
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

    /// Makes item `n` held here, its one content file in the store, and
    /// returns its id.
    async fn holds(&self, n: u16) -> ItemId {
        let item = item(n, self.content(n).await);
        assert!(self.holdings.recheck(&item).await.unwrap());
        item.id
    }

    /// Makes item `n` no longer held here: a second content file is added to
    /// it, which this node lacks.
    async fn no_longer_holds(&self, n: u16) {
        let mut item = item(n, self.content(n).await);
        item.files.insert(
            ContentHash::from_bytes([0xee; 32]),
            file(&format!("{n}-more")),
        );
        assert!(!self.holdings.recheck(&item).await.unwrap());
    }

    async fn content(&self, n: u16) -> ContentHash {
        let tag = self
            .blobs
            .add_bytes(n.to_be_bytes().to_vec())
            .await
            .unwrap();
        ContentHash::from_bytes(*tag.hash.as_bytes())
    }

    /// Hears the topic of `group` as it is, from the next message on.
    async fn overhears(&self, group: GroupId) -> GossipReceiver {
        let topic = self
            .gossip
            .subscribe(topic_for(group), Vec::new())
            .await
            .unwrap();
        topic.split().1
    }

    /// Waits until this node knows what `member` holds, and that it is
    /// exactly `held` among items 0 to `upto`.
    async fn sees_held(&self, member: MemberId, held: &HashSet<ItemId>, upto: u16) {
        let exact = || {
            self.availability.knows_holdings_of(&member)
                && (0..=upto).map(id).all(|item| {
                    self.availability.holders(&item).contains(&member) == held.contains(&item)
                })
        };
        tokio::time::timeout(SOON, async {
            while !exact() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("never saw exactly what {member} holds"));
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

/// Item `n`'s id, whatever its content.
fn id(n: u16) -> ItemId {
    item(n, ContentHash::from_bytes([0; 32])).id
}

fn item(n: u16, content: ContentHash) -> Item {
    let mut bytes = [0; 32];
    bytes[..2].copy_from_slice(&n.to_be_bytes());
    let mut item = Item::new(ItemId::from_bytes(bytes));
    item.files.insert(content, file(&n.to_string()));
    item
}

fn file(name: &str) -> FileRecord {
    FileRecord {
        role: FileRole::Content,
        format: "epub".to_owned(),
        size: 2,
        filename: format!("{name}.epub"),
        seq: None,
        disc: None,
        title: None,
        duration: None,
    }
}

/// Every beat `member` sends on `group`'s topic within `window`, in order.
async fn beats_from(
    heard: &mut GossipReceiver,
    group: GroupId,
    member: MemberId,
    window: Duration,
) -> Vec<Heartbeat> {
    let mut beats = Vec::new();
    let _ = tokio::time::timeout(window, async {
        while let Some(event) = heard.next().await {
            if let Event::Received(message) = event.unwrap() {
                let signed = SignedHeartbeat::decode(&message.content).unwrap();
                if signed.member() == member {
                    beats.push(signed.open(&group).unwrap().clone());
                }
            }
        }
    })
    .await;
    beats
}

/// The next beat `member` sends on `group`'s topic.
async fn next_beat_from(heard: &mut GossipReceiver, group: GroupId, member: MemberId) -> Heartbeat {
    tokio::time::timeout(SOON, async {
        loop {
            if let Some(Event::Received(message)) = heard.next().await.map(Result::unwrap) {
                let signed = SignedHeartbeat::decode(&message.content).unwrap();
                if signed.member() == member {
                    return signed.open(&group).unwrap().clone();
                }
            }
        }
    })
    .await
    .expect("no beat from the member")
}

fn sorted(ids: impl IntoIterator<Item = ItemId>) -> Vec<ItemId> {
    let mut ids: Vec<ItemId> = ids.into_iter().collect();
    ids.sort_unstable();
    ids
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

/// **An expelled member is offline at once**, not when its beat runs out —
/// and its beats count no more, though it is still sending them. What it
/// holds is forgotten with it.
#[tokio::test]
async fn an_expelled_member_is_offline_at_once() {
    let (alice_key, bob_key) = (SecretKey::generate(), SecretKey::generate());
    let (to_alice, alice_sees) = watch::channel(MembershipState::new());
    let (to_bob, bob_sees) = watch::channel(MembershipState::new());
    let interval = Duration::from_secs(10);
    let alice = Node::start(alice_key.clone(), alice_sees, interval).await;
    let bob = Node::start(bob_key.clone(), bob_sees, interval).await;
    bob.knows(&alice, &alice_key);

    let admitted = group(&[(&alice_key, &alice)], &[bob.id]);
    to_alice.send(admitted.clone()).unwrap();
    to_bob.send(admitted.clone()).unwrap();
    alice.sees_online(&[bob.id]).await;
    assert!(
        alice.availability.knows_holdings_of(&bob.id),
        "nothing, so far"
    );

    let mut expelled = admitted;
    let event = MembershipEvent::MemberExpelled {
        member: bob.id,
        reason: String::new(),
    };
    expelled
        .apply(
            3,
            &SignedEvent::sign(&alice_key, event, Timestamp::from_millis(3), 2).unwrap(),
        )
        .unwrap();
    assert!(!expelled.is_member(&bob.id));
    to_alice.send(expelled).unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        alice.availability.online().wait_for(BTreeSet::is_empty),
    )
    .await
    .expect("alice kept bob online for his 30 s TTL")
    .unwrap();
    assert!(!alice.availability.knows_holdings_of(&bob.id));

    // Bob, who has not heard, beats again at once; alice must not count it.
    bob.says_where_it_is(true);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(alice.availability.online().borrow().is_empty());

    alice.router.shutdown().await.unwrap();
    bob.router.shutdown().await.unwrap();
}

/// **A change to what a member holds is told promptly, and a burst once.**
///
/// Bob beats every minute, so only a change can prompt the beat that says
/// what he now holds — and two changes half a second apart make one beat, not
/// two, ten seconds after his last (D4's floor). Twice: the second burst
/// comes just after a beat a change prompted, and waits out its ten seconds
/// too.
#[tokio::test]
async fn a_change_to_what_a_member_holds_is_one_beat_promptly() {
    let (alice, bob, membership) = a_pair(Duration::from_secs(60)).await;
    let group = membership[0].borrow().group_id().unwrap();
    alice.sees_online(&[bob.id]).await;
    let mut heard = alice.overhears(group).await;

    let mut held = Vec::new();
    for burst in [[1, 2], [3, 4]] {
        held.push(bob.holds(burst[0]).await);
        tokio::time::sleep(Duration::from_millis(500)).await;
        held.push(bob.holds(burst[1]).await);

        let beats = beats_from(&mut heard, group, bob.id, Duration::from_secs(13)).await;
        assert_eq!(beats.len(), 1, "{beats:#?}");
        let holdings = &beats[0].holdings;
        assert_eq!((holdings.count, holdings.base), (held.len() as u64, None));
        assert_eq!(sorted(holdings.added.clone()), sorted(held.clone()));
        assert!(holdings.removed.is_empty());
    }

    alice.router.shutdown().await.unwrap();
    bob.router.shutdown().await.unwrap();
}

/// **A member whose holdings keep changing keeps beating.** The floor holds
/// back only the extra beat a change prompts, never the regular one: bob beats
/// every second, so a floor that held his beats back for ten seconds would
/// outlast his 3 s TTL and take him offline for being busy.
#[tokio::test]
async fn a_member_downloading_keeps_beating() {
    let (alice, bob, membership) = a_pair(Duration::from_secs(1)).await;
    let group = membership[0].borrow().group_id().unwrap();
    alice.sees_online(&[bob.id]).await;
    let mut heard = alice.overhears(group).await;

    let downloading = async {
        for n in 0..8 {
            bob.holds(n).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    let (beats, ()) = tokio::join!(
        beats_from(&mut heard, group, bob.id, Duration::from_secs(4)),
        downloading
    );
    assert!(beats.len() >= 3, "{beats:#?}");
    assert!(alice.availability.online().borrow().contains(&bob.id));

    alice.router.shutdown().await.unwrap();
    bob.router.shutdown().await.unwrap();
}

/// **More than a beat can carry is published as a base**, in the sender's own
/// store under the one tag, and what changes after it is counted from it.
#[tokio::test]
async fn what_a_beat_cannot_carry_is_published_as_a_base() {
    let (alice, bob, membership) = a_pair(Duration::from_secs(1)).await;
    let group = membership[0].borrow().group_id().unwrap();
    alice.sees_online(&[bob.id]).await;
    let mut held = HashSet::new();
    for n in 0..=DELTA_MAX as u16 {
        held.insert(bob.holds(n).await);
    }
    let mut heard = alice.overhears(group).await;

    let beat = tokio::time::timeout(SOON, async {
        loop {
            let beat = next_beat_from(&mut heard, group, bob.id).await;
            if beat.holdings.count == held.len() as u64 {
                return beat;
            }
        }
    })
    .await
    .expect("bob never said what he holds");
    let base = beat.holdings.base.expect("a base, the delta being too big");
    assert!(beat.holdings.added.is_empty() && beat.holdings.removed.is_empty());
    let list = bob
        .blobs
        .blobs()
        .get_bytes(iroh_blobs::Hash::from_bytes(*base.as_bytes()))
        .await
        .unwrap();
    assert_eq!(decode_base(&list).unwrap().collect::<HashSet<_>>(), held);
    let tagged = bob
        .blobs
        .tags()
        .get("distlib/availability/base")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tagged.hash.as_bytes(), base.as_bytes());

    bob.no_longer_holds(7).await;
    let gone = tokio::time::timeout(SOON, async {
        loop {
            let beat = next_beat_from(&mut heard, group, bob.id).await;
            if !beat.holdings.removed.is_empty() {
                return beat;
            }
        }
    })
    .await
    .expect("bob never said he no longer holds it");
    assert_eq!(gone.holdings.base, Some(base));
    assert_eq!(gone.holdings.count, held.len() as u64 - 1);
    assert_eq!(
        gone.holdings.removed,
        sorted([item(7, ContentHash::from_bytes([0; 32])).id])
    );
    assert!(gone.holdings.added.is_empty());

    alice.router.shutdown().await.unwrap();
    bob.router.shutdown().await.unwrap();
}

/// **A receiver knows exactly what a member holds** — from its base, then
/// from each beat's change on top, an item added and lost again included —
/// and fetches a base only when a beat names a new one, into memory and not
/// into its store.
#[tokio::test]
async fn a_receiver_knows_exactly_what_a_member_holds() {
    let (alice, bob, _membership) = a_pair(Duration::from_secs(1)).await;
    alice.sees_online(&[bob.id]).await;
    let first = DELTA_MAX as u16 + 1;
    let mut held = HashSet::new();
    for n in 0..first {
        held.insert(bob.holds(n).await);
    }
    alice.sees_held(bob.id, &held, 2 * first).await;
    assert_eq!(bob.served.load(Ordering::SeqCst), 1, "one base, one fetch");

    held.insert(bob.holds(first).await);
    alice.sees_held(bob.id, &held, 2 * first).await;
    bob.no_longer_holds(7).await;
    held.remove(&id(7));
    alice.sees_held(bob.id, &held, 2 * first).await;
    bob.no_longer_holds(first).await;
    held.remove(&id(first));
    alice.sees_held(bob.id, &held, 2 * first).await;
    // Beats enough to have fetched again, had they wanted to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(bob.served.load(Ordering::SeqCst), 1, "the same base");

    for n in first + 1..=2 * first {
        held.insert(bob.holds(n).await);
    }
    alice.sees_held(bob.id, &held, 2 * first).await;
    assert_eq!(bob.served.load(Ordering::SeqCst), 2, "a new base, fetched");

    assert!(
        alice
            .blobs
            .blobs()
            .list()
            .hashes()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(alice.blobs.tags().list().await.unwrap().count().await, 0);

    alice.router.shutdown().await.unwrap();
    bob.router.shutdown().await.unwrap();
}
