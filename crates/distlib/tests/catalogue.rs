//! Two whole nodes, assembled the way `distlib run` assembles them, sharing a
//! catalogue.
//!
//! `distlib-sync`'s own test proves the document converges between two
//! endpoints. This one proves the *process* does it: the group comes from the
//! membership log rather than from a channel a test wrote into, the catalogue
//! is opened off the back of that, and both subsystems are served on the one
//! router the runtime builds.

// Two whole runtimes, each with a founding election, a docs engine and an
// on-disk blob store: seconds, not milliseconds. Skipped by
// `--no-default-features`, like every other multi-node test here.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    time::Duration,
};

use distlib::Runtime;
use distlib_consensus::MembershipEvent;
use distlib_core::{Config, DataDir, Item, ItemId, MemberId, NodeAddr, Rating, Review};
use distlib_store::{Store, StoredItem};
use iroh::SecretKey;
use tempfile::TempDir;

mod common;
use common::{bound, config, following, init_logging, record};

/// Long enough for two in-process nodes to elect, replicate and reconcile.
const SOON: Duration = Duration::from_secs(30);

/// Two floors between one node's announcements: the silence a settled group
/// must manage before it is believed to be settled.
const QUIET: Duration = Duration::from_secs(10);

/// The window a negative assertion gets before it is believed.
///
/// Deliberately longer than an entry has ever taken to cross in these tests —
/// where the positive reads land in well under a second — because "it has not
/// arrived" and "it is not coming" are the same observation until enough time
/// has passed. Cheap, since the test only pays it when it passes.
const AMPLY: Duration = Duration::from_secs(10);

/// Two runtimes, founded as one group, sharing one catalogue.
async fn a_founded_pair() -> (TempDir, Runtime, Runtime) {
    init_logging();
    let dir = TempDir::new().unwrap();
    let alice_key = SecretKey::generate();
    let bob_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_id = MemberId::from(bob_key.public());
    let config = config(&[alice_id, bob_id]);

    let alice = Runtime::start(&alice_key, &config, &DataDir::new(dir.path().join("alice")))
        .await
        .unwrap();
    let bob = Runtime::start(&bob_key, &config, &DataDir::new(dir.path().join("bob")))
        .await
        .unwrap();

    alice
        .node()
        .init_group(
            vec![
                (record(alice_id, "alice"), bound(&alice)),
                (record(bob_id, "bob"), bound(&bob)),
            ],
            &alice_key,
        )
        .await
        .unwrap();

    (dir, alice, bob)
}

/// Reads one key, waiting out the window where the entry is here and its
/// content is not.
///
/// `MissingContent` is that window and this poll is meant to sit through it —
/// see `Catalogue::get`. Anything else is a real failure and is not retried.
///
/// On giving up it says *which* of the two ways it was still waiting, because
/// they have different causes and a bare timeout sends you log-diving to find
/// out which: no entry at all means the document did not reach this node, while
/// an entry whose content has not landed means the document arrived and the
/// blob behind it did not.
async fn read(catalogue: &distlib_sync::Catalogue, key: &str) -> Vec<u8> {
    let bound = SOON;
    let last = std::sync::Arc::new(std::sync::Mutex::new("nothing was read"));
    let seen = std::sync::Arc::clone(&last);
    tokio::time::timeout(bound, async move {
        loop {
            let outcome = match catalogue.get(key).await {
                Ok(Some(value)) => return value.to_vec(),
                Ok(None) => "no entry for that key has reached this node",
                Err(distlib_sync::SyncError::MissingContent { .. }) => {
                    "the entry is here, its content has not arrived"
                }
                Err(error) => panic!("reading the catalogue failed: {error}"),
            };
            *seen.lock().expect("not poisoned") = outcome;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "`{key}` never arrived within {bound:?}; last seen: {}",
            last.lock().expect("not poisoned")
        )
    })
}

/// Whether a key ever shows up, given a window to do it in.
///
/// The negative half of [`read`], and it cannot be anything but a timeout: a
/// node that has stopped receiving looks exactly like one that has not
/// received *yet*. So the window is the assertion, and it is generous —
/// several times what the positive case takes in the same test.
async fn ever_arrives(catalogue: &distlib_sync::Catalogue, key: &str, window: Duration) -> bool {
    tokio::time::timeout(window, async {
        loop {
            match catalogue.get(key).await {
                Ok(Some(_)) => return,
                // An expelled node keeps the document it already had, so
                // reading it must keep working — what must stop is new
                // entries arriving. A read that started failing outright
                // would be a different bug, and one this would hide.
                Ok(None) | Err(distlib_sync::SyncError::MissingContent { .. }) => {}
                Err(error) => panic!("reading the catalogue failed: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

/// One core node and two followers, every one of them a member.
struct Group {
    dir: TempDir,
    alice: Runtime,
    alice_key: SecretKey,
    bob: Runtime,
    carol: Runtime,
    carol_id: MemberId,
}

/// Founds a group on one core node and joins two followers to it.
///
/// Followers rather than a second core node, because that is the case the
/// address book does *not* record: `MemberRecord` carries an id, a name and a
/// pledge, and Raft's node map holds voters. Where bob and carol are is
/// written down nowhere — which is the whole question this file's follower
/// tests exist to answer.
async fn a_group_with_two_followers() -> Group {
    a_group_with_two_followers_beating(Config::default().availability.beat_interval_secs).await
}

/// [`a_group_with_two_followers`], every node beating every `interval`
/// seconds.
async fn a_group_with_two_followers_beating(interval: NonZeroU32) -> Group {
    init_logging();
    let dir = TempDir::new().unwrap();
    let alice_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());

    let beating = |mut config: Config| {
        config.availability.beat_interval_secs = interval;
        config
    };
    let alice = Runtime::start(
        &alice_key,
        &beating(config(&[alice_id])),
        &DataDir::new(dir.path().join("alice")),
    )
    .await
    .unwrap();
    alice
        .node()
        .init_group(vec![(record(alice_id, "alice"), bound(&alice))], &alice_key)
        .await
        .unwrap();

    // Admitted before they start, so each one finds itself a member on its
    // first fetch rather than waiting for a second.
    let bob_key = SecretKey::generate();
    let carol_key = SecretKey::generate();
    let carol_id = MemberId::from(carol_key.public());
    for (key, name) in [(&bob_key, "bob"), (&carol_key, "carol")] {
        alice
            .node()
            .propose(
                MembershipEvent::MemberAdded {
                    member: record(MemberId::from(key.public()), name),
                },
                &alice_key,
            )
            .await
            .unwrap();
    }

    let where_alice_is = bound(&alice);
    let joining = beating(following(alice_id, &where_alice_is));
    let bob = Runtime::start(&bob_key, &joining, &DataDir::new(dir.path().join("bob")))
        .await
        .unwrap();
    let carol = Runtime::start(
        &carol_key,
        &joining,
        &DataDir::new(dir.path().join("carol")),
    )
    .await
    .unwrap();

    for (who, runtime) in [("alice", &alice), ("bob", &bob), ("carol", &carol)] {
        tokio::time::timeout(SOON, runtime.catalogue().ready())
            .await
            .unwrap_or_else(|_| panic!("{who} never opened a catalogue"));
    }
    for (who, runtime) in [("bob", &bob), ("carol", &carol)] {
        assert!(!runtime.node().is_core(), "{who} must be a follower");
        // **Checked, not assumed.** `Catalogue::sync_with` reads the core
        // group out of the projection, and if that were empty on a follower
        // then `start_sync` would join no swarm and nothing would converge —
        // which looks exactly like an address failure while being a different
        // bug with a different fix.
        let core = runtime.node().membership().core().clone();
        let alice_is_at = core
            .get(&alice_id)
            .unwrap_or_else(|| panic!("{who}'s projection must carry the core group"));
        assert!(
            !alice_is_at.direct.is_empty(),
            "{who} must know where to start syncing, not just who with"
        );
    }

    Group {
        dir,
        alice,
        alice_key,
        bob,
        carol,
        carol_id,
    }
}

#[tokio::test]
async fn an_item_written_on_one_node_is_read_on_the_other() {
    let (_dir, alice, bob) = a_founded_pair().await;

    // Bob learns the group from the log, and only then has a catalogue: its
    // identity is derived from the group id.
    tokio::time::timeout(SOON, bob.catalogue().ready())
        .await
        .expect("bob opens his catalogue once the founding entry reaches him");
    tokio::time::timeout(SOON, alice.catalogue().ready())
        .await
        .unwrap();

    alice
        .catalogue()
        .put("item/1/title", "The Left Hand of Darkness")
        .await
        .unwrap();

    let title = read(bob.catalogue(), "item/1/title").await;
    assert_eq!(&title[..], b"The Left Hand of Darkness");

    alice.shutdown().await;
    bob.shutdown().await;
}

/// **4a-2's acceptance (C14): each node of a pair shows the other as its
/// neighbour, and when one stops, the other shows none — and says so.**
///
/// Also the regression for what 4.0 found: the document's events were
/// subscribed only when the read model first asked, after syncing had started,
/// so the first `NeighborUp` went unheard and a node showed no neighbours while
/// it had one.
#[tokio::test]
async fn each_of_a_pair_shows_the_other_as_its_neighbour_until_it_stops() {
    let (_dir, alice, bob) = a_founded_pair().await;
    let alice_id = MemberId::from(alice.endpoint().id());
    let bob_id = MemberId::from(bob.endpoint().id());

    for (who, runtime, other) in [("alice", &alice, bob_id), ("bob", &bob, alice_id)] {
        let mut sync = runtime.catalogue().sync_status();
        tokio::time::timeout(
            SOON,
            sync.wait_for(|state| state.neighbours.iter().eq([&other])),
        )
        .await
        .unwrap_or_else(|_| panic!("{who} never showed the other as a neighbour"))
        .unwrap();
    }

    // A graceful stop, as Ctrl-C makes, rather than a crash: the case iroh-gossip
    // left a minute stale until the runtime closed its connections first (P4-2).
    let mut sync = bob.catalogue().sync_status();
    alice.shutdown().await;
    tokio::time::timeout(SOON, sync.wait_for(|state| state.neighbours.is_empty()))
        .await
        .expect("bob still shows alice as a neighbour after she stopped")
        .unwrap();

    bob.shutdown().await;
}

/// **A node stopped cleanly is offline to its peers at once** (4b-3): its
/// shutdown says goodbye before the endpoint closes. At the default interval
/// its last beat would otherwise be believed for three minutes.
#[tokio::test]
async fn a_node_stopped_cleanly_is_offline_at_once() {
    let (_dir, alice, bob) = a_founded_pair().await;
    let bob_id = MemberId::from(bob.endpoint().id());
    let mut online = alice.availability().online();
    tokio::time::timeout(SOON, online.wait_for(|online| online.contains(&bob_id)))
        .await
        .expect("alice never saw bob online")
        .unwrap();

    bob.shutdown().await;
    tokio::time::timeout(AMPLY, online.wait_for(BTreeSet::is_empty))
        .await
        .expect("alice still counts bob online after he stopped")
        .unwrap();

    alice.shutdown().await;
}

/// The composed version of the agreement test `distlib-consensus` has.
///
/// The endpoint is bound from [`distlib::alpns`] before any handler exists, so
/// that list and the handlers the router is given are two declarations of one
/// fact — now across three crates. An endpoint advertising a protocol nothing
/// handles negotiates it and then refuses every stream (P1-11).
#[tokio::test]
async fn a_node_serves_exactly_the_protocols_its_endpoint_advertises() {
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let runtime = Runtime::start(
        &key,
        &config(&[MemberId::from(key.public())]),
        &DataDir::new(dir.path().join("node")),
    )
    .await
    .unwrap();

    let mut served: Vec<String> = runtime
        .node()
        .protocols()
        .into_iter()
        .chain(runtime.catalogue().protocols())
        .map(|(alpn, _)| String::from_utf8_lossy(&alpn).into_owned())
        .collect();
    let mut advertised: Vec<String> = distlib::alpns()
        .into_iter()
        .map(|alpn| String::from_utf8_lossy(&alpn).into_owned())
        .collect();
    served.sort();
    advertised.sort();

    assert_eq!(
        served, advertised,
        "what the router is given must be what the endpoint offers"
    );
    runtime.shutdown().await;
}

/// **The first acceptance run of 2a-2c: two followers, relays disabled.**
///
/// Neither bob nor carol is in the core group, so neither one's address is
/// recorded anywhere — not in a `MemberRecord`, not in Raft's node map, not in
/// the address book, which holds core nodes only. Each knows where *alice* is
/// and nothing else. What has to happen anyway is that what one writes reaches
/// the other.
///
/// It works, and the mechanism is worth naming because it is not ours:
/// `iroh_gossip::net::Gossip::spawn` installs a lookup of its own **onto the
/// endpoint** and fills it from the `PeerData` swarm members exchange in-band.
/// The catalogue's own live updates ride that swarm, so being in it is what
/// makes a follower resolvable — to iroh-docs' internal downloader as much as
/// to anything else dialing a bare id on this endpoint.
#[tokio::test]
async fn what_one_follower_writes_the_other_follower_reads() {
    let group = a_group_with_two_followers().await;

    group
        .bob
        .catalogue()
        .put("item/2/title", "A Wizard of Earthsea")
        .await
        .unwrap();

    let title = read(group.carol.catalogue(), "item/2/title").await;
    assert_eq!(&title[..], b"A Wizard of Earthsea");

    group.carol.shutdown().await;
    group.bob.shutdown().await;
    group.alice.shutdown().await;
}

/// **P2-14, closed.** A follower can now resolve another follower.
///
/// This test is the inverse of the one it replaces. That one asserted the gap —
/// `carol.endpoint().connect(bob_id, PING)` answering `No addressing
/// information available` while the group converged normally — and said in its
/// failure message that the day it failed was the day to write this.
///
/// **Nothing in the log carries this.** A `MemberRecord` has no address field
/// and Raft's node map holds voters, so bob's whereabouts reach carol only
/// because bob *said* where he is, on the group's gossip topic, signed by his
/// own key. Carol was not there to hear him say it the first time; she learns
/// it because *her* announcement reached bob, and hearing of somebody he did
/// not know is what makes bob say where he is again. Not because she became his
/// neighbour — `announce_address` explains why that trigger was tried and
/// deleted.
///
/// Waited for rather than probed once, because that last step is the point: the
/// guarantee is eventual, bounded by the floor between one node's
/// announcements, and a single probe would be asserting a promptness the design
/// does not offer.
#[tokio::test]
async fn a_follower_learns_where_another_follower_is() {
    let group = a_group_with_two_followers().await;
    let bob_id = MemberId::from(group.bob.endpoint().id());

    let found = tokio::time::timeout(SOON, async {
        loop {
            if let Some(addr) = group.carol.node().known_addresses().address_of(bob_id) {
                return addr;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("carol must learn where bob is, though nothing in the log says");

    // Compared against what bob's endpoint says about *itself*, not against
    // its bound sockets: a node announces what iroh has discovered it to be
    // reachable at, which is not the same list — binding `[::]` is a listening
    // socket and not somewhere anybody can dial.
    assert_eq!(
        found,
        NodeAddr::from(&group.bob.endpoint().addr()),
        "and it must be where bob says he is"
    );

    // The thing the directory exists for: iroh resolving a bare id, which is
    // how every protocol we did not write dials.
    group
        .carol
        .endpoint()
        .connect(group.bob.endpoint().id(), distlib_net::alpn::PING)
        .await
        .expect("a resolvable follower must be dialable by id alone");

    group.carol.shutdown().await;
    group.bob.shutdown().await;
    group.alice.shutdown().await;
}

/// And then everybody shuts up.
///
/// The triggers are events — joining, moving, and hearing of somebody new — so a
/// group that has finished converging should have nothing left to say. That did
/// not hold. `Directory::learn` answered "taken" for a statement that told us
/// nothing we did not already hold, because an equal log position must not be
/// rejected; every node therefore read its neighbours' repeats as news, and news
/// is a reason to announce. Three nodes with nothing happening announced at the
/// floor for ever, and each announcement made every receiver re-offer its
/// catalogue's peers — which dials all of them.
///
/// Asserted as *quiet* rather than as a count, and only after the group has
/// converged: the cascade that carries a late joiner is supposed to happen, and
/// what is being pinned is that it ends. Without the fix the silence never
/// arrives and this fails on the timeout.
#[tokio::test]
async fn a_settled_group_stops_talking_about_addresses() {
    // Beating every second, so the quiet a settled group must manage holds
    // with heartbeats in it, not merely between them.
    let group = a_group_with_two_followers_beating(NonZeroU32::MIN).await;
    let everyone = [
        ("alice", &group.alice),
        ("bob", &group.bob),
        ("carol", &group.carol),
    ];

    let heard = || {
        everyone
            .iter()
            .map(|(_, runtime)| *runtime.node().known_addresses().learned().borrow())
            .collect::<Vec<u64>>()
    };
    let everyone_knows_everyone = || {
        everyone.iter().all(|(_, runtime)| {
            everyone
                .iter()
                .filter(|(_, other)| other.endpoint().id() != runtime.endpoint().id())
                .all(|(_, other)| {
                    runtime
                        .node()
                        .known_addresses()
                        .address_of(MemberId::from(other.endpoint().id()))
                        .is_some()
                })
        })
    };

    // Converged first: silence before that would be the wrong kind.
    tokio::time::timeout(SOON, async {
        while !everyone_knows_everyone() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("every member must learn where the others are");

    let settled = tokio::time::timeout(SOON, async {
        loop {
            let before = heard();
            tokio::time::sleep(QUIET).await;
            if heard() == before {
                return before;
            }
        }
    })
    .await;

    assert!(
        settled.is_ok(),
        "a group with nothing happening must stop announcing, and this one was \
         still learning addresses after {SOON:?} — counts now {:?}",
        heard()
    );

    group.carol.shutdown().await;
    group.bob.shutdown().await;
    group.alice.shutdown().await;
}

/// How long a settled group must go without a catalogue sync round (phase 4's
/// 4b-6, Ivan's call): longer than the thirty-second re-offer that used to
/// start one between every pair, so that timer would show in it.
const NO_ROUNDS: Duration = Duration::from_secs(45);

/// **And it stops syncing, too** (4b-6).
///
/// A sync round is a dial and a set reconciliation, and between two nodes
/// with nothing new it finds nothing. The catalogue used to offer every peer
/// again every thirty seconds regardless, so a quiet group of N ran
/// N × (N − 1) such rounds every half-minute for as long as it ran. Changes
/// travel over gossip and the rounds a new neighbour starts; what is left of
/// the timer is a ten-minute backstop.
///
/// Counted by the rounds each node has finished, with whom and when — what
/// an offer costs, since iroh-docs dials every peer it is handed.
#[tokio::test]
async fn a_settled_group_stops_syncing() {
    let group = a_group_with_two_followers().await;
    let everyone = [&group.alice, &group.bob, &group.carol];
    let rounds = || {
        everyone
            .iter()
            .map(|runtime| runtime.catalogue().sync_status().borrow().last_sync.clone())
            .collect::<Vec<_>>()
    };

    // Settled first: a group that is still introducing itself is supposed to
    // sync, so this waits for a stretch with no rounds in it at all.
    let settled = tokio::time::timeout(SOON, async {
        loop {
            let before = rounds();
            tokio::time::sleep(QUIET).await;
            if rounds() == before {
                return before;
            }
        }
    })
    .await
    .expect("the group never settled");

    tokio::time::sleep(NO_ROUNDS).await;
    assert_eq!(
        rounds(),
        settled,
        "a settled group ran catalogue sync rounds within {NO_ROUNDS:?}"
    );

    group.carol.shutdown().await;
    group.bob.shutdown().await;
    group.alice.shutdown().await;
}

/// And so the catalogue no longer needs a core node in the middle.
///
/// The test P2-14 named as the acceptance for this work. Alice is the sole
/// voter, the sole address either follower was configured with, and the only
/// node either has ever been told how to reach. With her stopped, a write still
/// crosses from bob to carol.
///
/// Before this, it did not: the two followers converged only because alice
/// relayed, and stopping her stopped everything — measured at 150 s, so
/// "never" rather than "slowly".
#[tokio::test]
async fn two_followers_keep_converging_once_the_core_node_is_gone() {
    let group = a_group_with_two_followers().await;
    let bob_id = MemberId::from(group.bob.endpoint().id());
    let carol_id = MemberId::from(group.carol.endpoint().id());

    // Each has to know where the other is *before* the node that introduced
    // them goes away. That is the mechanism under test; waiting for it here is
    // what makes a later failure about convergence rather than about a race.
    for (who, runtime, other) in [
        ("carol", &group.carol, bob_id),
        ("bob", &group.bob, carol_id),
    ] {
        tokio::time::timeout(SOON, async {
            while runtime.node().known_addresses().address_of(other).is_none() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{who} never learned where the other follower is"));
    }

    // **4a-3's acceptance: they are each other's neighbours before alice goes.**
    // Knowing an address is not being connected. Each follower used to dial the
    // other on the membership topic before it knew where the other was, and
    // that one failed dial stranded the peer in iroh-gossip on every topic
    // (ground truth 18) — so they met only once the catalogue's timer offered
    // them again, thirty seconds on. That timer is a ten-minute backstop now
    // (4b-6), and this is bounded far under it: the first introduction has to
    // work.
    for (who, runtime, other) in [
        ("carol", &group.carol, bob_id),
        ("bob", &group.bob, carol_id),
    ] {
        let mut sync = runtime.catalogue().sync_status();
        tokio::time::timeout(
            AMPLY,
            sync.wait_for(|state| state.neighbours.contains(&other)),
        )
        .await
        .unwrap_or_else(|_| panic!("{who} never became the other follower's neighbour"))
        .unwrap();
    }

    // Something crosses first, so a failure after the shutdown is about the
    // shutdown rather than about a group that never converged at all.
    group
        .bob
        .catalogue()
        .put("item/3/title", "The Dispossessed")
        .await
        .unwrap();
    assert_eq!(
        &read(group.carol.catalogue(), "item/3/title").await[..],
        b"The Dispossessed"
    );

    // Markers for the log, for whoever has to read a failure of what follows.
    tracing::info!(%bob_id, %carol_id, "test: shutting alice down");
    group.alice.shutdown().await;

    group
        .bob
        .catalogue()
        .put("item/3/year", "1974")
        .await
        .unwrap();
    tracing::info!("test: bob wrote the year; waiting for carol to read it");
    assert_eq!(
        &read(group.carol.catalogue(), "item/3/year").await[..],
        b"1974",
        "a follower must reach a follower without the core node in the middle"
    );

    group.carol.shutdown().await;
    group.bob.shutdown().await;
}

/// **The second acceptance run: an expelled member stops receiving entries.**
///
/// The claim worth pinning, because the handler is not one we wrote:
/// `AllowlistHooks` sits on the *endpoint*, so iroh-docs and iroh-blobs
/// inherit membership enforcement without a line of per-handler checking. An
/// expulsion closes the connections an expelled peer is holding and refuses
/// the next one, and the catalogue is carried on exactly those connections.
///
/// Structured as before-and-after on purpose. A node that never received
/// anything would pass a bare "carol does not have it" assertion while proving
/// nothing, so carol is made to receive one entry *through the path under
/// test* before she is expelled, and the same path is then asked for a second.
#[tokio::test]
async fn an_expelled_member_stops_receiving_entries() {
    let group = a_group_with_two_followers().await;

    group
        .bob
        .catalogue()
        .put("item/4/title", "The Word for World Is Forest")
        .await
        .unwrap();
    assert_eq!(
        &read(group.carol.catalogue(), "item/4/title").await[..],
        b"The Word for World Is Forest",
        "carol must be receiving before her expulsion can be said to stop it"
    );
    // Whoever could pass her an entry must hold a connection to her first, or
    // seeing it gone below would prove nothing.
    let mut watches = Vec::new();
    for (who, runtime) in [("alice", &group.alice), ("bob", &group.bob)] {
        let mut sync = runtime.catalogue().sync_status();
        tokio::time::timeout(
            AMPLY,
            sync.wait_for(|state| state.neighbours.contains(&group.carol_id)),
        )
        .await
        .unwrap_or_else(|_| panic!("{who} never became carol's neighbour"))
        .unwrap();
        watches.push((who, sync));
    }

    group
        .alice
        .node()
        .propose(
            MembershipEvent::MemberExpelled {
                member: group.carol_id,
                reason: "the acceptance criteria say so".to_owned(),
            },
            &group.alice_key,
        )
        .await
        .unwrap();

    // Waited for where the connections are, because that is where she has to
    // be refused: the expulsion has to be enforced there, not merely committed
    // where it was proposed. Enforced, not applied: a node's membership turns
    // before its allowlist does, and Linux CI caught bob broadcasting to carol
    // in the 50 ms between. Alice too, because a peer still connected to her
    // relays whatever bob sends. Carol dropping out of their neighbours is the
    // connection actually closing.
    for (who, mut sync) in watches {
        tokio::time::timeout(
            SOON,
            sync.wait_for(|state| !state.neighbours.contains(&group.carol_id)),
        )
        .await
        .unwrap_or_else(|_| panic!("the expulsion must reach {who}"))
        .unwrap();
    }

    group
        .bob
        .catalogue()
        .put("item/4/year", "1972")
        .await
        .unwrap();

    assert!(
        !ever_arrives(group.carol.catalogue(), "item/4/year", AMPLY).await,
        "an expelled member must not go on receiving the group's catalogue"
    );
    // And what she already had is still hers to read — expulsion stops the
    // group talking to her, it does not reach into her store.
    assert_eq!(
        &read(group.carol.catalogue(), "item/4/title").await[..],
        b"The Word for World Is Forest"
    );

    group.carol.shutdown().await;
    group.bob.shutdown().await;
    group.alice.shutdown().await;
}

/// **4c-2's acceptance: an expelled member's ratings and reviews stop counting
/// in the read model** — and only theirs.
///
/// Before and after, as the test above is: bob's rating and review are in
/// alice's read model before he is expelled, so their going proves something,
/// and carol's stay, so it is bob's that went rather than everyone's. On the
/// way, a rating of an item nobody has written does not make one. And a
/// reindex afterwards changes nothing.
#[tokio::test]
async fn an_expelled_members_ratings_and_reviews_stop_counting() {
    let group = a_group_with_two_followers().await;
    let bob_id = MemberId::from(group.bob.endpoint().id());
    let carol_id = group.carol_id;
    let rating = |value| Rating::try_from(value).unwrap();
    let review = |text: &str| Review::try_from(text.to_owned()).unwrap();

    let id = ItemId::from_bytes([4; 32]);
    group
        .alice
        .catalogue()
        .write(&Item {
            title: Some("The Dispossessed".to_owned()),
            ..Item::new(id)
        })
        .await
        .unwrap();
    for (runtime, stars, words) in [(&group.bob, 2, "Too long"), (&group.carol, 5, "Too short")] {
        runtime.catalogue().rate(id, rating(stars)).await.unwrap();
        runtime
            .catalogue()
            .review(id, &review(words))
            .await
            .unwrap();
    }
    let nothing = ItemId::from_bytes([5; 32]);
    group
        .carol
        .catalogue()
        .rate(nothing, rating(3))
        .await
        .unwrap();

    let store = group.alice.store();
    let before = stored_when(store, id, "both members' ratings and reviews", |stored| {
        stored.ratings.len() == 2 && stored.reviews.len() == 2
    })
    .await;
    assert_eq!(
        before.ratings,
        BTreeMap::from([(bob_id, rating(2)), (carol_id, rating(5))])
    );
    assert_eq!(
        before.reviews,
        BTreeMap::from([
            (bob_id, review("Too long")),
            (carol_id, review("Too short"))
        ])
    );
    tokio::time::timeout(SOON, async {
        while !group
            .alice
            .catalogue()
            .ratings(nothing)
            .await
            .unwrap()
            .contains_key(&carol_id)
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("carol's rating of an item nobody wrote reaches alice");
    assert_eq!(
        store.item(nothing).await.unwrap(),
        None,
        "a rating alone is not an item"
    );

    group
        .alice
        .node()
        .propose(
            MembershipEvent::MemberExpelled {
                member: bob_id,
                reason: "4c-2's acceptance says so".to_owned(),
            },
            &group.alice_key,
        )
        .await
        .unwrap();

    let after = stored_when(store, id, "only carol's rating and review", |stored| {
        stored.ratings.len() == 1 && stored.reviews.len() == 1
    })
    .await;
    assert_eq!(after.ratings, BTreeMap::from([(carol_id, rating(5))]));
    assert_eq!(
        after.reviews,
        BTreeMap::from([(carol_id, review("Too short"))])
    );

    let projected = store.items().await.unwrap();
    group.alice.projection().reindex().await.unwrap();
    assert_eq!(
        store.items().await.unwrap(),
        projected,
        "a second replay changes nothing"
    );

    group.carol.shutdown().await;
    group.bob.shutdown().await;
    group.alice.shutdown().await;
}

/// Waits for `store` to hold item `id` as `ready` says it should, and
/// returns it — or fails saying `what` it was waiting for.
async fn stored_when(
    store: &Store,
    id: ItemId,
    what: &str,
    ready: impl Fn(&StoredItem) -> bool,
) -> StoredItem {
    tokio::time::timeout(SOON, async {
        loop {
            if let Some(stored) = store.item(id).await.unwrap()
                && ready(&stored)
            {
                return stored;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the read model never held {what} within {SOON:?}"))
}

/// **A member that joins long after everyone else still learns where they are.**
///
/// The case the `NeighborUp` trigger exists for, and the one no other test here
/// reaches. Bob announced where he was when he joined; the latecomer was not in
/// the swarm to hear it, and bob has no reason to say it again — his address has
/// not changed and nothing is on a timer. What makes him speak is the newcomer's
/// arrival making them neighbours.
///
/// **Why the wait is load-bearing rather than padding.** Early in a node's life
/// iroh is still discovering its own addresses, and every change prompts a
/// re-announcement; a member joining during that churn learns its peers by
/// accident. That is not hypothetical — it is why an earlier version of this
/// file still passed with the arrival trigger deleted. Letting the group settle
/// first removes the accident, so what is left is the mechanism.
#[tokio::test]
async fn a_late_joiner_still_learns_where_the_others_are() {
    let group = a_group_with_two_followers().await;
    let bob_id = MemberId::from(group.bob.endpoint().id());
    let alice_id = MemberId::from(group.alice.endpoint().id());

    // Long enough for address discovery to quiesce, so that a re-announcement
    // can only be the answer to an arrival.
    tokio::time::sleep(Duration::from_secs(12)).await;

    let latecomer_key = SecretKey::generate();
    let latecomer_id = MemberId::from(latecomer_key.public());
    group
        .alice
        .node()
        .propose(
            MembershipEvent::MemberAdded {
                member: record(latecomer_id, "latecomer"),
            },
            &group.alice_key,
        )
        .await
        .unwrap();

    let latecomer = Runtime::start(
        &latecomer_key,
        &following(alice_id, &bound(&group.alice)),
        &DataDir::new(group.dir.path().join("latecomer")),
    )
    .await
    .unwrap();

    let found = tokio::time::timeout(SOON, async {
        loop {
            if let Some(addr) = latecomer.node().known_addresses().address_of(bob_id) {
                return addr;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a member joining late must still learn where the others are");
    assert_eq!(found, NodeAddr::from(&group.bob.endpoint().addr()));

    latecomer.shutdown().await;
    group.carol.shutdown().await;
    group.bob.shutdown().await;
    group.alice.shutdown().await;
}

/// **2a-5.** A node resolves a member whose announcement it was not there to
/// hear.
///
/// The gap [`a_follower_learns_where_another_follower_is`] closes is the one
/// where both nodes are up: carol announces, bob hears of somebody new, bob
/// says where he is again, carol learns. That chain has a link in it that can
/// be missing — bob — and this is what happens when it is. Bob announces and
/// then goes; carol starts afterwards. Nothing will ever say where bob is
/// again: an announcement is made once, to whoever was listening, and there is
/// deliberately no timer behind it.
///
/// Alice was listening. That is the whole of 2a-5 — a core node hears every
/// member's announcement and can hand them on — and it costs no new trust,
/// because what she hands over is bob's own signature and carol checks it
/// herself.
///
/// **Bob is shut down on purpose, and it is what makes this test mean
/// anything.** Leave him running and he announces the moment he hears of
/// carol, so gossip alone passes this and the directory ask is never exercised.
/// Verified by removing the ask: carol then never resolves bob and this times
/// out.
#[tokio::test]
async fn a_late_joiner_resolves_a_member_it_never_heard_announce() {
    init_logging();
    let dir = TempDir::new().unwrap();

    let alice_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let alice = Runtime::start(
        &alice_key,
        &config(&[alice_id]),
        &DataDir::new(dir.path().join("alice")),
    )
    .await
    .unwrap();
    alice
        .node()
        .init_group(vec![(record(alice_id, "alice"), bound(&alice))], &alice_key)
        .await
        .unwrap();

    let bob_key = SecretKey::generate();
    let bob_id = MemberId::from(bob_key.public());
    let carol_key = SecretKey::generate();
    for (key, name) in [(&bob_key, "bob"), (&carol_key, "carol")] {
        alice
            .node()
            .propose(
                MembershipEvent::MemberAdded {
                    member: record(MemberId::from(key.public()), name),
                },
                &alice_key,
            )
            .await
            .unwrap();
    }

    let joining = following(alice_id, &bound(&alice));
    let bob = Runtime::start(&bob_key, &joining, &DataDir::new(dir.path().join("bob")))
        .await
        .unwrap();

    let where_bob_is = tokio::time::timeout(SOON, async {
        loop {
            if let Some(addr) = alice.node().known_addresses().address_of(bob_id) {
                return addr;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("alice must hear bob announce; the rest of this test rests on it");

    // And now the only node that could say it again is gone.
    bob.shutdown().await;

    let carol = Runtime::start(
        &carol_key,
        &joining,
        &DataDir::new(dir.path().join("carol")),
    )
    .await
    .unwrap();

    let found = tokio::time::timeout(SOON, async {
        loop {
            if let Some(addr) = carol.node().known_addresses().address_of(bob_id) {
                return addr;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("carol must resolve bob by asking the core group, having heard nothing");

    assert_eq!(
        found, where_bob_is,
        "and it must be what bob himself signed, relayed unaltered"
    );

    carol.shutdown().await;
    alice.shutdown().await;
}
