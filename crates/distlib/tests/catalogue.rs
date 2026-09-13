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
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib::Runtime;
use distlib_consensus::{MemberRecord, MembershipEvent};
use distlib_core::{Config, CoreMember, DataDir, MemberId, NodeAddr};
use iroh::SecretKey;
use tempfile::TempDir;

/// Long enough for two in-process nodes to elect, replicate and reconcile.
const SOON: Duration = Duration::from_secs(30);

/// The window a negative assertion gets before it is believed.
///
/// Deliberately longer than an entry has ever taken to cross in these tests —
/// where the positive reads land in well under a second — because "it has not
/// arrived" and "it is not coming" are the same observation until enough time
/// has passed. Cheap, since the test only pays it when it passes.
const AMPLY: Duration = Duration::from_secs(10);

/// A node's configuration: both members in the core group, nothing else on.
///
/// The configured addresses are empty on purpose. Before there is a log,
/// configuration is the only thing that says who votes — but *where* they are
/// is written into the founding entry a moment later, and every node reads it
/// from there. The same arrangement the consensus test harness uses.
fn config(core: &[MemberId]) -> Config {
    let mut config = Config::default();
    config.net.bind_addr_v4 = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.net.relay_mode = distlib_core::RelayMode::Disabled;
    config.api.enabled = false;
    config.consensus.core = core
        .iter()
        .map(|member| CoreMember {
            member: *member,
            name: String::new(),
            addrs: Vec::new(),
            relay: None,
        })
        .collect();
    config
}

fn record(id: MemberId, name: &str) -> MemberRecord {
    MemberRecord {
        member_id: id,
        display_name: name.to_owned(),
        pledge_bytes: 0,
    }
}

fn bound(runtime: &Runtime) -> NodeAddr {
    NodeAddr {
        relay: None,
        direct: runtime.endpoint().bound_sockets().into_iter().collect(),
    }
}

/// Two runtimes, founded as one group, sharing one catalogue.
async fn a_founded_pair() -> (TempDir, Runtime, Runtime) {
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

/// A follower's configuration: the core group it bootstraps from, *with* an
/// address, and this node deliberately not in it.
///
/// The address has to be here. A follower has no log yet, so configuration is
/// the only thing that can say where the group is — and with
/// `relay_mode = "disabled"` there is no lookup to fall back on. This is the
/// ticket's job in production; a test hands over the same two facts directly.
fn following(core: MemberId, addr: &NodeAddr) -> Config {
    let mut config = config(&[core]);
    config.consensus.core = vec![CoreMember {
        member: core,
        name: String::new(),
        addrs: addr.direct.iter().copied().collect(),
        relay: None,
    }];
    config
}

/// Reads one key, waiting out the window where the entry is here and its
/// content is not.
///
/// `MissingContent` is that window and this poll is meant to sit through it —
/// see `Catalogue::get`. Anything else is a real failure and is not retried.
async fn read(catalogue: &distlib_sync::Catalogue, key: &str) -> Vec<u8> {
    tokio::time::timeout(SOON, async {
        loop {
            match catalogue.get(key).await {
                Ok(Some(value)) => return value.to_vec(),
                Ok(None) | Err(distlib_sync::SyncError::MissingContent { .. }) => {}
                Err(error) => panic!("reading the catalogue failed: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("`{key}` never arrived"))
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
    _dir: TempDir,
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
    let joining = following(alice_id, &where_alice_is);
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
        _dir: dir,
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

/// **A gap, pinned as the measurement it is.**
///
/// This passes today, and the day it fails is the day P2-14 is fixed — at
/// which point the thing to write is its opposite: alice stopped, bob writes,
/// carol reads. That test does not exist yet because it would only ever have
/// been an ignored failure, which cannot regress and cannot tell anybody when
/// it starts working.
///
/// **What is measured.** Alice, the only core node, is up, and the catalogue is
/// converging normally — [`what_one_follower_writes_the_other_follower_reads`]
/// has just shown that. And still carol cannot dial bob by bare id. So that
/// test passes because alice *relays*, not because bob and carol ever speak;
/// stop alice and nothing crosses at all, at 150 s as much as at 30 s.
///
/// **Not a localhost artefact**, which was the first thing checked:
/// `endpoint().addr()` carries a usable direct address on every node here, so
/// gossip has something real to publish. The cause is that
/// `GossipAddressLookup` — which iroh-gossip *does* install on our shared
/// endpoint, so it would serve docs' downloader if it were filled — takes its
/// entries from the peer data on Join and ForwardJoin messages, and two
/// followers that joined through the same core node never learn each other
/// that way. `distlib-net`'s `addresses` module claimed otherwise until this
/// test disproved it.
///
/// The consequence, which nobody chose: **a follower reaches the group through
/// core nodes**, and that is where the catalogue's traffic goes.
#[tokio::test]
async fn a_follower_cannot_resolve_another_follower() {
    let group = a_group_with_two_followers().await;

    // Converging first. Without this the assertion below would also pass on a
    // group that had never worked at all.
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

    // No address supplied, which is how every protocol we did not write dials.
    let reached = group
        .carol
        .endpoint()
        .connect(group.bob.endpoint().id(), distlib_net::alpn::PING)
        .await;
    let Err(refused) = reached else {
        panic!(
            "P2-14 looks fixed: a follower resolved a follower. Write the convergence test \
             that belongs here — alice stopped, bob writes, carol reads — and update P2-14."
        )
    };
    assert!(
        format!("{refused}").contains("No addressing information"),
        "the gap is specifically that there is no address to be had; got {refused}"
    );

    group.carol.shutdown().await;
    group.bob.shutdown().await;
    group.alice.shutdown().await;
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

    // Waited for on bob, because bob is the one who has to refuse her: the
    // expulsion has to be applied where the connection is, not merely
    // committed where it was proposed.
    tokio::time::timeout(SOON, async {
        while group.bob.node().membership().is_member(&group.carol_id) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the expulsion must reach bob");

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
