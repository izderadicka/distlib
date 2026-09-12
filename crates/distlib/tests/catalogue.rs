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
use distlib_consensus::MemberRecord;
use distlib_core::{Config, CoreMember, DataDir, MemberId, NodeAddr};
use iroh::SecretKey;
use tempfile::TempDir;

/// Long enough for two in-process nodes to elect, replicate and reconcile.
const SOON: Duration = Duration::from_secs(30);

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

    let read = tokio::time::timeout(SOON, async {
        loop {
            // `MissingContent` is "the entry is here and its value is still
            // being fetched", which is a moment this poll is meant to wait
            // out rather than a failure — see `Catalogue::get`.
            match bob.catalogue().get("item/1/title").await {
                Ok(Some(value)) => return value,
                Ok(None) | Err(distlib_sync::SyncError::MissingContent { .. }) => {}
                Err(error) => panic!("reading the catalogue failed: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("what alice catalogued must reach bob");
    assert_eq!(&read[..], b"The Left Hand of Darkness");

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
