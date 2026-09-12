//! Two members, one catalogue: what one writes, the other reads.
//!
//! Deliberately without a running `MembershipNode`. What is under test is the
//! catalogue — the derived document key, the import, the sync — and the only
//! thing it needs from consensus is the projection, which a test can fold by
//! hand from a founding event. A failure here is iroh-docs or this crate, not
//! Raft.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipState, SignedEvent, Timestamp};
use distlib_core::{MemberId, NodeAddr};
use distlib_net::Transport;
use distlib_sync::{Catalogue, catalogue_key};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_blobs::store::mem::MemStore;
use iroh_gossip::net::Gossip;
use tokio::sync::watch;

/// A member with a catalogue, and the transport under it.
struct Node {
    catalogue: Catalogue,
    addr: NodeAddr,
    router: Router,
}

impl Node {
    async fn start(secret: SecretKey, membership: watch::Receiver<MembershipState>) -> Self {
        let endpoint = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .secret_key(secret.clone())
            .alpns(distlib_sync::alpns())
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .unwrap()
            .bind()
            .await
            .unwrap();
        let addr = NodeAddr {
            relay: None,
            direct: endpoint.bound_sockets().into_iter().collect(),
        };

        let gossip = Gossip::builder().spawn(endpoint.clone());
        // In memory: this test is about convergence, not about what survives a
        // restart, and `FsStore` spawns a runtime of its own per node.
        let blobs = MemStore::new();
        let catalogue = Catalogue::start(
            Transport {
                endpoint: endpoint.clone(),
                gossip,
            },
            (*blobs).clone(),
            None,
            &secret,
            membership,
        )
        .await
        .unwrap();
        let router = distlib_net::serve(endpoint, catalogue.protocols());

        Self {
            catalogue,
            addr,
            router,
        }
    }

    async fn shutdown(&self) {
        self.catalogue.shutdown();
        let _ = self.router.shutdown().await;
    }
}

/// The projection a founded two-member group produces, folded by hand.
fn founded(founders: Vec<(MemberRecord, NodeAddr)>, by: &SecretKey) -> MembershipState {
    let event = MembershipEvent::found(founders, Timestamp::from_millis(1)).unwrap();
    let signed = SignedEvent::sign(by, event, Timestamp::from_millis(1), 0).unwrap();
    let mut state = MembershipState::new();
    state.apply(1, &signed).unwrap();
    state
}

fn record(id: MemberId, name: &str) -> MemberRecord {
    MemberRecord {
        member_id: id,
        display_name: name.to_owned(),
        pledge_bytes: 0,
    }
}

/// Long enough for two nodes to find each other and reconcile; short enough
/// that a broken sync fails the run rather than hanging it.
const SOON: Duration = Duration::from_secs(30);

#[tokio::test]
async fn what_one_member_writes_the_other_reads() {
    let alice_key = SecretKey::generate();
    let bob_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_id = MemberId::from(bob_key.public());

    // Bound first, because founding records each member's address and the
    // catalogue dials what the log says.
    let (to_alice, alice_sees) = watch::channel(MembershipState::new());
    let (to_bob, bob_sees) = watch::channel(MembershipState::new());
    let alice = Node::start(alice_key.clone(), alice_sees).await;
    let bob = Node::start(bob_key, bob_sees).await;

    let founding = founded(
        vec![
            (record(alice_id, "alice"), alice.addr.clone()),
            (record(bob_id, "bob"), bob.addr.clone()),
        ],
        &alice_key,
    );
    to_alice.send(founding.clone()).unwrap();
    to_bob.send(founding).unwrap();

    tokio::time::timeout(SOON, alice.catalogue.ready())
        .await
        .expect("alice opens her catalogue once she knows the group");
    tokio::time::timeout(SOON, bob.catalogue.ready())
        .await
        .expect("and so does bob");

    alice.catalogue.put("item/1/title", "Dune").await.unwrap();

    let read = tokio::time::timeout(SOON, async {
        loop {
            // `MissingContent` is "the entry is here and its value is still
            // being fetched", which is a moment this poll is meant to wait
            // out rather than a failure — see `Catalogue::get`.
            match bob.catalogue.get("item/1/title").await {
                Ok(Some(value)) => return value,
                Ok(None) | Err(distlib_sync::SyncError::MissingContent { .. }) => {}
                Err(error) => panic!("reading the catalogue failed: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("what alice wrote must reach bob");
    assert_eq!(&read[..], b"Dune");

    alice.shutdown().await;
    bob.shutdown().await;
}

/// The document key is a wire fact: two nodes that compute it differently see
/// two empty catalogues and no error anywhere. Pinned to a golden value, like
/// the group id derivation it is built on.
#[test]
fn the_catalogue_key_derivation_is_fixed() {
    let group = distlib_core::GroupId::from_bytes([7u8; 32]);
    assert_eq!(
        data_encoding::HEXLOWER.encode(&catalogue_key(group).to_bytes()),
        "f3f1f6bdd8d025d7bfb4ebe346d0d83a123ff3b0e9006da69512fdb260cba882",
        "changing this splits every existing group's catalogue in two"
    );
}
