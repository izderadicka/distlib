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
    path::PathBuf,
    time::Duration,
};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipState, SignedEvent, Timestamp};
use distlib_core::{ContentHash, FileRecord, FileRole, Item, ItemId, ItemKind, MemberId, NodeAddr};
use distlib_net::Transport;
use distlib_sync::{Catalogue, catalogue_key};
use iroh::{
    Endpoint, SecretKey,
    endpoint::{RelayMode, presets},
    protocol::Router,
};
use iroh_blobs::store::mem::MemStore;
use iroh_gossip::net::{GOSSIP_ALPN, Gossip};
use tokio::sync::watch;

/// A member with a catalogue, and the transport under it.
struct Node {
    catalogue: Catalogue,
    addr: NodeAddr,
    /// Kept so one test can break the content store without touching the
    /// document store beside it.
    blobs: MemStore,
    router: Router,
}

impl Node {
    async fn start(secret: SecretKey, membership: watch::Receiver<MembershipState>) -> Self {
        Self::start_storing(secret, membership, None).await
    }

    /// [`Self::start`] with the document store named, so one test can stop a
    /// node and bring it back holding the entries it had.
    async fn start_storing(
        secret: SecretKey,
        membership: watch::Receiver<MembershipState>,
        documents: Option<PathBuf>,
    ) -> Self {
        let endpoint = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .secret_key(secret.clone())
            .alpns(
                distlib_sync::alpns()
                    .into_iter()
                    .chain([GOSSIP_ALPN.to_vec()])
                    .collect(),
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

        let gossip = Gossip::builder().spawn(endpoint.clone());
        // In memory: this test is about convergence, not about what survives a
        // restart, and `FsStore` spawns a runtime of its own per node.
        let blobs = MemStore::new();
        let catalogue = Catalogue::start(
            Transport::new(endpoint.clone(), gossip.clone()).unwrap(),
            (*blobs).clone(),
            documents,
            &secret,
            membership,
        )
        .await
        .unwrap();
        // **Gossip is served here, not by the catalogue.** A document's live
        // updates ride the process's gossip swarm — without it a node gets
        // only what the first reconciliation brought and never hears another
        // word. In production `MembershipNode` serves it, since one ALPN has
        // one owner; here the test has to.
        let router = distlib_net::serve(
            endpoint,
            catalogue
                .protocols()
                .into_iter()
                .chain([(
                    GOSSIP_ALPN.to_vec(),
                    Box::new(gossip.clone()) as Box<dyn iroh::protocol::DynProtocolHandler>,
                )])
                .collect(),
        );

        Self {
            catalogue,
            addr,
            blobs,
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

/// A whole item, written on one node and read back on the other.
///
/// The field-level test of what `what_one_member_writes_the_other_reads`
/// proves for a single key: an item is a set of entries, and the set survives
/// the crossing intact.
#[tokio::test]
async fn an_item_written_on_one_node_reads_back_whole_on_the_other() {
    let alice_key = SecretKey::generate();
    let bob_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_id = MemberId::from(bob_key.public());

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
        .unwrap();
    tokio::time::timeout(SOON, bob.catalogue.ready())
        .await
        .unwrap();

    let chapters = [[11u8; 32], [12u8; 32]];
    // The id is the fingerprint of the content files, which is what makes two
    // members adding the same files converge on one item (§5.2, P0-7).
    let mut item = Item::new(ItemId::from_content_hashes(&chapters));
    item.kind = Some(ItemKind::Audiobook);
    item.title = Some("The Dispossessed".to_owned());
    item.authors = Some(vec!["Ursula K. Le Guin".to_owned()]);
    item.year = Some(1974);
    for (index, chapter) in chapters.iter().enumerate() {
        item.files.insert(
            ContentHash::from_bytes(*chapter),
            FileRecord {
                role: FileRole::Content,
                format: "mp3".to_owned(),
                size: 4_200_000,
                filename: format!("{:02}.mp3", index + 1),
                seq: Some(index as u32 + 1),
                disc: None,
                title: None,
                duration: Some(1_800),
            },
        );
    }
    assert_eq!(item.fingerprint(), Some(item.id), "it is what it contains");

    alice.catalogue.write(&item).await.unwrap();

    let read = tokio::time::timeout(SOON, async {
        loop {
            // An item arrives entry by entry, so a read during the crossing is
            // a partial item rather than a failure — wait for the whole thing.
            match bob.catalogue.item(item.id).await {
                Ok(Some(read)) if read == item => return read,
                Ok(_) => {}
                Err(distlib_sync::SyncError::MissingContent { .. }) => {}
                Err(error) => panic!("reading the catalogue failed: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the whole item must reach bob");

    assert_eq!(read.title.as_deref(), Some("The Dispossessed"));
    assert_eq!(read.files.len(), 2);

    // **A second write, after the first has already crossed.** The first one
    // could ride the reconciliation two nodes do when they start syncing; this
    // one cannot, so it pins the live path — which is the half that goes
    // quietly missing when the document's gossip swarm is not connected.
    let mut correction = Item::new(item.id);
    correction.description = Some("Two planets, one wall.".to_owned());
    alice.catalogue.write(&correction).await.unwrap();

    tokio::time::timeout(SOON, async {
        loop {
            if let Ok(Some(read)) = bob.catalogue.item(item.id).await
                && read.description == correction.description
            {
                assert_eq!(
                    read.title.as_deref(),
                    Some("The Dispossessed"),
                    "a one-field write must not disturb the fields beside it"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a later edit must reach bob too");
    assert_eq!(
        bob.catalogue
            .item(ItemId::from_bytes([99u8; 32]))
            .await
            .unwrap(),
        None,
        "an item nobody has written is not an empty item"
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

/// A store that is *broken* must not look like a store that is merely behind.
///
/// The distinction is the whole of `MissingContent`: a caller polling for an
/// entry to arrive waits out that error, so anything else going wrong has to
/// be reported as something else — or a node with a closed, full or corrupt
/// store waits for ever for content that is never coming. Shutting the store
/// down is the one breakage a test can stage.
#[tokio::test]
async fn a_broken_store_is_not_reported_as_content_on_its_way() {
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let (to_node, sees) = watch::channel(MembershipState::new());
    let node = Node::start(key.clone(), sees).await;
    to_node
        .send(founded(
            vec![(record(id, "alice"), node.addr.clone())],
            &key,
        ))
        .unwrap();
    tokio::time::timeout(SOON, node.catalogue.ready())
        .await
        .unwrap();

    node.catalogue.put("item/1/title", "Dune").await.unwrap();
    assert!(node.catalogue.get("item/1/title").await.unwrap().is_some());

    // Only the content store, so the document store beside it still answers:
    // the entry is found, and its bytes cannot be. This is the state a read
    // hits while the router is tearing the store down, and it stands in for
    // every other way a store can be broken — full, corrupt, unreadable.
    node.blobs.shutdown().await.unwrap();

    match node.catalogue.get("item/1/title").await {
        Err(distlib_sync::SyncError::MissingContent { .. }) => {
            panic!("a closed store must not read as content that is still on its way")
        }
        Err(_) => {}
        Ok(value) => panic!("a closed store cannot answer with {value:?}"),
    }

    node.shutdown().await;
}

/// A node that keeps its entries and loses their content asks for it again.
///
/// **This is the state iroh-docs has no way out of, staged so that it is
/// certain rather than occasional.** An entry and its content replicate
/// separately, and the engine fetches content only when an entry *arrives*. A
/// node holding an entry whose bytes are not in its store is therefore finished
/// as far as the engine is concerned: set reconciliation finds nothing to send,
/// so there is no insert, so there is no download, ever.
///
/// The same dead end is reached by ordinary means and much less predictably. An
/// entry that arrives over gossip is credited with content at its sender only
/// when it came straight from the publisher — `engine/gossip.rs` checks
/// `msg.scope.is_direct()` — so a *relayed* entry starts no download at all and
/// waits on `Op::ContentReady`, which is sent once, to direct neighbours only,
/// and never by the author. Whoever is not a neighbour of the first node to
/// finish downloading keeps the key and never gets the value. That is the flake
/// this repairs, and the reason it is staged this way instead is that provoking
/// it for real means dictating a gossip topology, which no test can do.
///
/// Losing a content store is not a contrivance either: an in-memory store on a
/// node that restarts is exactly this, and so is a backup that kept the
/// documents and not the blobs.
#[tokio::test]
async fn a_node_that_lost_its_content_asks_for_it_again() {
    let alice_key = SecretKey::generate();
    let bob_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_id = MemberId::from(bob_key.public());

    // Bob's documents outlive his process; his content store does not.
    let documents = tempfile::TempDir::new().unwrap();

    let (to_alice, alice_sees) = watch::channel(MembershipState::new());
    let (to_bob, bob_sees) = watch::channel(MembershipState::new());
    let alice = Node::start(alice_key.clone(), alice_sees).await;
    let bob = Node::start_storing(
        bob_key.clone(),
        bob_sees,
        Some(documents.path().to_path_buf()),
    )
    .await;

    let founding = founded(
        vec![
            (record(alice_id, "alice"), alice.addr.clone()),
            (record(bob_id, "bob"), bob.addr.clone()),
        ],
        &alice_key,
    );
    to_alice.send(founding.clone()).unwrap();
    to_bob.send(founding.clone()).unwrap();
    tokio::time::timeout(SOON, alice.catalogue.ready())
        .await
        .unwrap();
    tokio::time::timeout(SOON, bob.catalogue.ready())
        .await
        .unwrap();

    alice.catalogue.put("item/1/title", "Dune").await.unwrap();
    tokio::time::timeout(SOON, async {
        while bob
            .catalogue
            .get("item/1/title")
            .await
            .ok()
            .flatten()
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("bob reads it the ordinary way first");

    // Bob comes back with the same entries and an empty content store. Nothing
    // in the engine will ever ask for those bytes again.
    bob.shutdown().await;
    let (to_bob, bob_sees) = watch::channel(MembershipState::new());
    let bob = Node::start_storing(bob_key, bob_sees, Some(documents.path().to_path_buf())).await;
    to_bob.send(founding).unwrap();
    tokio::time::timeout(SOON, bob.catalogue.ready())
        .await
        .unwrap();

    assert!(
        matches!(
            bob.catalogue.get("item/1/title").await,
            Err(distlib_sync::SyncError::MissingContent { .. })
        ),
        "the entry must survive the restart and its content must not — \
         otherwise this test proves nothing about fetching it back"
    );

    let read = tokio::time::timeout(SOON, async {
        loop {
            match bob.catalogue.get("item/1/title").await {
                Ok(Some(value)) => return value,
                Ok(None) | Err(distlib_sync::SyncError::MissingContent { .. }) => {}
                Err(error) => panic!("reading the catalogue failed: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("bob must ask alice for the content he is missing and get it");
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
