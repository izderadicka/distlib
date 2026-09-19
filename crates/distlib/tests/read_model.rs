//! The catalogue, projected into SQLite, across a restart.
//!
//! 2a-3's acceptance in one sentence: **kill and restart a node, and its SQLite
//! state matches a node that never restarted.** Both tests here are that
//! sentence; they differ in what the restarted node still has when it comes
//! back, and the difference is what makes each of them prove something.
//!
//! The comparison is between two *live* nodes rather than against an expected
//! set written out here, deliberately. An expected set would be a third
//! statement of the schema, able to be wrong in the same way the projection is
//! wrong; two nodes that agree is the property the design actually claims.

// Two whole runtimes with docs engines, on-disk blob stores and a founding
// election, one of them started twice: seconds, not milliseconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{collections::BTreeMap, path::Path, time::Duration};

use distlib::Runtime;
use distlib_core::{
    Config, ContentHash, DataDir, FileRecord, FileRole, Item, ItemId, ItemKind, MemberId, Series,
};
use distlib_store::StoredItem;
use iroh::SecretKey;
use tempfile::TempDir;

mod common;
use common::{bound, config, record};

/// Long enough for two in-process nodes to elect, replicate and reconcile, and
/// then for the projection behind that to catch up.
const SOON: Duration = Duration::from_secs(60);

/// One item, with enough of §5.2 filled in that a dropped column shows up.
///
/// Every kind of value the schema has to carry: a one-word enum, a list, a
/// struct split across two columns, a fractional number, an optional integer,
/// and a file row. A projection that lost any one of them would still pass a
/// test written around a title.
fn an_item(seed: u8, title: &str) -> Item {
    Item {
        kind: Some(ItemKind::Audiobook),
        title: Some(title.to_owned()),
        authors: Some(vec!["Frank Herbert".to_owned(), "Brian Herbert".to_owned()]),
        genres: Some(vec!["science fiction".to_owned()]),
        series: Some(Series {
            name: "Dune".to_owned(),
            index: Some(2.5),
        }),
        year: Some(1965),
        lang: Some("en".to_owned()),
        description: Some("A desert planet, and the spice.".to_owned()),
        replicas: Some(3),
        files: BTreeMap::from([(
            ContentHash::from_bytes([seed; 32]),
            FileRecord {
                role: FileRole::Content,
                format: "mp3".to_owned(),
                size: 1_234_567,
                filename: format!("{title}.mp3"),
                seq: Some(1),
                disc: None,
                title: Some(format!("{title}, chapter one")),
                duration: Some(3_600),
            },
        )]),
        ..Item::new(ItemId::from_bytes([seed; 32]))
    }
}

/// Waits until both nodes' read models hold the same `items` items.
///
/// **Both sides are re-read on every poll**, which is the whole subtlety here.
/// A node projects an item as soon as it has an entry for it, and fills the
/// rest in as content arrives — the writer included, since its own projection
/// is behind its own writes. Reading the node being compared against once, at
/// the start, snapshots a half-projected item and then waits for the other node
/// to converge on something that is no longer true.
///
/// The count is asserted too, so two empty stores are not "agreement".
async fn agree_on(node: &Runtime, against: &Runtime, items: usize, what: &str) {
    let mut here: Vec<StoredItem> = Vec::new();
    let mut there: Vec<StoredItem> = Vec::new();
    let caught_up = tokio::time::timeout(SOON, async {
        loop {
            here = node.store().items().await.unwrap();
            there = against.store().items().await.unwrap();
            if here.len() == items && here == there {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(
        caught_up.is_ok(),
        "{what}: the two read models never agreed on {items} items within {SOON:?}\n  the restarted node has {}: {here:#?}\n  the other has {}: {there:#?}",
        here.len(),
        there.len(),
    );
}

/// Waits until both nodes' search indexes rank `query` the same way.
///
/// Mirrors [`agree_on`]: both sides are re-read on every poll, for the same
/// reason — a node's own projection is behind its own writes, so reading the
/// side being compared against just once would snapshot a half-built index.
async fn agree_on_search(node: &Runtime, against: &Runtime, query: &str, what: &str) {
    let mut here = Vec::new();
    let mut there = Vec::new();
    let caught_up = tokio::time::timeout(SOON, async {
        loop {
            here = node.search().search(query, 10).await.unwrap();
            there = against.search().search(query, 10).await.unwrap();
            if !here.is_empty() && here == there {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(
        caught_up.is_ok(),
        "{what}: the two search indexes never agreed on {query:?} within {SOON:?}\n  here: {here:#?}\n  there: {there:#?}",
    );
}

/// Alice and bob, founded as one group, with bob's data directory named so the
/// test can start a second node on it.
async fn a_founded_pair(dir: &Path) -> (Runtime, Runtime, SecretKey, Config) {
    let alice_key = SecretKey::generate();
    let bob_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_id = MemberId::from(bob_key.public());
    let config = config(&[alice_id, bob_id]);

    let alice = Runtime::start(&alice_key, &config, &DataDir::new(dir.join("alice")))
        .await
        .unwrap();
    let bob = Runtime::start(&bob_key, &config, &DataDir::new(dir.join("bob")))
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

    (alice, bob, bob_key, config)
}

/// The acceptance, as the phase doc states it.
///
/// Bob misses a write while he is down, so catching up is not something the
/// restart could have got for free by having been there.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_node_holds_what_a_node_that_never_restarted_holds() {
    let dir = TempDir::new().unwrap();
    let (alice, bob, bob_key, config) = a_founded_pair(dir.path()).await;

    alice.catalogue().ready().await;
    alice.catalogue().write(&an_item(1, "Dune")).await.unwrap();
    alice
        .catalogue()
        .write(&an_item(2, "Dune Messiah"))
        .await
        .unwrap();
    agree_on(&bob, &alice, 2, "before the restart").await;

    // Down, and something happens while he is down.
    bob.shutdown().await;
    drop(bob);
    alice
        .catalogue()
        .write(&an_item(3, "Children of Dune"))
        .await
        .unwrap();

    let bob = Runtime::start(&bob_key, &config, &DataDir::new(dir.path().join("bob")))
        .await
        .unwrap();
    agree_on(&bob, &alice, 3, "after the restart").await;

    // **Against what was written, not against the other node.** Everything
    // above compares two projections of the same code: a field this crate
    // silently dropped on the way out of the catalogue would be dropped on both
    // nodes, and they would agree about it. `an_item` carries every shape the
    // schema has to hold — a one-word enum, a list, a struct split over two
    // columns, a fraction, an optional integer and a file row — so this pins
    // the whole chain against something a person wrote down.
    let projected = bob
        .store()
        .item(ItemId::from_bytes([1; 32]))
        .await
        .unwrap()
        .expect("the restarted node projected the first item");
    assert_eq!(projected.item, an_item(1, "Dune"));

    // The `members` table comes from the log rather than the document, so it
    // has its own path through the restart and its own chance to be missed.
    assert_eq!(
        bob.store().members().await.unwrap(),
        alice.store().members().await.unwrap(),
        "the restarted node projected the group differently",
    );

    bob.shutdown().await;
    alice.shutdown().await;
}

/// The same, with the read model deleted rather than merely stopped.
///
/// **This is the one that pins the cold-start replay**, and the other one
/// cannot: with the database still on disk, a restarted node is already almost
/// right, and the handful of entries it missed arrive as live changes. Delete
/// the database and nothing on the live path will ever mention the entries that
/// were already synced — only a full replay of the document rebuilds them. Take
/// the replay out of `distlib-store`'s projection and this test fails while the
/// one above still passes.
///
/// Not a contrived failure, either: it is what a node does the first time it
/// runs a build that has a read model, and after any schema change, since
/// `db/` is documented as the one directory under the root that can be deleted.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_model_that_was_lost_is_rebuilt_from_the_document() {
    let dir = TempDir::new().unwrap();
    let (alice, bob, bob_key, config) = a_founded_pair(dir.path()).await;

    alice.catalogue().ready().await;
    alice
        .catalogue()
        .write(&an_item(4, "Neuromancer"))
        .await
        .unwrap();
    alice
        .catalogue()
        .write(&an_item(5, "Count Zero"))
        .await
        .unwrap();
    agree_on(&bob, &alice, 2, "before the read model is deleted").await;

    bob.shutdown().await;
    drop(bob);

    let bobs_dir = DataDir::new(dir.path().join("bob"));
    assert!(bobs_dir.db_dir().exists(), "bob had a read model to delete");
    std::fs::remove_dir_all(bobs_dir.db_dir()).unwrap();

    let bob = Runtime::start(&bob_key, &config, &bobs_dir).await.unwrap();
    agree_on(&bob, &alice, 2, "after the read model is deleted").await;

    bob.shutdown().await;
    alice.shutdown().await;
}

/// 2a-3b's half of the acceptance: delete the search index alone, and the
/// replay that rebuilds `db/` rebuilds `index/` too — because, per P2-19,
/// a start and a reindex are one operation and there is only the one replay
/// to run either of them.
///
/// `db/` is left in place here, deliberately: this isolates the index's own
/// rebuild from SQLite's. If the index were instead fed from the tables
/// rather than from the same catalogue re-read, this would pass by accident
/// while a cold start of both together still failed.
#[tokio::test(flavor = "multi_thread")]
async fn a_search_index_that_was_lost_is_rebuilt_from_the_document() {
    let dir = TempDir::new().unwrap();
    let (alice, bob, bob_key, config) = a_founded_pair(dir.path()).await;

    alice.catalogue().ready().await;
    alice
        .catalogue()
        .write(&an_item(6, "Snow Crash"))
        .await
        .unwrap();
    agree_on_search(
        &bob,
        &alice,
        "Snow Crash",
        "before the search index is deleted",
    )
    .await;

    bob.shutdown().await;
    drop(bob);

    let bobs_dir = DataDir::new(dir.path().join("bob"));
    assert!(
        bobs_dir.index_dir().exists(),
        "bob had a search index to delete"
    );
    std::fs::remove_dir_all(bobs_dir.index_dir()).unwrap();

    let bob = Runtime::start(&bob_key, &config, &bobs_dir).await.unwrap();
    agree_on_search(
        &bob,
        &alice,
        "Snow Crash",
        "after the search index is deleted",
    )
    .await;

    bob.shutdown().await;
    alice.shutdown().await;
}
