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
use distlib_api::Api;
use distlib_core::{
    Config, ContentHash, DataDir, FileRecord, FileRole, Item, ItemId, ItemKind, MemberId,
    NetConfig, Series,
};
use distlib_store::StoredItem;
use iroh::SecretKey;
use serde_json::json;
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

/// `admin.reindex` actually rebuilds the search index from the document,
/// rather than being wired to a `replay` nothing here exercises.
///
/// The drift is manufactured directly against `SearchIndex`, bypassing the
/// projection, because that is the only way to get the index to disagree with
/// the document without waiting on a race: a corruption the projection would
/// otherwise repair on its own the moment anything touched the item again.
#[tokio::test(flavor = "multi_thread")]
async fn reindex_repairs_a_search_index_that_has_drifted_from_the_document() {
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let config = config(&[id]);
    let runtime = Runtime::start(&key, &config, &DataDir::new(dir.path().join("solo")))
        .await
        .unwrap();
    runtime
        .node()
        .init_group(vec![(record(id, "solo"), bound(&runtime))], &key)
        .await
        .unwrap();

    let item_id = ItemId::from_bytes([9; 32]);
    runtime.catalogue().ready().await;
    runtime
        .catalogue()
        .write(&an_item(9, "Dune"))
        .await
        .unwrap();
    tokio::time::timeout(SOON, async {
        loop {
            if runtime.search().search("Dune", 10).await.unwrap() == vec![item_id] {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the item becomes searchable");

    // Drift the index away from what the document says, directly.
    runtime
        .search()
        .index_item(Item {
            title: Some("an imposter".to_owned()),
            ..Item::new(item_id)
        })
        .await
        .unwrap();
    runtime.search().commit().await.unwrap();
    assert_eq!(
        runtime.search().search("Dune", 10).await.unwrap(),
        Vec::new(),
        "the drift did not take, so the rest of this test proves nothing"
    );

    runtime
        .projection()
        .reindex()
        .await
        .expect("admin.reindex completes");

    assert_eq!(
        runtime.search().search("Dune", 10).await.unwrap(),
        vec![item_id],
        "reindex did not restore the document's own title"
    );
    assert_eq!(
        runtime.search().search("imposter", 10).await.unwrap(),
        Vec::new(),
        "the drifted title should not have survived a reindex"
    );

    runtime.shutdown().await;
}

/// The regression this is named for: `run` awaits `catalogue.ready()` before
/// its main loop even starts, and a node that has been started but has not
/// yet founded or joined a group sits there forever. `admin.reindex` must not
/// be a hung HTTP request for that entirely ordinary case — `distlib run`
/// prints exactly this warning on a fresh node — so a request arriving before
/// the catalogue exists is acknowledged at once: an empty catalogue reindexes
/// to nothing.
#[tokio::test(flavor = "multi_thread")]
async fn admin_reindex_does_not_hang_before_a_group_exists() {
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let runtime = Runtime::start(
        &key,
        &config(&[MemberId::from(key.public())]),
        &DataDir::new(dir.path().join("node")),
    )
    .await
    .unwrap();

    let reindexed = tokio::time::timeout(Duration::from_secs(5), runtime.projection().reindex())
        .await
        .expect("admin.reindex hung on a node with no group yet");
    assert!(reindexed.is_ok());

    runtime.shutdown().await;
}

/// 2a-4's acceptance, the part of it that does not need `library.add`: an
/// item written through the catalogue — not seeded into `store`/`search`
/// directly, the way the `distlib-api` unit tests do it — becomes findable
/// and readable through `library.search` and `library.item`, over the same
/// `Api::call` dispatch `distlib-api serve` runs in production.
///
/// **What this does not cover.** The phase names its acceptance "two nodes,
/// add metadata on one, search by author on the other" — this is one node,
/// because the write path and the read path are the same code however many
/// nodes are running, and `Runtime`'s two-node convergence is already what
/// `a_restarted_node_holds_what_a_node_that_never_restarted_holds` and its
/// neighbours pin. What is new here, and untested until now, is the second
/// half: that `library.*` actually reads what the projection wrote, over the
/// real dispatch a caller uses — not `Store`/`SearchIndex` called directly.
#[tokio::test(flavor = "multi_thread")]
async fn library_search_and_item_read_what_the_catalogue_wrote() {
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let config = config(&[id]);
    let runtime = Runtime::start(&key, &config, &DataDir::new(dir.path().join("solo")))
        .await
        .unwrap();
    runtime
        .node()
        .init_group(vec![(record(id, "solo"), bound(&runtime))], &key)
        .await
        .unwrap();

    let item_id = ItemId::from_bytes([9; 32]);
    runtime.catalogue().ready().await;
    runtime
        .catalogue()
        .write(&an_item(9, "Dune"))
        .await
        .unwrap();
    // **Waits for what the assertions below actually need, which is not the
    // same thing as "the item is searchable".**
    //
    // This gate used to be `search("Dune")` — the *title* — while the first
    // assertion searches for *Herbert*, an author. Those are two separate
    // projection passes over the same item: the projection re-reads the whole
    // item on every change rather than applying what a change said (P2-19), so
    // a pass that runs while `Catalogue::write` is partway through its per-key
    // loop indexes an item that genuinely has a title and no authors yet.
    // That satisfied a title gate and then failed an author assertion, about
    // one run in seven on an idle machine — measured, after it turned up
    // twice in CI and was written off the first time as load.
    //
    // Nothing is wrong with the projection here: the item converges, and a
    // reader who asks again gets the whole of it. What was wrong is a test
    // waiting on one field and asserting another. So both halves of what
    // follows are waited for by name — the index, under the term the search
    // below actually uses, and the record's own fields out of SQLite, which
    // is a different store catching up on its own schedule.
    tokio::time::timeout(SOON, async {
        loop {
            let indexed = runtime.search().search("Herbert", 10).await.unwrap() == vec![item_id];
            let projected = runtime
                .store()
                .item(item_id)
                .await
                .unwrap()
                .is_some_and(|stored| {
                    stored.item.authors.is_some() && stored.item.files.len() == 1
                });
            if indexed && projected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the item becomes searchable by author, with its record fully projected");

    let api = Api {
        node: std::sync::Arc::clone(runtime.node()),
        secret: key,
        net: NetConfig::default(),
        reindex_handle: runtime.projection().reindex_handle(),
        catalogue: runtime.catalogue().clone(),
        blobs: runtime.blobs().clone(),
        store: runtime.store().clone(),
        search: runtime.search().clone(),
    };

    let found = api
        .call("library.search", Some(json!({ "query": "Herbert" })))
        .await
        .unwrap();
    let hits = found["results"].as_array().unwrap();
    assert_eq!(hits.len(), 1, "{found}");
    assert_eq!(hits[0]["item_id"], json!(item_id));
    assert_eq!(hits[0]["title"], json!("Dune"));

    let record = api
        .call("library.item", Some(json!({ "item_id": item_id })))
        .await
        .unwrap();
    assert_eq!(record["title"], json!("Dune"));
    assert_eq!(record["authors"], json!(["Frank Herbert", "Brian Herbert"]));
    assert_eq!(record["files"].as_object().unwrap().len(), 1);

    runtime.shutdown().await;
}
