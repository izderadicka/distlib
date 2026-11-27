//! Garbage collection (5a-1): a pass deletes what nothing keeps, and nothing
//! a node added, downloaded or synced is among it.

// Two whole runtimes with docs engines and on-disk blob stores, and a
// download between them: seconds, not milliseconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{path::Path, time::Duration};

use distlib::Runtime;
use distlib_consensus::MembershipEvent;
use distlib_core::{ContentHash, DataDir, ItemId, KeepReason, MemberId};
use iroh::SecretKey;
use secrecy::SecretString;
use serde_json::json;
use tempfile::TempDir;

mod common;
use common::{
    PLEDGE, api, bound, collecting, config, download, following, found, group_settings,
    init_logging, pledge, profile, until_a_pass_collects, until_projected, until_released,
};

/// Long enough for in-process nodes to elect, replicate, reconcile, project
/// and collect.
const SOON: Duration = Duration::from_secs(60);

/// The title `runtime` reads for `item`, which is missing if the content of
/// its entry was collected: an item reads as what of it is here.
async fn title_on(runtime: &Runtime, item: ItemId) -> Option<String> {
    runtime.catalogue().item(item).await.unwrap()?.title
}

/// A group of two, alice founding and bob following, both collecting every
/// second and with the catalogue open.
async fn alice_and_bob_collecting(dir: &Path) -> (Runtime, SecretKey, Runtime, SecretKey) {
    let alice_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_key = SecretKey::generate();
    let bob_id = MemberId::from(bob_key.public());

    let alice = Runtime::start(
        &alice_key,
        &collecting(config(&[alice_id])),
        &DataDir::new(dir.join("alice")),
    )
    .await
    .unwrap();
    found(&[&alice], &[(&alice_key, PLEDGE)], &group_settings()).await;
    alice
        .node()
        .propose(
            MembershipEvent::MemberAdded {
                member: profile(bob_id, "bob"),
            },
            &alice_key,
        )
        .await
        .unwrap();
    pledge(&alice, &bob_key, PLEDGE).await;
    let bob = Runtime::start(
        &bob_key,
        &collecting(following(alice_id, &bound(&alice))),
        &DataDir::new(dir.join("bob")),
    )
    .await
    .unwrap();
    for runtime in [&alice, &bob] {
        tokio::time::timeout(SOON, runtime.catalogue().ready())
            .await
            .unwrap();
    }

    (alice, alice_key, bob, bob_key)
}

/// Alice adds a book, bob syncs the catalogue and downloads it; then both
/// collect at least one whole pass.
///
/// **A whole pass.** A pass cannot be watched, so each node collects a first
/// blob nothing keeps, then a second, imported only after the first was gone:
/// the pass that took the first has then ended.
///
/// **Mutation checks:** no protect handler on the documents
/// (`protect: None` in `Runtime::start`) loses the titles; no `own/` tag in
/// `library.add` loses alice's file; no `cache/` tag in `library.download`
/// loses bob's.
#[tokio::test(flavor = "multi_thread")]
async fn a_collection_pass_keeps_what_was_added_downloaded_and_synced() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (alice, alice_key, bob, bob_key) = alice_and_bob_collecting(dir.path()).await;

    // Large enough to be a file in the store rather than a row inlined in
    // its database, which is how media is kept.
    let book = dir.path().join("dune.epub");
    std::fs::write(&book, b"the spice must flow\n".repeat(50_000)).unwrap();
    let added = api(&alice, &alice_key)
        .call(
            "library.add",
            Some(json!({ "kind": "ebook", "files": [&book], "title": "Dune" })),
        )
        .await
        .unwrap();
    let item: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    let file = *alice
        .catalogue()
        .item(item)
        .await
        .unwrap()
        .unwrap()
        .files
        .keys()
        .next()
        .unwrap();
    until_projected(&bob, item, 1, "bob").await;
    let dest = dir.path().join("bob-downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&bob, &bob_key), item, &dest).await;

    for (runtime, who) in [(&alice, "alice"), (&bob, "bob")] {
        until_a_pass_collects(runtime, dir.path(), &format!("{who}-first")).await;
        until_a_pass_collects(runtime, dir.path(), &format!("{who}-second")).await;
    }

    let held = |runtime: &Runtime, hash: ContentHash| {
        let blobs = runtime.blobs().clone();
        async move { blobs.has(hash).await.unwrap() }
    };
    assert!(held(&alice, file).await, "alice keeps what she added");
    assert!(held(&bob, file).await, "bob keeps what he downloaded");
    assert_eq!(
        title_on(&alice, item).await.as_deref(),
        Some("Dune"),
        "alice keeps the entry she wrote"
    );
    assert_eq!(
        title_on(&bob, item).await.as_deref(),
        Some("Dune"),
        "bob keeps the entry he synced"
    );

    bob.shutdown().await;
    alice.shutdown().await;
}

/// #185 finding 2, and #225: an add keeps its files before it writes the
/// item, so one that stopped in between leaves own tags of an item the
/// catalogue does not have. A lost catalogue leaves the same, so starting the
/// api keeps them through collection, and counts them until the catalogue
/// has the item: here, when the add is made again.
///
/// **Mutation check:** letting go of the unwritten add's tag at the top of
/// `distlib_api::serve` fails it.
#[tokio::test(flavor = "multi_thread")]
async fn starting_the_api_keeps_and_counts_the_files_of_an_add_that_stopped_before_it_was_written()
{
    init_logging();
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let alice = Runtime::start(
        &key,
        &collecting(config(&[MemberId::from(key.public())])),
        &DataDir::new(dir.path().join("alice")),
    )
    .await
    .unwrap();
    found(&[&alice], &[(&key, PLEDGE)], &group_settings()).await;
    tokio::time::timeout(SOON, alice.catalogue().ready())
        .await
        .unwrap();

    // What an add leaves when it stops after its tags, before its write.
    let stopped = dir.path().join("stopped.epub");
    std::fs::write(&stopped, b"never written\n".repeat(1_000)).unwrap();
    let imported = alice.catalogue().add_file(&stopped).await.unwrap();
    let unwritten = ItemId::from_content_hashes(&[*imported.hash.as_bytes()]);
    alice
        .blobs()
        .keep(KeepReason::Own, unwritten, imported.hash)
        .await
        .unwrap();
    let hash = imported.hash;
    drop(imported);

    let api = api(&alice, &key);
    let server = distlib_api::serve(
        "127.0.0.1:0".parse().unwrap(),
        api.clone(),
        SecretString::from("token"),
    )
    .await
    .unwrap();
    until_a_pass_collects(&alice, dir.path(), "marker").await;

    assert_eq!(
        alice
            .blobs()
            .kept_for(KeepReason::Own, unwritten)
            .await
            .unwrap(),
        [hash],
        "the unwritten add's files kept"
    );
    assert!(alice.blobs().has(hash).await.unwrap());
    let unpublished = || async {
        api.call("node.status", None).await.unwrap()["storage"]["unpublished_own_items"].clone()
    };
    assert_eq!(unpublished().await, 1);

    let added = api
        .call(
            "library.add",
            Some(json!({ "kind": "ebook", "files": [&stopped], "title": "Dune" })),
        )
        .await
        .unwrap();
    assert_eq!(
        added["item_id"],
        json!(unwritten),
        "the same files, the same item"
    );
    assert_eq!(unpublished().await, 0, "the catalogue has it now");
    drop(server);
    alice.shutdown().await;
}

/// #225's reproduction A, on a node alone: its catalogue lost while it is
/// stopped. Starts and collection keep the item's files, which export as
/// they were added. The catalogue's own values went with it (C65).
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_catalogue_does_not_cost_the_only_copy_of_an_items_files() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let alices_config = collecting(config(&[MemberId::from(key.public())]));
    let alices_dir = DataDir::new(dir.path().join("alice"));
    let start = || Runtime::start(&key, &alices_config, &alices_dir);

    let alice = start().await.unwrap();
    found(&[&alice], &[(&key, PLEDGE)], &group_settings()).await;
    tokio::time::timeout(SOON, alice.catalogue().ready())
        .await
        .unwrap();
    let (item, file, bytes) = add_a_book(&alice, &key, dir.path()).await;
    alice.shutdown().await;
    drop(alice);
    until_released(&alices_dir).await;

    std::fs::remove_dir_all(alices_dir.docs_dir()).unwrap();
    for start_count in ["first", "second"] {
        let alice = start().await.unwrap();
        let server = distlib_api::serve(
            "127.0.0.1:0".parse().unwrap(),
            api(&alice, &key),
            SecretString::from("token"),
        )
        .await
        .unwrap();
        until_a_whole_pass(&alice, dir.path(), start_count).await;
        assert_eq!(
            alice.blobs().kept_for(KeepReason::Own, item).await.unwrap(),
            [file],
            "{start_count} start: the item's files kept"
        );
        drop(server);
        alice.shutdown().await;
        drop(alice);
        until_released(&alices_dir).await;
    }

    let alice = start().await.unwrap();
    let exported = dir.path().join("exported.epub");
    alice.blobs().export(file, &exported).await.unwrap();
    assert_eq!(std::fs::read(&exported).unwrap(), bytes);
    alice.shutdown().await;
}

/// #225, in a group: bob's catalogue lost while he is stopped comes back from
/// alice, who holds no copy of his book, and his kept files serve it.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_catalogue_comes_back_from_a_peer_and_the_kept_files_serve_it() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (alice, _, bob, bob_key) = alice_and_bob_collecting(dir.path()).await;
    let (item, file, bytes) = add_a_book(&bob, &bob_key, dir.path()).await;
    until_projected(&alice, item, 1, "alice").await;
    assert!(
        !alice.blobs().has(file).await.unwrap(),
        "alice holds no copy"
    );

    let bobs_config = collecting(following(alice.node().id(), &bound(&alice)));
    let bobs_dir = DataDir::new(dir.path().join("bob"));
    bob.shutdown().await;
    drop(bob);
    until_released(&bobs_dir).await;
    std::fs::remove_dir_all(bobs_dir.docs_dir()).unwrap();

    let bob = Runtime::start(&bob_key, &bobs_config, &bobs_dir)
        .await
        .unwrap();
    let bobs_api = api(&bob, &bob_key);
    let server = distlib_api::serve(
        "127.0.0.1:0".parse().unwrap(),
        bobs_api.clone(),
        SecretString::from("token"),
    )
    .await
    .unwrap();
    until_a_whole_pass(&bob, dir.path(), "bob").await;
    until_projected(&bob, item, 1, "bob").await;
    let dest = dir.path().join("bob-downloads");
    std::fs::create_dir(&dest).unwrap();
    download(&bobs_api, item, &dest).await;
    assert_eq!(std::fs::read(dest.join("dune.epub")).unwrap(), bytes);

    drop(server);
    bob.shutdown().await;
    alice.shutdown().await;
}

/// Adds a book through `runtime`'s api: its item, its one file and its bytes.
/// Large enough to be a file in the store rather than a row inlined in its
/// database, which is how media is kept.
async fn add_a_book(
    runtime: &Runtime,
    key: &SecretKey,
    dir: &Path,
) -> (ItemId, ContentHash, Vec<u8>) {
    let book = dir.join("dune.epub");
    let bytes = b"the spice must flow\n".repeat(50_000);
    std::fs::write(&book, &bytes).unwrap();
    let added = api(runtime, key)
        .call(
            "library.add",
            Some(json!({ "kind": "ebook", "files": [&book], "title": "Dune" })),
        )
        .await
        .unwrap();
    let item: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    let file = runtime
        .blobs()
        .kept_for(KeepReason::Own, item)
        .await
        .unwrap()[0];
    (item, file, bytes)
}

/// Waits until `runtime` has collected at least one whole pass: a pass
/// cannot be watched, so it collects a first blob nothing keeps, then a
/// second imported only after the first was gone.
async fn until_a_whole_pass(runtime: &Runtime, dir: &Path, who: &str) {
    until_a_pass_collects(runtime, dir, &format!("{who}-first")).await;
    until_a_pass_collects(runtime, dir, &format!("{who}-second")).await;
}
