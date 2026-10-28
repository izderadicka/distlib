//! Garbage collection (5a-1): a pass deletes what nothing keeps, and nothing
//! a node added, downloaded or synced is among it.

// Two whole runtimes with docs engines and on-disk blob stores, and a
// download between them: seconds, not milliseconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{path::Path, time::Duration};

use distlib::Runtime;
use distlib_consensus::MembershipEvent;
use distlib_core::{ContentHash, DataDir, ItemId, MemberId};
use iroh::SecretKey;
use serde_json::json;
use tempfile::TempDir;

mod common;
use common::{
    PLEDGE, api, bound, collecting, config, download, following, found, group_settings, pledge,
    profile, until_a_pass_collects, until_projected,
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
