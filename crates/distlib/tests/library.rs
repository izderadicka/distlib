//! `library.add` (2b-2): hashing a file set into an item, and §6.1's
//! add-time dedup assist.
//!
//! `crates/distlib-sync/tests/converge.rs` already proves a document
//! converges between two endpoints, and `tests/catalogue.rs` proves the
//! *process* does the same off the back of a founded group. What is new here
//! is the write path itself — `library.add` computing a fingerprint, hashing
//! files into the blob store, and deciding what a second write to an id that
//! already exists is allowed to change.

// Two whole runtimes with docs engines, on-disk blob stores and a founding
// election: seconds, not milliseconds, like every other multi-node test here.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use distlib::Runtime;
use distlib_api::Api;
use distlib_core::{DataDir, FileRecord, FileRole, Item, ItemId, ItemKind, MemberId, NetConfig};
use iroh::SecretKey;
use serde_json::json;
use tempfile::TempDir;

mod common;
use common::{bound, config, record};

/// Long enough for two in-process nodes to elect, replicate and reconcile.
const SOON: Duration = Duration::from_secs(30);

/// An `Api` wired to `runtime`'s own pieces — the same assembly
/// `distlib::commands::run` does, minus the HTTP listener: these tests call
/// `Api::call` directly, the way `read_model.rs`'s own `library.*` test does.
fn api(runtime: &Runtime, key: &SecretKey) -> Api {
    Api {
        node: Arc::clone(runtime.node()),
        secret: key.clone(),
        net: NetConfig::default(),
        reindex_handle: runtime.projection().reindex_handle(),
        catalogue: runtime.catalogue().clone(),
        blobs: runtime.blobs().clone(),
        store: runtime.store().clone(),
        search: runtime.search().clone(),
    }
}

/// 2b-2's acceptance: two nodes each `library.add` the identical file set,
/// at the same time, and end up looking at one item rather than two.
///
/// **No coordination**, which is the claim — `tokio::join!` runs both calls
/// concurrently rather than waiting for one to finish before starting the
/// other, so neither node's write can be informed by what the other did.
/// What makes this hold regardless is §5.2's `item_id`: it is a pure
/// function of the content hashes, computed before either node asks the
/// catalogue anything, so both calls arrive at the same id independently of
/// which one the document ends up agreeing happened "first".
#[tokio::test]
async fn adding_the_same_files_twice_from_different_nodes_converges_on_one_item() {
    let dir = TempDir::new().unwrap();
    let alice_key = SecretKey::generate();
    let bob_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_id = MemberId::from(bob_key.public());
    let group_config = config(&[alice_id, bob_id]);

    let alice = Runtime::start(
        &alice_key,
        &group_config,
        &DataDir::new(dir.path().join("alice")),
    )
    .await
    .unwrap();
    let bob = Runtime::start(
        &bob_key,
        &group_config,
        &DataDir::new(dir.path().join("bob")),
    )
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
    alice.catalogue().ready().await;
    bob.catalogue().ready().await;

    // Identical bytes, each node's own copy on its own disk — a real file
    // set two members each downloaded or ripped for themselves, not one file
    // shared between them.
    let bytes = &b"the same forty-chapter audiobook, allegedly"[..];
    let alice_file = dir.path().join("alice-copy.mp3");
    std::fs::write(&alice_file, bytes).unwrap();
    let bob_file = dir.path().join("bob-copy.mp3");
    std::fs::write(&bob_file, bytes).unwrap();

    let params = |path: &std::path::Path| {
        json!({
            "kind": "audiobook",
            "files": [path],
            "title": "Dune",
            "authors": ["Frank Herbert"],
        })
    };

    let alice_api = api(&alice, &alice_key);
    let bob_api = api(&bob, &bob_key);
    let (alice_answer, bob_answer) = tokio::join!(
        alice_api.call("library.add", Some(params(&alice_file))),
        bob_api.call("library.add", Some(params(&bob_file))),
    );
    let alice_answer = alice_answer.unwrap();
    let bob_answer = bob_answer.unwrap();

    assert_eq!(
        alice_answer["item_id"], bob_answer["item_id"],
        "the same bytes fingerprint to the same item wherever they are hashed"
    );
    let item_id: ItemId = serde_json::from_value(alice_answer["item_id"].clone()).unwrap();

    // Whichever of the two writes the document resolves as "later" per key,
    // both nodes end up with one item holding one file — never a second item,
    // and never a file entry lost to the other's write.
    tokio::time::timeout(SOON, async {
        loop {
            let seen_by_alice = alice.catalogue().item(item_id).await.unwrap();
            let seen_by_bob = bob.catalogue().item(item_id).await.unwrap();
            if let (Some(a), Some(b)) = (&seen_by_alice, &seen_by_bob)
                && a.files.len() == 1
                && b.files.len() == 1
            {
                assert_eq!(a.title, Some("Dune".to_owned()));
                assert_eq!(b.title, Some("Dune".to_owned()));
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect(
        "both nodes converge on one item with one file, with no coordination between the two calls",
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

/// §6.1's add-time assist, on one node: a fingerprint that already names an
/// item leaves that item's metadata alone and only contributes the files it
/// did not already have.
///
/// The pre-existing item is seeded with only one of its two files under an
/// id computed from *both* — standing in for a write that was interrupted
/// partway through [`distlib_sync::Catalogue::write`]'s per-key loop, which
/// is the case this guard actually has to repair: an id that already exists
/// can still be missing some of the file entries its own fingerprint implies.
///
/// **Mutation check:** deleting the `!existing.files.contains_key(hash)`
/// filter and writing every field of a fresh `Item` regardless of `existing`
/// turns this red on the `title` assertion — the worse guess would overwrite
/// "Dune" — which is exactly the regression the guard exists to prevent.
#[tokio::test]
async fn adding_an_item_that_already_exists_leaves_its_metadata_alone_and_contributes_missing_files()
 {
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let solo_config = config(&[id]);
    let runtime = Runtime::start(&key, &solo_config, &DataDir::new(dir.path().join("solo")))
        .await
        .unwrap();
    runtime
        .node()
        .init_group(vec![(record(id, "solo"), bound(&runtime))], &key)
        .await
        .unwrap();
    runtime.catalogue().ready().await;

    let file_a = dir.path().join("chapter-1.mp3");
    std::fs::write(&file_a, b"chapter one").unwrap();
    let file_b = dir.path().join("chapter-2.mp3");
    std::fs::write(&file_b, b"chapter two").unwrap();

    let (hash_a, size_a) = runtime.catalogue().add_file(&file_a).await.unwrap();
    let (hash_b, _) = runtime.catalogue().add_file(&file_b).await.unwrap();
    let item_id = ItemId::from_content_hashes(&[*hash_a.as_bytes(), *hash_b.as_bytes()]);

    // Somebody already created this item, and only chapter one's file entry
    // ever made it in.
    runtime
        .catalogue()
        .write(&Item {
            kind: Some(ItemKind::Audiobook),
            title: Some("Dune".to_owned()),
            files: BTreeMap::from([(
                hash_a,
                FileRecord {
                    role: FileRole::Content,
                    format: "mp3".to_owned(),
                    size: size_a,
                    filename: "chapter-1.mp3".to_owned(),
                    seq: None,
                    disc: None,
                    title: None,
                    duration: None,
                },
            )]),
            ..Item::new(item_id)
        })
        .await
        .unwrap();

    let answer = api(&runtime, &key)
        .call(
            "library.add",
            Some(json!({
                "kind": "audiobook",
                "files": [&file_a, &file_b],
                "title": "a worse guess at the title",
            })),
        )
        .await
        .unwrap();

    assert_eq!(answer["item_id"], json!(item_id));
    assert_eq!(answer["created"], json!(false), "the id already existed");
    assert_eq!(
        answer["title"],
        json!("Dune"),
        "the existing title is reported back untouched"
    );
    let contributed = answer["contributed_files"].as_array().unwrap();
    assert_eq!(contributed, &[json!(hash_b)], "only the missing file");

    let item = runtime.catalogue().item(item_id).await.unwrap().unwrap();
    assert_eq!(
        item.title,
        Some("Dune".to_owned()),
        "not overwritten by the second, worse-informed caller"
    );
    assert_eq!(item.files.len(), 2, "chapter two is here now too");

    runtime.shutdown().await;
}

/// A call naming no files at all is the caller's mistake, not this method
/// doing nothing useful — and it is refused before anything is hashed or the
/// catalogue is asked anything, so it needs no group to be founded first.
#[tokio::test]
async fn adding_with_no_files_is_refused() {
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let solo_config = config(&[id]);
    let runtime = Runtime::start(&key, &solo_config, &DataDir::new(dir.path().join("solo")))
        .await
        .unwrap();

    let error = api(&runtime, &key)
        .call("library.add", Some(json!({ "kind": "ebook", "files": [] })))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("at least one file"), "{error}");

    runtime.shutdown().await;
}

/// Jules' review on PR #51: two different local paths that hash to the same
/// content must not let the id `library.add` computes drift from the item's
/// own [`Item::fingerprint`] — `distlib-sync/tests/converge.rs`'s
/// `it is what it contains` invariant, pinned here from the write side too.
///
/// **Mutation check:** reintroducing a separate `Vec<[u8; 32]>` built one
/// entry per *path* rather than reading it back off the deduplicated `files`
/// map turns this red — `ItemId::from_content_hashes(&[H, H])` differs from
/// `ItemId::from_content_hashes(&[H])`, the fingerprint of the one file
/// entry the map actually ends up holding.
#[tokio::test]
async fn adding_two_paths_with_identical_bytes_keeps_the_id_and_the_items_own_fingerprint_in_step()
{
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let solo_config = config(&[id]);
    let runtime = Runtime::start(&key, &solo_config, &DataDir::new(dir.path().join("solo")))
        .await
        .unwrap();
    runtime
        .node()
        .init_group(vec![(record(id, "solo"), bound(&runtime))], &key)
        .await
        .unwrap();
    runtime.catalogue().ready().await;

    // Two different paths, the same bytes — a caller who pointed at one
    // file twice under different names, not a mistake this method should
    // have to refuse.
    let bytes = b"the same file, named twice";
    let first = dir.path().join("copy-one.epub");
    std::fs::write(&first, bytes).unwrap();
    let second = dir.path().join("copy-two.epub");
    std::fs::write(&second, bytes).unwrap();

    let answer = api(&runtime, &key)
        .call(
            "library.add",
            Some(json!({
                "kind": "ebook",
                "files": [&first, &second],
                "title": "Dune",
            })),
        )
        .await
        .unwrap();
    let item_id: ItemId = serde_json::from_value(answer["item_id"].clone()).unwrap();

    let item = runtime.catalogue().item(item_id).await.unwrap().unwrap();
    assert_eq!(
        item.files.len(),
        1,
        "one blob, hashed twice under two names, is one file entry"
    );
    assert_eq!(
        item.fingerprint(),
        Some(item_id),
        "the id it was stored under has to be what the item's own files fingerprint to"
    );

    runtime.shutdown().await;
}
