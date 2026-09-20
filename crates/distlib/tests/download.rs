//! `library.download` (2b-3): getting an item's files from whoever has them,
//! and becoming somebody who has them.
//!
//! `distlib-net/tests/blobs.rs` already pins the transfer itself on two bare
//! endpoints, and `tests/library.rs` pins the write that put the bytes in a
//! store in the first place. What is new here is the method between them —
//! reading an item out of the read model, deciding who to ask and whether to
//! ask anybody at all, and writing the result somewhere an operator can open
//! it.
//!
//! **What this file is not.** §9's acceptance for the whole phase ends "node
//! B (fresh join) ... downloads a file, and after restart still serves it",
//! driven through the actual commands the way
//! [`founding.rs`](founding.rs) drives founding. That run is 2b-3b. The
//! second half of it — a node that downloaded a file still serving that file
//! after a restart, to a member with nowhere else to get it — is the claim
//! `library.download` itself has to carry, so it is here, in process, where
//! the fetch can be watched rather than inferred from a printed line.

// Three whole runtimes with docs engines, on-disk blob stores, a founding
// election and a restart: seconds, not milliseconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{path::Path, sync::Arc, time::Duration};

use distlib::Runtime;
use distlib_api::Api;
use distlib_consensus::MembershipEvent;
use distlib_core::{ContentHash, DataDir, ItemId, MemberId, NetConfig};
use iroh::SecretKey;
use serde_json::json;
use tempfile::TempDir;

mod common;
use common::{bound, config, following, record};

/// Long enough for in-process nodes to elect, replicate, reconcile and
/// project.
const SOON: Duration = Duration::from_secs(60);

/// An `Api` wired to `runtime`'s own pieces, as `library.rs` does it.
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

/// Writes a file and adds it as an ebook, answering with the item's id.
async fn add_an_ebook(api: &Api, path: &Path, bytes: &[u8], title: &str) -> ItemId {
    std::fs::write(path, bytes).unwrap();
    let answer = api
        .call(
            "library.add",
            Some(json!({
                "kind": "ebook",
                "files": [path],
                "title": title,
                "authors": ["Frank Herbert"],
            })),
        )
        .await
        .unwrap();
    serde_json::from_value(answer["item_id"].clone()).unwrap()
}

/// Waits until this node's read model holds `item` with all `files` of its
/// files — which is what `library.download` reads, so a download asked for
/// any sooner is asking about an item this node genuinely does not have yet.
///
/// **The file count, not merely the item.** An item's row appears as soon as
/// any one of its entries has been projected, and its per-file rows are
/// separate entries that arrive on their own schedule. Waiting only for the
/// row is waiting for the title.
async fn until_projected(runtime: &Runtime, item: ItemId, files: usize, who: &str) {
    tokio::time::timeout(SOON, async {
        loop {
            if let Some(stored) = runtime.store().item(item).await.unwrap()
                && stored.item.files.len() == files
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{who} never projected the item with its {files} file(s)"));
}

/// A directory to download into, made fresh so an assertion about what is in
/// it afterwards is about this download.
fn dest(dir: &Path, who: &str) -> std::path::PathBuf {
    let dest = dir.join(format!("{who}-downloads"));
    std::fs::create_dir_all(&dest).unwrap();
    dest
}

/// The one thing 2b-3 adds that nothing before it could have: a node that
/// downloaded a file is a node the *group* can download that file from, and
/// stays one across a restart.
///
/// The shape is §9's acceptance with the joining procedure left to 2b-3b:
/// alice adds a book, bob downloads it, bob is restarted, **alice is shut
/// down**, and only then does carol ask for it. With the only other holder
/// gone, a download that succeeds is bytes that came out of bob's own store —
/// which is the claim, and it is structural rather than observed, since there
/// is nowhere else in the group for them to have come from.
///
/// **Bob is restarted before alice leaves, not after.** A node binds an
/// ephemeral port, so the restarted bob is at an address nobody has heard of;
/// carol learns it the way `catalogue.rs` pins followers learning about each
/// other, which is a swarm that still has to be reachable. Waiting for carol
/// to hold the new address before shutting alice down is what makes the
/// download the thing under test rather than the address lookup.
///
/// **Bob deletes his copy of the file before the restart**, which is load
/// bearing rather than tidiness — see the comment where he does it.
///
/// **Mutation check:** exporting rather than copying — `ExportMode::TryReference`
/// in `distlib_net::Blobs::export` — turns this red at carol's download,
/// because the bytes bob exported were moved out of his store rather than
/// copied from it, and the file they moved to is gone. The same mutation
/// `distlib-net`'s own export test uses, here at the range it matters at —
/// and it passes without the deletion above, which is how that line was found
/// to be necessary.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_downloads_a_file_serves_it_after_a_restart() {
    let dir = TempDir::new().unwrap();
    let alice_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_key = SecretKey::generate();
    let bob_id = MemberId::from(bob_key.public());
    let carol_key = SecretKey::generate();
    let carol_id = MemberId::from(carol_key.public());

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
    for (member, name) in [(bob_id, "bob"), (carol_id, "carol")] {
        alice
            .node()
            .propose(
                MembershipEvent::MemberAdded {
                    member: record(member, name),
                },
                &alice_key,
            )
            .await
            .unwrap();
    }

    // Followers, so that shutting alice down later is the core node leaving
    // rather than the group losing quorum on top of it.
    let joining = following(alice_id, &bound(&alice));
    let bobs_dir = DataDir::new(dir.path().join("bob"));
    let bob = Runtime::start(&bob_key, &joining, &bobs_dir).await.unwrap();
    let carol = Runtime::start(
        &carol_key,
        &joining,
        &DataDir::new(dir.path().join("carol")),
    )
    .await
    .unwrap();
    for runtime in [&alice, &bob, &carol] {
        tokio::time::timeout(SOON, runtime.catalogue().ready())
            .await
            .unwrap();
    }

    // **A megabyte, not a sentence.** A blob small enough to sit inline in
    // the store's own database is exported by writing those bytes out, with
    // nothing to move and nothing to reference — so a small file here would
    // pass whatever `Blobs::export` did, and the mutation check above would
    // be a claim rather than a check. Found by running it at both sizes.
    let bytes = b"forty chapters of desert politics, abridged\n".repeat(24_000);
    let item = add_an_ebook(
        &api(&alice, &alice_key),
        &dir.path().join("dune.epub"),
        &bytes,
        "Dune",
    )
    .await;

    // Both followers sync the catalogue while the node that added it is still
    // up — carol's copy is what she downloads against later, and she must not
    // be asking about an item she first hears of from nobody.
    until_projected(&bob, item, 1, "bob").await;
    until_projected(&carol, item, 1, "carol").await;

    let bobs_dest = dest(dir.path(), "bob");
    let downloaded = api(&bob, &bob_key)
        .call(
            "library.download",
            Some(json!({ "item_id": item, "dest": &bobs_dest })),
        )
        .await
        .unwrap();
    let files = downloaded["files"].as_array().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0]["fetched"],
        json!(true),
        "bob did not have these bytes, so they came over the network"
    );
    assert_eq!(
        std::fs::read(bobs_dest.join("dune.epub")).unwrap(),
        bytes,
        "and they are the bytes alice added"
    );

    // Bob read the book and tidied up. **This is what makes the rest of the
    // test mean anything**: with the exported file still sitting there, bob
    // would go on serving the blob even if the export had moved it out of
    // his store rather than copied it, and the claim would hold by accident.
    std::fs::remove_file(bobs_dest.join("dune.epub")).unwrap();

    // The restart. Bob comes back on a fresh port with the same data
    // directory, which is the only thing carrying the blob across.
    bob.shutdown().await;
    // Dropped as well as shut down: the runtime holds the redb file open for
    // as long as it is alive, and `start` on the same directory would be
    // refused the lock.
    drop(bob);
    let bob = Runtime::start(&bob_key, &joining, &bobs_dir).await.unwrap();
    tokio::time::timeout(SOON, bob.catalogue().ready())
        .await
        .unwrap();
    tokio::time::timeout(SOON, async {
        loop {
            if carol.node().known_addresses().address_of(bob_id).is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("carol must find out where the restarted bob is");

    // And now the only other copy in the group goes away.
    alice.shutdown().await;

    let carols_dest = dest(dir.path(), "carol");
    let downloaded = tokio::time::timeout(SOON, async {
        loop {
            // Retried rather than asserted first time: carol may reach for
            // bob before her endpoint has finished learning how, and a fetch
            // that finds nobody fails rather than waiting. What is being
            // pinned is that it comes through, not how soon.
            match api(&carol, &carol_key)
                .call(
                    "library.download",
                    Some(json!({ "item_id": item, "dest": &carols_dest })),
                )
                .await
            {
                Ok(answer) => return answer,
                Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
    })
    .await
    .expect("carol must get the file from the node that downloaded it, with the adder gone");

    assert_eq!(
        downloaded["files"].as_array().unwrap()[0]["fetched"],
        json!(true)
    );
    assert_eq!(
        std::fs::read(carols_dest.join("dune.epub")).unwrap(),
        bytes,
        "bob served what bob downloaded"
    );

    carol.shutdown().await;
    bob.shutdown().await;
}

/// A node downloading an item it added itself asks nobody, and it works in a
/// group with nobody to ask.
///
/// **The case the naive provider list cannot cover.** `library.download`
/// offers every member but this one, and in a group of one that list is
/// empty — a fetch against it has no chance of succeeding. So the local
/// store is asked first, and this is what says so: a solo node exporting its
/// own file never reaches the network at all.
///
/// **Mutation check:** dropping the `Blobs::has` guard and always fetching
/// turns this red — `NetError::Fetch`, from a download offered no providers.
#[tokio::test]
async fn downloading_something_this_node_already_has_asks_nobody() {
    let (dir, runtime, key) = a_solo_node().await;
    let item = add_an_ebook(
        &api(&runtime, &key),
        &dir.path().join("dune.epub"),
        b"the same bytes, still here",
        "Dune",
    )
    .await;
    until_projected(&runtime, item, 1, "the solo node").await;

    let dest = dest(dir.path(), "me");
    let downloaded = api(&runtime, &key)
        .call(
            "library.download",
            Some(json!({ "item_id": item, "dest": &dest })),
        )
        .await
        .unwrap();

    assert_eq!(
        downloaded["files"].as_array().unwrap()[0]["fetched"],
        json!(false),
        "nothing was fetched: the bytes were already in this node's own store"
    );
    assert_eq!(
        std::fs::read(dest.join("dune.epub")).unwrap(),
        b"the same bytes, still here"
    );

    runtime.shutdown().await;
}

/// A file already sitting where the download would land is refused, and
/// refused without touching it.
///
/// The exported file is the operator's — they may have edited or replaced it
/// — so a download is not a reason to write over one. Deleting it is an
/// instruction; overwriting it would be a guess.
#[tokio::test]
async fn downloading_over_a_file_that_is_already_there_is_refused() {
    let (dir, runtime, key) = a_solo_node().await;
    let item = add_an_ebook(
        &api(&runtime, &key),
        &dir.path().join("dune.epub"),
        b"what the group holds",
        "Dune",
    )
    .await;
    until_projected(&runtime, item, 1, "the solo node").await;

    let dest = dest(dir.path(), "me");
    let in_the_way = dest.join("dune.epub");
    std::fs::write(&in_the_way, b"something of mine").unwrap();

    let error = api(&runtime, &key)
        .call(
            "library.download",
            Some(json!({ "item_id": item, "dest": &dest })),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already exists"), "{error}");
    assert_eq!(
        std::fs::read(&in_the_way).unwrap(),
        b"something of mine",
        "and the file that was there is untouched"
    );

    runtime.shutdown().await;
}

/// `file` takes one of an item's files and leaves the rest alone.
///
/// Also the reason it is a content hash rather than §7.1's `file_index`: the
/// hash is what the item's own record is keyed by, and what `library.item`
/// prints. See `Api::download`.
#[tokio::test]
async fn downloading_one_file_of_an_item_takes_only_that_file() {
    let (dir, runtime, key) = a_solo_node().await;
    let first = dir.path().join("chapter-1.mp3");
    std::fs::write(&first, b"chapter one").unwrap();
    let second = dir.path().join("chapter-2.mp3");
    std::fs::write(&second, b"chapter two").unwrap();

    let added = api(&runtime, &key)
        .call(
            "library.add",
            Some(json!({
                "kind": "audiobook",
                "files": [&first, &second],
                "title": "Dune",
            })),
        )
        .await
        .unwrap();
    let item: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    until_projected(&runtime, item, 2, "the solo node").await;

    let files = runtime
        .store()
        .item(item)
        .await
        .unwrap()
        .unwrap()
        .item
        .files;
    let wanted = *files
        .iter()
        .find(|(_, record)| record.filename == "chapter-2.mp3")
        .expect("both files are in the item")
        .0;

    let dest = dest(dir.path(), "me");
    let downloaded = api(&runtime, &key)
        .call(
            "library.download",
            Some(json!({ "item_id": item, "dest": &dest, "file": wanted })),
        )
        .await
        .unwrap();

    assert_eq!(downloaded["files"].as_array().unwrap().len(), 1);
    assert_eq!(
        std::fs::read(dest.join("chapter-2.mp3")).unwrap(),
        b"chapter two"
    );
    assert!(
        !dest.join("chapter-1.mp3").exists(),
        "the file that was not asked for is not here"
    );

    // A hash the item does not have is the caller's mistake, said as one.
    let error = api(&runtime, &key)
        .call(
            "library.download",
            Some(json!({
                "item_id": item,
                "dest": &dest,
                "file": ContentHash::from_bytes([0xee; 32]),
            })),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("has no file"), "{error}");

    runtime.shutdown().await;
}

/// A destination that is not a directory is refused before anything is
/// fetched, so a mistyped path is not paid for with a transfer first.
#[tokio::test]
async fn downloading_into_something_that_is_not_a_directory_is_refused() {
    let (dir, runtime, key) = a_solo_node().await;
    let item = add_an_ebook(
        &api(&runtime, &key),
        &dir.path().join("dune.epub"),
        b"never fetched",
        "Dune",
    )
    .await;
    until_projected(&runtime, item, 1, "the solo node").await;

    let error = api(&runtime, &key)
        .call(
            "library.download",
            Some(json!({ "item_id": item, "dest": dir.path().join("no-such-place") })),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not a directory"), "{error}");

    runtime.shutdown().await;
}

/// One node, founded alone: enough for everything about this method except
/// who it asks.
async fn a_solo_node() -> (TempDir, Runtime, SecretKey) {
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let runtime = Runtime::start(&key, &config(&[id]), &DataDir::new(dir.path().join("solo")))
        .await
        .unwrap();
    runtime
        .node()
        .init_group(vec![(record(id, "solo"), bound(&runtime))], &key)
        .await
        .unwrap();
    tokio::time::timeout(SOON, runtime.catalogue().ready())
        .await
        .unwrap();
    (dir, runtime, key)
}

/// Two files of one item that share a basename cannot both be written to one
/// directory, and the download says so rather than writing one over the
/// other.
///
/// **Reachable by ordinary use, not an edge case.** `library.add` takes a
/// file's name from its path, so `disc1/track01.mp3 disc2/track01.mp3` is an
/// item with two distinct hashes and one filename — which is why
/// `FileRecord` carries `disc` and `seq` at all. Without this check both
/// exports resolve to the same path, the second overwrites the first, and
/// the answer reports two files at one location.
#[tokio::test]
async fn downloading_an_item_whose_files_share_a_name_is_refused() {
    let (dir, runtime, key) = a_solo_node().await;
    let discs = dir.path().join("discs");
    std::fs::create_dir_all(discs.join("disc1")).unwrap();
    std::fs::create_dir_all(discs.join("disc2")).unwrap();
    let first = discs.join("disc1/track01.mp3");
    std::fs::write(&first, b"disc one, track one").unwrap();
    let second = discs.join("disc2/track01.mp3");
    std::fs::write(&second, b"disc two, track one").unwrap();

    let added = api(&runtime, &key)
        .call(
            "library.add",
            Some(json!({
                "kind": "audiobook",
                "files": [&first, &second],
                "title": "Dune",
            })),
        )
        .await
        .unwrap();
    let item: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    until_projected(&runtime, item, 2, "the solo node").await;

    let dest = dest(dir.path(), "me");
    let error = api(&runtime, &key)
        .call(
            "library.download",
            Some(json!({ "item_id": item, "dest": &dest })),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("track01.mp3"), "{error}");
    assert!(
        std::fs::read_dir(&dest).unwrap().next().is_none(),
        "refused before anything was written"
    );

    // And the way through: one at a time, which is what the refusal points
    // at. Named by hash, so which of the two is unambiguous even though
    // their filenames are not.
    let files = runtime
        .store()
        .item(item)
        .await
        .unwrap()
        .unwrap()
        .item
        .files;
    let one = *files.keys().next().unwrap();
    api(&runtime, &key)
        .call(
            "library.download",
            Some(json!({ "item_id": item, "dest": &dest, "file": one })),
        )
        .await
        .unwrap();
    assert!(dest.join("track01.mp3").exists());

    runtime.shutdown().await;
}
