//! Availability in the API (phase 4's 4b-5): what a hit and `library.item`
//! say about who has an item, between two whole runtimes.

// Two whole runtimes, a founding election and real heartbeats: seconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{num::NonZeroU32, path::Path, sync::Arc, time::Duration};

use distlib::Runtime;
use distlib_api::Api;
use distlib_core::{DataDir, Event, ItemId, MemberId, NetConfig, TaskId};
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::time::Instant;

mod common;
use common::{bound, config, init_logging, record};

/// Long enough for two in-process nodes to elect, replicate and reconcile.
const SOON: Duration = Duration::from_secs(30);

/// How long a beat every second is believed for: three of them.
const TTL: Duration = Duration::from_secs(3);

/// An `Api` wired to `runtime`'s own pieces, as `library.rs` has it.
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
        availability: runtime.availability().clone(),
        tasks: runtime.tasks().clone(),
        downloads: runtime.downloads().to_path_buf(),
        uploads: runtime.uploads().clone(),
    }
}

/// A group founded by `keys`, each a whole runtime beating every second.
async fn a_group<const N: usize>(dir: &Path, keys: [&SecretKey; N]) -> [Runtime; N] {
    let ids = keys.map(|key| MemberId::from(key.public()));
    let mut group_config = config(&ids);
    group_config.availability.beat_interval_secs = NonZeroU32::MIN;
    let mut runtimes = Vec::with_capacity(N);
    for (n, key) in keys.iter().enumerate() {
        let data = DataDir::new(dir.join(format!("node-{n}")));
        runtimes.push(Runtime::start(key, &group_config, &data).await.unwrap());
    }
    let founders = ids
        .iter()
        .zip(&runtimes)
        .enumerate()
        .map(|(n, (id, runtime))| (record(*id, &format!("node-{n}")), bound(runtime)))
        .collect();
    runtimes[0]
        .node()
        .init_group(founders, keys[0])
        .await
        .unwrap();
    for runtime in &runtimes {
        runtime.catalogue().ready().await;
    }
    runtimes
        .try_into()
        .unwrap_or_else(|_| unreachable!("one runtime per key"))
}

/// Adds `files` as one ebook through `api`, answering with the item's id.
async fn add_an_ebook(api: &Api, dir: &Path, files: &[(&str, &[u8])]) -> ItemId {
    let paths: Vec<_> = files
        .iter()
        .map(|(name, bytes)| {
            let path = dir.join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect();
    let added = api
        .call(
            "library.add",
            Some(json!({ "kind": "ebook", "files": paths, "title": "Dune" })),
        )
        .await
        .unwrap();
    serde_json::from_value(added["item_id"].clone()).unwrap()
}

/// `item_id`'s hit in `library.list`, once it is one that `wanted` accepts.
async fn hit_on(api: &Api, item_id: ItemId, wanted: impl Fn(&Value) -> bool) -> Value {
    let id = json!(item_id);
    tokio::time::timeout(SOON, async {
        loop {
            let page = api.call("library.list", Some(json!({}))).await.unwrap();
            let hit = page["results"]
                .as_array()
                .and_then(|hits| hits.iter().find(|hit| hit["item_id"] == id));
            if let Some(hit) = hit.filter(|hit| wanted(hit)) {
                return hit.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the hit never looked as wanted")
}

/// **§9's first half: stop the provider, and `providers` drops within the
/// TTL** — while nothing about the item changes: the same item, the same ids
/// in the catalogue, the log where it was. Availability is never replicated.
#[tokio::test]
async fn an_items_providers_drop_when_its_holder_stops() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (alice_key, bob_key) = (SecretKey::generate(), SecretKey::generate());
    let bob_id = MemberId::from(bob_key.public());
    let [alice, bob] = a_group(dir.path(), [&alice_key, &bob_key]).await;
    let alice_api = api(&alice, &alice_key);
    let bob_api = api(&bob, &bob_key);
    let item_id = add_an_ebook(&bob_api, dir.path(), &[("dune.epub", b"a desert planet")]).await;

    hit_on(&bob_api, item_id, |hit| hit["held"] == true).await;
    let hit = hit_on(&alice_api, item_id, |hit| hit["providers"] == 1).await;
    assert_eq!(hit["held"], false, "alice has not downloaded it");
    let item = alice_api
        .call("library.item", Some(json!({ "item_id": item_id })))
        .await
        .unwrap();
    assert_eq!(
        item["availability"],
        json!({ "held": false, "providers": 1, "holders": [bob_id], "unknown": [] })
    );
    let ids = alice.catalogue().item_ids().await.unwrap();
    let record = alice.catalogue().item(item_id).await.unwrap();
    let log = alice.node().membership().changed_at();

    // Stopped without a word, as a crash would: alice has only the TTL to go
    // by. A clean stop says goodbye and is offline at once.
    bob.availability().shutdown();
    let stopped = Instant::now();
    hit_on(&alice_api, item_id, |hit| hit["providers"] == 0).await;
    assert!(
        stopped.elapsed() <= TTL + Duration::from_secs(1),
        "providers dropped {:?} after bob stopped, past his {TTL:?} TTL",
        stopped.elapsed()
    );

    assert_eq!(alice.catalogue().item_ids().await.unwrap(), ids);
    assert_eq!(alice.catalogue().item(item_id).await.unwrap(), record);
    assert_eq!(alice.node().membership().changed_at(), log);

    alice.shutdown().await;
    bob.shutdown().await;
}

/// **§9's other half: a download with an offline provider does not wait out
/// a dial timeout on it.** Carol is a member and has stopped; bob is online
/// and holds the item. Every file is asked of bob first, so carol — whose
/// dial would take the downloader's whole one-second connect timeout — is
/// never tried.
///
/// Four files, each fetched on its own: with every file's providers shuffled,
/// as they were before, one of them would wait on carol fifteen times in
/// sixteen.
#[tokio::test]
async fn a_download_does_not_wait_on_an_offline_member() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let keys = [(); 3].map(|()| SecretKey::generate());
    let bob_id = MemberId::from(keys[1].public());
    let [alice, bob, carol] = a_group(dir.path(), [&keys[0], &keys[1], &keys[2]]).await;
    let alice_api = api(&alice, &keys[0]);
    let chapters: [(&str, &[u8]); 4] = [
        ("one.epub", b"chapter one"),
        ("two.epub", b"chapter two"),
        ("three.epub", b"chapter three"),
        ("four.epub", b"chapter four"),
    ];
    let item_id = add_an_ebook(&api(&bob, &keys[1]), dir.path(), &chapters).await;

    carol.shutdown().await;
    let mut online = alice.availability().online();
    tokio::time::timeout(SOON, online.wait_for(|online| online.iter().eq([&bob_id])))
        .await
        .expect("alice sees bob online, and carol gone")
        .unwrap();
    hit_on(&alice_api, item_id, |hit| hit["providers"] == 1).await;

    let dest = dir.path().join("alice-got");
    std::fs::create_dir(&dest).unwrap();
    let mut watching = alice.events().subscribe();
    let asked = Instant::now();
    let started = alice_api
        .call(
            "library.download",
            Some(json!({ "item_id": item_id, "dest": dest })),
        )
        .await
        .unwrap();
    let task: TaskId = serde_json::from_value(started["task_id"].clone()).unwrap();
    let ended = tokio::time::timeout(SOON, async {
        loop {
            match watching.recv().await.unwrap() {
                Event::DownloadFinished { task_id, .. } if task_id == task => return true,
                Event::DownloadFailed { task_id, .. } if task_id == task => return false,
                _ => {}
            }
        }
    })
    .await
    .expect("the download ends");
    assert!(ended, "the download finished");
    assert!(
        asked.elapsed() < Duration::from_secs(1),
        "took {:?}: a dial to carol was waited out",
        asked.elapsed()
    );

    alice.shutdown().await;
    bob.shutdown().await;
}
