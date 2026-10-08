//! Availability in the API (phase 4's 4b-5): what a hit and `library.item`
//! say about who has an item, between two whole runtimes.

// Two whole runtimes, a founding election and real heartbeats: seconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use distlib::Runtime;
use distlib_api::Api;
use distlib_core::{DataDir, ItemId, MemberId, NetConfig};
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
    let alice_id = MemberId::from(alice_key.public());
    let bob_id = MemberId::from(bob_key.public());
    let mut group_config = config(&[alice_id, bob_id]);
    group_config.availability.beat_interval_secs = NonZeroU32::MIN;
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
    let alice_api = api(&alice, &alice_key);
    let bob_api = api(&bob, &bob_key);

    let book = dir.path().join("dune.epub");
    std::fs::write(&book, b"a desert planet").unwrap();
    let added = bob_api
        .call(
            "library.add",
            Some(json!({ "kind": "ebook", "files": [book], "title": "Dune" })),
        )
        .await
        .unwrap();
    let item_id: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();

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
