//! The storage limit (5b-3a): what this node keeps, counted against
//! `[storage] limit_bytes`, and what may not grow past it.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{num::NonZeroU64, path::Path, time::Duration};

use distlib::Runtime;
use distlib_api::Api;
use distlib_core::{Config, DataDir, MemberId};
use distlib_custody::Storage;
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;
use common::{api, bound, collecting, config, record};

/// A group of one, founded, with its catalogue open and `limit` bytes of
/// storage.
async fn founded_alone(dir: &Path, limit: u64) -> (Runtime, Api) {
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let mut solo_config = config(&[id]);
    solo_config.storage.limit_bytes = NonZeroU64::new(limit);
    founded_alone_with(dir, &key, &solo_config).await
}

/// [`founded_alone`] as `solo_config` says, by `key`.
async fn founded_alone_with(dir: &Path, key: &SecretKey, solo_config: &Config) -> (Runtime, Api) {
    let id = MemberId::from(key.public());
    let runtime = Runtime::start(key, solo_config, &DataDir::new(dir.join("solo")))
        .await
        .unwrap();
    runtime
        .node()
        .init_group(
            vec![(record(id, "solo"), bound(&runtime))],
            Default::default(),
            key,
        )
        .await
        .unwrap();
    runtime.catalogue().ready().await;
    let api = api(&runtime, key);
    (runtime, api)
}

/// Adds a file of `size` bytes, its bytes told apart by `seed`.
async fn add_a_file(api: &Api, dir: &Path, seed: u8, size: usize) -> Result<Value, String> {
    let path = dir.join(format!("book-{seed}.epub"));
    std::fs::write(&path, vec![seed; size]).unwrap();
    api.call(
        "library.add",
        Some(json!({ "kind": "ebook", "files": [path], "title": format!("Book {seed}") })),
    )
    .await
    .map_err(|error| error.to_string())
}

async fn storage(api: &Api) -> Value {
    api.call("node.status", None).await.unwrap()["storage"].clone()
}

/// What is added counts as own; an add that would take own past the limit is
/// refused, and one that adds nothing new goes through even over it.
#[tokio::test]
async fn an_add_that_would_grow_own_past_the_limit_is_refused() {
    let dir = TempDir::new().unwrap();
    let (runtime, api) = founded_alone(dir.path(), 1_000).await;

    add_a_file(&api, dir.path(), 1, 600).await.unwrap();
    assert_eq!(
        storage(&api).await,
        json!({
            "limit_bytes": 1_000,
            "own_bytes": 600,
            "custody_bytes": 0,
            "cache_bytes": 0,
            "mandatory_over_limit": false,
            "last_collection_at": null,
        })
    );

    let refused = add_a_file(&api, dir.path(), 2, 600).await.unwrap_err();
    assert!(refused.contains("storage limit of 1000 bytes"), "{refused}");
    assert_eq!(storage(&api).await["own_bytes"], 600, "nothing kept");

    // The limit lowered under what is kept already: the same file again
    // grows nothing, so it goes through, and the status says it is over.
    let lowered = Api {
        storage: Storage::new(
            NonZeroU64::new(500),
            runtime.blobs().clone(),
            runtime.store().clone(),
            runtime.catalogue().holdings().clone(),
        ),
        ..api
    };
    let again = add_a_file(&lowered, dir.path(), 1, 600).await.unwrap();
    assert_eq!(again["created"], false);
    assert_eq!(storage(&lowered).await["mandatory_over_limit"], true);

    runtime.shutdown().await;
}

/// A pledge has to leave room under the limit for own items and the cache.
#[tokio::test]
async fn a_pledge_not_below_the_storage_limit_is_refused() {
    let dir = TempDir::new().unwrap();
    let (runtime, api) = founded_alone(dir.path(), 1_000).await;

    let refused = api
        .call("group.pledge_set", Some(json!({ "bytes": 1_000 })))
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("storage limit of 1000 bytes"), "{refused}");
    api.call("group.pledge_set", Some(json!({ "bytes": 999 })))
        .await
        .unwrap();

    runtime.shutdown().await;
}

/// When the last collection pass began is in the status, once one has.
#[tokio::test]
async fn the_last_collection_pass_is_reported() {
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let (runtime, api) = founded_alone_with(
        dir.path(),
        &key,
        &collecting(config(&[key.public().into()])),
    )
    .await;

    tokio::time::timeout(Duration::from_secs(10), async {
        while storage(&api).await["last_collection_at"].is_null() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a pass every second began");

    runtime.shutdown().await;
}
