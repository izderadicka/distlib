//! The quota rule (5b-5a, D8): what a member added requires it to pledge,
//! warned below that and refused below half of it, and never a pledge below
//! the group's minimum.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{path::Path, time::Duration};

use distlib::Runtime;
use distlib_api::Api;
use distlib_consensus::MemberRecord;
use distlib_core::{DataDir, GroupSettings, MemberId};
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;
use common::{api, bound, config};

const SOON: Duration = Duration::from_secs(30);

/// A group of one at `replicas` 3 with a minimum pledge of 10 bytes, its
/// founder pledging `pledge_bytes`.
async fn founded_alone(dir: &Path, pledge_bytes: u64) -> (Runtime, Api) {
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let runtime = Runtime::start(&key, &config(&[id]), &DataDir::new(dir.join("solo")))
        .await
        .unwrap();
    let founder = MemberRecord {
        member_id: id,
        display_name: "solo".to_owned(),
        pledge_bytes,
    };
    let settings = GroupSettings {
        min_pledge_bytes: 10,
        ..GroupSettings::default()
    };
    runtime
        .node()
        .init_group(vec![(founder, bound(&runtime))], settings, &key)
        .await
        .unwrap();
    runtime.catalogue().ready().await;
    let api = api(&runtime, &key);
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

async fn pledge(api: &Api, bytes: u64) -> Result<Value, String> {
    api.call("group.pledge_set", Some(json!({ "bytes": bytes })))
        .await
        .map_err(|error| error.to_string())
}

/// The founder's pledge and requirement, as the members list has them.
async fn listed(api: &Api) -> (Value, Value) {
    let members = api.call("group.members", None).await.unwrap();
    let founder = &members["members"][0];
    (
        founder["pledge_bytes"].clone(),
        founder["required_pledge_bytes"].clone(),
    )
}

/// Waits until the members list requires `required` of the founder: until
/// what it added is in the read model.
async fn until_required(api: &Api, required: u64) {
    tokio::time::timeout(SOON, async {
        while listed(api).await.1 != json!(required) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never required {required} bytes"));
}

/// 5b-5's acceptance, in bytes: a pledge of 100 and 60 added is warned,
/// told 120; 110 added is refused, told 220; raising the pledge clears the
/// warning, and lowering it below half the requirement is refused, as is
/// one below the minimum.
#[tokio::test]
async fn what_a_member_added_is_warned_then_refused_until_its_pledge_covers_it() {
    let dir = TempDir::new().unwrap();
    let (runtime, api) = founded_alone(dir.path(), 100).await;
    assert_eq!(listed(&api).await, (json!(100), json!(10)));
    let refused = pledge(&api, 9).await.unwrap_err();
    assert!(refused.contains("minimum of 10 bytes"), "{refused}");

    let warned = add_a_file(&api, dir.path(), 1, 60).await.unwrap();
    assert_eq!(
        warned["under_pledged"],
        json!({ "pledge_bytes": 100, "required_pledge_bytes": 120 })
    );
    until_required(&api, 120).await;

    let refused = add_a_file(&api, dir.path(), 2, 50).await.unwrap_err();
    assert!(refused.contains("a pledge of 220 bytes"), "{refused}");
    assert_eq!(listed(&api).await.1, json!(120), "nothing added");

    let enough = pledge(&api, 120).await.unwrap();
    assert_eq!(enough["under_pledged"], Value::Null);
    assert_eq!(listed(&api).await, (json!(120), json!(120)));

    let refused = pledge(&api, 59).await.unwrap_err();
    assert!(refused.contains("a pledge of 120 bytes"), "{refused}");
    let warned = pledge(&api, 60).await.unwrap();
    assert_eq!(
        warned["under_pledged"],
        json!({ "pledge_bytes": 60, "required_pledge_bytes": 120 })
    );
    assert_eq!(listed(&api).await.0, json!(60));

    runtime.shutdown().await;
}

/// An add that writes no new file key adds nothing to the requirement, so it
/// is neither refused nor counted twice.
#[tokio::test]
async fn adding_the_same_files_again_requires_nothing_more() {
    let dir = TempDir::new().unwrap();
    let (runtime, api) = founded_alone(dir.path(), 100).await;

    add_a_file(&api, dir.path(), 1, 60).await.unwrap();
    until_required(&api, 120).await;
    let again = add_a_file(&api, dir.path(), 1, 60).await.unwrap();
    assert_eq!(again["created"], false);
    assert_eq!(
        again["under_pledged"],
        json!({ "pledge_bytes": 100, "required_pledge_bytes": 120 })
    );

    runtime.shutdown().await;
}
