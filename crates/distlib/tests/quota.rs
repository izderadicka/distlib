//! The quota rule (5b-5a, D8): what a member added requires it to pledge,
//! warned below that and refused below half of it, and never a pledge below
//! the group's minimum; and nothing downloaded below that minimum (5b-5b).

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
use common::{api, bound, config, download, found, until_projected};

const SOON: Duration = Duration::from_secs(30);

/// The settings these tests found with: `replicas` 3, a minimum pledge of 10
/// bytes.
fn settings() -> GroupSettings {
    GroupSettings {
        min_pledge_bytes: 10,
        ..GroupSettings::default()
    }
}

/// A group of one, its founder pledging `pledge_bytes`.
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
    runtime
        .node()
        .init_group(vec![(founder, bound(&runtime))], settings(), &key)
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
    assert!(
        refused.contains("a pledge of 220 bytes would be enough"),
        "{refused}"
    );
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

/// 5b-5b's acceptance: a member below the minimum pledge syncs the catalogue
/// but is refused a download, and refused the content by the member serving
/// it even when it skips its own check; pledging the minimum lets both
/// through.
#[tokio::test]
async fn a_member_below_the_minimum_pledge_browses_but_takes_no_content() {
    let dir = TempDir::new().unwrap();
    let keys = [SecretKey::generate(), SecretKey::generate()];
    let ids = keys.each_ref().map(|key| MemberId::from(key.public()));
    let mut runtimes = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let data_dir = DataDir::new(dir.path().join(format!("node-{index}")));
        runtimes.push(Runtime::start(key, &config(&ids), &data_dir).await.unwrap());
    }
    found(
        &runtimes,
        &keys[0],
        &[(ids[0], 100), (ids[1], 0)],
        &settings(),
    )
    .await;
    let [founder, member] = [0, 1].map(|index| api(&runtimes[index], &keys[index]));
    runtimes[0].catalogue().ready().await;
    let added = add_a_file(&founder, dir.path(), 1, 20).await.unwrap();
    let item = serde_json::from_value(added["item_id"].clone()).unwrap();
    until_projected(&runtimes[0], item, 1, "the founder").await;
    let stored = runtimes[0].store().item(item).await.unwrap().unwrap();
    let file = *stored.item.files.keys().next().unwrap();

    until_projected(&runtimes[1], item, 1, "the member at pledge 0").await;
    let refused = member
        .call("library.download", Some(json!({ "item_id": item })))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        refused.contains("a pledge of at least 10 bytes"),
        "{refused}"
    );
    let blobs = runtimes[1].blobs();
    assert!(blobs.fetch(file, vec![ids[0]]).await.is_err());

    pledge(&member, 10).await.unwrap();
    tokio::time::timeout(SOON, async {
        while !runtimes[0].node().membership().pledges_the_minimum(&ids[1]) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the founder learns of the pledge");
    blobs.fetch(file, vec![ids[0]]).await.unwrap();
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&member, item, &dest).await;

    for runtime in runtimes {
        runtime.shutdown().await;
    }
}
