//! 5b-1's acceptance: five nodes agree on every item's custodians, and
//! custody follows a pledge change only once the membership has settled. And a
//! read model rebuilt without announcing anything is followed all the same.

// Five whole runtimes and a founding election: seconds, not milliseconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{collections::BTreeMap, num::NonZeroU8, time::Duration};

use distlib::Runtime;
use distlib_api::Api;
use distlib_consensus::MemberRecord;
use distlib_core::{
    ContentHash, CustodyGroup, CustodyItem, DataDir, FileRecord, FileRole, GroupSettings, Item,
    ItemId, MemberId,
};
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;
use common::{api, bound, config, init_logging, until_released};

const SOON: Duration = Duration::from_secs(60);
const GB: u64 = 1_000_000_000;
const SETTLE: Duration = Duration::from_secs(5);

/// One item of one file, `size_bytes` long.
fn an_item(seed: u8, size_bytes: u64) -> Item {
    Item {
        title: Some(format!("item {seed}")),
        files: BTreeMap::from([(
            ContentHash::from_bytes([seed; 32]),
            FileRecord {
                role: FileRole::Content,
                format: "epub".to_owned(),
                size: size_bytes,
                filename: format!("{seed}.epub"),
                seq: None,
                disc: None,
                title: None,
                duration: None,
            },
        )]),
        ..Item::new(ItemId::from_bytes([seed; 32]))
    }
}

/// What every node should say: each item's custodians under `pledges`.
fn expected(
    settings: &GroupSettings,
    pledges: &[(MemberId, u64)],
    items: &[CustodyItem],
) -> Vec<Vec<MemberId>> {
    let group = CustodyGroup::new(
        settings.replicas.get(),
        settings.weight_rule(),
        pledges.iter().copied(),
    );
    items.iter().map(|item| group.custodians(item)).collect()
}

/// What one node says: each item's custodians, and its own custody count.
async fn said_by(api: &Api, items: &[CustodyItem]) -> (Vec<Value>, Value) {
    let mut custodians = Vec::new();
    for item in items {
        let record = api
            .call("library.item", Some(json!({ "item_id": item.item })))
            .await
            .unwrap_or_default();
        custodians.push(record["custodians"].clone());
    }
    let status = api.call("node.status", None).await.unwrap();
    (custodians, status["custody"]["items"].clone())
}

/// Waits until every node says what `expected` says.
async fn until_all_say(
    apis: &[(MemberId, Api)],
    items: &[CustodyItem],
    expected: &[Vec<MemberId>],
    when: &str,
) {
    let mut last = Vec::new();
    let agreed = tokio::time::timeout(SOON, async {
        loop {
            last.clear();
            let mut all_agree = true;
            for (member, api) in apis {
                let (custodians, count) = said_by(api, items).await;
                let mine = expected.iter().filter(|list| list.contains(member)).count();
                all_agree &= custodians
                    == expected.iter().map(|list| json!(list)).collect::<Vec<_>>()
                    && count == json!(mine);
                last.push((custodians, count));
            }
            if all_agree {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(
        agreed.is_ok(),
        "{when}: the nodes never agreed within {SOON:?}\n  expected {expected:#?}\n  said {last:#?}"
    );
}

/// Founds the group of `runtimes`, each member pledging what `pledges` says.
async fn found(
    runtimes: &[Runtime],
    founder: &SecretKey,
    pledges: &[(MemberId, u64)],
    settings: &GroupSettings,
) {
    let founders = runtimes
        .iter()
        .zip(pledges)
        .map(|(runtime, (member_id, pledge_bytes))| {
            (
                MemberRecord {
                    member_id: *member_id,
                    display_name: member_id.to_string(),
                    pledge_bytes: *pledge_bytes,
                },
                bound(runtime),
            )
        })
        .collect();
    runtimes[0]
        .node()
        .init_group(founders, settings.clone(), founder)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn five_nodes_agree_on_custodians_and_follow_a_settled_pledge_change() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let keys: Vec<SecretKey> = (0..5).map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys
        .iter()
        .map(|key| MemberId::from(key.public()))
        .collect();
    let group_config = config(&ids);
    let mut runtimes = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let data_dir = DataDir::new(dir.path().join(index.to_string()));
        runtimes.push(Runtime::start(key, &group_config, &data_dir).await.unwrap());
    }

    let settings = GroupSettings {
        replicas: NonZeroU8::new(3).unwrap(),
        settle_secs: SETTLE.as_secs().try_into().unwrap(),
        max_settle_secs: 60,
        ..GroupSettings::default()
    };
    let mut pledges: Vec<(MemberId, u64)> = ids
        .iter()
        .zip(1..)
        .map(|(id, hundreds)| (*id, hundreds * 100 * GB))
        .collect();
    found(&runtimes, &keys[0], &pledges, &settings).await;

    // Twelve items added by the first member and four by the second.
    let mut items = Vec::new();
    for seed in 1..=16_u8 {
        let creator = usize::from(seed > 12);
        let item = an_item(seed, u64::from(seed) * GB);
        runtimes[creator].catalogue().ready().await;
        runtimes[creator].catalogue().write(&item).await.unwrap();
        items.push(CustodyItem {
            item: item.id,
            creators: vec![ids[creator]],
            size_bytes: u64::from(seed) * GB,
        });
    }

    let apis: Vec<(MemberId, Api)> = ids
        .iter()
        .zip(runtimes.iter().zip(&keys))
        .map(|(id, (runtime, key))| (*id, api(runtime, key)))
        .collect();
    let before = expected(&settings, &pledges, &items);
    until_all_say(&apis, &items, &before, "at founding").await;

    // The lightest member pledges the most it can weigh. It moves custody,
    // but not before the membership has gone `SETTLE` without a change.
    let (lightest, lightest_api) = &apis[0];
    lightest_api
        .call(
            "group.pledge_set",
            Some(json!({ "bytes": settings.cap_bytes })),
        )
        .await
        .unwrap();
    pledges[0] = (*lightest, settings.cap_bytes);
    let after = expected(&settings, &pledges, &items);
    assert_ne!(before, after, "the pledge change should move some custody");
    let (still, _) = said_by(lightest_api, &items).await;
    assert_eq!(
        still,
        before.iter().map(|list| json!(list)).collect::<Vec<_>>(),
        "custody moved before the membership settled"
    );
    until_all_say(&apis, &items, &after, "after the pledge change settled").await;

    for runtime in &runtimes {
        runtime.shutdown().await;
    }
}

/// Waits until `api`'s node says it is custodian of `items` items.
async fn until_custodian_of(api: &Api, items: u64, when: &str) {
    let mut last = Value::Null;
    let reached = tokio::time::timeout(SOON, async {
        loop {
            last = api.call("node.status", None).await.unwrap()["custody"].clone();
            if last["items"] == json!(items) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "{when}: custody never reached {items} items within {SOON:?}: {last}"
    );
}

/// A replay rewrites the read model without announcing an item, so custody
/// has to read every item again once it is done.
#[tokio::test(flavor = "multi_thread")]
async fn custody_follows_a_read_model_rebuilt_at_a_start() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let keys: Vec<SecretKey> = (0..2).map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys
        .iter()
        .map(|key| MemberId::from(key.public()))
        .collect();
    let group_config = config(&ids);
    let bobs_dir = DataDir::new(dir.path().join("bob"));
    let runtimes = vec![
        Runtime::start(
            &keys[0],
            &group_config,
            &DataDir::new(dir.path().join("alice")),
        )
        .await
        .unwrap(),
        Runtime::start(&keys[1], &group_config, &bobs_dir)
            .await
            .unwrap(),
    ];
    // Two copies: alice's own, and bob's as the only other member.
    let settings = GroupSettings {
        replicas: NonZeroU8::new(2).unwrap(),
        ..GroupSettings::default()
    };
    let pledges = [(ids[0], 100 * GB), (ids[1], 100 * GB)];
    found(&runtimes, &keys[0], &pledges, &settings).await;
    let [alice, bob] = <[Runtime; 2]>::try_from(runtimes).ok().unwrap();

    alice.catalogue().ready().await;
    for seed in 1..=3 {
        alice.catalogue().write(&an_item(seed, GB)).await.unwrap();
    }
    until_custodian_of(&api(&bob, &keys[1]), 3, "before the restart").await;

    // Bob comes back without a read model: the replay rebuilds it, and
    // announces none of the items it writes.
    bob.shutdown().await;
    drop(bob);
    until_released(&bobs_dir).await;
    std::fs::remove_dir_all(bobs_dir.db_dir()).unwrap();
    let bob = Runtime::start(&keys[1], &group_config, &bobs_dir)
        .await
        .unwrap();
    until_custodian_of(&api(&bob, &keys[1]), 3, "after the restart").await;

    bob.shutdown().await;
    alice.shutdown().await;
}
