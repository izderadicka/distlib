//! 5c-1's acceptance (phase 5's D10): a custodian that holds its items shows
//! full coverage, not blamed for what is over its pledge; one whose pin
//! manager is stopped falls behind once its assignment is older than
//! `max_settle_secs`, and is not blamed before.

// Three whole runtimes, a founding election, settling and fetching: seconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    num::{NonZeroU8, NonZeroU32, NonZeroU64},
    path::Path,
    time::Duration,
};

use distlib::Runtime;
use distlib_core::{DataDir, GroupSettings, ItemId, MemberId};
use distlib_custody::MemberCoverage;
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;
use common::{api, config, found, init_logging, until_projected};

const SOON: Duration = Duration::from_secs(60);
const GB: u64 = 1_000_000_000;
/// The size of a book.
const BOOK: usize = 1_000_000;
/// How old an assignment must be before it counts.
const MAX_SETTLE_SECS: u32 = 8;

/// Two copies of everything, the creator's one of them; steps of a byte, so
/// a pledge of a megabyte and a half weighs something.
fn settings() -> GroupSettings {
    GroupSettings {
        replicas: NonZeroU8::new(2).unwrap(),
        weight_step_bytes: NonZeroU64::MIN,
        min_pledge_bytes: 0,
        settle_secs: 1,
        max_settle_secs: MAX_SETTLE_SECS,
        ..GroupSettings::default()
    }
}

struct Node {
    runtime: Runtime,
    key: SecretKey,
    id: MemberId,
}

impl Node {
    async fn call(&self, method: &str, params: Value) -> Value {
        api(&self.runtime, &self.key)
            .call(method, Some(params))
            .await
            .unwrap()
    }

    /// What this node's members list says of `member`'s coverage.
    async fn coverage_of(&self, member: MemberId) -> Option<MemberCoverage> {
        let members = self.call("group.members", json!({})).await;
        let entry = members["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["member"] == json!(member))
            .unwrap()
            .clone();
        let coverage = &entry["coverage"];
        (!coverage.is_null()).then(|| MemberCoverage {
            expected: coverage["expected"].as_u64().unwrap() as usize,
            announced: coverage["announced"].as_u64().unwrap() as usize,
        })
    }

    /// Waits until this node says `member`'s coverage is `expected` of which
    /// `announced`.
    async fn until_coverage(&self, member: MemberId, expected: usize, announced: usize) {
        let wanted = Some(MemberCoverage {
            expected,
            announced,
        });
        tokio::time::timeout(SOON, async {
            while self.coverage_of(member).await != wanted {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{} never saw {}'s coverage at {announced} of {expected}",
                self.id.fmt_short(),
                member.fmt_short()
            )
        });
    }
}

/// Three nodes beating every second, so coverage is worked out every second;
/// founded with `pledges`, the founder first.
async fn three_nodes(dir: &Path, pledges: [u64; 3]) -> [Node; 3] {
    init_logging();
    let keys: Vec<SecretKey> = pledges.iter().map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys.iter().map(|key| key.public().into()).collect();
    let mut nodes = Vec::new();
    for (index, key) in keys.into_iter().enumerate() {
        let mut config = config(&ids);
        config.availability.beat_interval_secs = NonZeroU32::MIN;
        let dir = DataDir::new(dir.join(index.to_string()));
        let runtime = Runtime::start(&key, &config, &dir).await.unwrap();
        nodes.push(Node {
            runtime,
            id: key.public().into(),
            key,
        });
    }
    let runtimes: Vec<&Runtime> = nodes.iter().map(|node| &node.runtime).collect();
    let pledges: Vec<(MemberId, u64)> = ids.into_iter().zip(pledges).collect();
    found(&runtimes, &nodes[0].key, &pledges, &settings()).await;
    nodes.try_into().ok().unwrap()
}

/// Adds a book of about a megabyte through `node`.
async fn add_a_book(node: &Node, dir: &Path, title: &str) -> ItemId {
    node.runtime.catalogue().ready().await;
    let book = dir.join(format!("{title}.epub"));
    let line = format!("{title}\n");
    std::fs::write(&book, line.repeat(BOOK / line.len())).unwrap();
    let added = node
        .call(
            "library.add",
            json!({ "kind": "ebook", "files": [&book], "title": title }),
        )
        .await;
    serde_json::from_value(added["item_id"].clone()).unwrap()
}

/// The founder adds two books; the second member, the only other with a
/// weight, pledges room for one, so it is custodian of both and keeps one.
/// The founder counts it at one of one: the book over its pledge is not
/// blamed. The third member is custodian of nothing, and counts nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_holding_what_its_pledge_allows_shows_full_coverage() {
    let dir = TempDir::new().unwrap();
    let pledge = BOOK as u64 * 3 / 2;
    let [founder, second, third] = three_nodes(dir.path(), [100 * GB, pledge, 0]).await;
    add_a_book(&founder, dir.path(), "Dune").await;
    let emma = add_a_book(&founder, dir.path(), "Emma").await;
    until_projected(&second.runtime, emma, 1, "the second member").await;

    founder.until_coverage(second.id, 1, 1).await;
    founder.until_coverage(third.id, 0, 0).await;

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}

/// Both others are stopped from keeping anything; the founder adds a book,
/// and one of them is its custodian. Until the assignment is older than
/// `max_settle_secs` the founder counts nothing against it; after, one it
/// does not announce.
#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_that_never_fetches_falls_behind_once_its_assignment_is_old() {
    let dir = TempDir::new().unwrap();
    let [founder, second, third] = three_nodes(dir.path(), [100 * GB; 3]).await;
    second.runtime.pins().shutdown();
    third.runtime.pins().shutdown();
    let book = add_a_book(&founder, dir.path(), "Dune").await;
    let custodian = founder
        .call("library.item", json!({ "item_id": book }))
        .await["custodians"][0]
        .clone();
    let custodian: MemberId = serde_json::from_value(custodian).unwrap();

    // Judged already, and nothing yet counted against it.
    founder.until_coverage(custodian, 0, 0).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        founder.coverage_of(custodian).await,
        Some(MemberCoverage::default()),
        "blamed within max_settle_secs"
    );
    founder.until_coverage(custodian, 1, 0).await;

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}
