//! 5b-2a's acceptance: a custodian keeps an item without anybody asking, and
//! lets go of it into the cache only once its copies elsewhere make up for
//! this one; an item already cached is kept without a fetch.

// Three whole runtimes, a founding election, settling and fetching: seconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{num::NonZeroU8, path::Path, time::Duration};

use distlib::Runtime;
use distlib_core::{
    ContentHash, CustodyGroup, CustodyItem, DataDir, GroupSettings, ItemId, KeepReason, MemberId,
};
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;
use common::{
    api, collecting, config, download, found, init_logging, until_a_pass_collects, until_projected,
    until_released,
};

const SOON: Duration = Duration::from_secs(60);
const GB: u64 = 1_000_000_000;

/// Two copies of everything, and a membership that settles in two seconds.
fn settings() -> GroupSettings {
    GroupSettings {
        replicas: NonZeroU8::new(2).unwrap(),
        settle_secs: 2,
        max_settle_secs: 60,
        ..GroupSettings::default()
    }
}

/// One of the test's nodes: every node is a core node, so two of three still
/// commit while one is stopped.
struct Node {
    runtime: Runtime,
    key: SecretKey,
    id: MemberId,
    dir: DataDir,
}

impl Node {
    async fn start(key: SecretKey, core: &[MemberId], dir: DataDir) -> Self {
        let runtime = Runtime::start(&key, &collecting(config(core)), &dir)
            .await
            .unwrap();
        Self {
            runtime,
            id: key.public().into(),
            key,
            dir,
        }
    }

    /// Stops it, and waits until it could start again.
    async fn stop(self) -> (SecretKey, DataDir) {
        self.runtime.shutdown().await;
        drop(self.runtime);
        until_released(&self.dir).await;
        (self.key, self.dir)
    }

    async fn call(&self, method: &str, params: Value) -> Value {
        api(&self.runtime, &self.key)
            .call(method, Some(params))
            .await
            .unwrap()
    }

    /// Whether it keeps `file` of `item` for `reason`, and has its bytes.
    async fn keeps(&self, reason: KeepReason, item: ItemId, file: ContentHash) -> bool {
        let blobs = self.runtime.blobs();
        blobs.kept(reason).await.unwrap().contains(&(item, file)) && blobs.has(file).await.unwrap()
    }

    /// Waits until it keeps `file` of `item` for `reason`, or no longer does.
    async fn until_keeps(&self, keeps: bool, reason: KeepReason, item: ItemId, file: ContentHash) {
        tokio::time::timeout(SOON, async {
            while self.keeps(reason, item, file).await != keeps {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{} never came to keep the book for {reason:?} = {keeps} within {SOON:?}",
                self.id.fmt_short()
            )
        });
    }

    /// Whether a `custody/` tag for `file` of `item` is set, bytes or not.
    async fn tagged_for_custody(&self, item: ItemId, file: ContentHash) -> bool {
        let kept = self
            .runtime
            .blobs()
            .kept(KeepReason::Custody)
            .await
            .unwrap();
        kept.contains(&(item, file))
    }

    /// Waits until a `custody/` tag for `file` of `item` is set, or is not.
    async fn until_tagged_for_custody(&self, tagged: bool, item: ItemId, file: ContentHash) {
        tokio::time::timeout(SOON, async {
            while self.tagged_for_custody(item, file).await != tagged {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{}'s custody tag never came to be {tagged} within {SOON:?}",
                self.id.fmt_short()
            )
        });
    }

    /// Waits until it sees `holder` online and holding `item`.
    async fn until_it_sees(&self, holder: MemberId, item: ItemId) {
        let availability = self.runtime.availability();
        tokio::time::timeout(SOON, async {
            while !(availability.online().borrow().contains(&holder)
                && availability.holders(&item).contains(&holder))
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{} never saw {} holding the book within {SOON:?}",
                self.id.fmt_short(),
                holder.fmt_short()
            )
        });
    }

    /// Waits until it says it is custodian of `items` items.
    async fn until_custodian_of(&self, items: u64) {
        tokio::time::timeout(SOON, async {
            while self.call("node.status", json!({})).await["custody"]["items"] != json!(items) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{} never became custodian of {items} items within {SOON:?}",
                self.id.fmt_short()
            )
        });
    }
}

/// Three nodes, collecting every second, founded with `pledges`.
async fn three_nodes(dir: &Path, pledges: [u64; 3]) -> (Node, Node, Node) {
    let keys: Vec<SecretKey> = (0..3).map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys.iter().map(|key| key.public().into()).collect();
    let mut nodes = Vec::new();
    for (index, key) in keys.into_iter().enumerate() {
        nodes.push(Node::start(key, &ids, DataDir::new(dir.join(index.to_string()))).await);
    }
    let runtimes: Vec<&Runtime> = nodes.iter().map(|node| &node.runtime).collect();
    let pledges: Vec<(MemberId, u64)> = ids.into_iter().zip(pledges).collect();
    found(&runtimes, &nodes[0].key, &pledges, &settings()).await;
    let third = nodes.pop().unwrap();
    let second = nodes.pop().unwrap();
    let first = nodes.pop().unwrap();
    (first, second, third)
}

/// Adds a book through `node`: its item and its one file.
async fn add_a_book(node: &Node, dir: &Path) -> (ItemId, ContentHash) {
    node.runtime.catalogue().ready().await;
    // Large enough to be a file in the store rather than a row in its
    // database, which is how media is kept.
    let book = dir.join("dune.epub");
    std::fs::write(&book, b"the spice must flow\n".repeat(50_000)).unwrap();
    let added = node
        .call(
            "library.add",
            json!({ "kind": "ebook", "files": [&book], "title": "Dune" }),
        )
        .await;
    let item: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    let stored = node.runtime.catalogue().item(item).await.unwrap().unwrap();
    (item, *stored.files.keys().next().unwrap())
}

/// **A custodian fetches unasked, and lets go only once the item has its
/// copies elsewhere.** The founder adds a book; of the two others, custody
/// ranks one first, and it fetches the book. It then pledges nothing while
/// the other is stopped: custody moves to the stopped one, and the first keeps
/// the book, since the founder's is then the only other copy, and still after
/// a restart. Once the other is back and holds it, the first lets it fall into
/// the cache, which a collection pass leaves alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_fetches_unasked_and_lets_go_only_with_copies_elsewhere() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB; 3]).await;
    let core = [founder.id, second.id, third.id];
    let (item, file) = add_a_book(&founder, dir.path()).await;

    let group = CustodyGroup::new(
        2,
        settings().weight_rule(),
        core.iter().map(|id| (*id, 100 * GB)),
    );
    let ranked_first = group.custodians(&CustodyItem {
        item,
        creators: vec![founder.id],
        size_bytes: 0,
    })[0];
    let (custodian, other) = if ranked_first == second.id {
        (second, third)
    } else {
        (third, second)
    };
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    // The founder's copy counted, so one copy elsewhere is what it waits on.
    custodian.until_it_sees(founder.id, item).await;

    // The other is stopped, then the custodian pledges nothing.
    let (others_key, others_dir) = other.stop().await;
    custodian
        .call("group.pledge_set", json!({ "bytes": 0 }))
        .await;
    custodian.until_custodian_of(0).await;
    // Time for the pin manager to act on it, had it been going to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        custodian.keeps(KeepReason::Custody, item, file).await,
        "let go of the book while the founder's was the only other copy"
    );
    // Restarted while it waits: only its tags say there is something to
    // let go of.
    let (custodians_key, custodians_dir) = custodian.stop().await;
    let custodian = Node::start(custodians_key, &core, custodians_dir).await;
    custodian.until_custodian_of(0).await;

    // The other is back and keeps the book: now it may go, into the cache.
    let other = Node::start(others_key, &core, others_dir).await;
    other
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian
        .until_keeps(false, KeepReason::Custody, item, file)
        .await;
    assert!(custodian.keeps(KeepReason::Cache, item, file).await);
    until_a_pass_collects(&custodian.runtime, dir.path(), "after-the-release").await;
    assert!(
        custodian.runtime.blobs().has(file).await.unwrap(),
        "the book was collected rather than cached"
    );

    for node in [founder, custodian, other] {
        node.stop().await;
    }
}

/// **An item already cached is kept without a fetch.** Nobody but the founder
/// weighs anything, so the book has no custodian; a second member downloads
/// it, and the founder stops, so nobody online could serve it. The second
/// member pledges, becomes its custodian, and keeps it.
#[tokio::test(flavor = "multi_thread")]
async fn an_item_already_cached_is_kept_without_a_fetch() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB, 0, 0]).await;
    let (item, file) = add_a_book(&founder, dir.path()).await;

    until_projected(&second.runtime, item, 1, "the second member").await;
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&second.runtime, &second.key), item, &dest).await;
    founder.stop().await;

    second
        .call("group.pledge_set", json!({ "bytes": 100 * GB }))
        .await;
    second.until_custodian_of(1).await;
    second
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;

    for node in [second, third] {
        node.stop().await;
    }
}

/// **A custodian whose fetch failed asks again when a holder comes back, and
/// its tags outlive a restart.** The founder adds a book nobody else weighs
/// enough to keep, and stops. The second member pledges, becomes its
/// custodian, and tags it, but cannot fetch it. Restarted, it pledges nothing
/// and lets go of the book at once: it never held it. It pledges again, and
/// once the founder is back it fetches the book.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_fetch_is_asked_again_and_tags_outlive_a_restart() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB, 0, 0]).await;
    let core = [founder.id, second.id, third.id];
    let (item, file) = add_a_book(&founder, dir.path()).await;
    until_projected(&second.runtime, item, 1, "the second member").await;
    let (founders_key, founders_dir) = founder.stop().await;

    second
        .call("group.pledge_set", json!({ "bytes": 100 * GB }))
        .await;
    second.until_tagged_for_custody(true, item, file).await;
    assert!(!second.runtime.blobs().has(file).await.unwrap());

    let (seconds_key, seconds_dir) = second.stop().await;
    let second = Node::start(seconds_key, &core, seconds_dir).await;
    second.call("group.pledge_set", json!({ "bytes": 0 })).await;
    second.until_custodian_of(0).await;
    second.until_tagged_for_custody(false, item, file).await;

    second
        .call("group.pledge_set", json!({ "bytes": 100 * GB }))
        .await;
    second.until_custodian_of(1).await;
    let founder = Node::start(founders_key, &core, founders_dir).await;
    second
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;

    for node in [founder, second, third] {
        node.stop().await;
    }
}
