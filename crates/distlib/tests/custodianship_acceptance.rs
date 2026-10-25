//! §9's Phase 5 acceptance, reworded (P5-1): five nodes, `replicas = 3`; a
//! node whose pinned data is gone fails probes from two peers and is red on
//! every node; expelled, the next-ranked member fetches its item; with all but
//! one copy's holder stopped, another member fetches an emergency copy after
//! the delay.
//!
//! One story, one book: the founder adds it, and its copy is one of the three.

// Five whole runtimes, a founding election, fetches and an emergency delay:
// a minute or so.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::too_many_lines)] // a test is a scenario, read top to bottom

use std::{
    future::Future,
    num::{NonZeroU8, NonZeroU32},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use distlib::Runtime;
use distlib_core::{
    ContentHash, CustodyGroup, CustodyItem, DataDir, GroupSettings, ItemId, KeepReason, MemberId,
};
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;
use common::{api, config, found, init_logging, until_projected};

const SOON: Duration = Duration::from_secs(60);
const PLEDGE: u64 = 100_000_000_000;
/// The size of the book: well over what the blob store keeps inline.
const BOOK: usize = 1_000_000;
/// The delay before an emergency copy.
const EMERGENCY_AFTER: Duration = Duration::from_secs(10);

/// Three copies, the creator's one of them; a settle of a second and a probe
/// a second.
fn settings() -> GroupSettings {
    GroupSettings {
        replicas: NonZeroU8::new(3).unwrap(),
        min_pledge_bytes: 0,
        settle_secs: 1,
        emergency_after_secs: u32::try_from(EMERGENCY_AFTER.as_secs()).unwrap(),
        probe_every_secs: NonZeroU32::MIN,
        ..GroupSettings::default()
    }
}

/// One of the five, every one a core node.
struct Node {
    runtime: Runtime,
    key: SecretKey,
    id: MemberId,
    dir: DataDir,
}

impl Node {
    async fn call(&self, method: &str, params: Value) -> Value {
        api(&self.runtime, &self.key)
            .call(method, Some(params))
            .await
            .unwrap()
    }

    /// Whether it keeps `blob` of `item` for `reason`, and has its bytes.
    async fn keeps(&self, reason: KeepReason, item: ItemId, blob: ContentHash) -> bool {
        let blobs = self.runtime.blobs();
        blobs.kept(reason).await.unwrap().contains(&(item, blob)) && blobs.has(blob).await.unwrap()
    }

    /// Whether its `group.members` says `member` is red.
    async fn says_red(&self, member: MemberId) -> bool {
        let members = self.call("group.members", json!({})).await;
        members["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|listed| listed["member"] == json!(member) && listed["red"] == json!(true))
    }

    /// Where the blob store keeps `blob`'s bytes.
    fn bytes_of(&self, blob: ContentHash) -> PathBuf {
        self.dir
            .blobs_dir()
            .join("data")
            .join(format!("{blob}.data"))
    }
}

/// Waits until `ready` answers true, or fails saying `what` did not happen.
async fn until<F: Future<Output = bool>>(what: &str, mut ready: impl FnMut() -> F) {
    tokio::time::timeout(SOON, async {
        while !ready().await {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within {SOON:?}"));
}

/// Five nodes, all pledging [`PLEDGE`], founded with [`settings`]; the
/// founder first.
async fn five_nodes(dir: &Path) -> Vec<Node> {
    let keys: Vec<SecretKey> = (0..5).map(|_| SecretKey::generate()).collect();
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
            dir,
        });
    }
    let runtimes: Vec<&Runtime> = nodes.iter().map(|node| &node.runtime).collect();
    let pledges: Vec<(MemberId, u64)> = ids.into_iter().map(|id| (id, PLEDGE)).collect();
    found(&runtimes, &nodes[0].key, &pledges, &settings()).await;
    nodes
}

/// The founder adds a book, which every node has projected: the book and its
/// one file.
async fn add_a_book(nodes: &[Node], dir: &Path) -> (ItemId, ContentHash) {
    let founder = &nodes[0];
    founder.runtime.catalogue().ready().await;
    let file = dir.join("dune.epub");
    std::fs::write(&file, "Dune\n".repeat(BOOK / 5)).unwrap();
    let added = founder
        .call(
            "library.add",
            json!({ "kind": "ebook", "files": [&file], "title": "Dune" }),
        )
        .await;
    let book: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    for node in nodes {
        until_projected(&node.runtime, book, 1, &node.id.fmt_short().to_string()).await;
    }
    let stored = founder.runtime.store().item(book).await.unwrap().unwrap();
    (book, *stored.item.files.keys().next().unwrap())
}

/// The custody group of `members`, all pledging [`PLEDGE`].
fn custody_group(members: &[MemberId]) -> CustodyGroup {
    let settings = settings();
    CustodyGroup::new(
        settings.replicas.get(),
        settings.weight_rule(),
        members.iter().map(|member| (*member, PLEDGE)),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn three_copies_are_kept_through_a_lost_copy_an_expulsion_and_stopped_holders() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let nodes = five_nodes(dir.path()).await;
    let (book, blob) = add_a_book(&nodes, dir.path()).await;
    let ids: Vec<MemberId> = nodes.iter().map(|node| node.id).collect();
    let ranked = CustodyItem {
        item: book,
        creators: vec![ids[0]],
        size_bytes: 0,
    };
    let group = custody_group(&ids);
    let custodians = group.custodians(&ranked);
    let next_ranked = group
        .first_after_custodians(&ranked, |_| true)
        .expect("two members are left after the custodians");
    let index_of = |id: MemberId| ids.iter().position(|member| *member == id).unwrap();
    let last = (1..5)
        .find(|index| !custodians.contains(&ids[*index]) && ids[*index] != next_ranked)
        .unwrap();
    let mut nodes: Vec<Option<Node>> = nodes.into_iter().map(Some).collect();
    let [founder, lost, custodian, next, last] = [
        0,
        index_of(custodians[0]),
        index_of(custodians[1]),
        index_of(next_ranked),
        last,
    ]
    .map(|index| nodes[index].take().unwrap());
    for holder in [&lost, &custodian] {
        until("both custodians keep the book", || {
            holder.keeps(KeepReason::Custody, book, blob)
        })
        .await;
    }

    // A custodian's pinned data is gone, while its store still calls the
    // blob complete: the two other holders' probes fail, and it is red on
    // every node.
    std::fs::write(lost.bytes_of(blob), b"").unwrap();
    for node in [&founder, &lost, &custodian, &next, &last] {
        until("the custodian is red on every node", || {
            node.says_red(lost.id)
        })
        .await;
    }

    // Expelled with the evidence, by a majority of the core: the next-ranked
    // member takes its place, and fetches the book.
    let proposed = founder
        .call(
            "group.propose_expel",
            json!({ "member": lost.id, "with_evidence": true }),
        )
        .await;
    let reason = proposed["reason"].as_str().unwrap();
    assert!(reason.starts_with("failed probes by 2 members"), "{reason}");
    for approver in [&next, &last] {
        approver
            .call("group.approve", json!({ "proposal": proposed["proposal"] }))
            .await;
    }
    until("the expulsion is applied", || async {
        !founder.runtime.node().membership().is_member(&lost.id)
    })
    .await;
    lost.runtime.shutdown().await;
    let members: Vec<MemberId> = ids.iter().copied().filter(|id| *id != lost.id).collect();
    let group = custody_group(&members);
    assert_eq!(
        group.custodians(&ranked),
        [custodian.id, next.id],
        "the next-ranked member takes the expelled custodian's place"
    );
    until("the next-ranked member keeps the book", || {
        next.keeps(KeepReason::Custody, book, blob)
    })
    .await;

    // All but the founder's copy stopped: after the delay, the one member
    // left keeps an emergency copy.
    custodian.runtime.shutdown().await;
    let alone_from = Instant::now();
    next.runtime.shutdown().await;
    until("the member left keeps an emergency copy", || {
        last.keeps(KeepReason::Cache, book, blob)
    })
    .await;
    assert!(
        alone_from.elapsed() >= EMERGENCY_AFTER,
        "copied {:?} after the book was left alone, before the delay",
        alone_from.elapsed()
    );

    founder.runtime.shutdown().await;
    last.runtime.shutdown().await;
}
