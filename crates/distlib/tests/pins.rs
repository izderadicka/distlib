//! 5b-2a's acceptance: a custodian keeps an item without anybody asking, and
//! lets go of it into the cache only once its copies elsewhere make up for
//! this one; an item already cached is kept without a fetch. 5b-2b's: the
//! items with the fewest copies are fetched first, at a download limit raised
//! to the group's minimum. 5b-2c's: what a node serves is limited, the limit
//! raised to the group's minimum too. 5b-3b-2's: a node at its storage
//! limit evicts the oldest cached item to take on a new download or a new
//! custody, never an own or custody item, and leaves the exported file be.

// Three whole runtimes, a founding election, settling and fetching: seconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    num::{NonZeroU8, NonZeroU64, NonZeroUsize},
    path::Path,
    time::{Duration, Instant, SystemTime},
};

use distlib::Runtime;
use distlib_core::{
    ContentHash, CustodyGroup, CustodyItem, DataDir, GroupSettings, ItemId, KeepReason, MemberId,
    TransferConfig,
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
/// The size of a book, unless a test needs a larger one.
const BOOK: usize = 1_000_000;
/// The group's minimum, low enough that a limited fetch takes seconds.
const MIN_TRANSFER_RATE: u64 = 500_000;

/// Two copies of everything, and a membership that settles in two seconds.
fn settings() -> GroupSettings {
    GroupSettings {
        replicas: NonZeroU8::new(2).unwrap(),
        settle_secs: 2,
        max_settle_secs: 60,
        min_transfer_rate: MIN_TRANSFER_RATE,
        ..GroupSettings::default()
    }
}

/// How one of the test's nodes differs from the defaults.
#[derive(Default)]
struct Tuning {
    transfer: TransferConfig,
    storage_limit: Option<NonZeroU64>,
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
        Self::start_with(key, core, dir, Tuning::default()).await
    }

    async fn start_with(key: SecretKey, core: &[MemberId], dir: DataDir, tuning: Tuning) -> Self {
        let mut config = collecting(config(core));
        config.transfer = tuning.transfer;
        config.storage.limit_bytes = tuning.storage_limit;
        let runtime = Runtime::start(&key, &config, &dir).await.unwrap();
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

    /// Waits until `node.status` says `count` of its pins are in `state`.
    async fn until_pins(&self, state: &str, count: u64) {
        tokio::time::timeout(SOON, async {
            while self.call("node.status", json!({})).await["pins"][state] != json!(count) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{} never had {count} pins {state} within {SOON:?}",
                self.id.fmt_short()
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
    three_nodes_with(dir, pledges, Default::default()).await
}

/// [`three_nodes`], each tuned as its entry in `tunings` says.
async fn three_nodes_with(
    dir: &Path,
    pledges: [u64; 3],
    tunings: [Tuning; 3],
) -> (Node, Node, Node) {
    let keys: Vec<SecretKey> = (0..3).map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys.iter().map(|key| key.public().into()).collect();
    let mut nodes = Vec::new();
    for (index, (key, tuning)) in keys.into_iter().zip(tunings).enumerate() {
        let dir = DataDir::new(dir.join(index.to_string()));
        nodes.push(Node::start_with(key, &ids, dir, tuning).await);
    }
    let runtimes: Vec<&Runtime> = nodes.iter().map(|node| &node.runtime).collect();
    let pledges: Vec<(MemberId, u64)> = ids.into_iter().zip(pledges).collect();
    found(&runtimes, &nodes[0].key, &pledges, &settings()).await;
    let third = nodes.pop().unwrap();
    let second = nodes.pop().unwrap();
    let first = nodes.pop().unwrap();
    (first, second, third)
}

/// Checks that `took` is about `size` bytes over the group's minimum: not
/// less, so a limit held, and not over three times, so it was raised to the
/// minimum rather than left lower.
fn assert_about_the_minimum(took: Duration, size: usize, what: &str) {
    let at_the_minimum = Duration::from_secs_f64(size as f64 / MIN_TRANSFER_RATE as f64);
    assert!(
        took >= at_the_minimum.mul_f64(0.85) && took <= at_the_minimum * 3,
        "{what} took {took:?}, about {at_the_minimum:?} at the group's minimum"
    );
}

/// What [`add_a_book`] writes for `title`: about `size` bytes.
fn book_text(title: &str, size: usize) -> String {
    let line = format!("{title}\n");
    line.repeat(size / line.len())
}

/// Adds a book of about `size` bytes through `node`: its item and its one
/// file.
async fn add_a_book(node: &Node, dir: &Path, title: &str, size: usize) -> (ItemId, ContentHash) {
    node.runtime.catalogue().ready().await;
    // Large enough to be a file in the store rather than a row in its
    // database, which is how media is kept.
    let book = dir.join(format!("{title}.epub"));
    std::fs::write(&book, book_text(title, size)).unwrap();
    let added = node
        .call(
            "library.add",
            json!({ "kind": "ebook", "files": [&book], "title": title }),
        )
        .await;
    let item: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    let stored = node.runtime.catalogue().item(item).await.unwrap().unwrap();
    (item, *stored.files.keys().next().unwrap())
}

/// **A custodian fetches unasked, and lets go only once the item has its
/// copies elsewhere.** The founder adds a book; of the two others, custody
/// ranks one first, and it fetches the book. The other downloads it too, and
/// stops; the first then pledges nothing. Custody moves to the stopped one,
/// and the first keeps the book, since the founder's is then the only other
/// copy online, and still after a restart. Once the other is back and holds it, the first lets it fall into
/// the cache, which a collection pass leaves alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_fetches_unasked_and_lets_go_only_with_copies_elsewhere() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB; 3]).await;
    let core = [founder.id, second.id, third.id];
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;

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
    // The other downloads the book, so that once it stops it is a holder
    // that is not online, which counts for nothing.
    until_projected(&other.runtime, item, 1, "the other member").await;
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&other.runtime, &other.key), item, &dest).await;
    custodian.until_it_sees(other.id, item).await;

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
        "let go of the book while the founder's was the only other copy online"
    );
    // Restarted while it waits: only its tags say there is something to
    // let go of.
    let (custodians_key, custodians_dir) = custodian.stop().await;
    let custodian = Node::start(custodians_key, &core, custodians_dir).await;
    custodian.until_custodian_of(0).await;

    // The other is back and keeps the book: now it may go, into the cache.
    let before_the_release = SystemTime::now();
    let other = Node::start(others_key, &core, others_dir).await;
    other
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian
        .until_keeps(false, KeepReason::Custody, item, file)
        .await;
    assert!(custodian.keeps(KeepReason::Cache, item, file).await);
    // Stamped once the tags have moved, so waited for.
    tokio::time::timeout(SOON, async {
        let store = custodian.runtime.store();
        while store.last_used().await.unwrap().get(&item) < Some(&before_the_release) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("arriving in the cache is a use of it");
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
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;

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
/// fetches what a restart left half kept.** The founder adds a book nobody
/// else weighs enough to keep, and stops. The second member pledges, becomes
/// its custodian and tags it, but cannot fetch it; pledging nothing, it lets
/// go of the book at once, since it never held it. It pledges again and is
/// restarted, still its custodian, with the tag but not the book, and its
/// fetch fails. Once the founder is back, it fetches the book.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_fetch_is_asked_again_and_a_restart_finishes_it() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB, 0, 0]).await;
    let core = [founder.id, second.id, third.id];
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    until_projected(&second.runtime, item, 1, "the second member").await;
    let (founders_key, founders_dir) = founder.stop().await;

    second
        .call("group.pledge_set", json!({ "bytes": 100 * GB }))
        .await;
    second.until_tagged_for_custody(true, item, file).await;
    assert!(!second.runtime.blobs().has(file).await.unwrap());
    second.call("group.pledge_set", json!({ "bytes": 0 })).await;
    second.until_custodian_of(0).await;
    second.until_tagged_for_custody(false, item, file).await;

    second
        .call("group.pledge_set", json!({ "bytes": 100 * GB }))
        .await;
    second.until_tagged_for_custody(true, item, file).await;
    let (seconds_key, seconds_dir) = second.stop().await;
    let second = Node::start(seconds_key, &core, seconds_dir).await;
    second.until_custodian_of(1).await;
    // Given up while nobody could serve it, so only the founder coming back
    // asks for it again.
    second.until_pins("failed", 1).await;
    let founder = Node::start(founders_key, &core, founders_dir).await;
    second
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;

    for node in [founder, second, third] {
        node.stop().await;
    }
}

/// **The items with the fewest copies are fetched first, at no less than the
/// group's minimum.** The founder adds two books, and the third member
/// downloads one of them, so it has two copies and the other one. Once the
/// second member has seen every copy, it pledges and becomes custodian of
/// both at once; it fetches one item at a time, at a limit below the group's
/// minimum. It fetches the book with one copy first, and the other takes
/// about its size over the group's minimum, not over the lower limit.
///
/// Both books are queued by the same pass, so the order is the copies' alone.
/// A fetch that fails is normal, and is asked again, so nothing here waits on
/// one fetch holding the slot.
#[tokio::test(flavor = "multi_thread")]
async fn the_fewest_copies_go_first_at_no_less_than_the_groups_minimum() {
    // Four seconds at the group's minimum.
    const SIZE: usize = 2_000_000;
    init_logging();
    let dir = TempDir::new().unwrap();
    let one_at_a_time_below_the_minimum = TransferConfig {
        max_fetches: NonZeroUsize::MIN,
        max_download_rate: NonZeroU64::new(MIN_TRANSFER_RATE / 5),
        ..TransferConfig::default()
    };
    let (founder, second, third) = three_nodes_with(
        dir.path(),
        [100 * GB, 0, 0],
        [
            Tuning::default(),
            Tuning {
                transfer: one_at_a_time_below_the_minimum,
                ..Tuning::default()
            },
            Tuning::default(),
        ],
    )
    .await;

    let (copied, copied_file) = add_a_book(&founder, dir.path(), "Emma", SIZE).await;
    let (single, single_file) = add_a_book(&founder, dir.path(), "Ulysses", SIZE).await;
    until_projected(&third.runtime, copied, 1, "the third member").await;
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&third.runtime, &third.key), copied, &dest).await;
    for (holder, book) in [
        (third.id, copied),
        (founder.id, copied),
        (founder.id, single),
    ] {
        second.until_it_sees(holder, book).await;
    }

    second
        .call("group.pledge_set", json!({ "bytes": 100 * GB }))
        .await;
    // One at a time: one book fetched, the other waiting its turn.
    second.until_pins("fetching", 1).await;
    second.until_pins("queued", 1).await;
    second
        .until_keeps(true, KeepReason::Custody, single, single_file)
        .await;
    let single_kept = Instant::now();
    assert!(
        !second.keeps(KeepReason::Custody, copied, copied_file).await,
        "the book with two copies was fetched before the one with one"
    );
    second
        .until_keeps(true, KeepReason::Custody, copied, copied_file)
        .await;
    assert_about_the_minimum(single_kept.elapsed(), SIZE, "the book with two copies");

    for node in [founder, second, third] {
        node.stop().await;
    }
}

/// **What a node serves is limited, to no less than the group's minimum.**
/// The founder serves at a limit below the group's minimum. The second member,
/// custodian of everything, fetches the founder's book unasked, and it takes
/// about its size over the group's minimum, not over the founder's lower
/// limit.
#[tokio::test(flavor = "multi_thread")]
async fn what_a_node_serves_is_limited_to_no_less_than_the_groups_minimum() {
    const LARGE: usize = 5_000_000;
    init_logging();
    let dir = TempDir::new().unwrap();
    let serving_below_the_minimum = TransferConfig {
        max_upload_rate: NonZeroU64::new(MIN_TRANSFER_RATE / 5),
        ..TransferConfig::default()
    };
    let (founder, second, third) = three_nodes_with(
        dir.path(),
        [100 * GB, 100 * GB, 0],
        [
            Tuning {
                transfer: serving_below_the_minimum,
                ..Tuning::default()
            },
            Tuning::default(),
            Tuning::default(),
        ],
    )
    .await;

    let started = Instant::now();
    let (book, file) = add_a_book(&founder, dir.path(), "Dune", LARGE).await;
    second
        .until_keeps(true, KeepReason::Custody, book, file)
        .await;
    assert_about_the_minimum(started.elapsed(), LARGE, "the book");

    for node in [founder, second, third] {
        node.stop().await;
    }
}

/// Whether `node`'s storage status says own and custody alone are over its
/// limit.
async fn over_the_limit(node: &Node) -> bool {
    node.call("node.status", json!({})).await["storage"]["mandatory_over_limit"] == json!(true)
}

/// **A download makes room by evicting the cache, oldest first**, never what
/// the node added, and the file it exported stays. One that would not fit
/// even with the whole cache gone is refused, and evicts nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_download_evicts_the_oldest_cached_item_and_one_without_room_is_refused() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, _third) = three_nodes_with(
        dir.path(),
        [0; 3],
        [
            Tuning::default(),
            Tuning {
                storage_limit: NonZeroU64::new(3 * BOOK as u64 + BOOK as u64 / 2),
                ..Tuning::default()
            },
            Tuning::default(),
        ],
    )
    .await;
    let (own, own_file) = add_a_book(&second, dir.path(), "Own", BOOK).await;
    let mut books = Vec::new();
    for title in ["Oldest", "Newer", "Newest"] {
        books.push(add_a_book(&founder, dir.path(), title, BOOK).await);
    }
    let (too_large, _) = add_a_book(&founder, dir.path(), "Too large", 3 * BOOK).await;
    let [
        (oldest, oldest_file),
        (newer, newer_file),
        (newest, newest_file),
    ] = books[..]
    else {
        unreachable!("three books were added")
    };
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    let api = api(&second.runtime, &second.key);
    for item in [oldest, newer, too_large] {
        until_projected(&second.runtime, item, 1, "the second member").await;
    }
    download(&api, oldest, &dest).await;
    download(&api, newer, &dest).await;
    until_projected(&second.runtime, newest, 1, "the second member").await;

    download(&api, newest, &dest).await;

    assert!(
        !second.keeps(KeepReason::Cache, oldest, oldest_file).await,
        "the oldest stayed cached"
    );
    assert!(!second.runtime.catalogue().holdings().holds(oldest));
    assert!(second.keeps(KeepReason::Cache, newer, newer_file).await);
    assert!(second.keeps(KeepReason::Cache, newest, newest_file).await);
    assert!(second.keeps(KeepReason::Own, own, own_file).await);
    until_a_pass_collects(&second.runtime, dir.path(), "after-the-eviction").await;
    assert!(!second.runtime.blobs().has(oldest_file).await.unwrap());
    assert_eq!(
        std::fs::read_to_string(dest.join("Oldest.epub")).unwrap(),
        book_text("Oldest", BOOK),
        "the exported file is the user's"
    );

    let refused = api
        .call(
            "library.download",
            Some(json!({ "item_id": too_large, "dest": &dest })),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("no room for this download"), "{refused}");
    assert!(
        second.keeps(KeepReason::Cache, newer, newer_file).await,
        "a refused download evicted the cache"
    );
}

/// The title of a book [`add_a_book`] would add through `founder` of which
/// `custodian` is ranked custodian, from titles `prefix` 0, 1, 2 and on, and
/// the item it would be.
fn a_title_kept_by(
    custodian: MemberId,
    founder: MemberId,
    group: &CustodyGroup,
    prefix: &str,
) -> (String, ItemId) {
    (0..)
        .map(|number| {
            let title = format!("{prefix} {number}");
            let hash = iroh_blobs::Hash::new(book_text(&title, BOOK));
            (title, ItemId::from_content_hashes(&[*hash.as_bytes()]))
        })
        .find(|(_, item)| {
            let ranked = CustodyItem {
                item: *item,
                creators: vec![founder],
                size_bytes: 0,
            };
            group.custodians(&ranked)[0] == custodian
        })
        .expect("some title is ranked to each member")
}

/// **Custody makes room by evicting the cache**, and is fetched anyway when
/// even the whole cache is not room enough.
#[tokio::test(flavor = "multi_thread")]
async fn custody_evicts_the_cache_and_is_kept_even_without_room() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes_with(
        dir.path(),
        [100 * GB; 3],
        [
            Tuning::default(),
            Tuning {
                storage_limit: NonZeroU64::new(BOOK as u64 + BOOK as u64 / 2),
                ..Tuning::default()
            },
            Tuning::default(),
        ],
    )
    .await;
    let group = CustodyGroup::new(
        2,
        settings().weight_rule(),
        [founder.id, second.id, third.id]
            .iter()
            .map(|id| (*id, 100 * GB)),
    );
    let add_one_kept_by = async |custodian: &Node, prefix: &str| {
        let (title, ranked) = a_title_kept_by(custodian.id, founder.id, &group, prefix);
        let added = add_a_book(&founder, dir.path(), &title, BOOK).await;
        assert_eq!(added.0, ranked, "the item ranked is the item added");
        added
    };

    // The second caches a book the third keeps for the group.
    let (cached, cached_file) = add_one_kept_by(&third, "Cached").await;
    third
        .until_keeps(true, KeepReason::Custody, cached, cached_file)
        .await;
    until_projected(&second.runtime, cached, 1, "the second member").await;
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&second.runtime, &second.key), cached, &dest).await;
    assert!(second.keeps(KeepReason::Cache, cached, cached_file).await);

    // Its first custody needs the cached book's room.
    let (kept, kept_file) = add_one_kept_by(&second, "Kept").await;
    second
        .until_keeps(true, KeepReason::Custody, kept, kept_file)
        .await;
    assert!(!second.keeps(KeepReason::Cache, cached, cached_file).await);
    assert!(!second.runtime.catalogue().holdings().holds(cached));
    assert!(!over_the_limit(&second).await);

    // Its second has no room to be made, and is kept anyway.
    let (over, over_file) = add_one_kept_by(&second, "Over").await;
    second
        .until_keeps(true, KeepReason::Custody, over, over_file)
        .await;
    assert!(over_the_limit(&second).await);
}
