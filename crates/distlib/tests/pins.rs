//! 5b-2a's acceptance: a custodian keeps an item without anybody asking, and
//! lets go of it into the cache only once its copies elsewhere make up for
//! this one; an item already cached is kept without a fetch. 5b-2b's: the
//! items with the fewest copies are fetched first, at a download limit raised
//! to the group's minimum. 5b-2c's: what a node serves is limited, the limit
//! raised to the group's minimum too. 5b-3b-2's: a node at its storage
//! limit evicts the oldest cached item to take on a new download or a new
//! custody, never an own or custody item, and leaves the exported file be.
//! 5c-1c's: custody that would not fit even so waits, evicting nothing,
//! until letting go of another custody makes room. 5c-2b's: a copy that
//! failed a probe does not count towards letting go. 5d-8b's: a node that
//! waits for an upgrade lets go of nothing. 5e-6's: only copies someone
//! must keep count towards letting go, not a cached one. 5e-7's: an
//! emergency copy stays, past the limit, until the custodians hold the item,
//! and is fetched within `max_fetches`. 5e-8's: the cache goes first where
//! the item has the most copies elsewhere. 5f-2's: a file list somebody
//! else writes for an item changes nothing anybody holds or keeps. 5i-4's: a
//! failed custody fetch or emergency copy is tried again within its backoff,
//! with nothing else changing.

// Three whole runtimes, a founding election, settling and fetching: seconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    num::{NonZeroU8, NonZeroU32, NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

use distlib::Runtime;
use distlib_consensus::{MemberProfile, MembershipEvent};
use distlib_core::{
    Config, ContentHash, CustodyGroup, CustodyItem, DataDir, FileRecord, GroupSettings, Item,
    ItemId, KeepReason, Key, MemberId, ProbeFailure, TransferConfig,
};
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;
use common::{
    api, bound, collecting, config, download, following, found_and_settled, init_logging, pledge,
    until, until_a_pass_collects, until_projected, until_released, until_watched, until_within,
};

const SOON: Duration = Duration::from_secs(60);
/// Within a first retry, a minute after a failure, with time to fetch.
const RETRIED_WITHIN: Duration = Duration::from_secs(90);
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
        // So a member may pledge nothing again, and give up its custody.
        min_pledge_bytes: 0,
        ..GroupSettings::default()
    }
}

/// How one of the test's nodes differs from the defaults.
#[derive(Default)]
struct Tuning {
    transfer: TransferConfig,
    storage_limit: Option<NonZeroU64>,
    /// In place of a collection every second.
    gc_interval_secs: Option<NonZeroU32>,
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
        if let Some(interval) = tuning.gc_interval_secs {
            config.storage.gc_interval_secs = interval;
        }
        Self::start_as(key, &config, dir).await
    }

    /// A follower of `founder`, which is the whole core.
    async fn follow(key: SecretKey, founder: &Node, dir: DataDir) -> Self {
        let config = collecting(following(founder.id, &bound(&founder.runtime)));
        Self::start_as(key, &config, dir).await
    }

    async fn start_as(key: SecretKey, config: &Config, dir: DataDir) -> Self {
        let runtime = Runtime::start(&key, config, &dir).await.unwrap();
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
        let tagged_and_here = || async {
            let blobs = self.runtime.blobs();
            let tagged = blobs.kept(reason).await.unwrap().contains(&(item, file));
            (tagged, blobs.has(file).await.unwrap())
        };
        until(
            &format!(
                "{} keeps the book for {reason:?} = {keeps}",
                self.id.fmt_short()
            ),
            tagged_and_here,
            |(tagged, here)| (*tagged && *here) == keeps,
        )
        .await;
    }

    /// Where the blob store keeps `file`'s bytes.
    fn bytes_of(&self, file: ContentHash) -> PathBuf {
        self.dir
            .blobs_dir()
            .join("data")
            .join(format!("{file}.data"))
    }

    /// Flips the first byte of its copy of `file`, which its store still
    /// calls whole, and returns the bytes that put it right.
    fn damage(&self, file: ContentHash) -> Vec<u8> {
        let bytes = std::fs::read(self.bytes_of(file)).unwrap();
        let mut damaged = bytes.clone();
        damaged[0] ^= 1;
        std::fs::write(self.bytes_of(file), damaged).unwrap();
        bytes
    }

    /// Waits until it keeps `file` of `item` for `reason`, within a retry.
    async fn until_keeps_after_a_retry(&self, reason: KeepReason, item: ItemId, file: ContentHash) {
        until_within(
            RETRIED_WITHIN,
            &format!("{} keeps the book for {reason:?}", self.id.fmt_short()),
            || self.keeps(reason, item, file),
            |keeps| *keeps,
        )
        .await;
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
        until(
            &format!("{}'s custody tag is {tagged}", self.id.fmt_short()),
            || self.tagged_for_custody(item, file),
            |is| *is == tagged,
        )
        .await;
    }

    /// Waits until its custody counts its pledge as `pledge_bytes`.
    async fn until_custody_counts(&self, pledge_bytes: u64) {
        until(
            "custody counts the node's pledge once it has settled",
            || async {
                self.runtime
                    .custody()
                    .status()
                    .map(|status| status.pledge_bytes)
            },
            |counted| *counted == Some(pledge_bytes),
        )
        .await;
    }

    /// Waits until it sees `holder` online and holding `item`.
    async fn until_it_sees(&self, holder: MemberId, item: ItemId) {
        let availability = self.runtime.availability();
        let online_and_holding = || async {
            let online = availability.online().borrow().contains(&holder);
            (online, availability.holders(&item).contains(&holder))
        };
        until(
            &format!(
                "{} sees {} online and holding the book",
                self.id.fmt_short(),
                holder.fmt_short()
            ),
            online_and_holding,
            |(online, holding)| *online && *holding,
        )
        .await;
    }

    /// Waits until `node.status` says `count` of its pins are in `state`.
    async fn until_pins(&self, state: &str, count: u64) {
        until(
            &format!("{} has {count} pins {state}", self.id.fmt_short()),
            || async { self.call("node.status", json!({})).await["pins"].clone() },
            |pins| pins[state] == json!(count),
        )
        .await;
    }

    /// Waits until it says it is custodian of `items` items.
    async fn until_custodian_of(&self, items: u64) {
        until(
            &format!("{} is custodian of {items} items", self.id.fmt_short()),
            || async { self.call("node.status", json!({})).await["custody"].clone() },
            |custody| custody["items"] == json!(items),
        )
        .await;
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
    let mut nodes = founded(dir, &pledges, tunings, &settings()).await;
    let third = nodes.pop().unwrap();
    let second = nodes.pop().unwrap();
    let first = nodes.pop().unwrap();
    (first, second, third)
}

/// A node for each of `pledges`, collecting every second, tuned as its entry
/// in `tunings` says, founded with `settings`; the founder first.
async fn founded(
    dir: &Path,
    pledges: &[u64],
    tunings: impl IntoIterator<Item = Tuning>,
    settings: &GroupSettings,
) -> Vec<Node> {
    let keys: Vec<SecretKey> = pledges.iter().map(|_| SecretKey::generate()).collect();
    let ids: Vec<MemberId> = keys.iter().map(|key| key.public().into()).collect();
    let mut nodes = Vec::new();
    for (index, (key, tuning)) in keys.into_iter().zip(tunings).enumerate() {
        let dir = DataDir::new(dir.join(index.to_string()));
        nodes.push(Node::start_with(key, &ids, dir, tuning).await);
    }
    let runtimes: Vec<&Runtime> = nodes.iter().map(|node| &node.runtime).collect();
    let pledges: Vec<(&SecretKey, u64)> = nodes
        .iter()
        .map(|node| &node.key)
        .zip(pledges.iter().copied())
        .collect();
    found_and_settled(&runtimes, &pledges, settings).await;
    nodes
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
    let (custodian, other) = custodian_and_other(&founder, second, third, item);
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
    let store = custodian.runtime.store();
    until(
        "arriving in the cache is a use of it",
        || async { store.last_used().await.unwrap().get(&item).copied() },
        |used| *used >= Some(before_the_release),
    )
    .await;
    until_a_pass_collects(&custodian.runtime, dir.path(), "after-the-release").await;
    assert!(
        custodian.runtime.blobs().has(file).await.unwrap(),
        "the book was collected rather than cached"
    );

    for node in [founder, custodian, other] {
        node.stop().await;
    }
}

/// **A file list somebody else writes changes nothing anybody holds or
/// keeps** (#200 finding 4). As the test above up to the custodian keeping
/// the book, with the founder's the only other copy. Then the other member
/// writes a list for the book with one more file, which nobody has: it does
/// not fingerprint to the book's id, so the custodian and the founder still
/// hold the book, and nothing fails to fetch. The other is stopped and the
/// custodian pledges nothing: it keeps the book, since the founder's is the
/// only other copy online.
#[tokio::test(flavor = "multi_thread")]
async fn a_stray_file_list_changes_nothing_a_custodian_keeps() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB; 3]).await;
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    let (custodian, other) = custodian_and_other(&founder, second, third, item);
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian.until_it_sees(founder.id, item).await;

    write_a_stray_list(&other, item, &[&custodian, &founder]).await;
    for node in [&custodian, &founder] {
        let stored = node.runtime.store().item(item).await.unwrap().unwrap();
        assert_eq!(stored.item.files.keys().collect::<Vec<_>>(), [&file]);
        assert_eq!(
            stored.creator, founder.id,
            "the stray's writer added nothing"
        );
        assert!(node.runtime.catalogue().holdings().holds(item));
    }
    custodian.until_pins("kept", 1).await;
    assert_eq!(
        custodian.call("node.status", json!({})).await["pins"]["failed"],
        json!(0),
        "the stray file is not the custodian's to fetch"
    );

    other.stop().await;
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

    for node in [founder, custodian] {
        node.stop().await;
    }
}

/// **A custodian fetches only the files an item was added with** (#200
/// finding 4). The third member writes a list for the founder's book with one
/// more file, which nobody has, before the second pledges and becomes the
/// book's custodian: it keeps the book, and asks nobody for the stray file.
#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_fetches_only_the_files_an_item_was_added_with() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB, 0, 0]).await;
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    write_a_stray_list(&third, item, &[&second]).await;
    second.until_it_sees(founder.id, item).await;

    second
        .call("group.pledge_set", json!({ "bytes": 100 * GB }))
        .await;
    second.until_custodian_of(1).await;
    second
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    second.until_pins("kept", 1).await;
    assert!(second.runtime.catalogue().holdings().holds(item));

    for node in [founder, second, third] {
        node.stop().await;
    }
}

/// Has `writer` write a file list for `item` with one more file than it was
/// added with, naming a file nobody has: what any member can write, by a bug
/// or on purpose. Waits until each of `readers` has the list and has
/// projected the item since.
async fn write_a_stray_list(writer: &Node, item: ItemId, readers: &[&Node]) {
    until_projected(&writer.runtime, item, 1, "the stray's writer").await;
    let mut files = writer
        .runtime
        .store()
        .item(item)
        .await
        .unwrap()
        .unwrap()
        .item
        .files;
    files.insert(
        ContentHash::from_bytes([9; 32]),
        FileRecord {
            format: "epub".to_owned(),
            size: 1,
            filename: "stray.epub".to_owned(),
            seq: None,
            disc: None,
            title: None,
            duration: None,
        },
    );
    let stray = serde_json::to_vec(&files).unwrap();
    let key = Key::Files { item }.encode();
    writer
        .runtime
        .catalogue()
        .put(&key, stray.clone())
        .await
        .unwrap();
    for reader in readers {
        until(
            "the stray list reaches every reader",
            || async { reader.runtime.catalogue().get(&key).await.ok().flatten() },
            |read| read.as_deref() == Some(stray.as_slice()),
        )
        .await;
    }

    // Written after the list is everywhere, so its projection reads the list.
    let marker = Item {
        title: Some("after the stray".to_owned()),
        ..Item::new(item)
    };
    writer.runtime.catalogue().write(&marker).await.unwrap();
    for reader in readers {
        until(
            "every reader projects the item after the stray list",
            || async { reader.runtime.store().item(item).await.unwrap() },
            |stored| {
                stored
                    .as_ref()
                    .is_some_and(|stored| stored.item.title == marker.title)
            },
        )
        .await;
    }
}

/// Of the two members after the founder, all three pledging the same,
/// `item`'s custodian and the other.
fn custodian_and_other(founder: &Node, second: Node, third: Node, item: ItemId) -> (Node, Node) {
    let group = CustodyGroup::new(
        2,
        settings().weight_rule(),
        [founder.id, second.id, third.id].map(|id| (id, 100 * GB)),
    );
    let custodians = group.custodians(&CustodyItem {
        item,
        creator: founder.id,
        size_bytes: 0,
    });
    if custodians == [second.id] {
        (second, third)
    } else {
        (third, second)
    }
}

/// A founder that is the whole core, and two followers, all three pledging
/// the same; returns once both followers' custody counts their pledge.
async fn a_founder_and_two_followers(dir: &Path, settings: &GroupSettings) -> (Node, Node, Node) {
    let mut nodes = founded(dir, &[100 * GB], [Tuning::default()], settings).await;
    let founder = nodes.pop().unwrap();
    let mut followers = Vec::new();
    for index in 1..=2 {
        let key = SecretKey::generate();
        let id = MemberId::from(key.public());
        founder
            .runtime
            .node()
            .propose(
                MembershipEvent::MemberAdded {
                    member: MemberProfile {
                        member_id: id,
                        display_name: index.to_string(),
                    },
                },
                &founder.key,
            )
            .await
            .unwrap();
        pledge(&founder.runtime, &key, 100 * GB).await;
        let dir = DataDir::new(dir.join(index.to_string()));
        followers.push(Node::follow(key, &founder, dir).await);
    }
    for follower in &followers {
        follower.until_custody_counts(100 * GB).await;
    }
    let third = followers.pop().unwrap();
    let second = followers.pop().unwrap();
    until_each_ranks_the_other(&founder, [&second, &third], settings).await;
    (founder, second, third)
}

/// Waits until each follower's custody ranks the other too. Until then a
/// follower whose view lacks the other is the only one ranked, so custodian
/// of every item: one added meanwhile is fetched by both, and stays cached
/// with the one that is not its custodian.
async fn until_each_ranks_the_other(
    founder: &Node,
    followers: [&Node; 2],
    settings: &GroupSettings,
) {
    let ids = [founder.id, followers[0].id, followers[1].id];
    let group = CustodyGroup::new(2, settings.weight_rule(), ids.map(|id| (id, 100 * GB)));
    for (follower, other) in [(followers[0], followers[1]), (followers[1], followers[0])] {
        let probe = (0..=u8::MAX)
            .map(|seed| CustodyItem {
                item: ItemId::from_bytes([seed; 32]),
                creator: founder.id,
                size_bytes: 0,
            })
            .find(|item| group.custodians(item) == [other.id])
            .expect("some item is the other follower's");
        until(
            &format!("{} ranks {}", follower.id.fmt_short(), other.id.fmt_short()),
            || async { follower.runtime.custody().custodians(&probe) },
            |custodians| *custodians == Some(vec![other.id]),
        )
        .await;
    }
}

/// **A node that waits for an upgrade lets go of nothing** (5d-8). As the
/// test above, but the custodian and the other are followers of a founder
/// that is the whole core. While the custodian waits to let go, the group
/// raises its protocol past this build, so both followers stop. The other is
/// back and keeps the book, which would let the custodian go; it keeps it.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_waits_for_an_upgrade_lets_go_of_nothing() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = a_founder_and_two_followers(dir.path(), &settings()).await;
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    let (custodian, other) = custodian_and_other(&founder, second, third, item);
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian.until_it_sees(founder.id, item).await;
    until_projected(&other.runtime, item, 1, "the other member").await;
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&other.runtime, &other.key), item, &dest).await;
    custodian.until_it_sees(other.id, item).await;
    let (others_key, others_dir) = other.stop().await;
    custodian
        .call("group.pledge_set", json!({ "bytes": 0 }))
        .await;
    custodian.until_custodian_of(0).await;

    raise_the_protocol_past(&founder, &custodian).await;

    let other = Node::follow(others_key, &founder, others_dir).await;
    other
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian.until_it_sees(other.id, item).await;
    // Time for the pin manager to act on it, had it been going to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        custodian.keeps(KeepReason::Custody, item, file).await,
        "let go of the book while waiting for an upgrade"
    );

    for node in [founder, custodian, other] {
        node.stop().await;
    }
}

/// Has `founder`, the whole core, raise the group's protocol past this build,
/// and waits until `follower` stops for an upgrade.
async fn raise_the_protocol_past(founder: &Node, follower: &Node) {
    for event in [
        MembershipEvent::ProtocolSupported {
            member: founder.id,
            max: 2,
        },
        MembershipEvent::ProtocolRaised { to: 2 },
    ] {
        founder
            .runtime
            .node()
            .propose(event, &founder.key)
            .await
            .unwrap();
    }
    until_watched(
        &format!("{} stops for an upgrade", follower.id.fmt_short()),
        &mut follower.runtime.node().subscribe_upgrade_required(),
        Option::is_some,
    )
    .await;
}

/// **A node that waits for an upgrade keeps its emergency copies** (5d-8,
/// 5e-7). The custodian and the other are followers of a founder that is the
/// whole core. The custodian stops, and after the delay the other keeps an
/// emergency copy. The group raises its protocol past this build, so the
/// other stops following; the custodian is back and holds the book, which
/// would move the copy to the cache. It stays an emergency copy.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_waits_for_an_upgrade_keeps_its_emergency_copies() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let settings = GroupSettings {
        emergency_after_secs: 3,
        ..settings()
    };
    let (founder, second, third) = a_founder_and_two_followers(dir.path(), &settings).await;
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    let (custodian, other) = custodian_and_other(&founder, second, third, item);
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    let (custodians_key, custodians_dir) = custodian.stop().await;
    other
        .until_keeps(true, KeepReason::Emergency, item, file)
        .await;

    raise_the_protocol_past(&founder, &other).await;
    let custodian = Node::follow(custodians_key, &founder, custodians_dir).await;
    other.until_it_sees(custodian.id, item).await;
    // Time for the emergency task to act on it, had it been going to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        other.keeps(KeepReason::Emergency, item, file).await,
        "moved the emergency copy to the cache while waiting for an upgrade"
    );

    for node in [founder, custodian, other] {
        node.stop().await;
    }
}

/// **An emergency copy is looked at again when custody is recomputed**
/// (5e-7). Four members, the last pledging nothing at first; custody ranks
/// the first after the founder, so the second is next. The first stops, and
/// after the delay the second keeps an emergency copy. The third downloads
/// the book, which obliges it to nothing, so the copy stays. The third then
/// pledges, and custody moves to it: nobody's availability changes, but the
/// group's obliged copies are now enough, and the copy moves to the cache.
#[tokio::test(flavor = "multi_thread")]
async fn an_emergency_copy_moves_to_the_cache_when_a_holder_becomes_custodian() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let settings = GroupSettings {
        emergency_after_secs: 3,
        ..settings()
    };
    let tunings = (0..4).map(|_| Tuning::default());
    let mut nodes = founded(
        dir.path(),
        &[100 * GB, 100 * GB, 100 * GB, 0],
        tunings,
        &settings,
    )
    .await;
    let bystander = nodes.pop().unwrap();
    let copier = nodes.pop().unwrap();
    let custodian = nodes.pop().unwrap();
    let founder = nodes.pop().unwrap();
    let ranking = |bystanders_pledge| {
        let pledges = [100 * GB, 100 * GB, 100 * GB, bystanders_pledge];
        let ids = [founder.id, custodian.id, copier.id, bystander.id];
        CustodyGroup::new(2, settings.weight_rule(), ids.into_iter().zip(pledges))
    };
    let (before, after) = (ranking(0), ranking(1_000 * GB));
    let (title, ranked) = a_title_ranked(
        "Dune",
        BOOK,
        founder.id,
        &[(&before, custodian.id), (&after, bystander.id)],
    );
    let (item, file) = add_a_book(&founder, dir.path(), &title, BOOK).await;
    assert_eq!(item, ranked, "the item ranked is the item added");
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian.stop().await;
    copier
        .until_keeps(true, KeepReason::Emergency, item, file)
        .await;

    until_projected(&bystander.runtime, item, 1, "the bystander").await;
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&bystander.runtime, &bystander.key), item, &dest).await;
    copier.until_it_sees(bystander.id, item).await;
    // Time for the emergency task to act on it, had it been going to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        copier.keeps(KeepReason::Emergency, item, file).await,
        "a cached copy, which nobody must keep, let the emergency copy go"
    );

    bystander
        .call("group.pledge_set", json!({ "bytes": 1_000 * GB }))
        .await;
    bystander.until_custodian_of(1).await;
    copier
        .until_keeps(false, KeepReason::Emergency, item, file)
        .await;
    assert!(copier.keeps(KeepReason::Cache, item, file).await);

    for node in [founder, copier, bystander] {
        node.stop().await;
    }
}

/// **A copy that failed a probe does not count** (D11) towards letting go.
/// The other member downloads the book, and the founder records a failure
/// of its copy. The custodian then pledges nothing: custody moves to the
/// other, but the founder's is the only real copy elsewhere, so the
/// custodian keeps the book until the failure is cleared.
#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_does_not_let_go_while_the_copy_elsewhere_failed_a_probe() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB; 3]).await;
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    let (custodian, other) = custodian_and_other(&founder, second, third, item);
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian.until_it_sees(founder.id, item).await;
    until_projected(&other.runtime, item, 1, "the other member").await;
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&other.runtime, &other.key), item, &dest).await;
    custodian.until_it_sees(other.id, item).await;

    let failure = ProbeFailure {
        blob: file,
        chunk: 0,
        in_a_row: 1,
    };
    founder
        .runtime
        .catalogue()
        .record_probe_failure(other.id, item, &failure)
        .await
        .unwrap();
    until(
        "the custodian takes the failure in",
        || async { custodian.runtime.availability().real_holders(&item) },
        |real| !real.contains(&other.id),
    )
    .await;

    custodian
        .call("group.pledge_set", json!({ "bytes": 0 }))
        .await;
    custodian.until_custodian_of(0).await;
    other
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    // Time for the pin manager to act on it, had it been going to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        custodian.keeps(KeepReason::Custody, item, file).await,
        "let go of the book while the founder's was the only real copy elsewhere"
    );
    custodian.until_pins("releasing", 1).await;

    founder
        .runtime
        .catalogue()
        .clear_probe_failure(other.id, item)
        .await
        .unwrap();
    custodian
        .until_keeps(false, KeepReason::Custody, item, file)
        .await;

    for node in [founder, custodian, other] {
        node.stop().await;
    }
}

/// **Only copies someone must keep count towards letting go** (5e-6). Four
/// members; custody ranks the first after the founder before it pledges
/// nothing and the second after, which has room for half a book. The third
/// caches the book. Once the first pledges nothing, the second waits for
/// room: the founder's copy and the cached one make two, as many as
/// `replicas`, but only the founder must keep its own, so the first keeps
/// custody. The second is restarted with room, keeps the book, and the first
/// lets go.
#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_does_not_let_go_while_its_successor_waits_for_room() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let mut nodes = founded(
        dir.path(),
        &[100 * GB; 4],
        [
            Tuning::default(),
            Tuning::default(),
            Tuning {
                storage_limit: NonZeroU64::new(BOOK as u64 / 2),
                ..Tuning::default()
            },
            Tuning::default(),
        ],
        &settings(),
    )
    .await;
    let bystander = nodes.pop().unwrap();
    let successor = nodes.pop().unwrap();
    let custodian = nodes.pop().unwrap();
    let founder = nodes.pop().unwrap();
    let core = [founder.id, custodian.id, successor.id, bystander.id];
    let ranking = |custodians_pledge| {
        let pledges = [100 * GB, custodians_pledge, 100 * GB, 100 * GB];
        CustodyGroup::new(2, settings().weight_rule(), core.into_iter().zip(pledges))
    };
    let (before, after) = (ranking(100 * GB), ranking(0));
    let (title, ranked) = a_title_ranked(
        "Dune",
        BOOK,
        founder.id,
        &[(&before, custodian.id), (&after, successor.id)],
    );
    let (item, file) = add_a_book(&founder, dir.path(), &title, BOOK).await;
    assert_eq!(item, ranked, "the item ranked is the item added");
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian.until_it_sees(founder.id, item).await;
    until_projected(&bystander.runtime, item, 1, "the bystander").await;
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    download(&api(&bystander.runtime, &bystander.key), item, &dest).await;
    custodian.until_it_sees(bystander.id, item).await;

    custodian
        .call("group.pledge_set", json!({ "bytes": 0 }))
        .await;
    custodian.until_custodian_of(0).await;
    successor.until_pins("waiting_for_room", 1).await;
    // Time for the pin manager to act on it, had it been going to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        custodian.keeps(KeepReason::Custody, item, file).await,
        "let go of the book while only a cached copy made up the count"
    );

    let (successors_key, successors_dir) = successor.stop().await;
    let successor = Node::start(successors_key, &core, successors_dir).await;
    successor
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    custodian
        .until_keeps(false, KeepReason::Custody, item, file)
        .await;
    assert!(custodian.keeps(KeepReason::Cache, item, file).await);

    for node in [founder, custodian, successor, bystander] {
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

/// **Custody past the pledge is kept all the same.** The founder adds two
/// books of about a megabyte; the second member, the only other with a
/// weight, pledges room for one. It is custodian of both, keeps both, and
/// says it is custodian of more than it pledged.
#[tokio::test(flavor = "multi_thread")]
async fn custody_past_the_pledge_is_kept_all_the_same() {
    init_logging();
    let dir = TempDir::new().unwrap();
    // Steps of a byte, so a pledge of a megabyte and a half weighs something.
    let settings = GroupSettings {
        weight_step_bytes: NonZeroU64::MIN,
        ..settings()
    };
    let pledge = BOOK as u64 * 3 / 2;
    let tunings = std::iter::repeat_with(Tuning::default);
    let nodes = founded(dir.path(), &[100 * GB, 0, 0], tunings, &settings).await;
    let [founder, second, third] = <[Node; 3]>::try_from(nodes).ok().unwrap();
    let books = [
        add_a_book(&founder, dir.path(), "Dune", BOOK).await,
        add_a_book(&founder, dir.path(), "Emma", BOOK).await,
    ];
    until_projected(&second.runtime, books[1].0, 1, "the second member").await;

    second
        .call("group.pledge_set", json!({ "bytes": pledge }))
        .await;
    second.until_custodian_of(2).await;
    for (item, file) in books {
        second
            .until_keeps(true, KeepReason::Custody, item, file)
            .await;
    }
    let custody = &second.call("node.status", json!({})).await["custody"];
    assert_eq!(custody["pledge_bytes"], json!(pledge));
    assert!(custody["bytes"].as_u64().unwrap() > pledge, "{custody}");

    for node in [founder, second, third] {
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

/// **A failed custody fetch is tried again within its backoff** (#185
/// finding 10). The founder's copy of a book is damaged, so the second
/// member, made its custodian, fails to fetch it. The copy is put right, and
/// with no change of holdings or membership the fetch is made within a
/// retry.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_custody_fetch_is_tried_again_within_its_backoff() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes(dir.path(), [100 * GB, 0, 0]).await;
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    until_projected(&second.runtime, item, 1, "the second member").await;
    // Its holdings told, so nothing more changes availability.
    second.until_it_sees(founder.id, item).await;
    let whole = founder.damage(file);

    second
        .call("group.pledge_set", json!({ "bytes": 100 * GB }))
        .await;
    second.until_pins("failed", 1).await;
    std::fs::write(founder.bytes_of(file), whole).unwrap();
    second
        .until_keeps_after_a_retry(KeepReason::Custody, item, file)
        .await;

    for node in [founder, second, third] {
        node.stop().await;
    }
}

/// **A failed emergency copy is tried again within its backoff** (#185
/// finding 10). The founder's copy of a book is damaged and its custodian
/// stops, so the other member's emergency copy fails. The copy is put right,
/// and with no change of holdings or membership the emergency copy is made
/// within a retry.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_emergency_copy_is_tried_again_within_its_backoff() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let settings = GroupSettings {
        emergency_after_secs: 3,
        ..settings()
    };
    let (founder, second, third) = a_founder_and_two_followers(dir.path(), &settings).await;
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    let (custodian, other) = custodian_and_other(&founder, second, third, item);
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    let whole = founder.damage(file);
    custodian.stop().await;

    // Tagged before it is fetched: the copy has begun.
    until(
        "the emergency copy begins",
        || async {
            other
                .runtime
                .blobs()
                .kept(KeepReason::Emergency)
                .await
                .unwrap()
        },
        |kept| kept.contains(&(item, file)),
    )
    .await;
    // Time for it to fail on the first chunk.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!other.runtime.blobs().has(file).await.unwrap());
    std::fs::write(founder.bytes_of(file), whole).unwrap();
    other
        .until_keeps_after_a_retry(KeepReason::Emergency, item, file)
        .await;

    for node in [founder, other] {
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

/// Whether `node`'s storage status says what is not cache is over its limit
/// on its own.
async fn over_the_limit(node: &Node) -> bool {
    node.call("node.status", json!({})).await["storage"]["mandatory_over_limit"] == json!(true)
}

/// **A download makes room by evicting the cache, oldest first**, never what
/// the node added, and the file it exported stays. An add makes room too.
/// One that would not fit even with the whole cache gone is refused, the
/// cache evicted all the same, since it is likely to be asked for again.
#[tokio::test(flavor = "multi_thread")]
async fn a_download_evicts_the_oldest_cached_item_and_one_without_room_is_refused_after_evicting() {
    init_logging();
    let dir = TempDir::new().unwrap();
    // Below the weight step: each may add, and none is a custodian.
    let (founder, second, _third) = three_nodes_with(
        dir.path(),
        [GB; 3],
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

    // An add makes room the same way.
    let (added, added_file) = add_a_book(&second, dir.path(), "Added", BOOK).await;
    assert!(second.keeps(KeepReason::Own, added, added_file).await);
    assert!(!second.keeps(KeepReason::Cache, newer, newer_file).await);
    assert!(second.keeps(KeepReason::Cache, newest, newest_file).await);

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
        !second.keeps(KeepReason::Cache, newest, newest_file).await,
        "refused, with the cache kept for a download that will be asked for again"
    );
    assert!(second.keeps(KeepReason::Own, added, added_file).await);
}

/// **The cache goes first where the item has the most copies elsewhere**
/// (5e-8), though used more recently. Nobody but the founder pledges, so no
/// book has a custodian. The second member, with room for two and a half
/// books, downloads the older book, then the newer, which the third downloads
/// too; a third download evicts the newer, with two copies elsewhere, and
/// keeps the older, with only the founder's. With as many copies the oldest
/// goes first: the test above, every book there the founder's alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_download_evicts_the_cached_item_with_the_most_copies_elsewhere_first() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes_with(
        dir.path(),
        [100 * GB, 0, 0],
        [
            Tuning::default(),
            Tuning {
                storage_limit: NonZeroU64::new(2 * BOOK as u64 + BOOK as u64 / 2),
                ..Tuning::default()
            },
            Tuning::default(),
        ],
    )
    .await;
    let mut books = Vec::new();
    for title in ["Older", "Newer", "Wanted"] {
        books.push(add_a_book(&founder, dir.path(), title, BOOK).await);
    }
    let [
        (older, older_file),
        (newer, newer_file),
        (wanted, wanted_file),
    ] = books[..]
    else {
        unreachable!("three books were added")
    };
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    let (seconds, thirds) = (
        api(&second.runtime, &second.key),
        api(&third.runtime, &third.key),
    );
    for item in [older, newer, wanted] {
        until_projected(&second.runtime, item, 1, "the second member").await;
    }
    download(&seconds, older, &dest).await;
    download(&seconds, newer, &dest).await;
    until_projected(&third.runtime, newer, 1, "the third member").await;
    let thirds_dest = dest.join("third");
    std::fs::create_dir_all(&thirds_dest).unwrap();
    download(&thirds, newer, &thirds_dest).await;
    second.until_it_sees(third.id, newer).await;

    download(&seconds, wanted, &dest).await;

    assert!(
        !second.keeps(KeepReason::Cache, newer, newer_file).await,
        "the newer book, with two copies elsewhere, stayed cached"
    );
    assert!(second.keeps(KeepReason::Cache, older, older_file).await);
    assert!(second.keeps(KeepReason::Cache, wanted, wanted_file).await);

    // They hold the runtimes' databases open.
    drop((seconds, thirds));
    for node in [founder, second, third] {
        node.stop().await;
    }
}

/// The title of a book of `size` [`add_a_book`] would add through `founder`
/// whose custodian is the one `ranked` wants under each of `groups`, from
/// titles `prefix` 0, 1, 2 and on, and the item it would be.
fn a_title_ranked(
    prefix: &str,
    size: usize,
    founder: MemberId,
    ranked: &[(&CustodyGroup, MemberId)],
) -> (String, ItemId) {
    (0..)
        .map(|number| {
            let title = format!("{prefix} {number}");
            let hash = iroh_blobs::Hash::new(book_text(&title, size));
            (title, ItemId::from_content_hashes(&[*hash.as_bytes()]))
        })
        .find(|(_, item)| {
            let item = CustodyItem {
                item: *item,
                creator: founder,
                size_bytes: 0,
            };
            ranked
                .iter()
                .all(|(group, custodian)| group.custodians(&item)[0] == *custodian)
        })
        .expect("some title is ranked as wanted")
}

/// Rates `item` through `node`, so the projection rechecks it while `file`'s
/// bytes are still here, evicted but not collected: it is not held.
async fn rechecked_while_its_bytes_are_here(node: &Node, item: ItemId, file: ContentHash) {
    node.call("community.rate", json!({ "item_id": item, "rating": 3 }))
        .await;
    until(
        "the rating is projected",
        || async { node.runtime.store().item(item).await.unwrap() },
        |stored| {
            stored
                .as_ref()
                .is_some_and(|stored| stored.ratings.contains_key(&node.id))
        },
    )
    .await;
    assert!(node.runtime.blobs().has(file).await.unwrap());
    assert!(
        !node.runtime.catalogue().holdings().holds(item),
        "announced as held again once rechecked"
    );
}

/// How custody ranks `ids`, the third pledging `third_pledge` and the others
/// 100 GB.
fn ranking(ids: [MemberId; 3], third_pledge: u64) -> CustodyGroup {
    let pledges = [100 * GB, 100 * GB, third_pledge];
    CustodyGroup::new(2, settings().weight_rule(), ids.into_iter().zip(pledges))
}

/// **Custody makes room by evicting the cache, and waits when even the whole
/// cache is not room enough**, evicting nothing; it is fetched once letting
/// go of another custody makes room. An evicted item rechecked before
/// collection deletes it is not held.
#[tokio::test(flavor = "multi_thread")]
async fn custody_evicts_the_cache_and_waits_when_even_that_is_not_room() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let (founder, second, third) = three_nodes_with(
        dir.path(),
        [100 * GB; 3],
        [
            Tuning::default(),
            Tuning {
                storage_limit: NonZeroU64::new(BOOK as u64 + BOOK as u64 / 2),
                // So what it evicts is still here when it is rechecked.
                gc_interval_secs: NonZeroU32::new(3600),
                ..Tuning::default()
            },
            Tuning::default(),
        ],
    )
    .await;
    // Before and after the third raises its pledge.
    let ids = [founder.id, second.id, third.id];
    let (before, after) = (ranking(ids, 100 * GB), ranking(ids, 1_000 * GB));
    let add_one_ranked = async |prefix: &str, size: usize, ranked: &[(&CustodyGroup, MemberId)]| {
        let (title, item) = a_title_ranked(prefix, size, founder.id, ranked);
        let added = add_a_book(&founder, dir.path(), &title, size).await;
        assert_eq!(added.0, item, "the item ranked is the item added");
        added
    };
    let dest = dir.path().join("downloads");
    std::fs::create_dir_all(&dest).unwrap();
    let cache = async |item: ItemId, file: ContentHash| {
        until_projected(&second.runtime, item, 1, "the second member").await;
        download(&api(&second.runtime, &second.key), item, &dest).await;
        assert!(second.keeps(KeepReason::Cache, item, file).await);
    };

    // The second caches a book the third keeps for the group.
    let (cached, cached_file) = add_one_ranked("Cached", BOOK, &[(&before, third.id)]).await;
    third
        .until_keeps(true, KeepReason::Custody, cached, cached_file)
        .await;
    cache(cached, cached_file).await;

    // Its first custody needs the cached book's room. The third will take it
    // over once it pledges more.
    let (kept, kept_file) =
        add_one_ranked("Kept", BOOK, &[(&before, second.id), (&after, third.id)]).await;
    second
        .until_keeps(true, KeepReason::Custody, kept, kept_file)
        .await;
    assert!(!second.keeps(KeepReason::Cache, cached, cached_file).await);
    assert!(!second.runtime.catalogue().holdings().holds(cached));
    assert!(!over_the_limit(&second).await);

    rechecked_while_its_bytes_are_here(&second, cached, cached_file).await;

    // A small book cached, then a second custody with no room to be made,
    // even with the small one gone: it waits, and the small one stays.
    let small_size = BOOK * 2 / 5;
    let third_keeps_it = [(&before, third.id), (&after, third.id)];
    let (small, small_file) = add_one_ranked("Small", small_size, &third_keeps_it).await;
    third
        .until_keeps(true, KeepReason::Custody, small, small_file)
        .await;
    cache(small, small_file).await;
    let (over, over_file) =
        add_one_ranked("Over", BOOK, &[(&before, second.id), (&after, second.id)]).await;
    second.until_pins("waiting_for_room", 1).await;
    assert!(!second.keeps(KeepReason::Custody, over, over_file).await);
    assert!(
        second.keeps(KeepReason::Cache, small, small_file).await,
        "evicted for nothing"
    );
    assert!(!over_the_limit(&second).await);

    // The third takes the first custody over; once it holds it, the second
    // lets go of it into the cache, and that is room for the one waiting.
    third
        .call("group.pledge_set", json!({ "bytes": 1_000 * GB }))
        .await;
    third
        .until_keeps(true, KeepReason::Custody, kept, kept_file)
        .await;
    second
        .until_keeps(true, KeepReason::Custody, over, over_file)
        .await;
    second.until_pins("waiting_for_room", 0).await;
    assert!(!over_the_limit(&second).await);
}

/// Node `index` of `nodes`, running.
fn node(nodes: &[Option<Node>], index: usize) -> &Node {
    nodes[index].as_ref().expect("a node running")
}

/// The delay before an emergency copy, in the test below.
const EMERGENCY_AFTER: Duration = Duration::from_secs(10);

/// D6's acceptance. Five nodes, `replicas = 3`: the founder's copy and two
/// custodians'. One custodian stopped leaves two copies, and nothing is
/// fetched; both stopped, and one back within the delay, nothing either.
/// Stopped again for longer, the next member in score order after the
/// custodians keeps an emergency copy, and nobody else does.
#[tokio::test(flavor = "multi_thread")]
async fn the_next_member_after_the_custodians_copies_an_item_left_with_one_copy() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let settings = GroupSettings {
        replicas: NonZeroU8::new(3).unwrap(),
        emergency_after_secs: u32::try_from(EMERGENCY_AFTER.as_secs()).unwrap(),
        ..settings()
    };
    let mut nodes: Vec<Option<Node>> = founded(
        dir.path(),
        &[100 * GB; 5],
        (0..5).map(|_| Tuning::default()),
        &settings,
    )
    .await
    .into_iter()
    .map(Some)
    .collect();
    let core: Vec<MemberId> = nodes.iter().map(|node| node.as_ref().unwrap().id).collect();
    let (item, file) = add_a_book(node(&nodes, 0), dir.path(), "Dune", BOOK).await;
    let ranked = CustodyItem {
        item,
        creator: core[0],
        size_bytes: 0,
    };
    let group = CustodyGroup::new(
        3,
        settings.weight_rule(),
        core.iter().map(|id| (*id, 100 * GB)),
    );
    let index_of = |id: MemberId| core.iter().position(|member| *member == id).unwrap();
    let custodians: Vec<usize> = group
        .custodians(&ranked)
        .into_iter()
        .map(index_of)
        .collect();
    let copier = index_of(
        group
            .first_after_custodians(&ranked, |_| true)
            .expect("two members are left after the custodians"),
    );
    let bystander = (1..5)
        .find(|index| !custodians.contains(index) && *index != copier)
        .unwrap();
    for custodian in &custodians {
        node(&nodes, *custodian)
            .until_keeps(true, KeepReason::Custody, item, file)
            .await;
    }
    // One custodian of three away: two copies, so nobody copies.
    let _first_away = nodes[custodians[0]].take().unwrap().stop().await;
    tokio::time::sleep(EMERGENCY_AFTER + Duration::from_secs(3)).await;
    for index in [copier, bystander] {
        assert!(
            !node(&nodes, index)
                .keeps(KeepReason::Emergency, item, file)
                .await,
            "member {index} copied a book that was never alone"
        );
    }

    // The other away too, for longer than the delay: the next in score
    // order copies. A holder back within the delay is `AloneItems`'s test: a
    // restarted node's holdings take as long to reach the others.
    let _second_away = nodes[custodians[1]].take().unwrap().stop().await;
    node(&nodes, copier)
        .until_keeps(true, KeepReason::Emergency, item, file)
        .await;
    // Long enough for a copy the bystander began alongside to land.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !node(&nodes, bystander)
            .keeps(KeepReason::Emergency, item, file)
            .await
    );

    // The founder away too: the copier's copy is the last, so the next
    // after it copies.
    let _founder_away = nodes[0].take().unwrap().stop().await;
    node(&nodes, bystander)
        .until_keeps(true, KeepReason::Emergency, item, file)
        .await;
}

/// **Emergency copies take places among `max_fetches`** (5e-7). The
/// custodian of three books has room for half of one, so all three are left
/// with the founder's copy alone; the other, fetching one item at a time at
/// the group's minimum, keeps an emergency copy of each, one after another:
/// never two tagged while their bytes are still to come.
#[tokio::test(flavor = "multi_thread")]
async fn emergency_copies_are_fetched_within_max_fetches() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let settings = GroupSettings {
        emergency_after_secs: 3,
        ..settings()
    };
    let one_at_a_time = Tuning {
        transfer: TransferConfig {
            max_fetches: NonZeroUsize::MIN,
            max_download_rate: NonZeroU64::new(MIN_TRANSFER_RATE),
            ..TransferConfig::default()
        },
        ..Tuning::default()
    };
    let half_a_book = Tuning {
        storage_limit: NonZeroU64::new(BOOK as u64 / 2),
        ..Tuning::default()
    };
    let tunings = [Tuning::default(), half_a_book, one_at_a_time];
    let nodes = founded(dir.path(), &[100 * GB; 3], tunings, &settings).await;
    let [founder, custodian, copier] = <[Node; 3]>::try_from(nodes).ok().unwrap();
    let group = CustodyGroup::new(
        2,
        settings.weight_rule(),
        [founder.id, custodian.id, copier.id].map(|id| (id, 100 * GB)),
    );
    let mut books = Vec::new();
    for prefix in ["First", "Second", "Third"] {
        let (title, _) = a_title_ranked(prefix, BOOK, founder.id, &[(&group, custodian.id)]);
        books.push(add_a_book(&founder, dir.path(), &title, BOOK).await);
    }

    let blobs = copier.runtime.blobs();
    let mut most_at_once = 0;
    tokio::time::timeout(SOON, async {
        loop {
            let tagged = blobs.kept(KeepReason::Emergency).await.unwrap();
            let mut arriving = 0;
            let mut kept = 0;
            for book in &books {
                if tagged.contains(book) {
                    if blobs.has(book.1).await.unwrap() {
                        kept += 1;
                    } else {
                        arriving += 1;
                    }
                }
            }
            most_at_once = most_at_once.max(arriving);
            if kept == books.len() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the other keeps an emergency copy of every book");
    assert_eq!(most_at_once, 1, "emergency copies fetched past max_fetches");

    for node in [founder, custodian, copier] {
        node.stop().await;
    }
}

/// **An emergency copy stays past the storage limit until the custodian
/// holds the item** (D6, 5e-7). Both others have room for half a book: the
/// custodian waits for room, so the founder's copy stays the only one, and
/// after the delay the other makes the emergency copy all the same. A
/// smaller book the copier is custodian of waits for room there, and evicts
/// nothing of it. The custodian, restarted with room, keeps the book: with
/// the founder's, that is `replicas` copies someone must keep, so the
/// emergency copy moves to the cache, and that is room for the one waiting.
#[tokio::test(flavor = "multi_thread")]
async fn an_emergency_copy_stays_past_the_limit_until_the_custodian_holds_the_item() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let settings = GroupSettings {
        emergency_after_secs: 3,
        ..settings()
    };
    let half_a_book = || Tuning {
        storage_limit: NonZeroU64::new(BOOK as u64 / 2),
        ..Tuning::default()
    };
    let tunings = [Tuning::default(), half_a_book(), half_a_book()];
    let nodes = founded(dir.path(), &[100 * GB; 3], tunings, &settings).await;
    let [founder, second, third] = <[Node; 3]>::try_from(nodes).ok().unwrap();
    let core = [founder.id, second.id, third.id];
    let group = CustodyGroup::new(2, settings.weight_rule(), core.map(|id| (id, 100 * GB)));
    let (item, file) = add_a_book(&founder, dir.path(), "Dune", BOOK).await;
    let ranked = CustodyItem {
        item,
        creator: founder.id,
        size_bytes: 0,
    };
    let (custodian, copier) = if group.custodians(&ranked) == [second.id] {
        (second, third)
    } else {
        (third, second)
    };

    custodian.until_pins("waiting_for_room", 1).await;
    copier
        .until_keeps(true, KeepReason::Emergency, item, file)
        .await;
    assert!(!custodian.keeps(KeepReason::Custody, item, file).await);

    // A smaller book in the copier's custody needs the emergency copy's room.
    // Left alone, the custodian makes an emergency copy of it: an
    // availability change the copier looks at, with its own copy still needed.
    let quarter = BOOK / 4;
    let (title, _) = a_title_ranked("Small", quarter, founder.id, &[(&group, copier.id)]);
    let (small, small_file) = add_a_book(&founder, dir.path(), &title, quarter).await;
    copier.until_pins("waiting_for_room", 1).await;
    copier.until_it_sees(custodian.id, small).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        copier.keeps(KeepReason::Emergency, item, file).await,
        "the emergency copy was evicted while it was still needed"
    );
    assert!(!copier.keeps(KeepReason::Custody, small, small_file).await);

    let (custodians_key, custodians_dir) = custodian.stop().await;
    let custodian = Node::start(custodians_key, &core, custodians_dir).await;
    custodian
        .until_keeps(true, KeepReason::Custody, item, file)
        .await;
    copier
        .until_keeps(false, KeepReason::Emergency, item, file)
        .await;
    copier
        .until_keeps(true, KeepReason::Custody, small, small_file)
        .await;

    for node in [founder, custodian, copier] {
        node.stop().await;
    }
}
