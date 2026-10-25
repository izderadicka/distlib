//! 5c-2a's acceptance (phase 5's D11 and D12): a custodian holding the item
//! passes and nothing is written; one whose bytes changed underneath it fails,
//! its key appears on every node, and its next pass clears the key; an
//! unreachable custodian is skipped; a group founded with `probes` off makes
//! no probes, and neither does a member below the minimum pledge. 5c-2b's:
//! a failed copy that was the item's last real one gets an emergency copy.
//!
//! The founder adds a book; the second member, the only other with a weight,
//! is its custodian and keeps it, and the founder probes it every second.

// Three whole runtimes, a founding election, and a fetch: seconds.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    future::Future,
    num::{NonZeroU8, NonZeroU32},
    path::{Path, PathBuf},
    time::Duration,
};

use distlib::Runtime;
use distlib_core::{
    ContentHash, CustodyGroup, CustodyItem, DataDir, GroupSettings, ItemId, KeepReason, MemberId,
};
use distlib_custody::ProbeCounts;
use distlib_sync::RecordedFailure;
use iroh::SecretKey;
use serde_json::json;
use tempfile::TempDir;

mod common;
use common::{api, config, found, init_logging, until_projected};

const SOON: Duration = Duration::from_secs(60);
const GB: u64 = 1_000_000_000;
/// The size of a book: well over what the blob store keeps inline, so its
/// bytes are a file of their own.
const BOOK: usize = 1_000_000;

/// The founder's, its book's custodian's and a member with no weight's.
const PLEDGES: [u64; 3] = [100 * GB, 100 * GB, 0];

/// Two copies of everything, the creator's one of them; a probe a second.
fn settings() -> GroupSettings {
    GroupSettings {
        replicas: NonZeroU8::new(2).unwrap(),
        min_pledge_bytes: 0,
        settle_secs: 1,
        probe_every_secs: NonZeroU32::MIN,
        ..GroupSettings::default()
    }
}

struct Node {
    runtime: Runtime,
    key: SecretKey,
    id: MemberId,
    dir: DataDir,
}

impl Node {
    async fn failures(&self) -> Vec<RecordedFailure> {
        self.runtime.catalogue().probe_failures().await.unwrap()
    }

    fn probed(&self) -> ProbeCounts {
        self.runtime.probes().counts()
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

/// The founder, its book's custodian, and a member with no weight, founded
/// with `settings` and `pledges`.
async fn three_nodes(dir: &Path, settings: &GroupSettings, pledges: [u64; 3]) -> [Node; 3] {
    init_logging();
    let keys: Vec<SecretKey> = (0..3).map(|_| SecretKey::generate()).collect();
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
    let pledges: Vec<(MemberId, u64)> = ids.into_iter().zip(pledges).collect();
    found(&runtimes, &nodes[0].key, &pledges, settings).await;
    nodes.try_into().ok().unwrap()
}

/// The founder adds a book, which it and the second member have projected:
/// the book and its one file.
async fn add_a_book(founder: &Node, second: &Node, dir: &Path) -> (ItemId, ContentHash) {
    founder.runtime.catalogue().ready().await;
    let file = dir.join("dune.epub");
    std::fs::write(&file, "Dune\n".repeat(BOOK / 5)).unwrap();
    let added = api(&founder.runtime, &founder.key)
        .call(
            "library.add",
            Some(json!({ "kind": "ebook", "files": [&file], "title": "Dune" })),
        )
        .await
        .unwrap();
    let book: ItemId = serde_json::from_value(added["item_id"].clone()).unwrap();
    until_projected(&founder.runtime, book, 1, "the founder").await;
    until_projected(&second.runtime, book, 1, "the second member").await;
    let stored = founder.runtime.store().item(book).await.unwrap().unwrap();
    (book, *stored.item.files.keys().next().unwrap())
}

/// [`three_nodes`], and a book added and kept by its custodian.
async fn a_book_kept(
    dir: &Path,
    settings: &GroupSettings,
    pledges: [u64; 3],
) -> ([Node; 3], ItemId, ContentHash) {
    let [founder, second, third] = three_nodes(dir, settings, pledges).await;
    let (book, blob) = add_a_book(&founder, &second, dir).await;
    until("the custodian holds the book", || async {
        second.runtime.catalogue().holdings().holds(book)
    })
    .await;
    ([founder, second, third], book, blob)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_fails_once_its_bytes_are_not_the_book_until_its_next_pass() {
    let dir = TempDir::new().unwrap();
    let ([founder, second, third], book, blob) =
        a_book_kept(dir.path(), &settings(), PLEDGES).await;

    // Passes, and nothing is written.
    until("the founder's probes pass", || async {
        founder.probed().passed >= 2
    })
    .await;
    assert_eq!(founder.probed().failed, 0);
    assert!(founder.failures().await.is_empty());

    // Its bytes changed underneath a store that still calls them complete.
    let bytes = second.bytes_of(blob);
    let book_bytes = std::fs::read(&bytes).unwrap();
    std::fs::write(&bytes, vec![0; book_bytes.len()]).unwrap();
    for node in [&founder, &second, &third] {
        until("the failure reaches every node", || async {
            node.failures().await.iter().any(|recorded| {
                (recorded.custodian, recorded.prober, recorded.failure.item)
                    == (second.id, founder.id, book)
            })
        })
        .await;
    }

    // Back, and the founder's next pass clears it everywhere.
    std::fs::write(&bytes, &book_bytes).unwrap();
    for node in [&founder, &second, &third] {
        until("the clearing reaches every node", || async {
            node.failures().await.is_empty()
        })
        .await;
    }

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}

/// The delay before an emergency copy, in the test below: longer than the
/// custodian takes to fetch the book, so the founder's copy alone before
/// that is not what makes it.
const EMERGENCY_AFTER_SECS: u32 = 10;

/// All three pledge, so the member after the custodian can keep an emergency
/// copy. The custodian's copy fails, which leaves the founder's the book's
/// last real one: after the delay, the other member keeps an emergency copy.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_copy_that_was_the_last_real_one_gets_an_emergency_copy() {
    let dir = TempDir::new().unwrap();
    let settings = GroupSettings {
        emergency_after_secs: EMERGENCY_AFTER_SECS,
        ..settings()
    };
    let [founder, second, third] = three_nodes(dir.path(), &settings, [100 * GB; 3]).await;
    let (book, blob) = add_a_book(&founder, &second, dir.path()).await;
    let group = CustodyGroup::new(
        2,
        settings.weight_rule(),
        [founder.id, second.id, third.id].map(|id| (id, 100 * GB)),
    );
    let custodians = group.custodians(&CustodyItem {
        item: book,
        creators: vec![founder.id],
        size_bytes: 0,
    });
    let (custodian, other) = if custodians == [second.id] {
        (&second, &third)
    } else {
        (&third, &second)
    };
    until("the custodian holds the book", || async {
        custodian.runtime.catalogue().holdings().holds(book)
    })
    .await;
    let emergency_copy = || async {
        let blobs = other.runtime.blobs();
        blobs
            .kept(KeepReason::Cache)
            .await
            .unwrap()
            .contains(&(book, blob))
            && blobs.has(blob).await.unwrap()
    };
    assert!(
        !emergency_copy().await,
        "copied while the custodian was fetching: the delay is too short"
    );

    let bytes = custodian.bytes_of(blob);
    let book_bytes = std::fs::read(&bytes).unwrap();
    std::fs::write(&bytes, vec![0; book_bytes.len()]).unwrap();
    until("the other member keeps an emergency copy", emergency_copy).await;
    assert!(
        founder
            .failures()
            .await
            .iter()
            .any(|recorded| recorded.custodian == custodian.id),
        "the failure is recorded"
    );

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_custodian_is_skipped_not_failed() {
    let dir = TempDir::new().unwrap();
    let ([founder, second, third], _book, _blob) =
        a_book_kept(dir.path(), &settings(), PLEDGES).await;
    until("the founder's probes pass", || async {
        founder.probed().passed >= 1
    })
    .await;

    second.runtime.shutdown().await;
    until("the founder skips the stopped custodian", || async {
        founder.probed().skipped >= 1
    })
    .await;
    assert_eq!(founder.probed().failed, 0);
    assert!(founder.failures().await.is_empty());

    for node in [founder, third] {
        node.runtime.shutdown().await;
    }
}

/// The custodian never fetches the book, so never announces it: nobody probes
/// it for a book it does not claim, which is coverage's business (D10).
#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_that_does_not_announce_the_item_is_not_probed() {
    let dir = TempDir::new().unwrap();
    let [founder, second, third] = three_nodes(dir.path(), &settings(), PLEDGES).await;
    second.runtime.pins().shutdown();
    add_a_book(&founder, &second, dir.path()).await;

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(founder.probed(), ProbeCounts::default());
    assert!(founder.failures().await.is_empty());

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_founded_without_probes_makes_none() {
    let dir = TempDir::new().unwrap();
    let probes_off = GroupSettings {
        probes: false,
        ..settings()
    };
    let ([founder, second, third], _book, _blob) =
        a_book_kept(dir.path(), &probes_off, PLEDGES).await;

    // Long enough for a few probes, a second apart, had there been any.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(founder.probed(), ProbeCounts::default());

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}

/// The founder pledges 60 GB of the group's 100 GB minimum: its add is warned,
/// not refused, but it is not sent content, so its probes would all fail.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_below_the_minimum_pledge_makes_no_probes() {
    let dir = TempDir::new().unwrap();
    let minimum = GroupSettings {
        min_pledge_bytes: 100 * GB,
        ..settings()
    };
    let ([founder, second, third], _book, _blob) =
        a_book_kept(dir.path(), &minimum, [60 * GB, 100 * GB, 0]).await;

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(founder.probed(), ProbeCounts::default());

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}
