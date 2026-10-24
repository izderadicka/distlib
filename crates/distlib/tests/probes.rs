//! 5c-2a's acceptance (phase 5's D11 and D12): a custodian holding the item
//! passes and nothing is written; one whose bytes changed underneath it fails,
//! its key appears on every node, and its next pass clears the key; an
//! unreachable custodian is skipped; a group founded with `probes` off makes
//! no probes.
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
use distlib_core::{ContentHash, DataDir, GroupSettings, ItemId, MemberId};
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

/// Two copies of everything, the creator's one of them; a probe a second.
fn settings(probes: bool) -> GroupSettings {
    GroupSettings {
        replicas: NonZeroU8::new(2).unwrap(),
        min_pledge_bytes: 0,
        settle_secs: 1,
        probes,
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

/// The founder, its book's custodian, and a member with no weight; the book
/// added, and kept by its custodian.
async fn a_book_kept(dir: &Path, probes: bool) -> ([Node; 3], ItemId, ContentHash) {
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
    let pledges: Vec<(MemberId, u64)> = ids.into_iter().zip([100 * GB, 100 * GB, 0]).collect();
    found(&runtimes, &nodes[0].key, &pledges, &settings(probes)).await;
    let [founder, second, third]: [Node; 3] = nodes.try_into().ok().unwrap();

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
    until_projected(&second.runtime, book, 1, "the second member").await;
    let stored = founder.runtime.store().item(book).await.unwrap().unwrap();
    let blob = *stored.item.files.keys().next().unwrap();
    until("the custodian holds the book", || async {
        second.runtime.catalogue().holdings().holds(book)
    })
    .await;
    ([founder, second, third], book, blob)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_custodian_fails_once_its_bytes_are_not_the_book_until_its_next_pass() {
    let dir = TempDir::new().unwrap();
    let ([founder, second, third], book, blob) = a_book_kept(dir.path(), true).await;

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

#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_custodian_is_skipped_not_failed() {
    let dir = TempDir::new().unwrap();
    let ([founder, second, third], _book, _blob) = a_book_kept(dir.path(), true).await;
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

#[tokio::test(flavor = "multi_thread")]
async fn a_group_founded_without_probes_makes_none() {
    let dir = TempDir::new().unwrap();
    let ([founder, second, third], _book, _blob) = a_book_kept(dir.path(), false).await;

    // Long enough for a few probes, a second apart, had there been any.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(founder.probed(), ProbeCounts::default());

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}
