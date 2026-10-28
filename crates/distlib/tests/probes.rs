//! 5c-2a's acceptance (phase 5's D11 and D12): a custodian holding the item
//! passes and nothing is written; one whose bytes changed underneath it fails,
//! its key appears on every node, and its next pass clears the key; an
//! unreachable custodian is skipped; a group founded with `probes` off makes
//! no probes, and neither does a member below the minimum pledge. 5c-2b's:
//! a failed copy that was the item's last real one gets an emergency copy.
//! 5c-2c's: a failure another member recorded is re-checked at its chunk.
//! 5c-3a's: failures from two probers turn a member red on every node, and
//! one prober's alone does not; check now probes a member at once. 5c-3b's:
//! an expulsion with evidence names the failures, within `MAX_REASON`.
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
    ProbeFailure,
};
use distlib_custody::ProbeCounts;
use distlib_sync::RecordedFailure;
use iroh::SecretKey;
use serde_json::{Value, json};
use tempfile::TempDir;

mod common;
use common::{api, config, found_and_settled, init_logging, until_projected};

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

    async fn call(&self, method: &str, params: Value) -> Result<Value, distlib_api::rpc::Error> {
        api(&self.runtime, &self.key)
            .call(method, Some(params))
            .await
    }

    /// Whether its `group.members` says `member` is red.
    async fn says_red(&self, member: MemberId) -> bool {
        let members = self.call("group.members", json!({})).await.unwrap();
        members["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|listed| listed["member"] == json!(member) && listed["red"] == json!(true))
    }

    /// How many probers' failures of `member` its `group.suspects` lists.
    async fn failed_by(&self, member: MemberId) -> usize {
        let listed = self.call("group.suspects", json!({})).await.unwrap();
        listed["suspects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|suspect| suspect["member"] == json!(member))
            .map_or(0, |suspect| suspect["failures"].as_array().unwrap().len())
    }

    async fn check(&self, member: MemberId) -> Value {
        self.call("group.check", json!({ "member": member }))
            .await
            .unwrap()["outcome"]
            .clone()
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
    let pledges: Vec<(&SecretKey, u64)> = nodes.iter().map(|node| &node.key).zip(pledges).collect();
    found_and_settled(&runtimes, &pledges, settings).await;
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
    // Every chunk is wrong, so the next probe fails too, and is counted.
    until("the failures are counted in a row", || async {
        founder
            .failures()
            .await
            .iter()
            .any(|recorded| recorded.failure.in_a_row >= 2)
    })
    .await;

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

/// The chunk the tests below make wrong, of the book's 977.
const CHUNK: u64 = 500;

/// The custodian's copy is wrong in one chunk of the book's 977, and the
/// third member has recorded a failure there. Random probes would come to that
/// chunk once in 977; the founder's suspect share re-checks it in seconds,
/// fails it too, and records its own failure at it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failure_another_member_recorded_is_rechecked_at_its_chunk() {
    let dir = TempDir::new().unwrap();
    let ([founder, second, third], book, blob) =
        a_book_kept(dir.path(), &settings(), PLEDGES).await;

    let bytes = second.bytes_of(blob);
    let mut book_bytes = std::fs::read(&bytes).unwrap();
    let start = usize::try_from(CHUNK * 1024).unwrap();
    book_bytes[start..start + 1024].fill(0);
    std::fs::write(&bytes, &book_bytes).unwrap();
    let failure = ProbeFailure {
        item: book,
        blob,
        chunk: CHUNK,
        in_a_row: 1,
    };
    third.runtime.catalogue().ready().await;
    third
        .runtime
        .catalogue()
        .record_probe_failure(second.id, &failure)
        .await
        .unwrap();

    until("the founder fails the custodian at that chunk", || async {
        founder.failures().await.iter().any(|recorded| {
            (recorded.custodian, recorded.prober, recorded.failure.chunk)
                == (second.id, founder.id, CHUNK)
        })
    })
    .await;

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}

/// The founder's failure of the second member alone does not make it red on
/// any node; the third's as well does, on every node. Probes are off, so
/// nothing clears the two written here.
#[tokio::test(flavor = "multi_thread")]
async fn two_probers_turn_a_member_red_on_every_node_and_one_does_not() {
    let dir = TempDir::new().unwrap();
    let probes_off = GroupSettings {
        probes: false,
        ..settings()
    };
    let nodes = three_nodes(dir.path(), &probes_off, PLEDGES).await;
    let [founder, second, third] = &nodes;
    founder.runtime.catalogue().ready().await;
    let failure = ProbeFailure {
        item: ItemId::from_bytes([1; 32]),
        blob: ContentHash::from_bytes([2; 32]),
        chunk: 0,
        in_a_row: 1,
    };

    founder
        .runtime
        .catalogue()
        .record_probe_failure(second.id, &failure)
        .await
        .unwrap();
    for node in &nodes {
        until("the founder's failure reaches every node", || async {
            node.failed_by(second.id).await == 1
        })
        .await;
        assert!(!node.says_red(second.id).await, "one prober is not enough");
    }

    third
        .runtime
        .catalogue()
        .record_probe_failure(second.id, &failure)
        .await
        .unwrap();
    for node in &nodes {
        until("the second member is red on every node", || async {
            node.says_red(second.id).await
        })
        .await;
        assert!(!node.says_red(founder.id).await);
    }

    for node in nodes {
        node.runtime.shutdown().await;
    }
}

/// The founder and the third member have each recorded a failure of the
/// second. An expulsion with evidence proposes with a reason that names both,
/// after the reason given, within the log's `MAX_REASON`; one of a member
/// with no failures, or without a reason, is refused.
#[tokio::test(flavor = "multi_thread")]
async fn an_expulsion_with_evidence_names_the_failures_within_the_limit() {
    let dir = TempDir::new().unwrap();
    let probes_off = GroupSettings {
        probes: false,
        ..settings()
    };
    let [founder, second, third] = three_nodes(dir.path(), &probes_off, PLEDGES).await;
    let failure = ProbeFailure {
        item: ItemId::from_bytes([1; 32]),
        blob: ContentHash::from_bytes([2; 32]),
        chunk: 7,
        in_a_row: 2,
    };
    for prober in [&founder, &third] {
        let catalogue = prober.runtime.catalogue();
        catalogue.ready().await;
        catalogue
            .record_probe_failure(second.id, &failure)
            .await
            .unwrap();
    }
    until("both failures reach the founder", || async {
        founder.failed_by(second.id).await == 2
    })
    .await;

    for refused in [
        json!({ "member": third.id, "with_evidence": true }),
        json!({ "member": second.id }),
    ] {
        assert!(
            founder.call("group.propose_expel", refused).await.is_err(),
            "no evidence to give, or no reason"
        );
    }
    let proposed = founder
        .call(
            "group.propose_expel",
            json!({ "member": second.id, "reason": "lost its disk", "with_evidence": true }),
        )
        .await
        .unwrap();
    let reason = proposed["reason"].as_str().unwrap();
    assert!(
        reason.starts_with("lost its disk — failed probes by 2 members within 7 days: "),
        "{reason}"
    );
    for prober in [&founder, &third] {
        assert!(
            reason.contains(&format!("by {}", prober.id.fmt_short())),
            "{reason}"
        );
    }
    assert!(reason.contains("chunk 7, 2 in a row"), "{reason}");
    assert!(reason.len() <= distlib_consensus::MAX_REASON);
    let pending = founder.call("group.pending", json!({})).await.unwrap();
    assert_eq!(
        pending["pending"].as_array().unwrap().len(),
        1,
        "a core member's expulsion waits for the others"
    );

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}

/// A node knows its group a moment before its catalogue opens, and in no
/// group it has none: nothing is red then, and nobody a suspect.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_its_catalogue_lists_no_suspects() {
    init_logging();
    let dir = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let me: MemberId = key.public().into();
    let runtime = Runtime::start(&key, &config(&[me]), &DataDir::new(dir.path().join("0")))
        .await
        .unwrap();
    let api = api(&runtime, &key);

    let members = api.call("group.members", None).await.unwrap();
    assert_eq!(members["members"], json!([]));
    let listed = api.call("group.suspects", None).await.unwrap();
    assert_eq!(listed["suspects"], json!([]));

    runtime.shutdown().await;
}

/// Probes come an hour apart, so only check now makes one: it passes, fails
/// once the custodian's bytes are wrong and records the failure, and passes
/// again once they are back, which clears it.
#[tokio::test(flavor = "multi_thread")]
async fn check_now_probes_a_member_at_once_and_records_how_it_came_out() {
    let dir = TempDir::new().unwrap();
    let hourly = GroupSettings {
        probe_every_secs: NonZeroU32::new(3600).unwrap(),
        ..settings()
    };
    let ([founder, second, third], book, blob) = a_book_kept(dir.path(), &hourly, PLEDGES).await;
    until("the founder sees the custodian hold the book", || async {
        founder.check(second.id).await == json!("passed")
    })
    .await;

    let bytes = second.bytes_of(blob);
    let book_bytes = std::fs::read(&bytes).unwrap();
    std::fs::write(&bytes, vec![0; book_bytes.len()]).unwrap();
    assert_eq!(founder.check(second.id).await, json!("failed"));
    assert_eq!(founder.failed_by(second.id).await, 1);

    std::fs::write(&bytes, &book_bytes).unwrap();
    assert_eq!(founder.check(second.id).await, json!("passed"));
    assert!(founder.failures().await.is_empty());

    // Wrong in one chunk of 977, where the third member recorded a failure:
    // the check goes to that chunk, where a random one would pass.
    let mut one_chunk_wrong = book_bytes.clone();
    let start = usize::try_from(CHUNK * 1024).unwrap();
    one_chunk_wrong[start..start + 1024].fill(0);
    std::fs::write(&bytes, &one_chunk_wrong).unwrap();
    let failure = ProbeFailure {
        item: book,
        blob,
        chunk: CHUNK,
        in_a_row: 1,
    };
    third.runtime.catalogue().ready().await;
    third
        .runtime
        .catalogue()
        .record_probe_failure(second.id, &failure)
        .await
        .unwrap();
    until("the third member's failure reaches the founder", || async {
        founder.failed_by(second.id).await == 1
    })
    .await;
    assert_eq!(founder.check(second.id).await, json!("failed"));
    assert!(
        founder
            .failures()
            .await
            .iter()
            .any(|recorded| recorded.prober == founder.id && recorded.failure.chunk == CHUNK)
    );

    assert!(
        founder
            .call("group.check", json!({ "member": founder.id }))
            .await
            .is_err(),
        "a node does not probe itself"
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
    assert!(
        founder
            .call("group.check", json!({ "member": second.id }))
            .await
            .is_err(),
        "nor checks"
    );

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}

/// The third member has not pledged, below the group's 100 GB minimum — the
/// only way to be below it, as a pledge is never set lower (P5-40). It is not
/// sent content, so its probes would all fail.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_below_the_minimum_pledge_makes_no_probes() {
    let dir = TempDir::new().unwrap();
    let minimum = GroupSettings {
        min_pledge_bytes: 100 * GB,
        ..settings()
    };
    let ([founder, second, third], book, _blob) =
        a_book_kept(dir.path(), &minimum, [100 * GB, 100 * GB, 0]).await;
    until_projected(&third.runtime, book, 1, "the third member").await;

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(third.probed(), ProbeCounts::default());

    for node in [founder, second, third] {
        node.runtime.shutdown().await;
    }
}
