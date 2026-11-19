//! What every whole-process test here needs to stand a node up.
//!
//! Shared rather than copied because these three are the *arrangement* the
//! tests agree on, not incidental setup: both files bind to an ephemeral
//! loopback port with relays off, both leave the configured core addresses
//! empty so the founding entry is what says where anybody is, and both read the
//! bound address back off the runtime rather than predicting it.

#![allow(dead_code)] // each test file uses a subset; the module is shared

/// Running the binary itself, for the tests that drive the commands rather
/// than the library.
pub mod process;

use std::{
    borrow::Borrow,
    collections::BTreeMap,
    fmt::Debug,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU32,
    path::Path,
    sync::Arc,
    time::Duration,
};

use distlib::Runtime;
use distlib_api::Api;
use distlib_consensus::{MemberProfile, MembershipEvent};
use distlib_core::{
    Config, ContentHash, CoreMember, DataDir, FileRecord, GroupSettings, Item, ItemId, MemberId,
    NetConfig, NodeAddr,
};
use iroh::SecretKey;
use serde_json::json;
use tokio::sync::{broadcast, watch};

/// An item of one made-up file told apart by `n`, with nothing else said:
/// its own file list, so it is an item (P5-66).
pub fn listed_item(n: u32) -> Item {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&n.to_le_bytes());
    let file = FileRecord {
        format: "epub".to_owned(),
        size: 1,
        filename: format!("{n}.epub"),
        seq: None,
        disc: None,
        title: None,
        duration: None,
    };
    Item::of_files(BTreeMap::from([(ContentHash::from_bytes(hash), file)])).expect("one file")
}

/// Waits until a stopped runtime has let go of every database in `data_dir`.
///
/// Dropping a runtime does not release them at once: tasks it spawned are
/// aborted, and finish being dropped a moment later. Starting again on the
/// same directory before then is refused the lock — found on macOS CI.
pub async fn until_released(data_dir: &DataDir) {
    let databases = [
        data_dir.raft_db(),
        data_dir.blobs_dir().join("blobs.db"),
        data_dir.docs_dir().join("docs.redb"),
    ];
    let still_open = || async {
        databases
            .iter()
            .find(|database| {
                matches!(
                    redb::Database::create(database),
                    Err(redb::DatabaseError::DatabaseAlreadyOpen)
                )
            })
            .cloned()
    };
    until_within(
        Duration::from_secs(30),
        "the stopped runtime lets go of its databases",
        still_open,
        Option::is_none,
    )
    .await;
}

/// Logs to this test's output, filtered by `RUST_LOG`.
///
/// CI sets `RUST_LOG`, and nextest shows a test's output only when it fails,
/// so a failure that will not reproduce arrives with the log of what led to
/// it. Unset, only errors are logged. One subscriber serves every node in the
/// process: the first call installs it and the rest do nothing.
pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// A follower's configuration: one core node, and where it is.
///
/// The address has to be here. A follower has no log yet, so configuration is
/// the only thing that can say where the group is — and with
/// `relay_mode = "disabled"` there is no lookup to fall back on. This is the
/// ticket's job in production; a test hands over the same two facts directly.
pub fn following(core: MemberId, addr: &NodeAddr) -> Config {
    let mut config = config(&[core]);
    config.consensus.core = vec![CoreMember {
        member: core,
        name: String::new(),
        addrs: addr.direct.iter().copied().collect(),
        relay: None,
    }];
    config
}

/// A node's configuration: `core` in the core group, nothing else on.
///
/// The configured addresses are empty on purpose. Before there is a log,
/// configuration is the only thing that says who votes — but *where* they are
/// is written into the founding entry a moment later, and every node reads it
/// from there. The same arrangement the consensus test harness uses.
pub fn config(core: &[MemberId]) -> Config {
    let mut config = Config::default();
    config.net.bind_addr_v4 = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.net.relay_mode = distlib_core::RelayMode::Disabled;
    config.api.enabled = false;
    config.consensus.core = core
        .iter()
        .map(|member| CoreMember {
            member: *member,
            name: String::new(),
            addrs: Vec::new(),
            relay: None,
        })
        .collect();
    config
}

/// What a test's founder pledges: below the default weight step, so it weighs
/// nothing and is custodian of nothing it did not add, yet meets
/// [`group_settings`]'s minimum, so it may add.
pub const PLEDGE: u64 = 1_000_000_000;

/// The settings a test group is founded with: the defaults, with a minimum
/// pledge that [`PLEDGE`] meets.
pub fn group_settings() -> GroupSettings {
    GroupSettings {
        min_pledge_bytes: PLEDGE,
        ..GroupSettings::default()
    }
}

/// A member's profile for founding or admission; the member pledges itself,
/// see [`pledge`].
pub fn profile(id: MemberId, name: &str) -> MemberProfile {
    MemberProfile {
        member_id: id,
        display_name: name.to_owned(),
    }
}

/// The member of `key` pledges `pledge_bytes`, through `runtime`'s node.
pub async fn pledge(runtime: &Runtime, key: &SecretKey, pledge_bytes: u64) {
    let event = MembershipEvent::PledgeChanged {
        member: MemberId::from(key.public()),
        pledge_bytes,
    };
    runtime.node().propose(event, key).await.unwrap();
}

pub fn bound(runtime: &Runtime) -> NodeAddr {
    NodeAddr {
        relay: None,
        direct: runtime.endpoint().bound_sockets().into_iter().collect(),
    }
}

/// Waits until this node's read model holds `item` with its `files` files —
/// which is what `library.download` reads, so a download asked for any sooner
/// is asking about an item this node genuinely does not have yet.
pub async fn until_projected(runtime: &Runtime, item: ItemId, files: usize, who: &str) {
    until(
        &format!("{who} projects the item with its {files} file(s)"),
        || async { runtime.store().item(item).await.unwrap() },
        |stored| {
            stored
                .as_ref()
                .is_some_and(|stored| stored.item.files.len() == files)
        },
    )
    .await;
}

/// An `Api` wired to `runtime`'s own pieces: what `distlib::commands::run`
/// assembles, minus the HTTP listener, for tests that call `Api::call`.
pub fn api(runtime: &Runtime, key: &SecretKey) -> Api {
    Api {
        node: Arc::clone(runtime.node()),
        secret: key.clone(),
        net: NetConfig::default(),
        reindex_handle: runtime.projection().reindex_handle(),
        catalogue: runtime.catalogue().clone(),
        blobs: runtime.blobs().clone(),
        store: runtime.store().clone(),
        search: runtime.search().clone(),
        availability: runtime.availability().clone(),
        tasks: runtime.tasks().clone(),
        downloads: runtime.downloads().to_path_buf(),
        uploads: runtime.uploads().clone(),
        custody: runtime.custody().clone(),
        coverage: runtime.coverage().clone(),
        probes: runtime.probes().clone(),
        pins: runtime.pins().counts(),
        storage: runtime.storage().clone(),
        collections: runtime.collections(),
    }
}

/// Long enough for in-process nodes to elect, replicate, reconcile, project,
/// fetch and collect.
pub const SOON: Duration = Duration::from_secs(60);

/// How long a wait sleeps between two reads.
const READ_AGAIN_AFTER: Duration = Duration::from_millis(50);

/// Reads `read` until `done` holds of what it returns, and returns that.
///
/// Past [`SOON`] it panics with `what` it waited for and the last value it
/// read, so a failure says how far things got.
pub async fn until<T: Debug, F: Future<Output = T>>(
    what: &str,
    read: impl FnMut() -> F,
    done: impl Fn(&T) -> bool,
) -> T {
    until_within(SOON, what, read, done).await
}

/// [`until`], giving up after `within`.
pub async fn until_within<T: Debug, F: Future<Output = T>>(
    within: Duration,
    what: &str,
    mut read: impl FnMut() -> F,
    done: impl Fn(&T) -> bool,
) -> T {
    let mut last = None;
    let reached = tokio::time::timeout(within, async {
        loop {
            let value = read().await;
            if done(&value) {
                return value;
            }
            last = Some(value);
            tokio::time::sleep(READ_AGAIN_AFTER).await;
        }
    })
    .await;
    reached.unwrap_or_else(|_| panic!("{what} within {within:?}; last read: {last:?}"))
}

/// Reads `events` until one is `wanted`, and returns it; one missed to a lag
/// is passed over.
///
/// Past [`SOON`] it panics with `what` it waited for and the last event it
/// heard.
pub async fn until_heard<T: Debug + Clone>(
    what: &str,
    events: &mut broadcast::Receiver<T>,
    wanted: impl Fn(&T) -> bool,
) -> T {
    let mut last = None;
    let heard = tokio::time::timeout(SOON, async {
        loop {
            match events.recv().await {
                Ok(event) if wanted(&event) => return event,
                Ok(event) => last = Some(event),
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => panic!("{what}: the bus closed"),
            }
        }
    })
    .await;
    heard.unwrap_or_else(|_| panic!("{what} within {SOON:?}; last heard: {last:?}"))
}

/// Waits until `done` holds of what `watched` holds, and returns a copy.
///
/// Past [`SOON`] it panics with `what` it waited for and what `watched` held
/// then.
pub async fn until_watched<T: Debug + Clone>(
    what: &str,
    watched: &mut watch::Receiver<T>,
    done: impl FnMut(&T) -> bool,
) -> T {
    until_watched_within(SOON, what, watched, done).await
}

/// [`until_watched`], giving up after `within`.
pub async fn until_watched_within<T: Debug + Clone>(
    within: Duration,
    what: &str,
    watched: &mut watch::Receiver<T>,
    done: impl FnMut(&T) -> bool,
) -> T {
    let reached = tokio::time::timeout(within, watched.wait_for(done))
        .await
        .map(|reached| {
            reached
                .expect("the watched value outlives the wait")
                .clone()
        });
    reached.unwrap_or_else(|_| {
        panic!(
            "{what} within {within:?}; last held: {:?}",
            *watch::Receiver::borrow(watched)
        )
    })
}

/// A configuration that collects every second.
pub fn collecting(mut config: Config) -> Config {
    config.storage.gc_interval_secs = NonZeroU32::MIN;
    config
}

/// Waits for `runtime` to collect a blob nothing keeps: a file imported and
/// let go, as a refused `library.add` leaves one.
pub async fn until_a_pass_collects(runtime: &Runtime, dir: &Path, marker: &str) {
    let path = dir.join(marker);
    std::fs::write(&path, marker).unwrap();
    let hash = runtime.catalogue().add_file(&path).await.unwrap().hash;
    until(
        &format!("{marker} is collected"),
        || async { runtime.blobs().has(hash).await.unwrap() },
        |here| !here,
    )
    .await;
}

/// Downloads all of `item` through `api`, waiting until the task finishes.
pub async fn download(api: &Api, item: ItemId, dest: &Path) {
    let started = api
        .call(
            "library.download",
            Some(json!({ "item_id": item, "dest": dest })),
        )
        .await
        .unwrap();
    let task = json!({ "task_id": started["task_id"] });
    let ended = until(
        "the download ends",
        || async { api.call("library.task", Some(task.clone())).await.unwrap() },
        |state| state["state"] != "running",
    )
    .await;
    assert_eq!(
        ended["state"], "finished",
        "the download did not finish: {ended}"
    );
}

/// Founds the group of `runtimes`, the first key founding, then each member
/// pledging what `pledges` says; a member given 0 does not pledge. Returns once
/// every node's membership has every pledge.
pub async fn found<R: Borrow<Runtime>>(
    runtimes: &[R],
    pledges: &[(&SecretKey, u64)],
    settings: &GroupSettings,
) {
    let founders = runtimes
        .iter()
        .zip(pledges)
        .map(|(runtime, (key, _))| {
            let member_id = MemberId::from(key.public());
            (
                profile(member_id, &member_id.to_string()),
                bound(runtime.borrow()),
            )
        })
        .collect();
    let founder = runtimes[0].borrow();
    founder
        .node()
        .init_group(founders, settings.clone(), pledges[0].0)
        .await
        .unwrap();
    for (key, pledge_bytes) in pledges.iter().filter(|(_, bytes)| *bytes > 0) {
        pledge(founder, key, *pledge_bytes).await;
    }
    let wanted: Vec<Option<u64>> = pledges.iter().map(|(_, bytes)| Some(*bytes)).collect();
    for runtime in runtimes {
        let pledged = || async {
            let membership = runtime.borrow().node().membership();
            pledges
                .iter()
                .map(|(key, _)| {
                    let member = membership.member(&MemberId::from(key.public()));
                    member.map(|record| record.pledge_bytes)
                })
                .collect::<Vec<_>>()
        };
        until(
            "every node learns every founder's pledge",
            pledged,
            |seen| *seen == wanted,
        )
        .await;
    }
}

/// [`found`], returning once each node's custody counts the pledges too.
///
/// Custody takes the founding group at once, but a pledge made after it only
/// once the membership has been still for `settle_secs`. A node's own pledge
/// stands for the whole group's: the pledges land within milliseconds of each
/// other, well inside any test's `settle_secs`, so they settle together.
pub async fn found_and_settled<R: Borrow<Runtime>>(
    runtimes: &[R],
    pledges: &[(&SecretKey, u64)],
    settings: &GroupSettings,
) {
    found(runtimes, pledges, settings).await;
    until_custody_counts_pledges(runtimes, pledges).await;
}

/// Waits until each of `runtimes`' custody counts its own pledge in `pledges`.
async fn until_custody_counts_pledges<R: Borrow<Runtime>>(
    runtimes: &[R],
    pledges: &[(&SecretKey, u64)],
) {
    for (runtime, (_, pledge_bytes)) in runtimes.iter().zip(pledges) {
        let counted = || async {
            let status = runtime.borrow().custody().status();
            status.map(|status| status.pledge_bytes)
        };
        until(
            "custody counts the node's pledge once it has settled",
            counted,
            |counted| *counted == Some(*pledge_bytes),
        )
        .await;
    }
}
