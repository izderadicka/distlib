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
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU32,
    path::Path,
    sync::Arc,
    time::Duration,
};

use distlib::Runtime;
use distlib_api::Api;
use distlib_consensus::MemberRecord;
use distlib_core::{
    Config, CoreMember, DataDir, GroupSettings, ItemId, MemberId, NetConfig, NodeAddr,
};
use iroh::SecretKey;
use serde_json::json;

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
    tokio::time::timeout(Duration::from_secs(30), async {
        for database in &databases {
            while matches!(
                redb::Database::create(database),
                Err(redb::DatabaseError::DatabaseAlreadyOpen)
            ) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await
    .expect("the stopped runtime to let go of its databases");
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

pub fn record(id: MemberId, name: &str) -> MemberRecord {
    MemberRecord {
        member_id: id,
        display_name: name.to_owned(),
        pledge_bytes: PLEDGE,
    }
}

pub fn bound(runtime: &Runtime) -> NodeAddr {
    NodeAddr {
        relay: None,
        direct: runtime.endpoint().bound_sockets().into_iter().collect(),
    }
}

/// How long a test waits for a node to project an item it was sent.
const PROJECTED_WITHIN: Duration = SOON;

/// Waits until this node's read model holds `item` with all `files` of its
/// files — which is what `library.download` reads, so a download asked for
/// any sooner is asking about an item this node genuinely does not have yet.
///
/// **The file count, not merely the item.** An item's row appears as soon as
/// any one of its entries has been projected, and its per-file rows are
/// separate entries that arrive on their own schedule. Waiting only for the
/// row is waiting for the title.
pub async fn until_projected(runtime: &Runtime, item: ItemId, files: usize, who: &str) {
    tokio::time::timeout(PROJECTED_WITHIN, async {
        loop {
            if let Some(stored) = runtime.store().item(item).await.unwrap()
                && stored.item.files.len() == files
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{who} never projected the item with its {files} file(s)"));
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
        pins: runtime.pins().counts(),
        storage: runtime.storage().clone(),
        collections: runtime.collections(),
    }
}

/// Long enough for in-process nodes to elect, replicate, reconcile, project,
/// fetch and collect.
const SOON: Duration = Duration::from_secs(60);

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
    tokio::time::timeout(SOON, async {
        while runtime.blobs().has(hash).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{marker} was never collected"));
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
    tokio::time::timeout(SOON, async {
        loop {
            let state = api.call("library.task", Some(task.clone())).await.unwrap();
            match state["state"].as_str() {
                Some("running") => tokio::time::sleep(Duration::from_millis(50)).await,
                Some("finished") => return,
                _ => panic!("the download did not finish: {state}"),
            }
        }
    })
    .await
    .unwrap();
}

/// Founds the group of `runtimes`, each member pledging what `pledges` says.
pub async fn found<R: Borrow<Runtime>>(
    runtimes: &[R],
    founder: &SecretKey,
    pledges: &[(MemberId, u64)],
    settings: &GroupSettings,
) {
    let founders = runtimes
        .iter()
        .zip(pledges)
        .map(|(runtime, (member_id, pledge_bytes))| {
            let runtime = runtime.borrow();
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
        .borrow()
        .node()
        .init_group(founders, settings.clone(), founder)
        .await
        .unwrap();
}
