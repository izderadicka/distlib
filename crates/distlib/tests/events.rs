//! What the read model tells its watchers: 3a-2's catalogue tap.
//!
//! Driven through `Runtime`, the way `distlib run` assembles a node, and read
//! straight off its bus. The HTTP half — that a watcher of `/events` receives
//! what is on the bus — is `distlib-api`'s own test; what is in question here
//! is what the projection puts there, and when.

// Whole runtimes with docs engines and on-disk stores: seconds, not
// milliseconds. Skipped by `--no-default-features`.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{path::Path, time::Duration};

use distlib::Runtime;
use distlib_core::{DataDir, Event, Item, ItemId, MemberId};
use iroh::SecretKey;
use tempfile::TempDir;
use tokio::sync::broadcast::{self, error::TryRecvError};

mod common;
use common::{bound, config, record};

/// Long enough for two in-process nodes to elect, replicate and reconcile, and
/// for the projection behind that to catch up.
const SOON: Duration = Duration::from_secs(60);

/// An item id for each `n`, and more of them than a byte of seed gives.
fn id(n: u16) -> ItemId {
    let mut bytes = [0; 32];
    bytes[..2].copy_from_slice(&n.to_le_bytes());
    bytes[31] = 1;
    ItemId::from_bytes(bytes)
}

fn titled(n: u16, title: &str) -> Item {
    Item {
        title: Some(title.to_owned()),
        ..Item::new(id(n))
    }
}

/// Waits until `runtime`'s projection is past its start-up replay and
/// publishing.
///
/// `catalogue.ready()` returning is not that. The projection subscribes to the
/// document and then replays it, and a write that lands before the
/// subscription is picked up silently by the replay — so a sentinel written
/// once may never be heard of. It is rewritten until it is, which can only
/// happen once the projection is reading the change stream.
async fn live(runtime: &Runtime) {
    let mut events = runtime.events().subscribe();
    let sentinel = id(u16::MAX);
    let heard = tokio::time::timeout(SOON, async {
        for attempt in 0_u32.. {
            runtime
                .catalogue()
                .write(&Item {
                    title: Some(format!("sentinel {attempt}")),
                    ..Item::new(sentinel)
                })
                .await
                .unwrap();
            let answered = tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if let Ok(Event::ItemAdded { item_id } | Event::ItemChanged { item_id }) =
                        events.recv().await
                        && item_id == sentinel
                    {
                        return;
                    }
                }
            })
            .await;
            if answered.is_ok() {
                return;
            }
        }
    })
    .await;
    heard.expect("the projection never went live");
}

/// One node, founded as a group of one, with its catalogue open.
async fn a_node(dir: &Path) -> Runtime {
    let key = SecretKey::generate();
    let id = MemberId::from(key.public());
    let runtime = Runtime::start(&key, &config(&[id]), &DataDir::new(dir.join("solo")))
        .await
        .unwrap();
    runtime
        .node()
        .init_group(vec![(record(id, "solo"), bound(&runtime))], &key)
        .await
        .unwrap();
    runtime.catalogue().ready().await;
    live(&runtime).await;
    runtime
}

/// Reads events until one satisfies `wanted`, checking every catalogue event
/// on the way against what the read model can answer at that moment.
///
/// That check is the ordering rule: an event goes out after its batch is
/// committed, search index included, so whatever title the store holds for an
/// item it is told about must already be findable. A projection that
/// published per item, before its commit, fails it.
async fn until(
    runtime: &Runtime,
    events: &mut broadcast::Receiver<Event>,
    what: &str,
    wanted: impl Fn(&Event) -> bool,
) -> Event {
    let found = tokio::time::timeout(SOON, async {
        loop {
            let event = events.recv().await.unwrap();
            if let Event::ItemAdded { item_id } | Event::ItemChanged { item_id } = &event {
                let stored = runtime.store().item(*item_id).await.unwrap();
                let stored = stored.unwrap_or_else(|| {
                    panic!("told about {item_id}, which the store does not hold")
                });
                // An item can be projected before its title entry has arrived;
                // then there is nothing yet to search it by.
                if let Some(title) = stored.item.title {
                    assert!(
                        runtime
                            .search()
                            .search(&format!("\"{title}\""), 10)
                            .await
                            .unwrap()
                            .contains(item_id),
                        "told about {item_id} before {title:?} was searchable"
                    );
                }
            }
            if wanted(&event) {
                return event;
            }
        }
    })
    .await;
    found.unwrap_or_else(|_| panic!("{what} within {SOON:?}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_item_written_on_one_node_is_news_on_the_other() {
    // 3a-2's acceptance: a page open on bob hears about what alice adds, and
    // then about what she changes — as two different things.
    let dir = TempDir::new().unwrap();
    let alice_key = SecretKey::generate();
    let bob_key = SecretKey::generate();
    let alice_id = MemberId::from(alice_key.public());
    let bob_id = MemberId::from(bob_key.public());
    let config = config(&[alice_id, bob_id]);
    let alice = Runtime::start(&alice_key, &config, &DataDir::new(dir.path().join("alice")))
        .await
        .unwrap();
    let bob = Runtime::start(&bob_key, &config, &DataDir::new(dir.path().join("bob")))
        .await
        .unwrap();
    alice
        .node()
        .init_group(
            vec![
                (record(alice_id, "alice"), bound(&alice)),
                (record(bob_id, "bob"), bound(&bob)),
            ],
            &alice_key,
        )
        .await
        .unwrap();
    alice.catalogue().ready().await;
    bob.catalogue().ready().await;
    live(&alice).await;
    live(&bob).await;

    let mut watching_bob = bob.events().subscribe();
    let dune = titled(1, "Dune");
    alice.catalogue().write(&dune).await.unwrap();

    until(
        &bob,
        &mut watching_bob,
        "bob hears the item was added",
        |event| *event == Event::ItemAdded { item_id: dune.id },
    )
    .await;

    alice
        .catalogue()
        .write(&titled(1, "Dune Messiah"))
        .await
        .unwrap();
    let changed = until(&bob, &mut watching_bob, "bob hears it changed", |event| {
        matches!(event, Event::ItemAdded { item_id } | Event::ItemChanged { item_id } if *item_id == dune.id)
    })
    .await;
    assert_eq!(
        changed,
        Event::ItemChanged { item_id: dune.id },
        "a second write to an item bob holds is a change, not an addition"
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watcher_that_stops_reading_does_not_hold_the_projection_up() {
    // D1's claim, asserted rather than argued: the projection publishes and
    // moves on, whoever is not keeping up. More items than the bus holds, and
    // a watcher that reads none of them — the read model must still reach the
    // last one, and the watcher finds out it fell behind.
    let dir = TempDir::new().unwrap();
    let node = a_node(dir.path()).await;
    let mut asleep = node.events().subscribe();
    // And one that does keep reading, whose sighting of the last item is how
    // the test knows every event has been published — the store has an item
    // before its batch's events go out, since those wait for the commit.
    let mut awake = node.events().subscribe();

    let count = u16::try_from(distlib_api::events::CAPACITY).unwrap() + 20;
    for n in 0..count {
        node.catalogue()
            .write(&titled(n, &format!("book {n}")))
            .await
            .unwrap();
    }
    let last = id(count - 1);
    tokio::time::timeout(SOON, async {
        loop {
            match awake.recv().await {
                Ok(Event::ItemAdded { item_id } | Event::ItemChanged { item_id })
                    if item_id == last =>
                {
                    return;
                }
                // Reading, but a batch can still outrun it; the newest events
                // are the ones kept, so the last item's is not among the lost.
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => panic!("the bus closed"),
            }
        }
    })
    .await
    .expect("the projection kept going past a watcher that was not reading");

    assert!(
        matches!(asleep.try_recv(), Err(TryRecvError::Lagged(_))),
        "the sleeping watcher is told it fell behind, rather than having held anything up"
    );

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reindex_is_not_news() {
    // A replay runs on every start and on every `admin.reindex`, and rebuilds
    // the read model from a document it already held. Publishing that would
    // put every watcher into `resync` at startup for nothing. The reindex is
    // the deterministic way in: it runs the same replay a start does, and it
    // returns when the replay is done, so anything it published is already
    // waiting.
    let dir = TempDir::new().unwrap();
    let node = a_node(dir.path()).await;
    let mut before = node.events().subscribe();
    let dune = titled(1, "Dune");
    node.catalogue().write(&dune).await.unwrap();
    until(&node, &mut before, "the write is projected", |event| {
        *event == Event::ItemAdded { item_id: dune.id }
    })
    .await;

    let mut watching = node.events().subscribe();
    node.projection().reindex().await.unwrap();

    assert_eq!(
        watching.try_recv(),
        Err(TryRecvError::Empty),
        "a reindex told a watcher something"
    );

    node.shutdown().await;
}
