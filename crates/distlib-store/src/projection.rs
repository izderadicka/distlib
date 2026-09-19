//! The task that keeps the read model in step with the catalogue.
//!
//! **The unit of work is one item, and the work is to re-read it.** A change
//! never says what a field became — it says which item to look at — and the
//! projection then asks the document for that whole item and overwrites its
//! rows. Nothing accumulates, so replaying the same change twice writes the
//! same rows twice, and replaying two changes in either order ends in the same
//! place. §10's idempotence is a property of the shape rather than of the care
//! taken.
//!
//! **The order at start-up is not arbitrary.** This subscribes *before* it
//! replays, because anything that arrives between a replay and a subscription
//! is otherwise lost until something else happens to touch the same item — the
//! kind of gap that shows up months later as an item that is simply missing on
//! one node. Subscribing first costs nothing: what lands in the gap merges into
//! the set the first drain takes.
//!
//! **Content arriving is handled by remembering what was left out.** An entry
//! and its bytes arrive separately, so an item can project with a field
//! missing; `ContentReady` then says only *that some* content landed, not whose.
//! Rather than index which items are waiting on which hashes — state to rebuild
//! after every restart and keep true through every overwrite — this keeps the
//! set of items it last projected incomplete. It is derived rather than
//! maintained, it is empty in the steady state, and re-reading an item that
//! turned out not to need it costs one read and writes the same rows.
//!
//! **That set is also what covers content this crate is never told about, and
//! the mechanism is worth stating because it is not obvious.**
//! `LiveEvent::ContentReady` is emitted from the live actor's `on_download_ready`
//! (`iroh-docs-0.101.0/src/engine/live.rs:645`) — that is, only for downloads
//! *iroh-docs itself* started. Bytes that reach the store by any other route
//! emit nothing, and the catalogue's own repair sweep (P2-18) is exactly such a
//! route: it drives the blobs `Downloader` directly, for content the docs
//! engine already gave up on. Without a second nudge, an item repaired by the
//! sweep would stay half-projected until something else happened to touch it.
//!
//! `PendingContentReady` is what closes it, and not by luck: it is emitted on
//! **every** sync round that finishes with nothing queued
//! (`live.rs:613-620`), and the catalogue re-offers its peers on a 15-second
//! timer whether or not anything changed — which produces a round. Measured on
//! a quiet two-node group rather than argued: nudges at 0.03 s, 14.9 s, 29.9 s
//! and 44.9 s, so an item repaired behind the engine's back is re-read within
//! about fifteen seconds. A node with no reachable peer gets no rounds and no
//! nudge, which costs nothing: with no peer there is nothing for the sweep to
//! fetch either.

use std::collections::BTreeSet;

use distlib_consensus::MembershipState;
use distlib_core::ItemId;
use distlib_sync::{Batch, Catalogue};
use tokio::{sync::watch, task::JoinHandle};

use crate::store::{Store, StoredItem, StoredMember};

/// The projection task, stopped when this is dropped.
#[derive(Debug)]
pub struct Projection {
    task: JoinHandle<()>,
}

impl Drop for Projection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Projection {
    /// Starts projecting `catalogue` into `store`.
    ///
    /// Returns as soon as the task is spawned, which is before the catalogue
    /// exists: a node that has just joined has not fetched the log yet, so the
    /// task waits for the document the same way everything else does.
    pub fn start(
        catalogue: Catalogue,
        store: Store,
        membership: watch::Receiver<MembershipState>,
    ) -> Self {
        Self {
            task: tokio::spawn(run(catalogue, store, membership)),
        }
    }

    /// Stops the task. Dropping this does the same.
    pub fn shutdown(&self) {
        self.task.abort();
    }
}

async fn run(catalogue: Catalogue, store: Store, mut membership: watch::Receiver<MembershipState>) {
    catalogue.ready().await;

    // Before the replay. See the module docs: the other order loses whatever
    // arrives while the replay is running.
    let mut changes = match catalogue.changes() {
        Ok(changes) => changes,
        Err(error) => {
            tracing::error!(%error, "the read model could not watch the catalogue");
            return;
        }
    };

    // Cold start: the whole document, every time. What this costs is one read
    // and one upsert per item, and what it buys is that these tables are a
    // function of the document rather than of everything this node has ever
    // seen — which is the difference between a node that restarted and one that
    // did not holding the same rows.
    let mut incomplete = BTreeSet::new();
    replay(&catalogue, &store, &mut incomplete).await;
    project_members(&store, &mut membership).await;

    loop {
        tokio::select! {
            batch = changes.take() => {
                let Some(Batch { mut items, content_arrived }) = batch else {
                    tracing::debug!("the catalogue stopped reporting changes; the read model is now cold");
                    return;
                };
                if content_arrived {
                    items.extend(incomplete.iter().copied());
                }
                project(&catalogue, &store, items, &mut incomplete).await;
            }
            changed = membership.changed() => {
                if changed.is_err() {
                    tracing::error!("the membership log is gone; stopping the read model's projection");
                    return;
                }
                project_members(&store, &mut membership).await;
            }
        }
    }
}

/// Re-reads each item and writes it over whatever the tables held.
///
/// Failures are logged per item rather than returned: one item that cannot be
/// read must not stop the rest of a replay, and the next change to it — or the
/// next start — reads it again.
async fn project(
    catalogue: &Catalogue,
    store: &Store,
    items: BTreeSet<ItemId>,
    incomplete: &mut BTreeSet<ItemId>,
) {
    for id in items {
        let read = match catalogue.read_item(id).await {
            Ok(Some(read)) => read,
            // No entry for this item at all. Nothing that reaches here should
            // produce it — every id came from a key that parsed — so it is
            // skipped rather than written as an empty row, which is the only
            // answer that cannot make the tables say something untrue.
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(%id, %error, "could not read a catalogue item to project it");
                continue;
            }
        };

        if read.waiting_for_content {
            incomplete.insert(id);
        } else {
            incomplete.remove(&id);
        }

        if let Err(error) = store
            .upsert_item(StoredItem {
                item: read.item,
                last_modified: read.last_modified,
            })
            .await
        {
            tracing::warn!(%id, %error, "could not write a catalogue item to the read model");
        }
    }
}

/// Writes the group's membership as the log currently has it.
async fn project_members(store: &Store, membership: &mut watch::Receiver<MembershipState>) {
    // Collected before the await: the borrow guard is not `Send`, and holding
    // it across one would also hold the watch against whoever writes it next.
    let members = {
        let seen = membership.borrow_and_update();
        seen.members()
            .map(|record| StoredMember {
                member: record.member_id,
                display_name: record.display_name.clone(),
                pledge_bytes: record.pledge_bytes,
                is_core: seen.is_core(&record.member_id),
            })
            .collect::<Vec<_>>()
    };
    if let Err(error) = store.set_members(members).await {
        tracing::warn!(%error, "could not write the group's members to the read model");
    }
}

/// Re-reads every item in the document into the read model.
///
/// What a start does, and what `admin.reindex` will drive in 2a-3b — one
/// function, because they are the same operation and two spellings of it would
/// be two things to keep true. It is the whole of the restart acceptance: a
/// node that starts is a node that has just reindexed.
async fn replay(catalogue: &Catalogue, store: &Store, incomplete: &mut BTreeSet<ItemId>) {
    let ids = match catalogue.item_ids().await {
        Ok(ids) => ids,
        Err(error) => {
            tracing::error!(%error, "could not replay the catalogue into the read model");
            return;
        }
    };
    tracing::info!(
        items = ids.len(),
        "replaying the catalogue into the read model"
    );
    project(catalogue, store, ids, incomplete).await;
}
