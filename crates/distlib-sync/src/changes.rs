//! What the read model is told to re-read.
//!
//! The projection stream §5.4 asks for, deferred out of 2a-2 to here on the
//! grounds that only its consumer could decide its shape. The consumer exists
//! now (`distlib-store`), and it decided two things.
//!
//! **It is a set of item ids, not a stream of entries.** An entry says a field
//! changed; the read model's unit of work is a whole item, because it projects
//! by re-reading one — which is what makes a replay idempotent by construction
//! rather than by argument. Ten writes to one item are therefore one re-read,
//! and the coalescing has to happen here, before a slow reader can see them.
//!
//! **It must never carry the entry's value.** The value is a blob that may not
//! have arrived, so a stream carrying values either blocks or lies. This
//! carries neither: it says *which* item to look at, and the reader asks the
//! document, which is the only thing that knows.
//!
//! **The pump below must stay trivial, and that is a hard constraint rather
//! than a preference.** `Doc::subscribe` is an `async_channel::bounded` written
//! with `sender.send(event).await` (`iroh-docs-0.101.0/src/engine.rs:212`,
//! `engine/live.rs:877`), so a subscriber that is slow does not drop events —
//! it **stalls iroh-docs' own live actor**, and with it the document's sync. So
//! the pump parses a key, touches a set or a `watch`, and goes back to the stream. Every
//! slow thing — reading entries, reading content, writing SQLite — happens on
//! the reader's side of this type.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use distlib_core::{ItemId, Key, MemberId};
use futures_lite::stream::{Stream, StreamExt as _};
use iroh_docs::engine::LiveEvent;
use tokio::sync::watch;

/// Everything that has changed since the reader last looked.
///
/// A *set* and a flag rather than a queue, because both questions the reader
/// asks are idempotent: "which items should I re-read" and "might something
/// that was unreadable be readable now".
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Batch {
    /// Items with at least one entry written locally or arrived from a peer.
    pub items: BTreeSet<ItemId>,
    /// Content landed for some entry — which item's is not said.
    ///
    /// **Deliberately not resolved to an item here.** `LiveEvent::ContentReady`
    /// carries a hash, and turning a hash into an item would mean this type
    /// keeping an index of which items are waiting on which content — state
    /// that has to be rebuilt after a restart and kept true through every
    /// overwrite. The reader already knows which items it projected with
    /// something missing, because it is what left them out; it is a far smaller
    /// set and it is derived rather than maintained. So this is a nudge, and
    /// the reader decides what it is a nudge about.
    pub content_arrived: bool,
}

impl Batch {
    /// Whether there is nothing here to do.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty() && !self.content_arrived
    }
}

/// What the document's swarm looks like from here: who this node is directly
/// connected to for the catalogue, and how its last sync round with each peer
/// went (C14).
///
/// Kept by the pump from the events iroh-docs sends anyway, because iroh-docs
/// has no call that answers it: `Doc::get_sync_peers` is the peers it
/// *remembers*, not the ones it is connected to now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncState {
    /// The members this node is a gossip neighbour of for the catalogue —
    /// what live changes travel over, as opposed to sync rounds.
    pub neighbours: BTreeSet<MemberId>,
    /// The last sync round with each peer that has had one.
    pub last_sync: BTreeMap<MemberId, LastSync>,
}

/// How the last sync round with one peer ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastSync {
    /// When it finished, by this node's clock.
    pub finished: SystemTime,
    /// Whether it succeeded.
    pub ok: bool,
}

/// The two halves of the one subscription to the document: the [`Pump`] that
/// reads it and the [`Feed`] that is read from.
///
/// Made when the catalogue starts and subscribed when the document opens —
/// **before** it starts syncing, so nothing it reports is missed. A
/// subscription made later, by whoever asks for [`Changes`] first, misses the
/// neighbours the document found while nobody was listening, and then shows
/// none while it has some (found in phase 4's 4.0).
pub(crate) fn feed() -> (Pump, Feed) {
    let pending = Arc::new(Mutex::new(Batch::default()));
    let (woke, woken) = watch::channel(0);
    let (sync, synced) = watch::channel(SyncState::default());
    (
        Pump {
            pending: Arc::clone(&pending),
            woke,
            sync,
        },
        Feed {
            pending,
            woken,
            synced,
        },
    )
}

/// The reading end: what [`Changes`] and [`SyncState`] are read from.
#[derive(Debug)]
pub(crate) struct Feed {
    pending: Arc<Mutex<Batch>>,
    woken: watch::Receiver<u64>,
    synced: watch::Receiver<SyncState>,
}

impl Feed {
    /// The document's changes, from when it opened.
    ///
    /// **One reader**: every `Changes` drains the same batch, so two would
    /// each see some of the changes. The projection is the one.
    pub(crate) fn changes(&self) -> Changes {
        Changes {
            pending: Arc::clone(&self.pending),
            woken: self.woken.clone(),
        }
    }

    /// The swarm's state, kept current for as long as the document is open.
    pub(crate) fn sync_status(&self) -> watch::Receiver<SyncState> {
        self.synced.clone()
    }
}

/// A live view of what the document is changing, coalesced.
///
/// Take it *before* replaying the document, not after: anything landing
/// between a replay and the first [`Self::take`] is then in the set it
/// drains. Anything from before is there too, since the set has been kept
/// since the document opened — which costs one spare re-read of each, and a
/// re-read is idempotent.
#[derive(Debug)]
pub struct Changes {
    pending: Arc<Mutex<Batch>>,
    woken: watch::Receiver<u64>,
}

impl Changes {
    /// Waits until there is something to do, then takes all of it.
    ///
    /// `None` once the document's event stream has ended, which is shutdown.
    pub async fn take(&mut self) -> Option<Batch> {
        loop {
            // Marked seen *before* the set is looked at, so the two orderings
            // that matter both end well: news landing after this and before the
            // look is found in the set, and news landing after the look has
            // already moved the version past what was marked, so the wait below
            // returns at once instead of sleeping through it.
            self.woken.borrow_and_update();
            let taken = std::mem::take(
                &mut *self
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            if !taken.is_empty() {
                return Some(taken);
            }
            self.woken.changed().await.ok()?;
        }
    }
}

/// The writing end, run by the catalogue's task once the document is open.
#[derive(Debug)]
pub(crate) struct Pump {
    pending: Arc<Mutex<Batch>>,
    woke: watch::Sender<u64>,
    sync: watch::Sender<SyncState>,
}

impl Pump {
    /// Reads the document's events and records what they are about, until the
    /// stream ends.
    ///
    /// See the module docs for why there is nothing else in this loop: every
    /// arm is a lock or a `watch` update, and nothing awaits but the stream.
    pub(crate) async fn run<S>(self, mut events: S)
    where
        S: Stream<Item = anyhow::Result<LiveEvent>> + Unpin,
    {
        while let Some(event) = events.next().await {
            let event = match event {
                Ok(event) => event,
                // One event failing to arrive is not the stream ending, and the
                // reader's own sweep is what covers the gap.
                Err(error) => {
                    tracing::warn!(%error, "a catalogue change could not be read");
                    continue;
                }
            };
            if self.note(event) {
                self.woke.send_modify(|woke| *woke = woke.wrapping_add(1));
            }
        }
    }

    /// Records one event, answering whether the read model has news.
    fn note(&self, event: LiveEvent) -> bool {
        match event {
            LiveEvent::InsertLocal { entry } | LiveEvent::InsertRemote { entry, .. } => {
                note(&mut self.batch(), entry.key())
            }
            LiveEvent::ContentReady { .. } | LiveEvent::PendingContentReady => {
                let mut batch = self.batch();
                let first = !batch.content_arrived;
                batch.content_arrived = true;
                first
            }
            // Neighbours and sync rounds are the swarm's business, not the read
            // model's: what a sync *found* arrives as the inserts above.
            LiveEvent::NeighborUp(peer) => {
                tracing::debug!(%peer, "catalogue neighbour up");
                self.sync
                    .send_if_modified(|state| state.neighbours.insert(MemberId::from(peer)));
                false
            }
            LiveEvent::NeighborDown(peer) => {
                tracing::debug!(%peer, "catalogue neighbour down");
                self.sync
                    .send_if_modified(|state| state.neighbours.remove(&MemberId::from(peer)));
                false
            }
            LiveEvent::SyncFinished(sync) => {
                tracing::debug!(
                    peer = %sync.peer,
                    origin = ?sync.origin,
                    error = sync.result.as_ref().err(),
                    "catalogue sync round finished",
                );
                self.sync.send_modify(|state| {
                    state.last_sync.insert(
                        MemberId::from(sync.peer),
                        LastSync {
                            finished: sync.finished,
                            ok: sync.result.is_ok(),
                        },
                    );
                });
                false
            }
        }
    }

    fn batch(&self) -> std::sync::MutexGuard<'_, Batch> {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Records the item an entry belongs to, answering whether it is news.
///
/// A key this build does not recognise is left alone, for the reason
/// [`Key::parse`] gives: the catalogue is one document for the whole group and
/// grows keys over time.
fn note(batch: &mut Batch, key: &[u8]) -> bool {
    let Some(key) = Key::parse(key) else {
        return false;
    };
    batch.items.insert(key.item())
}

/// The item ids a set of keys is about, for a reader replaying a document.
///
/// Here rather than in the caller because it is the same rule as [`note`] — a
/// key this build does not recognise belongs to somebody else — and one rule
/// stated twice is one rule that drifts.
pub(crate) fn items_in<'a>(keys: impl Iterator<Item = &'a [u8]>) -> BTreeSet<ItemId> {
    keys.filter_map(Key::parse).map(|key| key.item()).collect()
}
