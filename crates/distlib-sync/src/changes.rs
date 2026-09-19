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
//! the pump parses a key, touches a set, and goes back to the stream. Every
//! slow thing — reading entries, reading content, writing SQLite — happens on
//! the reader's side of this type.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use distlib_core::{ItemId, Key};
use futures_lite::stream::StreamExt as _;
use iroh_docs::{api::Doc, engine::LiveEvent};
use tokio::{sync::watch, task::JoinHandle};

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

/// A live view of what the document is changing, coalesced.
///
/// Start it *before* replaying the document, not after: anything landing
/// between a replay and a subscription is otherwise lost until something else
/// happens to touch the same item. Subscribing first costs nothing, because
/// what arrives in the gap merges into the set the first [`Self::take`] drains.
#[derive(Debug)]
pub struct Changes {
    pending: Arc<Mutex<Batch>>,
    woken: watch::Receiver<u64>,
    pump: JoinHandle<()>,
}

impl Drop for Changes {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

impl Changes {
    /// Subscribes to `doc` and starts coalescing what it reports.
    pub(crate) fn start(doc: Doc) -> Self {
        let pending = Arc::new(Mutex::new(Batch::default()));
        let (woke, woken) = watch::channel(0);
        let pump = tokio::spawn(pump(doc, Arc::clone(&pending), woke));
        Self {
            pending,
            woken,
            pump,
        }
    }

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

/// Reads the document's events and records what they are about.
///
/// See the module docs for why there is nothing else in this loop.
async fn pump(doc: Doc, pending: Arc<Mutex<Batch>>, woke: watch::Sender<u64>) {
    let mut events = match doc.subscribe().await {
        Ok(events) => events,
        Err(error) => {
            tracing::error!(%error, "could not watch the catalogue for changes; the read model will only have what the first replay found");
            return;
        }
    };

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

        let mut batch = pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let news = match event {
            LiveEvent::InsertLocal { entry } | LiveEvent::InsertRemote { entry, .. } => {
                note(&mut batch, entry.key())
            }
            LiveEvent::ContentReady { .. } | LiveEvent::PendingContentReady => {
                let first = !batch.content_arrived;
                batch.content_arrived = true;
                first
            }
            // Neighbours and sync rounds are the swarm's business, not the read
            // model's: what a sync *found* arrives as the inserts above.
            LiveEvent::NeighborUp(_) | LiveEvent::NeighborDown(_) | LiveEvent::SyncFinished(_) => {
                false
            }
        };
        drop(batch);

        if news {
            woke.send_modify(|woke| *woke = woke.wrapping_add(1));
        }
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
