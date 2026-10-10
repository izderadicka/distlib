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
//! the pump parses a key, touches a set or a `watch` or sends on an unbounded
//! channel, and goes back to the stream. Every
//! slow thing — reading entries, reading content, writing SQLite — happens on
//! the reader's side of this type.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use distlib_core::{CommunityKey, ItemId, Key, MemberId};
use futures_lite::stream::{Stream, StreamExt as _};
use iroh::EndpointId;
use iroh_docs::engine::{LiveEvent, SyncEvent};
use tokio::sync::{mpsc, watch};

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
            news: News {
                pending: Arc::clone(&pending),
                woke,
            },
            sync,
            up_since: HashMap::new(),
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
    /// `None` once the catalogue's task has ended — the pump and the content
    /// sweep both — which is shutdown.
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

/// Where the read model's news is left, and what wakes it to read it.
///
/// The pump's, and cloned for the catalogue's content sweep (4b-6): bytes the
/// sweep fetches reach the store behind iroh-docs' back, so no `ContentReady`
/// ever says they landed. Every sync round used to say it anyway, with a
/// `PendingContentReady`, because the catalogue re-offered its peers every
/// thirty seconds. It is a ten-minute backstop now, and this does the second
/// job that timer was doing.
#[derive(Debug, Clone)]
pub(crate) struct News {
    pending: Arc<Mutex<Batch>>,
    woke: watch::Sender<u64>,
}

impl News {
    /// Some content landed: the reader re-reads what it left incomplete.
    pub(crate) fn content_landed(&self) {
        if self.mark_content_landed() {
            self.wake();
        }
    }

    /// Marks that content landed, answering whether that is news — once is
    /// enough until the reader has taken the batch.
    fn mark_content_landed(&self) -> bool {
        !std::mem::replace(&mut self.batch().content_arrived, true)
    }

    fn wake(&self) {
        self.woke.send_modify(|woke| *woke = woke.wrapping_add(1));
    }

    fn batch(&self) -> std::sync::MutexGuard<'_, Batch> {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The writing end, run by the catalogue's task once the document is open.
#[derive(Debug)]
pub(crate) struct Pump {
    news: News,
    sync: watch::Sender<SyncState>,
    /// When each current neighbour was heard to come up, by this node's clock
    /// — see [`Pump::began_before_neighbour`].
    up_since: HashMap<EndpointId, SystemTime>,
}

impl Pump {
    /// A handle for news that does not come through the document's events.
    pub(crate) fn news(&self) -> News {
        self.news.clone()
    }

    /// Reads the document's events and records what they are about, until the
    /// stream ends.
    ///
    /// See the module docs for why there is nothing else in this loop: every
    /// arm is a lock or a `watch` update, and nothing awaits but the stream.
    ///
    /// A neighbour whose sync round may have missed something is sent on
    /// `sync_again`, for the catalogue to sync with it once more.
    pub(crate) async fn run<S>(
        mut self,
        mut events: S,
        sync_again: &mpsc::UnboundedSender<EndpointId>,
    ) where
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
            if self.note(event, sync_again) {
                self.news.wake();
            }
        }
    }

    /// Records one event, answering whether the read model has news.
    fn note(&mut self, event: LiveEvent, sync_again: &mpsc::UnboundedSender<EndpointId>) -> bool {
        match event {
            LiveEvent::InsertLocal { entry } | LiveEvent::InsertRemote { entry, .. } => {
                note(&mut self.news.batch(), entry.key())
            }
            LiveEvent::ContentReady { .. } | LiveEvent::PendingContentReady => {
                self.news.mark_content_landed()
            }
            // Neighbours and sync rounds are the swarm's business, not the read
            // model's: what a sync *found* arrives as the inserts above.
            LiveEvent::NeighborUp(peer) => {
                tracing::debug!(%peer, "catalogue neighbour up");
                self.up_since.insert(peer, SystemTime::now());
                self.sync
                    .send_if_modified(|state| state.neighbours.insert(MemberId::from(peer)));
                false
            }
            LiveEvent::NeighborDown(peer) => {
                tracing::debug!(%peer, "catalogue neighbour down");
                self.up_since.remove(&peer);
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
                if self.began_before_neighbour(&sync) {
                    // Unbounded so the pump never waits; at most one per round.
                    let _ = sync_again.send(sync.peer);
                }
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

    /// Whether a sync round with a neighbour began before it became one, and
    /// so may have missed what was written in between (C31).
    ///
    /// iroh-docs syncs with every new neighbour — but not while a round with
    /// that peer is already running: `start_connect` drops the request, and
    /// only a `SyncReport` queues another (`iroh-docs-0.101.0`
    /// `engine/state.rs:195-206`). A write made in that window is broadcast
    /// before the neighbour is there to hear it, and the running round compared
    /// the two sides before the write existed, so neither carries it: it waits
    /// for the next round with that peer, which may be the catalogue's
    /// ten-minute backstop.
    /// Found on macOS CI, with the write and the neighbour 0.1 ms apart. One
    /// round more, asked for once this one is over, is the round iroh-docs
    /// dropped.
    ///
    /// "Up" is when the pump *heard* it, which is no earlier than it happened,
    /// so the comparison can cost one spare round and never a missed one. The
    /// round it asks for starts after that, so it never asks again.
    fn began_before_neighbour(&self, sync: &SyncEvent) -> bool {
        self.up_since
            .get(&sync.peer)
            .is_some_and(|up| sync.started < *up)
    }
}

/// Records the item an entry is about, answering whether it is news.
///
/// One of the item's own keys, or a member's rating or review of it (D8): the
/// read model re-reads an item whole, so either is a reason to read it again.
/// A bookmark is not part of the item it points into, so not here.
/// A key this build does not recognise is left alone, for the reason
/// [`Key::parse`] gives: the catalogue is one document for the whole group and
/// grows keys over time.
fn note(batch: &mut Batch, key: &[u8]) -> bool {
    let item =
        Key::parse(key)
            .map(|parsed| parsed.item())
            .or_else(|| match CommunityKey::parse(key)? {
                CommunityKey::Rating { item, .. } | CommunityKey::Review { item, .. } => Some(item),
                CommunityKey::Bookmark { .. } => None,
            });
    item.is_some_and(|item| batch.items.insert(item))
}

/// The item ids a set of keys is about, for a reader replaying a document.
///
/// An item's own keys only: a rating with no item behind it is not an item.
/// Here rather than in the caller because it is the rule [`note`] applies to
/// those keys — a key this build does not recognise belongs to somebody else
/// — and one rule stated twice is one rule that drifts.
pub(crate) fn items_in<'a>(keys: impl Iterator<Item = &'a [u8]>) -> BTreeSet<ItemId> {
    keys.filter_map(Key::parse).map(|key| key.item()).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use std::time::Duration;

    use distlib_core::BookmarkId;
    use iroh::SecretKey;
    use iroh_docs::engine::{Origin, SyncReason};

    use super::*;

    fn round(peer: EndpointId, started: SystemTime) -> anyhow::Result<LiveEvent> {
        Ok(LiveEvent::SyncFinished(SyncEvent {
            peer,
            origin: Origin::Connect(SyncReason::DirectJoin),
            finished: SystemTime::now(),
            started,
            result: Err("only when it started matters here".to_owned()),
        }))
    }

    /// Who the pump asks to sync with again, after it has read `events`.
    async fn asked_after(events: Vec<anyhow::Result<LiveEvent>>) -> Vec<EndpointId> {
        let (pump, _feed) = feed();
        let (sync_again, mut asked) = mpsc::unbounded_channel();
        pump.run(futures_lite::stream::iter(events), &sync_again)
            .await;
        drop(sync_again);
        let mut peers = Vec::new();
        while let Some(peer) = asked.recv().await {
            peers.push(peer);
        }
        peers
    }

    /// A member's rating or review is news of the item it is about; a key
    /// this build does not read is news of nothing.
    #[test]
    fn a_rating_or_a_review_is_news_of_its_item() {
        let item = ItemId::from_bytes([3; 32]);
        let member = MemberId::from(SecretKey::generate().public());
        for key in [
            CommunityKey::Rating { item, member },
            CommunityKey::Review { item, member },
        ] {
            let mut batch = Batch::default();
            assert!(note(&mut batch, key.encode().as_bytes()));
            assert_eq!(batch.items, BTreeSet::from([item]));
        }
        assert!(!note(&mut Batch::default(), b"bookmark/somebody-elses"));
        let bookmark = CommunityKey::Bookmark {
            item,
            member,
            bookmark: BookmarkId::from_bytes([1; 16]),
        };
        assert!(
            !note(&mut Batch::default(), bookmark.encode().as_bytes()),
            "a bookmark is not part of its item"
        );
    }

    /// The content sweep's nudge: one wake however often it is told, until
    /// the reader takes it.
    #[tokio::test]
    async fn content_landed_behind_the_documents_back_wakes_the_reader_once() {
        let (pump, feed) = feed();
        let news = pump.news();
        let mut changes = feed.changes();

        news.content_landed();
        news.content_landed();
        assert_eq!(*feed.woken.borrow(), 1, "the second was not news");
        assert_eq!(
            changes.take().await,
            Some(Batch {
                content_arrived: true,
                ..Batch::default()
            })
        );

        news.content_landed();
        assert_eq!(*feed.woken.borrow(), 2, "taken, so news again");
    }

    #[tokio::test]
    async fn a_round_that_began_before_its_neighbour_came_up_is_run_again() {
        let peer = SecretKey::generate().public();
        let before = SystemTime::now() - Duration::from_secs(1);
        let after = SystemTime::now() + Duration::from_secs(3600);

        assert_eq!(
            asked_after(vec![Ok(LiveEvent::NeighborUp(peer)), round(peer, before)]).await,
            vec![peer],
            "the round iroh-docs dropped for the new neighbour is asked for"
        );
        assert_eq!(
            asked_after(vec![Ok(LiveEvent::NeighborUp(peer)), round(peer, after)]).await,
            Vec::new(),
            "a round that began once the neighbour was up already covers it"
        );
        assert_eq!(
            asked_after(vec![round(peer, before)]).await,
            Vec::new(),
            "a peer that is not a neighbour has nothing it could have missed by broadcast"
        );
        assert_eq!(
            asked_after(vec![
                Ok(LiveEvent::NeighborUp(peer)),
                Ok(LiveEvent::NeighborDown(peer)),
                round(peer, before),
            ])
            .await,
            Vec::new(),
            "nor does one that has gone again"
        );
    }
}
