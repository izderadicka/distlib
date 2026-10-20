//! The pin manager (phase 5's D7): keeps what this node is custodian of with a
//! `custody/` tag on each of the item's files, fetching what is not here, and
//! lets go of what it no longer is, into the cache, once the item has
//! `replicas` live copies elsewhere.

use std::{
    collections::{BTreeSet, HashMap},
    num::NonZeroUsize,
    sync::{Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use distlib_consensus::MembershipState;
use distlib_core::{ContentHash, Event, ItemId, KeepReason, MemberId};
use distlib_net::{Blobs, NetError};
use distlib_store::{Store, StoreError};
use distlib_sync::{Availability, Holdings, SyncError, availability::ask_order};
use serde::Serialize;
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        watch,
    },
    task::{self, JoinHandle, JoinSet},
    time::{Instant, sleep_until},
};

use crate::{
    Custody, CustodyChange,
    storage::{Room, Storage, StorageError},
};

/// How long a fetch may go without a byte arriving before it is given up,
/// to be asked again when availability changes.
const STALLED_AFTER: Duration = Duration::from_secs(60);

/// How long availability changes are gathered before they are acted on:
/// each one is a look at every item, and a group's heartbeats bring many.
const AVAILABILITY_GATHERED_FOR: Duration = Duration::from_secs(1);

/// What the pin manager keeps items with, and decides from.
pub struct PinSources {
    /// For the group's `replicas` and its members.
    pub membership: watch::Receiver<MembershipState>,
    /// An item's files.
    pub store: Store,
    pub blobs: Blobs,
    /// Who is online, and what they hold.
    pub availability: Availability,
    /// What this node holds, rechecked once an item is fetched.
    pub holdings: Holdings,
    /// Where availability changes are announced.
    pub events: broadcast::Receiver<Event>,
    /// Where room is made for what custody fetches.
    pub storage: Storage,
}

/// How many items the pin manager has in each state, for `node.status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PinCounts {
    pub queued: usize,
    pub fetching: usize,
    pub failed: usize,
    pub kept: usize,
    pub releasing: usize,
}

/// Keeps this node's custody in its blob store, in the background.
#[derive(Debug)]
pub struct PinManager {
    task: Mutex<Option<JoinHandle<()>>>,
    counts: watch::Receiver<PinCounts>,
}

impl Drop for PinManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl PinManager {
    /// Starts keeping `me`'s custody, as `custody` computes it, fetching up
    /// to `max_fetches` items at once.
    pub fn start(
        me: MemberId,
        custody: &Custody,
        sources: PinSources,
        max_fetches: NonZeroUsize,
    ) -> Self {
        let (counts_sender, counts) = watch::channel(PinCounts::default());
        let PinSources {
            membership,
            store,
            blobs,
            availability,
            holdings,
            events,
            storage,
        } = sources;
        let pins = Pins {
            keeper: Keeper {
                me,
                membership,
                store,
                blobs,
                availability,
                holdings,
                storage,
            },
            custody: custody.clone(),
            pins: HashMap::new(),
            next_turn: Turn::default(),
            max_fetches,
            counts: counts_sender,
        };
        // Subscribed here, before the task runs: a pass that finishes while
        // it starts is still heard.
        let changes = custody.changes();
        Self {
            task: Mutex::new(Some(tokio::spawn(pins.run(changes, events)))),
            counts,
        }
    }

    /// How many items are in each state, kept up to date.
    pub fn counts(&self) -> watch::Receiver<PinCounts> {
        self.counts.clone()
    }

    /// Stops the task. Dropping this does the same.
    pub fn shutdown(&self) {
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
        }
    }
}

/// Where one item the pin manager looks after stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pin {
    /// Waiting its turn to be fetched: the items with the fewest live copies
    /// go first, counted when a slot frees, so a queue is reordered as
    /// copies come and go and a fetch under way is never restarted. Among
    /// items with as many copies, the earliest `turn` goes first.
    Queued { turn: Turn },
    /// Being fetched by `task`, and what happened meanwhile.
    Fetching {
        task: task::Id,
        /// The item changed: it is fetched again however this fetch ends,
        /// since a file may have been added after the fetch read it.
        wanted_again: bool,
        /// A fetch that then fails is asked again at once, not at the next
        /// change.
        availability_changed: bool,
    },
    /// Its fetch failed: asked again when availability changes.
    Failed,
    /// Tagged, and every file here.
    Kept,
    /// No longer this node's: kept until enough copies are elsewhere.
    Releasing,
}

/// When an item joined the queue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Turn(u64);

impl Turn {
    /// This turn, moving `self` on to the next one.
    fn take(&mut self) -> Self {
        let turn = *self;
        self.0 += 1;
        turn
    }
}

/// The task's state: every item with `custody/` tags, or about to have them.
struct Pins {
    keeper: Keeper,
    custody: Custody,
    pins: HashMap<ItemId, Pin>,
    /// The turn the next item to join the queue gets.
    next_turn: Turn,
    max_fetches: NonZeroUsize,
    counts: watch::Sender<PinCounts>,
}

impl Pins {
    async fn run(
        mut self,
        mut changes: broadcast::Receiver<CustodyChange>,
        mut events: broadcast::Receiver<Event>,
    ) {
        match self.keeper.blobs.kept(KeepReason::Custody).await {
            Ok(kept) => self
                .pins
                .extend(kept.into_iter().map(|(item, _)| (item, Pin::Kept))),
            Err(error) => {
                tracing::error!(%error, "could not read what this node keeps for custody; the pin manager stops");
                return;
            }
        }
        // Custody computed before this started: nothing will announce it.
        self.read_custody_again().await;

        let mut fetches = JoinSet::new();
        let mut look_at_availability: Option<Instant> = None;
        loop {
            self.start_fetches(&mut fetches);
            self.publish_counts();
            tokio::select! {
                change = changes.recv() => match change {
                    Ok(CustodyChange::Recomputed) => self.read_custody_again().await,
                    Ok(CustodyChange::Item { item, mine: true }) => self.want(item),
                    Ok(CustodyChange::Item { item, mine: false }) => self.lose(item).await,
                    Err(RecvError::Lagged(missed)) => {
                        tracing::warn!(missed, "the pin manager missed custody changes; reading them all again");
                        self.read_custody_again().await;
                    }
                    Err(RecvError::Closed) => {
                        tracing::error!("custody is gone; the pin manager stops");
                        return;
                    }
                },
                event = events.recv() => match event {
                    Ok(Event::AvailabilityChanged { .. }) | Err(RecvError::Lagged(_)) => {
                        look_at_availability
                            .get_or_insert_with(|| Instant::now() + AVAILABILITY_GATHERED_FOR);
                    }
                    Ok(_) => {}
                    Err(RecvError::Closed) => {
                        tracing::error!("the event bus is gone; the pin manager stops");
                        return;
                    }
                },
                () = sleep_until(look_at_availability.unwrap_or_else(Instant::now)),
                    if look_at_availability.is_some() =>
                {
                    look_at_availability = None;
                    self.availability_changed().await;
                }
                Some(joined) = fetches.join_next_with_id() => match joined {
                    Ok((_, (item, kept))) => self.fetched(item, kept).await,
                    Err(error) => self.fetch_panicked(&error),
                },
            }
        }
    }

    fn publish_counts(&self) {
        let counts = self
            .pins
            .values()
            .fold(PinCounts::default(), |mut counts, pin| {
                match pin {
                    Pin::Queued { .. } => counts.queued += 1,
                    Pin::Fetching { .. } => counts.fetching += 1,
                    Pin::Failed => counts.failed += 1,
                    Pin::Kept => counts.kept += 1,
                    Pin::Releasing => counts.releasing += 1,
                }
                counts
            });
        self.counts.send_if_modified(|published| {
            let changed = *published != counts;
            *published = counts;
            changed
        });
    }

    /// Every item this node keeps, against what it looks after: run after
    /// each whole pass. Nothing before custody's first pass, which would
    /// release everything.
    async fn read_custody_again(&mut self) {
        let Some(mine) = self.custody.my_items() else {
            return;
        };
        for item in &mine {
            let needs_keeping = match self.pins.get(item) {
                None | Some(Pin::Releasing) => true,
                Some(Pin::Kept) => !self.keeper.holdings.holds(*item),
                Some(Pin::Queued { .. } | Pin::Fetching { .. } | Pin::Failed) => false,
            };
            if needs_keeping {
                self.want(*item);
            }
        }
        let lost: Vec<ItemId> = self
            .pins
            .keys()
            .filter(|item| !mine.contains(item))
            .copied()
            .collect();
        for item in lost {
            self.lose(item).await;
        }
    }

    /// Queues `item` to be kept, or, if it is being fetched, to be fetched
    /// again once that ends.
    fn want(&mut self, item: ItemId) {
        let pin = wanted(self.pins.get(&item).copied(), self.next_turn.take());
        self.pins.insert(item, pin);
    }

    /// The state of an item joining the queue now, behind those already in.
    fn queued(&mut self) -> Pin {
        Pin::Queued {
            turn: self.next_turn.take(),
        }
    }

    /// `item` is no longer this node's. One being fetched is decided once
    /// the fetch ends.
    async fn lose(&mut self, item: ItemId) {
        match self.pins.get(&item) {
            None | Some(Pin::Fetching { .. }) => {}
            Some(_) => self.release_if_safe(item).await,
        }
    }

    fn start_fetches(&mut self, fetches: &mut JoinSet<(ItemId, Result<(), KeepError>)>) {
        let free = self.max_fetches.get().saturating_sub(fetches.len());
        if free == 0 {
            return;
        }
        for item in self.fewest_copies_first(free) {
            let keeper = self.keeper.clone();
            let task = fetches
                .spawn(async move { (item, keeper.keep(item).await) })
                .id();
            self.pins.insert(
                item,
                Pin::Fetching {
                    task,
                    wanted_again: false,
                    availability_changed: false,
                },
            );
        }
    }

    /// Up to `count` of the queued items, those with the fewest live copies
    /// first.
    fn fewest_copies_first(&self, count: usize) -> Vec<ItemId> {
        if !self
            .pins
            .values()
            .any(|pin| matches!(pin, Pin::Queued { .. }))
        {
            return Vec::new();
        }
        // Cloned rather than borrowed: the holders' lock is taken per item.
        let online = self.keeper.availability.online().borrow().clone();
        next_to_fetch(&self.pins, count, |item| {
            self.keeper.live_copies(item, &online)
        })
    }

    async fn fetched(&mut self, item: ItemId, kept: Result<(), KeepError>) {
        let Some(Pin::Fetching {
            wanted_again,
            availability_changed,
            ..
        }) = self.pins.get(&item).copied()
        else {
            return;
        };
        if !self.custody.keeps(item) {
            // Lost while it was being fetched.
            self.release_if_safe(item).await;
            return;
        }
        let next = match kept {
            Ok(()) => {
                tracing::debug!(%item, "kept for custody");
                if wanted_again {
                    self.queued()
                } else {
                    Pin::Kept
                }
            }
            Err(error) => {
                if error.is_unavailable() {
                    tracing::warn!(%item, %error, "could not fetch an item this node keeps; asking again when availability changes");
                } else {
                    tracing::error!(%item, %error, "could not keep an item for custody; trying again when availability changes");
                }
                if wanted_again || availability_changed {
                    self.queued()
                } else {
                    Pin::Failed
                }
            }
        };
        self.pins.insert(item, next);
    }

    fn fetch_panicked(&mut self, error: &task::JoinError) {
        tracing::error!(%error, "a custody fetch failed");
        let failed = self
            .pins
            .values_mut()
            .find(|pin| matches!(pin, Pin::Fetching { task, .. } if *task == error.id()));
        if let Some(pin) = failed {
            *pin = Pin::Failed;
        }
    }

    /// A member came or went, or announced what it holds: a fetch that
    /// failed may now succeed, and an item may now have its copies elsewhere.
    async fn availability_changed(&mut self) {
        let mut releasing = Vec::new();
        for (item, pin) in &mut self.pins {
            match pin {
                Pin::Fetching {
                    availability_changed,
                    ..
                } => *availability_changed = true,
                Pin::Failed => {
                    *pin = Pin::Queued {
                        turn: self.next_turn.take(),
                    };
                }
                Pin::Releasing => releasing.push(*item),
                Pin::Queued { .. } | Pin::Kept => {}
            }
        }
        for item in releasing {
            self.release_if_safe(item).await;
        }
    }

    /// Lets go of `item` into the cache, once its copies elsewhere make up
    /// for this one, if this node holds it.
    async fn release_if_safe(&mut self, item: ItemId) {
        if self.keeper.holdings.holds(item) && !self.keeper.enough_copies_elsewhere(item) {
            self.pins.insert(item, Pin::Releasing);
            return;
        }
        match self.keeper.release(item).await {
            Ok(()) => {
                self.pins.remove(&item);
                tracing::debug!(%item, "no longer custodian; kept as cache");
            }
            Err(error) => {
                tracing::error!(%item, %error, "could not release an item into the cache; trying again when availability changes");
                self.pins.insert(item, Pin::Releasing);
            }
        }
    }
}

/// What wanting an item makes of its `pin`: queued at `turn`, unless it is
/// waiting already, which keeps its place, or being fetched, which is then
/// fetched again.
fn wanted(pin: Option<Pin>, turn: Turn) -> Pin {
    match pin {
        None | Some(Pin::Failed | Pin::Kept | Pin::Releasing) => Pin::Queued { turn },
        Some(queued @ Pin::Queued { .. }) => queued,
        Some(Pin::Fetching {
            task,
            availability_changed,
            ..
        }) => Pin::Fetching {
            task,
            wanted_again: true,
            availability_changed,
        },
    }
}

/// Up to `count` of the queued items in `pins`: those with the fewest live
/// copies as `live_copies` counts them, an item down to one copy before one
/// at two, and among as many the earliest turn.
fn next_to_fetch(
    pins: &HashMap<ItemId, Pin>,
    count: usize,
    live_copies: impl Fn(ItemId) -> usize,
) -> Vec<ItemId> {
    let mut queued: Vec<((usize, Turn), ItemId)> = pins
        .iter()
        .filter_map(|(item, pin)| match pin {
            Pin::Queued { turn } => Some(((live_copies(*item), *turn), *item)),
            _ => None,
        })
        .collect();
    if queued.len() > count {
        queued.select_nth_unstable(count);
        queued.truncate(count);
    }
    queued.into_iter().map(|(_, item)| item).collect()
}

/// Why an item could not be kept.
#[derive(Debug, thiserror::Error)]
enum KeepError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Blobs(#[from] NetError),
    #[error(transparent)]
    Holdings(#[from] SyncError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("no byte arrived for {STALLED_AFTER:?}")]
    Stalled,
}

impl KeepError {
    /// Whether nobody could serve it, which is ordinary while holders are
    /// away, rather than something wrong here.
    fn is_unavailable(&self) -> bool {
        matches!(self, Self::Stalled | Self::Blobs(NetError::Fetch { .. }))
    }
}

/// What a fetch needs, cloned into its task.
#[derive(Clone)]
struct Keeper {
    me: MemberId,
    membership: watch::Receiver<MembershipState>,
    store: Store,
    blobs: Blobs,
    availability: Availability,
    holdings: Holdings,
    storage: Storage,
}

impl Keeper {
    /// Tags each of `item_id`'s files for custody, makes room for them, and
    /// fetches the ones not here. A file already here, cached by a download
    /// say, is only tagged.
    ///
    /// Tagged before room is made, so a cached copy is kept by custody before
    /// anything is let go of. Fetched even when the whole cache is not room
    /// enough: custody is kept whatever the limit.
    async fn keep(&self, item_id: ItemId) -> Result<(), KeepError> {
        let Some(stored) = self.store.item(item_id).await? else {
            return Ok(());
        };
        let files: Vec<(ContentHash, u64)> = stored
            .item
            .files
            .iter()
            .map(|(hash, file)| (*hash, file.size))
            .collect();
        for (hash, _) in &files {
            self.blobs.keep(KeepReason::Custody, item_id, *hash).await?;
        }
        let room = self
            .storage
            .make_room(item_id, files.iter().copied())
            .await?;
        if let Room::Short(short_by) = room {
            tracing::warn!(item = %item_id, short_by, "kept for custody over the storage limit, with the whole cache evicted");
        }
        let providers = self.providers(item_id);
        for (hash, _) in files {
            if !self.blobs.has(hash).await? {
                fetch_until_stalled(&self.blobs, hash, providers.clone()).await?;
            }
        }
        self.holdings.recheck(&stored.item).await?;
        Ok(())
    }

    /// Every other member, in the order to ask them for `item`.
    fn providers(&self, item: ItemId) -> Vec<MemberId> {
        let online = self.availability.online().borrow().clone();
        let members: Vec<MemberId> = self
            .membership
            .borrow()
            .members()
            .map(|record| record.member_id)
            .filter(|member| *member != self.me)
            .collect();
        ask_order(&self.availability.holders(&item), &online, members)
    }

    /// Whether `replicas` online members other than this node hold `item`.
    /// Not while anyone online has not said what they hold.
    fn enough_copies_elsewhere(&self, item: ItemId) -> bool {
        let online = self.availability.online().borrow().clone();
        if online
            .iter()
            .any(|member| *member != self.me && !self.availability.knows_holdings_of(member))
        {
            return false;
        }
        self.live_copies(item, &online)
            >= usize::from(self.membership.borrow().settings().replicas.get())
    }

    /// How many members in `online`, other than this node, hold `item`.
    fn live_copies(&self, item: ItemId, online: &BTreeSet<MemberId>) -> usize {
        self.availability
            .holders(&item)
            .iter()
            .filter(|member| **member != self.me && online.contains(member))
            .count()
    }

    /// Moves `item`'s blobs from custody to the cache: the cache's tag first,
    /// so no collection in between finds them unkept. Arriving in the cache
    /// counts as a use, so it is not the first to go.
    async fn release(&self, item: ItemId) -> Result<(), KeepError> {
        for hash in self.blobs.kept_for(KeepReason::Custody, item).await? {
            self.blobs.keep(KeepReason::Cache, item, hash).await?;
            self.blobs.release(KeepReason::Custody, item, hash).await?;
        }
        self.store.mark_used(item, SystemTime::now()).await?;
        Ok(())
    }
}

/// [`Blobs::fetch_with_progress`], given up once [`STALLED_AFTER`] passes
/// without a byte: how long a file takes is a matter of its size, how long it
/// goes without progress is not.
async fn fetch_until_stalled(
    blobs: &Blobs,
    hash: ContentHash,
    providers: Vec<MemberId>,
) -> Result<(), KeepError> {
    let (progressed, mut progress) = watch::channel(());
    let fetch = blobs.fetch_with_progress(hash, providers, move |_offset| {
        progressed.send_replace(());
    });
    tokio::pin!(fetch);
    loop {
        tokio::select! {
            biased;
            fetched = &mut fetch => return Ok(fetched?),
            moved = tokio::time::timeout(STALLED_AFTER, progress.changed()) => {
                if moved.is_err() {
                    return Err(KeepError::Stalled);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn item(byte: u8) -> ItemId {
        ItemId::from_content_hashes(&[[byte; 32]])
    }

    /// What `next_to_fetch` picks from `queued`, given as (item, live
    /// copies, turn), as a set: those picked start together.
    fn picked(queued: &[(ItemId, usize, u64)], count: usize) -> BTreeSet<ItemId> {
        let pins = queued
            .iter()
            .map(|(item, _, turn)| (*item, Pin::Queued { turn: Turn(*turn) }))
            .collect();
        let copies = |wanted: ItemId| {
            queued
                .iter()
                .find(|(item, _, _)| *item == wanted)
                .map_or(0, |(_, copies, _)| *copies)
        };
        next_to_fetch(&pins, count, copies).into_iter().collect()
    }

    #[test]
    fn fewer_copies_go_before_an_earlier_turn() {
        let queued = [(item(1), 2, 0), (item(2), 1, 5)];
        assert_eq!(picked(&queued, 1), BTreeSet::from([item(2)]));
    }

    #[test]
    fn among_as_many_copies_the_earliest_turn_goes_first() {
        // The latest turn goes to the lowest id, so an order by id would
        // pick it.
        let mut ids = [item(1), item(2), item(3)];
        ids.sort();
        let [lowest, middle, highest] = ids;
        let queued = [(lowest, 1, 7), (highest, 1, 3), (middle, 1, 4)];
        assert_eq!(picked(&queued, 2), BTreeSet::from([highest, middle]));
    }

    #[test]
    fn no_more_than_asked_for_all_when_fewer_wait_and_only_the_queued() {
        let queued = [(item(1), 0, 0), (item(2), 0, 1)];
        assert_eq!(picked(&queued, 1).len(), 1);
        assert_eq!(picked(&queued, 5), BTreeSet::from([item(1), item(2)]));
        let pins = HashMap::from([(item(1), Pin::Kept), (item(2), Pin::Failed)]);
        assert!(next_to_fetch(&pins, 5, |_| 0).is_empty());
    }

    #[test]
    fn an_item_wanted_while_waiting_keeps_its_place() {
        let waiting = Pin::Queued { turn: Turn(2) };
        assert_eq!(wanted(Some(waiting), Turn(9)), waiting);
    }

    #[test]
    fn an_item_new_kept_or_failed_joins_the_queue_at_the_turn_given() {
        for pin in [
            None,
            Some(Pin::Kept),
            Some(Pin::Failed),
            Some(Pin::Releasing),
        ] {
            assert_eq!(wanted(pin, Turn(4)), Pin::Queued { turn: Turn(4) });
        }
    }

    #[test]
    fn each_turn_is_later_than_the_last() {
        let mut next = Turn::default();
        let first = next.take();
        assert!(next.take() > first);
    }
}
