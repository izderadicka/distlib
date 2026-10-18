//! The pin manager (phase 5's D7): keeps what this node is custodian of with a
//! `custody/` tag on each of the item's files, fetching what is not here, and
//! lets go of what it no longer is, into the cache, once the item has
//! `replicas` live copies elsewhere.

use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    sync::{Mutex, PoisonError},
    time::Duration,
};

use distlib_consensus::MembershipState;
use distlib_core::{ContentHash, Event, ItemId, KeepReason, MemberId};
use distlib_net::{Blobs, NetError};
use distlib_store::{Store, StoreError};
use distlib_sync::{Availability, Holdings, SyncError, availability::ask_order};
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        watch,
    },
    task::{self, JoinHandle, JoinSet},
    time::{Instant, sleep_until},
};

use crate::{Custody, CustodyChange};

/// How long a fetch may go without a byte arriving before it is given up,
/// to be asked again when availability changes.
const STALLED_AFTER: Duration = Duration::from_secs(60);

/// How many items are fetched at once.
const FETCHES_AT_ONCE: usize = 10;

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
}

/// Keeps this node's custody in its blob store, in the background.
#[derive(Debug)]
pub struct PinManager {
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for PinManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl PinManager {
    /// Starts keeping `me`'s custody, as `custody` computes it.
    pub fn start(me: MemberId, custody: &Custody, sources: PinSources) -> Self {
        let PinSources {
            membership,
            store,
            blobs,
            availability,
            holdings,
            events,
        } = sources;
        let pins = Pins {
            keeper: Keeper {
                me,
                membership,
                store,
                blobs,
                availability,
                holdings,
            },
            custody: custody.clone(),
            pins: HashMap::new(),
            queue: VecDeque::new(),
        };
        // Subscribed here, before the task runs: a pass that finishes while
        // it starts is still heard.
        let changes = custody.changes();
        Self {
            task: Mutex::new(Some(tokio::spawn(pins.run(changes, events)))),
        }
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
    /// Waiting its turn to be fetched.
    Queued,
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

/// The task's state: every item with `custody/` tags, or about to have them.
struct Pins {
    keeper: Keeper,
    custody: Custody,
    pins: HashMap<ItemId, Pin>,
    /// The order of the `Queued` items. An item that left `Queued` meanwhile
    /// is skipped when its turn comes.
    queue: VecDeque<ItemId>,
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
                Some(Pin::Queued | Pin::Fetching { .. } | Pin::Failed) => false,
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
        match self.pins.entry(item) {
            Entry::Occupied(mut pin) => match pin.get_mut() {
                Pin::Fetching { wanted_again, .. } => *wanted_again = true,
                Pin::Queued => {}
                pin @ (Pin::Failed | Pin::Kept | Pin::Releasing) => {
                    *pin = Pin::Queued;
                    self.queue.push_back(item);
                }
            },
            Entry::Vacant(slot) => {
                slot.insert(Pin::Queued);
                self.queue.push_back(item);
            }
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
        while fetches.len() < FETCHES_AT_ONCE {
            let Some(item) = self.queue.pop_front() else {
                return;
            };
            let Some(pin @ Pin::Queued) = self.pins.get_mut(&item) else {
                continue;
            };
            let keeper = self.keeper.clone();
            let task = fetches
                .spawn(async move { (item, keeper.keep(item).await) })
                .id();
            *pin = Pin::Fetching {
                task,
                wanted_again: false,
                availability_changed: false,
            };
        }
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
                if wanted_again { Pin::Queued } else { Pin::Kept }
            }
            Err(error) => {
                if error.is_unavailable() {
                    tracing::warn!(%item, %error, "could not fetch an item this node keeps; asking again when availability changes");
                } else {
                    tracing::error!(%item, %error, "could not keep an item for custody; trying again when availability changes");
                }
                if wanted_again || availability_changed {
                    Pin::Queued
                } else {
                    Pin::Failed
                }
            }
        };
        if next == Pin::Queued {
            self.queue.push_back(item);
        }
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
                    *pin = Pin::Queued;
                    self.queue.push_back(*item);
                }
                Pin::Releasing => releasing.push(*item),
                Pin::Queued | Pin::Kept => {}
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

/// Why an item could not be kept.
#[derive(Debug, thiserror::Error)]
enum KeepError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Blobs(#[from] NetError),
    #[error(transparent)]
    Holdings(#[from] SyncError),
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
}

impl Keeper {
    /// Tags each of `item_id`'s files for custody and fetches the ones not
    /// here. A file already here, cached by a download say, is only tagged.
    async fn keep(&self, item_id: ItemId) -> Result<(), KeepError> {
        let Some(stored) = self.store.item(item_id).await? else {
            return Ok(());
        };
        let providers = self.providers(item_id);
        for hash in stored.item.files.keys().copied() {
            self.blobs.keep(KeepReason::Custody, item_id, hash).await?;
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
        let others = || online.iter().filter(|member| **member != self.me);
        if others().any(|member| !self.availability.knows_holdings_of(member)) {
            return false;
        }
        let holders = self.availability.holders(&item);
        let copies = others().filter(|member| holders.contains(member)).count();
        copies >= usize::from(self.membership.borrow().settings().replicas.get())
    }

    /// Moves `item`'s blobs from custody to the cache: the cache's tag first,
    /// so no collection in between finds them unkept.
    async fn release(&self, item: ItemId) -> Result<(), NetError> {
        for hash in self.blobs.kept_for(KeepReason::Custody, item).await? {
            self.blobs.keep(KeepReason::Cache, item, hash).await?;
            self.blobs.release(KeepReason::Custody, item, hash).await?;
        }
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
