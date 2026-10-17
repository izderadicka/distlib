//! The pin manager (phase 5's D7): keeps what this node is custodian of with a
//! `custody/` tag on each of the item's files, fetching what is not here, and
//! lets go of what it no longer is, into the cache, once the item has
//! `replicas` live copies elsewhere.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Mutex, PoisonError},
    time::Duration,
};

use distlib_consensus::MembershipState;
use distlib_core::{ContentHash, Event, ItemId, KeepReason, MemberId};
use distlib_net::{Blobs, NetError};
use distlib_store::Store;
use distlib_sync::{Availability, Holdings, availability::ask_order};
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        watch,
    },
    task::{JoinHandle, JoinSet},
};

use crate::{Custody, CustodyChange};

/// How long a fetch may go without a byte arriving before it is given up,
/// to be asked again when availability changes.
const STALLED_AFTER: Duration = Duration::from_secs(60);

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
            tagged: HashMap::new(),
            queue: VecDeque::new(),
            queued: HashSet::new(),
            fetching: None,
            availability_changed_while_fetching: false,
            waiting: HashSet::new(),
            releasing: HashSet::new(),
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

/// The task's state: what is tagged, and what is still to do.
struct Pins {
    keeper: Keeper,
    custody: Custody,
    /// The blobs each item's `custody/` tags keep, whether here yet or not.
    tagged: HashMap<ItemId, BTreeSet<ContentHash>>,
    /// Items to fetch, in the order they were wanted; `queued` is the same
    /// set, to look up.
    queue: VecDeque<ItemId>,
    queued: HashSet<ItemId>,
    /// The item being fetched now.
    fetching: Option<ItemId>,
    /// Whether availability changed while it was: a fetch that then fails is
    /// asked again at once, not at the next change.
    availability_changed_while_fetching: bool,
    /// Items whose fetch failed, asked for again when availability changes.
    waiting: HashSet<ItemId>,
    /// Items no longer this node's, kept until enough copies are elsewhere.
    releasing: HashSet<ItemId>,
}

impl Pins {
    async fn run(
        mut self,
        mut changes: broadcast::Receiver<CustodyChange>,
        mut events: broadcast::Receiver<Event>,
    ) {
        match self.keeper.blobs.kept(KeepReason::Custody).await {
            Ok(kept) => {
                for (item, blob) in kept {
                    self.tagged.entry(item).or_default().insert(blob);
                }
            }
            Err(error) => {
                tracing::error!(%error, "could not read what this node keeps for custody; the pin manager stops");
                return;
            }
        }
        // Custody computed before this started: nothing will announce it.
        self.read_custody_again().await;

        let mut fetches = JoinSet::new();
        loop {
            self.start_next_fetch(&mut fetches);
            tokio::select! {
                change = changes.recv() => match change {
                    Ok(CustodyChange::Recomputed) => self.read_custody_again().await,
                    Ok(CustodyChange::Item { item, mine: true }) => self.want(item),
                    Ok(CustodyChange::Item { item, mine: false }) => {
                        if self.tagged.contains_key(&item) {
                            self.release_if_safe(item).await;
                        }
                    }
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
                        self.availability_changed().await;
                    }
                    Ok(_) => {}
                    Err(RecvError::Closed) => {
                        tracing::error!("the event bus is gone; the pin manager stops");
                        return;
                    }
                },
                Some(joined) = fetches.join_next() => {
                    let item = self.fetching.take();
                    match joined {
                        Ok(fetched) => self.fetched(fetched).await,
                        Err(error) => {
                            tracing::error!(%error, "a custody fetch failed");
                            self.waiting.extend(item);
                        }
                    }
                }
            }
        }
    }

    /// Every item this node keeps, against what it has tagged: run after
    /// each whole pass. Nothing before custody's first pass, which would
    /// release everything.
    async fn read_custody_again(&mut self) {
        let Some(mine) = self.custody.my_items() else {
            return;
        };
        for item in &mine {
            let kept = self.tagged.contains_key(item) && self.keeper.holdings.holds(*item);
            if !kept {
                self.want(*item);
            }
        }
        let lost: Vec<ItemId> = self
            .tagged
            .keys()
            .filter(|item| !mine.contains(item))
            .copied()
            .collect();
        for item in lost {
            self.release_if_safe(item).await;
        }
    }

    /// Queues `item` to be kept.
    fn want(&mut self, item: ItemId) {
        self.releasing.remove(&item);
        self.waiting.remove(&item);
        if self.fetching != Some(item) && self.queued.insert(item) {
            self.queue.push_back(item);
        }
    }

    /// One fetch at a time.
    fn start_next_fetch(&mut self, fetches: &mut JoinSet<Fetched>) {
        if self.fetching.is_some() {
            return;
        }
        let Some(item) = self.queue.pop_front() else {
            return;
        };
        self.queued.remove(&item);
        self.fetching = Some(item);
        self.availability_changed_while_fetching = false;
        let keeper = self.keeper.clone();
        fetches.spawn(async move { keeper.keep(item).await });
    }

    async fn fetched(&mut self, fetched: Fetched) {
        let Fetched { item, tagged, done } = fetched;
        if !tagged.is_empty() {
            self.tagged.entry(item).or_default().extend(tagged);
        }
        if !self.custody.keeps(item) {
            // Lost while it was being fetched.
            self.release_if_safe(item).await;
        } else if !done && self.availability_changed_while_fetching {
            self.want(item);
        } else if !done {
            self.waiting.insert(item);
        }
    }

    /// A member came or went, or announced what it holds: a fetch that
    /// failed may now succeed, and an item may now have its copies elsewhere.
    async fn availability_changed(&mut self) {
        self.availability_changed_while_fetching = self.fetching.is_some();
        for item in std::mem::take(&mut self.waiting) {
            if self.custody.keeps(item) {
                self.want(item);
            }
        }
        let releasing: Vec<ItemId> = self.releasing.iter().copied().collect();
        for item in releasing {
            self.release_if_safe(item).await;
        }
    }

    /// Lets go of `item`, which this node is no longer custodian of, into the
    /// cache — once its copies elsewhere make up for this one, if this node
    /// holds it.
    async fn release_if_safe(&mut self, item: ItemId) {
        if self.fetching == Some(item) {
            // Decided once the fetch is done.
            return;
        }
        if self.keeper.holdings.holds(item) && !self.keeper.enough_copies_elsewhere(item) {
            self.releasing.insert(item);
            return;
        }
        let Some(blobs) = self.tagged.remove(&item) else {
            self.releasing.remove(&item);
            return;
        };
        match self.keeper.release(item, &blobs).await {
            Ok(()) => {
                self.releasing.remove(&item);
                tracing::debug!(%item, "no longer custodian; kept as cache");
            }
            Err(error) => {
                tracing::error!(%item, %error, "could not release an item into the cache; trying again when availability changes");
                self.tagged.insert(item, blobs);
                self.releasing.insert(item);
            }
        }
    }
}

/// One item's fetch, as it ended.
struct Fetched {
    item: ItemId,
    /// The blobs tagged for custody on the way.
    tagged: Vec<ContentHash>,
    /// Whether every file is here.
    done: bool,
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
    async fn keep(&self, item_id: ItemId) -> Fetched {
        let mut fetched = Fetched {
            item: item_id,
            tagged: Vec::new(),
            done: false,
        };
        let item = match self.store.item(item_id).await {
            Ok(Some(stored)) => stored.item,
            Ok(None) => {
                fetched.done = true;
                return fetched;
            }
            Err(error) => {
                tracing::error!(item = %item_id, %error, "could not read an item to keep it");
                return fetched;
            }
        };
        let providers = self.providers(item_id);
        for hash in item.files.keys().copied() {
            if let Err(error) = self.blobs.keep(KeepReason::Custody, item_id, hash).await {
                tracing::error!(item = %item_id, %error, "could not keep a file for custody");
                return fetched;
            }
            fetched.tagged.push(hash);
            match self.blobs.has(hash).await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    tracing::error!(item = %item_id, %error, "could not tell whether a file is here");
                    return fetched;
                }
            }
            if let Err(error) = fetch_until_stalled(&self.blobs, hash, providers.clone()).await {
                tracing::warn!(item = %item_id, %hash, %error, "could not fetch a file this node keeps; asking again when availability changes");
                return fetched;
            }
        }
        if let Err(error) = self.holdings.recheck(&item).await {
            tracing::warn!(item = %item_id, %error, "could not work out whether this node now holds an item");
        }
        tracing::debug!(item = %item_id, "kept for custody");
        fetched.done = true;
        fetched
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

    /// Moves `blobs` of `item` from custody to the cache: the cache's tag
    /// first, so no collection in between finds them unkept.
    async fn release(&self, item: ItemId, blobs: &BTreeSet<ContentHash>) -> Result<(), NetError> {
        for hash in blobs {
            self.blobs.keep(KeepReason::Cache, item, *hash).await?;
            self.blobs.release(KeepReason::Custody, item, *hash).await?;
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
) -> Result<(), String> {
    let (progressed, mut progress) = watch::channel(());
    let fetch = blobs.fetch_with_progress(hash, providers, move |_offset| {
        progressed.send_replace(());
    });
    tokio::pin!(fetch);
    loop {
        tokio::select! {
            biased;
            fetched = &mut fetch => return fetched.map_err(|error| error.to_string()),
            moved = tokio::time::timeout(STALLED_AFTER, progress.changed()) => {
                if moved.is_err() {
                    return Err(format!("no progress for {STALLED_AFTER:?}"));
                }
            }
        }
    }
}
