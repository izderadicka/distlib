//! The emergency copy (D6): an item down to one live copy for the group's
//! `emergency_after_secs` is fetched by the first member after its custodians
//! in score order that is online and does not hold it. A copy that failed a
//! probe is not a live one (D11). It is kept, past the storage limit if need
//! be, until the members obliged to keep the item hold enough copies (5e-6),
//! and then moved to the cache (5e-7). Its fetch takes one of custody's
//! `max_fetches` places, so many due at once arrive no faster than custody.
//! An item whose last holder is away keeps how long it has been alone, and is
//! due once the holder is back (5f-5).
//!
//! **Per observer, like availability itself.** Each node works out from who
//! it sees online whether the copy is its own to make; two that disagree may
//! both fetch, or neither until the next look.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use distlib_consensus::UpgradeRequired;
use distlib_core::{Event, ItemId, KeepReason, MemberId};
use distlib_store::StoreError;
use distlib_sync::availability::SoleHolder;
use tokio::{
    sync::{
        Notify,
        broadcast::{self, error::RecvError},
        watch,
    },
    task::JoinSet,
    time::{Instant, sleep_until},
};
use tracing::Instrument as _;

use crate::{
    Custody, CustodyChange,
    pins::{AVAILABILITY_GATHERED_FOR, KeepError, Keeper},
    service::custody_group,
};

/// Where an item this node does not hold, down to one live copy, stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Alone {
    /// One live copy since then.
    Since(Instant),
    /// One live copy for long enough. Whose the copy is to make depends on
    /// who is online, so it is worked out again at each look.
    Due,
    /// Being copied here.
    Copying,
}

/// Every item down to one live copy that this node does not hold.
#[derive(Debug, Default)]
struct AloneItems(HashMap<ItemId, Alone>);

impl AloneItems {
    /// Brings the items up to those alone `now`, and returns the ones due:
    /// alone for `delay` since they last had more than one copy, not being
    /// copied, and with their holder online. One whose holder is `away`
    /// keeps its time, and one being copied stays, alone or not (5f-5).
    fn update(
        &mut self,
        alone_now: &HashSet<ItemId>,
        away: impl IntoIterator<Item = ItemId>,
        now: Instant,
        delay: Duration,
    ) -> Vec<ItemId> {
        let mut previous = std::mem::take(&mut self.0);
        let mut alone: HashMap<ItemId, Alone> = alone_now
            .iter()
            .map(|item| (*item, previous.remove(item).unwrap_or(Alone::Since(now))))
            .collect();
        alone.extend(
            away.into_iter()
                .filter_map(|item| Some((item, previous.remove(&item)?))),
        );
        alone.extend(
            previous
                .into_iter()
                .filter(|(_, alone)| *alone == Alone::Copying),
        );
        let due = alone
            .iter_mut()
            .filter_map(|(item, alone)| {
                if let Alone::Since(since) = *alone
                    && now >= since + delay
                {
                    *alone = Alone::Due;
                }
                (*alone == Alone::Due && alone_now.contains(item)).then_some(*item)
            })
            .collect();
        self.0 = alone;
        due
    }

    /// When the next item alone becomes due.
    fn next_due(&self, delay: Duration) -> Option<Instant> {
        self.0
            .values()
            .filter_map(|alone| match alone {
                Alone::Since(since) => Some(*since + delay),
                Alone::Due | Alone::Copying => None,
            })
            .min()
    }

    fn copying(&mut self, item: ItemId) {
        self.0.insert(item, Alone::Copying);
    }

    /// A copy done is no longer alone here; one failed is due again.
    fn copied(&mut self, item: ItemId, succeeded: bool) {
        if succeeded {
            self.0.remove(&item);
        } else {
            self.0.insert(item, Alone::Due);
        }
    }
}

/// The task's state.
pub(crate) struct Emergency {
    keeper: Keeper,
    /// The settled group, which says who is obliged to keep an item.
    custody: Custody,
    /// Set once this node stops following the log until it is upgraded:
    /// from then on no emergency copy becomes cache (5d-8).
    upgrade_required: watch::Receiver<Option<UpgradeRequired>>,
    /// Told when an emergency copy becomes cache, for custody waiting for
    /// room.
    room_made: Arc<Notify>,
    alone: AloneItems,
}

impl Emergency {
    pub(crate) fn new(
        keeper: Keeper,
        custody: Custody,
        upgrade_required: watch::Receiver<Option<UpgradeRequired>>,
        room_made: Arc<Notify>,
    ) -> Self {
        Self {
            keeper,
            custody,
            upgrade_required,
            room_made,
            alone: AloneItems::default(),
        }
    }

    pub(crate) async fn run(
        mut self,
        mut events: broadcast::Receiver<Event>,
        mut changes: broadcast::Receiver<CustodyChange>,
    ) {
        let mut copies = JoinSet::new();
        // A first look, for whatever was alone before this started.
        let mut look_at_availability = Some(Instant::now() + AVAILABILITY_GATHERED_FOR);
        loop {
            let next_due = self.alone.next_due(self.delay());
            tokio::select! {
                event = events.recv() => match event {
                    Ok(Event::AvailabilityChanged { .. }) | Err(RecvError::Lagged(_)) => {
                        look_at_availability
                            .get_or_insert_with(|| Instant::now() + AVAILABILITY_GATHERED_FOR);
                    }
                    Ok(_) => {}
                    Err(RecvError::Closed) => {
                        tracing::error!("the event bus is gone; emergency copies stop");
                        return;
                    }
                },
                // A new settled group may oblige members who hold a copy
                // already, which changes nobody's availability.
                change = changes.recv() => match change {
                    Ok(CustodyChange::Recomputed) | Err(RecvError::Lagged(_)) => {
                        look_at_availability
                            .get_or_insert_with(|| Instant::now() + AVAILABILITY_GATHERED_FOR);
                    }
                    Ok(CustodyChange::Item { .. }) => {}
                    Err(RecvError::Closed) => {
                        tracing::error!("custody is gone; emergency copies stop");
                        return;
                    }
                },
                () = sleep_until(look_at_availability.unwrap_or_else(Instant::now)),
                    if look_at_availability.is_some() =>
                {
                    look_at_availability = None;
                    self.look(&mut copies).await;
                }
                () = sleep_until(next_due.unwrap_or_else(Instant::now)), if next_due.is_some() => {
                    self.look(&mut copies).await;
                }
                Some(joined) = copies.join_next() => match joined {
                    Ok((item, copied)) => self.copied(item, copied),
                    Err(error) => {
                        tracing::error!(%error, "an emergency copy panicked; it is not made again until the item is alone again");
                    }
                },
            }
        }
    }

    /// The group's `emergency_after_secs`.
    fn delay(&self) -> Duration {
        Duration::from_secs(
            self.keeper
                .membership
                .borrow()
                .settings()
                .emergency_after_secs
                .into(),
        )
    }

    /// Brings the items alone up to date, starts a copy of each one due that
    /// is this node's to make, and moves to the cache the copies no longer
    /// needed.
    async fn look(&mut self, copies: &mut JoinSet<(ItemId, Result<(), KeepError>)>) {
        self.move_unneeded_copies_to_cache().await;
        let online = self.keeper.availability.online().borrow().clone();
        let mut holders: HashMap<ItemId, MemberId> = HashMap::new();
        let mut away = Vec::new();
        for (item, holder) in self
            .keeper
            .availability
            .sole_holders(&online)
            .into_iter()
            .filter(|(item, _)| !self.keeper.holdings.holds(*item))
        {
            match holder {
                SoleHolder::Online(holder) => {
                    holders.insert(item, holder);
                }
                SoleHolder::Away => away.push(item),
            }
        }
        let alone_now = holders.keys().copied().collect();
        let due = self
            .alone
            .update(&alone_now, away, Instant::now(), self.delay());
        for item in due {
            let holder = holders[&item];
            match self.is_mine_to_copy(item, holder, &online).await {
                Ok(true) => {
                    tracing::info!(%item, holder = %holder.fmt_short(), "one copy left for too long; keeping an emergency copy");
                    self.alone.copying(item);
                    let keeper = self.keeper.clone();
                    copies.spawn(
                        async move { (item, keeper.keep(item, KeepReason::Emergency).await) }
                            .in_current_span(),
                    );
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%item, %error, "could not work out whose emergency copy an item is; asked again at the next look");
                }
            }
        }
    }

    /// Moves each emergency copy whose item has enough copies elsewhere, of
    /// the members obliged to keep it, to the cache. Never while this node
    /// waits for an upgrade: its view of the group is behind (5d-8).
    async fn move_unneeded_copies_to_cache(&self) {
        let Some(group) = self.custody.group() else {
            return;
        };
        if self.upgrade_required.borrow().is_some() {
            return;
        }
        let items = match self.keeper.kept_items(KeepReason::Emergency).await {
            Ok(items) => items,
            Err(error) => {
                tracing::warn!(%error, "could not list the emergency copies; asked again at the next look");
                return;
            }
        };
        for item in items {
            let moved = match self.keeper.enough_copies_elsewhere(item, &group).await {
                Ok(true) => self.keeper.move_to_cache(item, KeepReason::Emergency).await,
                Ok(false) => continue,
                Err(error) => Err(error.into()),
            };
            match moved {
                Ok(()) => {
                    tracing::info!(%item, "the item has its copies again; its emergency copy is now cache");
                    self.room_made.notify_one();
                }
                Err(error) => {
                    tracing::warn!(%item, %error, "could not move an emergency copy to the cache; asked again at the next look")
                }
            }
        }
    }

    /// Whether this node is the first after `item`'s custodians, of those
    /// online and not `holder`.
    async fn is_mine_to_copy(
        &self,
        item: ItemId,
        holder: MemberId,
        online: &BTreeSet<MemberId>,
    ) -> Result<bool, StoreError> {
        let Some(stored) = self.keeper.store.item(item).await? else {
            return Ok(false);
        };
        let me = self.keeper.me;
        let group = custody_group(&self.keeper.membership.borrow());
        let first = group.first_after_custodians(&stored.custody_item(), |member| {
            member != holder && (member == me || online.contains(&member))
        });
        Ok(first == Some(me))
    }

    fn copied(&mut self, item: ItemId, copied: Result<(), KeepError>) {
        if let Err(error) = &copied {
            tracing::warn!(%item, %error, "could not keep an emergency copy; trying again at the next look");
        }
        self.alone.copied(item, copied.is_ok());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DELAY: Duration = Duration::from_secs(10);

    fn item(seed: u8) -> ItemId {
        ItemId::from_bytes([seed; 32])
    }

    #[test]
    fn an_item_is_due_once_alone_for_the_delay() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        assert!(
            alone
                .update(&HashSet::from([item(1)]), [], start, DELAY)
                .is_empty()
        );
        assert_eq!(alone.next_due(DELAY), Some(start + DELAY));
        assert!(
            alone
                .update(&HashSet::from([item(1)]), [], start + DELAY / 2, DELAY)
                .is_empty()
        );
        assert_eq!(
            alone.update(&HashSet::from([item(1)]), [], start + DELAY, DELAY),
            [item(1)]
        );
        assert_eq!(alone.next_due(DELAY), None);
    }

    #[test]
    fn a_second_copy_starts_the_delay_over() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        alone.update(&HashSet::from([item(1)]), [], start, DELAY);
        alone.update(&HashSet::from([]), [], start + DELAY / 2, DELAY);
        assert!(
            alone
                .update(&HashSet::from([item(1)]), [], start + DELAY, DELAY)
                .is_empty()
        );
        assert_eq!(alone.next_due(DELAY), Some(start + DELAY * 2));
    }

    /// #200's finding 1, reversed (5f-5): at a look made while the last
    /// holder is away the item keeps its time, and it is due at the first
    /// look after the holder is back once the delay has passed.
    #[test]
    fn a_last_holder_away_at_a_look_keeps_the_items_time() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        let one = HashSet::from([item(1)]);
        assert!(alone.update(&one, [], start, DELAY).is_empty());
        assert!(
            alone
                .update(&HashSet::new(), [item(1)], start + DELAY / 2, DELAY)
                .is_empty()
        );
        assert_eq!(alone.update(&one, [], start + DELAY, DELAY), [item(1)]);

        // Away when the delay passes: not due until it is back.
        let mut alone = AloneItems::default();
        alone.update(&one, [], start, DELAY);
        assert!(
            alone
                .update(&HashSet::new(), [item(1)], start + DELAY * 2, DELAY)
                .is_empty(),
            "nobody online to copy it from"
        );
        assert_eq!(alone.next_due(DELAY), None, "no look until it is back");
        assert_eq!(alone.update(&one, [], start + DELAY * 2, DELAY), [item(1)]);
    }

    #[test]
    fn an_item_away_that_was_not_alone_is_not_timed() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        alone.update(&HashSet::new(), [item(1)], start, DELAY);
        assert_eq!(alone.next_due(DELAY), None);
        assert!(
            alone
                .update(&HashSet::from([item(1)]), [], start + DELAY, DELAY)
                .is_empty(),
            "its time starts when it is seen alone"
        );
    }

    #[test]
    fn an_item_being_copied_is_not_due_and_a_failed_copy_is() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        alone.update(&HashSet::from([item(1), item(2)]), [], start, DELAY);
        alone.update(&HashSet::from([item(1), item(2)]), [], start + DELAY, DELAY);
        alone.copying(item(1));
        assert_eq!(
            alone.update(&HashSet::from([item(2)]), [], start + DELAY, DELAY),
            [item(2)]
        );
        // Alone again while still being copied: not a second copy.
        alone.update(&HashSet::from([item(1), item(2)]), [], start + DELAY, DELAY);
        assert_eq!(
            alone.update(
                &HashSet::from([item(1), item(2)]),
                [],
                start + DELAY * 2,
                DELAY
            ),
            [item(2)]
        );
        alone.copied(item(1), false);
        let mut due = alone.update(&HashSet::from([item(1), item(2)]), [], start + DELAY, DELAY);
        due.sort_unstable();
        assert_eq!(due, [item(1), item(2)]);
        alone.copying(item(1));
        alone.copied(item(1), true);
        assert!(
            alone
                .update(&HashSet::from([]), [], start + DELAY, DELAY)
                .is_empty()
        );
    }
}
