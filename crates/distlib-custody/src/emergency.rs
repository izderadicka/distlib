//! The emergency copy (D6): an item down to one live copy for the group's
//! `emergency_after_secs` is fetched, as cache, by the first member after its
//! custodians in score order that is online and does not hold it. A copy that
//! failed a probe is not a live one (D11).
//!
//! **Per observer, like availability itself.** Each node works out from who
//! it sees online whether the copy is its own to make; two that disagree may
//! both fetch, or neither until the next look.

use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, SystemTime},
};

use distlib_core::{Event, ItemId, KeepReason, MemberId};
use distlib_store::StoreError;
use tokio::{
    sync::broadcast::{self, error::RecvError},
    task::JoinSet,
    time::{Instant, sleep_until},
};

use crate::{
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
    /// alone for `delay` since they last had more than one copy, and not
    /// being copied. One being copied stays, alone or not.
    fn update(
        &mut self,
        alone_now: impl IntoIterator<Item = ItemId>,
        now: Instant,
        delay: Duration,
    ) -> Vec<ItemId> {
        let mut previous = std::mem::take(&mut self.0);
        let mut alone: HashMap<ItemId, Alone> = alone_now
            .into_iter()
            .map(|item| (item, previous.remove(&item).unwrap_or(Alone::Since(now))))
            .collect();
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
                (*alone == Alone::Due).then_some(*item)
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
    alone: AloneItems,
}

impl Emergency {
    pub(crate) fn new(keeper: Keeper) -> Self {
        Self {
            keeper,
            alone: AloneItems::default(),
        }
    }

    pub(crate) async fn run(mut self, mut events: broadcast::Receiver<Event>) {
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

    /// Brings the items alone up to date, and starts a copy of each one due
    /// that is this node's to make.
    async fn look(&mut self, copies: &mut JoinSet<(ItemId, Result<(), KeepError>)>) {
        let online = self.keeper.availability.online().borrow().clone();
        let holders: HashMap<ItemId, MemberId> = self
            .keeper
            .availability
            .sole_holders(&online)
            .into_iter()
            .filter(|(item, _)| !self.keeper.holdings.holds(*item))
            .collect();
        let due = self
            .alone
            .update(holders.keys().copied(), Instant::now(), self.delay());
        for item in due {
            let holder = holders[&item];
            match self.is_mine_to_copy(item, holder, &online).await {
                Ok(true) => {
                    tracing::info!(%item, holder = %holder.fmt_short(), "one copy left for too long; keeping an emergency copy");
                    self.alone.copying(item);
                    let keeper = self.keeper.clone();
                    copies.spawn(async move { (item, copy(&keeper, item).await) });
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%item, %error, "could not work out whose emergency copy an item is; asked again at the next look");
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

/// Keeps `item` as cache, counted as used first so it is never the first to
/// go, not even just after it lands.
async fn copy(keeper: &Keeper, item: ItemId) -> Result<(), KeepError> {
    keeper.store.mark_used(item, SystemTime::now()).await?;
    keeper.keep(item, KeepReason::Cache).await
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
        assert!(alone.update([item(1)], start, DELAY).is_empty());
        assert_eq!(alone.next_due(DELAY), Some(start + DELAY));
        assert!(alone.update([item(1)], start + DELAY / 2, DELAY).is_empty());
        assert_eq!(alone.update([item(1)], start + DELAY, DELAY), [item(1)]);
        assert_eq!(alone.next_due(DELAY), None);
    }

    #[test]
    fn a_second_copy_starts_the_delay_over() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        alone.update([item(1)], start, DELAY);
        alone.update([], start + DELAY / 2, DELAY);
        assert!(alone.update([item(1)], start + DELAY, DELAY).is_empty());
        assert_eq!(alone.next_due(DELAY), Some(start + DELAY * 2));
    }

    #[test]
    fn an_item_being_copied_is_not_due_and_a_failed_copy_is() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        alone.update([item(1), item(2)], start, DELAY);
        alone.update([item(1), item(2)], start + DELAY, DELAY);
        alone.copying(item(1));
        assert_eq!(alone.update([item(2)], start + DELAY, DELAY), [item(2)]);
        // Alone again while still being copied: not a second copy.
        alone.update([item(1), item(2)], start + DELAY, DELAY);
        assert_eq!(
            alone.update([item(1), item(2)], start + DELAY * 2, DELAY),
            [item(2)]
        );
        alone.copied(item(1), false);
        let mut due = alone.update([item(1), item(2)], start + DELAY, DELAY);
        due.sort_unstable();
        assert_eq!(due, [item(1), item(2)]);
        alone.copying(item(1));
        alone.copied(item(1), true);
        assert!(alone.update([], start + DELAY, DELAY).is_empty());
    }
}
