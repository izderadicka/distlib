//! The emergency copy (D6): an item down to one live copy for the group's
//! `emergency_after_secs` is fetched by the first member after its custodians
//! in score order that is online and does not hold it. A copy that failed a
//! probe is not a live one (D11). It is kept, past the storage limit if need
//! be, until the members obliged to keep the item hold enough copies (5e-6),
//! and then moved to the cache (5e-7). Its fetch takes one of custody's
//! `max_fetches` places, so many due at once arrive no faster than custody.
//! While it stays due, one more member in that order makes the copy each
//! [`EMERGENCY_FALLBACK_AFTER`], in case those before keep failing (5i-6).
//! An item whose last holder is away keeps how long it has been alone, and is
//! due once the holder is back (5f-5). A copy that failed, or panicked, is
//! due again once its retry is (5i-4). An item seen alone keeps its time until a
//! look sees its copies, or it leaves the catalogue (5i-5): a copy failing a
//! probe, or its holder's index being fetched again, does not start it over.
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
    task::{self, JoinSet},
    time::{Instant, sleep_until},
};
use tracing::Instrument as _;

use crate::{
    Custody, CustodyChange,
    pins::{AVAILABILITY_GATHERED_FOR, KeepError, Keeper},
    retry::Failures,
    service::custody_group,
};

/// How long an item stays due before the next member after its custodians
/// makes the copy too, in case those before keep failing (5i-6).
const EMERGENCY_FALLBACK_AFTER: Duration = Duration::from_secs(60 * 60);

/// An item this node does not hold, down to one live copy since `since`, and
/// its copy here. Due once alone for the group's delay; whose the copy is to
/// make depends on who is online, so it is worked out again at each look.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Alone {
    since: Instant,
    copy: CopyHere,
}

/// Where an item's emergency copy here stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyHere {
    /// Not under way: made once the item is due and it is this node's turn.
    NotStarted { failures: Failures },
    /// Being made by `task`.
    Copying { task: task::Id, failures: Failures },
    /// Failed: not started again before `retry_at`.
    Failed {
        failures: Failures,
        retry_at: Instant,
    },
}

/// Every item down to one live copy that this node does not hold.
#[derive(Debug, Default)]
struct AloneItems(HashMap<ItemId, Alone>);

impl AloneItems {
    /// Brings the items up to those alone `now`, and returns the ones due,
    /// with how long they have been: alone for `delay`, no copy under way or
    /// waiting for its retry, and with their holder online. One
    /// `without_a_source`, its holder away or no copy known, keeps its time
    /// (5f-5, 5i-5), and one being copied stays, alone or not.
    fn update(
        &mut self,
        alone_now: &HashSet<ItemId>,
        without_a_source: impl IntoIterator<Item = ItemId>,
        now: Instant,
        delay: Duration,
    ) -> Vec<(ItemId, Duration)> {
        let mut previous = std::mem::take(&mut self.0);
        let new = Alone {
            since: now,
            copy: CopyHere::NotStarted {
                failures: Failures::default(),
            },
        };
        let mut alone: HashMap<ItemId, Alone> = alone_now
            .iter()
            .map(|item| (*item, previous.remove(item).unwrap_or(new)))
            .collect();
        alone.extend(
            without_a_source
                .into_iter()
                .filter_map(|item| Some((item, previous.remove(&item)?))),
        );
        alone.extend(
            previous
                .into_iter()
                .filter(|(_, alone)| matches!(alone.copy, CopyHere::Copying { .. })),
        );
        let due = alone
            .iter_mut()
            .filter_map(|(item, alone)| {
                if let CopyHere::Failed { failures, retry_at } = alone.copy
                    && now >= retry_at
                {
                    alone.copy = CopyHere::NotStarted { failures };
                }
                let due_for = now.checked_duration_since(alone.since + delay)?;
                (matches!(alone.copy, CopyHere::NotStarted { .. }) && alone_now.contains(item))
                    .then_some((*item, due_for))
            })
            .collect();
        self.0 = alone;
        due
    }

    /// Every item alone here.
    fn items(&self) -> impl Iterator<Item = ItemId> + '_ {
        self.0.keys().copied()
    }

    /// When the next look is due after `now`: an item becoming due, the next
    /// member's turn at one due (5i-6), or a failed copy's retry.
    fn next_due(&self, delay: Duration, now: Instant) -> Option<Instant> {
        self.0
            .values()
            .filter_map(|alone| match alone.copy {
                CopyHere::NotStarted { .. } => next_turn(alone.since + delay, now),
                CopyHere::Failed { retry_at, .. } => Some(retry_at),
                CopyHere::Copying { .. } => None,
            })
            .min()
    }

    /// `item`, due, is being copied by `task`.
    fn copying(&mut self, item: ItemId, task: task::Id) {
        if let Some(alone) = self.0.get_mut(&item)
            && let CopyHere::NotStarted { failures } = alone.copy
        {
            alone.copy = CopyHere::Copying { task, failures };
        }
    }

    /// The item `task` is copying.
    fn copied_by(&self, task: task::Id) -> Option<ItemId> {
        self.0.iter().find_map(|(item, alone)| {
            matches!(alone.copy, CopyHere::Copying { task: copying, .. } if copying == task)
                .then_some(*item)
        })
    }

    /// A copy done at `now` is no longer alone here; one failed is due again
    /// once its retry is.
    fn copied(&mut self, item: ItemId, succeeded: bool, now: Instant) {
        if succeeded {
            self.0.remove(&item);
        } else if let Some(alone) = self.0.get_mut(&item)
            && let CopyHere::Copying { failures, .. } = alone.copy
        {
            let (failures, retry_at) = failures.one_more(now);
            alone.copy = CopyHere::Failed { failures, retry_at };
        }
    }
}

/// When the next member's turn comes, after `now`, for an item due from
/// `due_at`: `due_at` itself, then each [`EMERGENCY_FALLBACK_AFTER`] on.
fn next_turn(due_at: Instant, now: Instant) -> Option<Instant> {
    let Some(due_for) = now.checked_duration_since(due_at) else {
        return Some(due_at);
    };
    let turns_passed = due_for.as_secs() / EMERGENCY_FALLBACK_AFTER.as_secs();
    let next = u32::try_from(turns_passed.saturating_add(1)).ok()?;
    due_at.checked_add(EMERGENCY_FALLBACK_AFTER.checked_mul(next)?)
}

/// Whether the member at `rank` after an item's custodians, counting from
/// 0, makes its copy once it has been due for `due_for`: one more member each
/// [`EMERGENCY_FALLBACK_AFTER`] (5i-6).
fn is_turn_of(rank: usize, due_for: Duration) -> bool {
    u32::try_from(rank)
        .ok()
        .and_then(|rank| EMERGENCY_FALLBACK_AFTER.checked_mul(rank))
        .is_some_and(|turn| due_for >= turn)
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
            let next_due = self.alone.next_due(self.delay(), Instant::now());
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
                Some(joined) = copies.join_next_with_id() => match joined {
                    Ok((_, (item, copied))) => self.copied(item, copied),
                    Err(error) => {
                        if let Some(item) = self.alone.copied_by(error.id()) {
                            self.copied(item, Err(KeepError::Panicked(error)));
                        }
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
        let mut without_a_source = Vec::new();
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
                SoleHolder::Away => without_a_source.push(item),
            }
        }
        let alone_before: Vec<ItemId> = self
            .alone
            .items()
            .filter(|item| !holders.contains_key(item))
            .collect();
        for item in alone_before {
            if !self.no_longer_alone(item, &online).await {
                without_a_source.push(item);
            }
        }
        let alone_now = holders.keys().copied().collect();
        let due = self
            .alone
            .update(&alone_now, without_a_source, Instant::now(), self.delay());
        for (item, due_for) in due {
            let holder = holders[&item];
            match self.is_mine_to_copy(item, holder, &online, due_for).await {
                Ok(true) => {
                    tracing::info!(%item, holder = %holder.fmt_short(), "one copy left for too long; keeping an emergency copy");
                    let keeper = self.keeper.clone();
                    let task = copies
                        .spawn(
                            async move { (item, keeper.keep(item, KeepReason::Emergency).await) }
                                .in_current_span(),
                        )
                        .id();
                    self.alone.copying(item, task);
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%item, %error, "could not work out whose emergency copy an item is; asked again at the next look");
                }
            }
        }
    }

    /// Whether `item`, alone at an earlier look, has its copies now, or has
    /// left the catalogue.
    async fn no_longer_alone(&self, item: ItemId, online: &BTreeSet<MemberId>) -> bool {
        let held_here = self.keeper.holdings.holds(item);
        if has_its_copies(
            held_here,
            &self.keeper.availability.real_holders(&item),
            online,
        ) {
            return true;
        }
        match self.keeper.store.item(item).await {
            Ok(stored) => stored.is_none(),
            Err(error) => {
                tracing::warn!(%item, %error, "could not read an item alone; it keeps its time until the next look");
                false
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

    /// Whether it is this node's turn to copy `item`, due for `due_for`: by
    /// its rank after the custodians, of those online and not `holder`.
    async fn is_mine_to_copy(
        &self,
        item: ItemId,
        holder: MemberId,
        online: &BTreeSet<MemberId>,
        due_for: Duration,
    ) -> Result<bool, StoreError> {
        let Some(stored) = self.keeper.store.item(item).await? else {
            return Ok(false);
        };
        let me = self.keeper.me;
        let group = custody_group(&self.keeper.membership.borrow());
        let rank = group
            .after_custodians(&stored.custody_item())
            .filter(|member| *member != holder && (*member == me || online.contains(member)))
            .position(|member| member == me);
        Ok(rank.is_some_and(|rank| is_turn_of(rank, due_for)))
    }

    fn copied(&mut self, item: ItemId, copied: Result<(), KeepError>) {
        if let Err(error) = &copied {
            tracing::warn!(%item, %error, "could not keep an emergency copy; trying again once its retry is due");
        }
        self.alone.copied(item, copied.is_ok(), Instant::now());
    }
}

/// Whether an item has its copies: this node holds it, or two of
/// `real_holders` are `online`.
fn has_its_copies(held_here: bool, real_holders: &[MemberId], online: &BTreeSet<MemberId>) -> bool {
    held_here
        || real_holders
            .iter()
            .filter(|member| online.contains(member))
            .nth(1)
            .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DELAY: Duration = Duration::from_secs(10);
    const HOUR: Duration = EMERGENCY_FALLBACK_AFTER;

    impl AloneItems {
        /// [`Self::update`]'s items due, leaving out for how long.
        fn due(
            &mut self,
            alone_now: &HashSet<ItemId>,
            without_a_source: impl IntoIterator<Item = ItemId>,
            now: Instant,
            delay: Duration,
        ) -> Vec<ItemId> {
            self.update(alone_now, without_a_source, now, delay)
                .into_iter()
                .map(|(item, _)| item)
                .collect()
        }
    }

    /// One more member after the custodians makes the copy each hour the
    /// item stays due: the first at once, the second after an hour, the third
    /// after two (5i-6).
    #[test]
    fn one_more_member_takes_its_turn_each_hour_an_item_is_due() {
        let second = Duration::from_secs(1);
        assert!(is_turn_of(0, Duration::ZERO));
        assert!(!is_turn_of(1, HOUR - second));
        assert!(is_turn_of(1, HOUR));
        assert!(
            !is_turn_of(2, HOUR * 2 - second),
            "the third does not start"
        );
        assert!(is_turn_of(2, HOUR * 2));
    }

    /// A look comes when an item becomes due, and at each member's turn on.
    #[test]
    fn a_look_comes_at_each_members_turn() {
        let (due_at, second) = (Instant::now(), Duration::from_secs(1));
        assert_eq!(next_turn(due_at + second, due_at), Some(due_at + second));
        assert_eq!(next_turn(due_at, due_at), Some(due_at + HOUR));
        assert_eq!(
            next_turn(due_at, due_at + HOUR + second),
            Some(due_at + HOUR * 2)
        );
    }

    #[test]
    fn an_item_due_is_told_with_how_long_it_has_been() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        let one = HashSet::from([item(1)]);
        alone.update(&one, [], start, DELAY);
        assert_eq!(
            alone.update(&one, [], start + DELAY + HOUR, DELAY),
            [(item(1), HOUR)]
        );
    }

    /// An item seen alone keeps its time until it has two online real
    /// copies or this node holds it (5i-5): one online and one away is still
    /// one.
    #[test]
    fn an_item_has_its_copies_with_two_online_or_one_here() {
        let member = || MemberId::from(iroh::SecretKey::generate().public());
        let (bob, carol, dave) = (member(), member(), member());
        let online = BTreeSet::from([bob, carol]);

        assert!(has_its_copies(false, &[bob, carol], &online));
        assert!(
            !has_its_copies(false, &[bob, dave], &online),
            "dave is away"
        );
        assert!(!has_its_copies(false, &[], &online));
        assert!(has_its_copies(true, &[], &online));
    }

    fn item(seed: u8) -> ItemId {
        ItemId::from_bytes([seed; 32])
    }

    #[test]
    fn an_item_is_due_once_alone_for_the_delay() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        assert!(
            alone
                .due(&HashSet::from([item(1)]), [], start, DELAY)
                .is_empty()
        );
        assert_eq!(alone.next_due(DELAY, start), Some(start + DELAY));
        assert!(
            alone
                .due(&HashSet::from([item(1)]), [], start + DELAY / 2, DELAY)
                .is_empty()
        );
        assert_eq!(
            alone.due(&HashSet::from([item(1)]), [], start + DELAY, DELAY),
            [item(1)]
        );
        assert_eq!(
            alone.next_due(DELAY, start + DELAY),
            Some(start + DELAY + HOUR),
            "the next member's turn"
        );
    }

    #[test]
    fn a_second_copy_starts_the_delay_over() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        alone.due(&HashSet::from([item(1)]), [], start, DELAY);
        alone.due(&HashSet::from([]), [], start + DELAY / 2, DELAY);
        assert!(
            alone
                .due(&HashSet::from([item(1)]), [], start + DELAY, DELAY)
                .is_empty()
        );
        assert_eq!(
            alone.next_due(DELAY, start + DELAY),
            Some(start + DELAY * 2)
        );
    }

    /// #200's finding 1, reversed (5f-5): at a look made while the last
    /// holder is away the item keeps its time, and it is due at the first
    /// look after the holder is back once the delay has passed.
    #[test]
    fn a_last_holder_away_at_a_look_keeps_the_items_time() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        let one = HashSet::from([item(1)]);
        assert!(alone.due(&one, [], start, DELAY).is_empty());
        assert!(
            alone
                .due(&HashSet::new(), [item(1)], start + DELAY / 2, DELAY)
                .is_empty()
        );
        assert_eq!(alone.due(&one, [], start + DELAY, DELAY), [item(1)]);

        // Away when the delay passes: not due until it is back.
        let mut alone = AloneItems::default();
        alone.due(&one, [], start, DELAY);
        assert!(
            alone
                .due(&HashSet::new(), [item(1)], start + DELAY * 2, DELAY)
                .is_empty(),
            "nobody online to copy it from"
        );
        assert_eq!(
            alone.next_due(DELAY, start + DELAY * 2),
            Some(start + DELAY + HOUR),
            "a look at the next member's turn, and when it is back"
        );
        assert_eq!(alone.due(&one, [], start + DELAY * 2, DELAY), [item(1)]);
    }

    #[test]
    fn an_item_away_that_was_not_alone_is_not_timed() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        alone.due(&HashSet::new(), [item(1)], start, DELAY);
        assert_eq!(alone.next_due(DELAY, start), None);
        assert!(
            alone
                .due(&HashSet::from([item(1)]), [], start + DELAY, DELAY)
                .is_empty(),
            "its time starts when it is seen alone"
        );
    }

    /// A copy under way is not due again, alone or not; one that failed is
    /// due once its retry is, with no other look (5i-4); one done is gone.
    #[tokio::test]
    async fn an_item_being_copied_is_not_due_and_a_failed_copy_is_once_its_retry_is() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        let both = HashSet::from([item(1), item(2)]);
        alone.due(&both, [], start, DELAY);
        alone.due(&both, [], start + DELAY, DELAY);
        let task = tokio::spawn(async {}).id();
        alone.copying(item(1), task);
        assert_eq!(alone.copied_by(task), Some(item(1)));
        assert_eq!(
            alone.due(&HashSet::from([item(2)]), [], start + DELAY, DELAY),
            [item(2)]
        );
        // Alone again while still being copied: not a second copy.
        assert_eq!(alone.due(&both, [], start + DELAY * 2, DELAY), [item(2)]);

        let failed_at = start + DELAY * 2;
        alone.copied(item(1), false, failed_at);
        assert_eq!(alone.copied_by(task), None);
        let (_, retry_at) = Failures::default().one_more(failed_at);
        assert_eq!(
            alone.next_due(DELAY, failed_at),
            Some(retry_at),
            "a look then"
        );
        assert_eq!(alone.due(&both, [], retry_at - DELAY, DELAY), [item(2)]);
        let mut due = alone.due(&both, [], retry_at, DELAY);
        due.sort_unstable();
        assert_eq!(due, [item(1), item(2)]);

        alone.copying(item(1), task);
        alone.copied(item(1), true, retry_at);
        assert!(alone.due(&HashSet::new(), [], retry_at, DELAY).is_empty());
        assert_eq!(alone.next_due(DELAY, retry_at), None);
    }

    /// Each failure in a row waits longer: the count goes from the failed
    /// copy through the next one.
    #[tokio::test]
    async fn a_second_failure_in_a_row_waits_longer() {
        let (mut alone, start) = (AloneItems::default(), Instant::now());
        let one = HashSet::from([item(1)]);
        alone.due(&one, [], start, DELAY);
        alone.due(&one, [], start + DELAY, DELAY);
        let task = tokio::spawn(async {}).id();
        alone.copying(item(1), task);
        alone.copied(item(1), false, start);
        let (once, first_retry) = Failures::default().one_more(start);
        assert_eq!(alone.next_due(DELAY, start), Some(first_retry));
        assert_eq!(alone.due(&one, [], first_retry, DELAY), [item(1)]);
        alone.copying(item(1), task);
        alone.copied(item(1), false, start);

        let (_, second_retry) = once.one_more(start);
        assert!(second_retry > first_retry);
        assert_eq!(alone.next_due(DELAY, start), Some(second_retry));
    }
}
