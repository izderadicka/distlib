//! This node's custody, as of the last settled membership.

use std::collections::{HashMap, HashSet};

use distlib_core::{CustodyGroup, CustodyItem, ItemId, MemberId};
use serde::Serialize;
use tokio::time::Instant;

/// What `node.status` says about this node's custody.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct CustodyStatus {
    /// How many items this node is custodian of.
    pub items: usize,
    /// Their size, which weighted rendezvous may take past the pledge: it
    /// is all kept even so.
    pub bytes: u64,
    /// This node's pledge, as the settled membership has it.
    pub pledge_bytes: u64,
    /// What every item asks of the group's pledges, over their total (D9).
    /// `None` when nobody has pledged anything that weighs.
    pub load_factor: Option<f64>,
}

/// One item's part in the totals.
#[derive(Debug, Clone, Copy)]
struct Part {
    size_bytes: u64,
    demand_bytes: u64,
    mine: bool,
    /// When its custodians were last worked out: at the whole pass, or when
    /// it arrived or its files changed since (D10 counts only older ones).
    since: Instant,
}

/// The settled group, and every item's part in this node's custody and in the
/// group's load. Totals are kept as items come and change, so a status is
/// read without a pass over every item.
#[derive(Debug, Clone)]
pub(crate) struct CustodyView {
    group: CustodyGroup,
    me: MemberId,
    items: HashMap<ItemId, Part>,
    my_items: usize,
    my_bytes: u128,
    demand_bytes: u128,
}

impl CustodyView {
    /// Custody of every one of `items` under `group`: the whole pass.
    pub(crate) fn compute(group: CustodyGroup, me: MemberId, items: &[CustodyItem]) -> Self {
        let mine: HashSet<ItemId> = group.custody_of(me, items).into_iter().collect();
        let mut view = Self {
            group,
            me,
            items: HashMap::with_capacity(items.len()),
            my_items: 0,
            my_bytes: 0,
            demand_bytes: 0,
        };
        let now = Instant::now();
        for item in items {
            view.insert(item, mine.contains(&item.item), now);
        }
        view
    }

    /// One item added or changed since the pass. Answers whether this node
    /// keeps it.
    pub(crate) fn update(&mut self, item: &CustodyItem) -> bool {
        // A rating or a review changes nothing about who keeps it.
        let since = self
            .items
            .get(&item.item)
            .filter(|old| {
                old.size_bytes == item.size_bytes
                    && old.demand_bytes == self.group.demand_bytes(item)
            })
            .map_or_else(Instant::now, |old| old.since);
        let is_mine = self.group.is_custodian(item, self.me);
        self.insert(item, is_mine, since);
        is_mine
    }

    /// Whether this node keeps `item`.
    pub(crate) fn is_mine(&self, item: ItemId) -> bool {
        self.items.get(&item).is_some_and(|part| part.mine)
    }

    /// The items this node keeps.
    pub(crate) fn my_items(&self) -> HashSet<ItemId> {
        self.items
            .iter()
            .filter(|(_, part)| part.mine)
            .map(|(item, _)| *item)
            .collect()
    }

    pub(crate) fn custodians(&self, item: &CustodyItem) -> Vec<MemberId> {
        self.group.custodians(item)
    }

    pub(crate) fn group(&self) -> &CustodyGroup {
        &self.group
    }

    /// When `item`'s custodians were last worked out, if it is known.
    pub(crate) fn assigned_since(&self, item: ItemId) -> Option<Instant> {
        self.items.get(&item).map(|part| part.since)
    }

    pub(crate) fn status(&self) -> CustodyStatus {
        CustodyStatus {
            items: self.my_items,
            bytes: u64::try_from(self.my_bytes).unwrap_or(u64::MAX),
            pledge_bytes: self.group.pledge_of(self.me),
            load_factor: self.group.load_factor(self.demand_bytes),
        }
    }

    fn insert(&mut self, item: &CustodyItem, mine: bool, since: Instant) {
        let part = Part {
            size_bytes: item.size_bytes,
            demand_bytes: self.group.demand_bytes(item),
            mine,
            since,
        };
        if let Some(old) = self.items.insert(item.item, part) {
            self.remove_from_totals(old);
        }
        self.add_to_totals(part);
    }

    fn add_to_totals(&mut self, part: Part) {
        self.demand_bytes += u128::from(part.demand_bytes);
        if part.mine {
            self.my_items += 1;
            self.my_bytes += u128::from(part.size_bytes);
        }
    }

    fn remove_from_totals(&mut self, part: Part) {
        self.demand_bytes -= u128::from(part.demand_bytes);
        if part.mine {
            self.my_items -= 1;
            self.my_bytes -= u128::from(part.size_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use distlib_core::WeightRule;

    use super::*;

    fn member(seed: u8) -> MemberId {
        MemberId::from(iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    fn item(seed: u8, creator: MemberId, size_bytes: u64) -> CustodyItem {
        CustodyItem {
            item: ItemId::from_bytes([seed; 32]),
            creators: vec![creator],
            size_bytes,
        }
    }

    /// Two members and three replicas: each is custodian of everything the
    /// other added, and of everything an outsider added.
    fn pair() -> (CustodyGroup, MemberId, MemberId) {
        let (me, other) = (member(1), member(2));
        let rule = WeightRule {
            cap_bytes: 1_000,
            step_bytes: 1,
            min_bytes: 0,
        };
        let group = CustodyGroup::new(3, rule, [(me, 500), (other, 500)]);
        (group, me, other)
    }

    #[test]
    fn the_totals_follow_items_as_they_change_past_the_pledge_too() {
        let (group, me, other) = pair();
        let outsider = member(3);
        let mut view = CustodyView::compute(
            group,
            me,
            &[item(1, other, 100), item(2, me, 40), item(3, outsider, 10)],
        );
        // Mine: 1 and 3. Demand: 2 × 100 + 2 × 40 + 3 × 10 over 1,000.
        assert_eq!(
            view.status(),
            CustodyStatus {
                items: 2,
                bytes: 110,
                pledge_bytes: 500,
                load_factor: Some(0.31),
            }
        );

        // Item 1 grows past the pledge, and item 4 arrives: all still kept.
        assert!(view.update(&item(1, other, 600)));
        assert!(view.update(&item(4, other, 5)));
        assert_eq!(
            view.status(),
            CustodyStatus {
                items: 3,
                bytes: 615,
                pledge_bytes: 500,
                load_factor: Some(1.32),
            }
        );
    }

    #[test]
    fn an_items_custodians_are_dated_anew_only_when_its_files_change() {
        let (group, me, other) = pair();
        let mut view = CustodyView::compute(group, me, &[item(1, other, 100)]);
        let worked_out = view.assigned_since(ItemId::from_bytes([1; 32]));
        std::thread::sleep(std::time::Duration::from_millis(5));

        // A rating or a review: the same files.
        view.update(&item(1, other, 100));
        assert_eq!(view.assigned_since(ItemId::from_bytes([1; 32])), worked_out);
        // A file added.
        view.update(&item(1, other, 150));
        assert!(view.assigned_since(ItemId::from_bytes([1; 32])) > worked_out);
    }
}
