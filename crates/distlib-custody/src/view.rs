//! This node's custody, as of the last settled membership.

use std::collections::{HashMap, HashSet};

use distlib_core::{CustodyGroup, CustodyItem, ItemId, MemberId};
use serde::Serialize;

/// What `node.status` says about this node's custody.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct CustodyStatus {
    /// How many items this node is custodian of.
    pub items: usize,
    /// Their size.
    pub bytes: u64,
    /// What every item asks of the group's pledges, over their total (D9).
    /// `None` when nobody has pledged anything that weighs.
    pub load_factor: Option<f64>,
}

/// One item's part in the totals.
#[derive(Debug, Clone, Copy)]
struct Kept {
    size_bytes: u64,
    demand_bytes: u64,
    mine: bool,
}

/// The settled group, and every item's part in this node's custody and in the
/// group's load. Totals are kept as items come and change, so a status is
/// read without a pass over every item.
#[derive(Debug, Clone)]
pub(crate) struct CustodyView {
    group: CustodyGroup,
    me: MemberId,
    items: HashMap<ItemId, Kept>,
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
        for item in items {
            view.insert(item, mine.contains(&item.item));
        }
        view
    }

    /// One item added or changed since the pass. Answers whether this node
    /// keeps it.
    pub(crate) fn update(&mut self, item: &CustodyItem) -> bool {
        let is_mine = self.group.is_custodian(item, self.me);
        self.insert(item, is_mine);
        is_mine
    }

    /// Whether this node keeps `item`.
    pub(crate) fn is_mine(&self, item: ItemId) -> bool {
        self.items.get(&item).is_some_and(|kept| kept.mine)
    }

    /// The items this node keeps.
    pub(crate) fn my_items(&self) -> HashSet<ItemId> {
        self.items
            .iter()
            .filter(|(_, kept)| kept.mine)
            .map(|(item, _)| *item)
            .collect()
    }

    pub(crate) fn custodians(&self, item: &CustodyItem) -> Vec<MemberId> {
        self.group.custodians(item)
    }

    pub(crate) fn status(&self) -> CustodyStatus {
        CustodyStatus {
            items: self.my_items,
            bytes: u64::try_from(self.my_bytes).unwrap_or(u64::MAX),
            load_factor: self.group.load_factor(self.demand_bytes),
        }
    }

    fn insert(&mut self, item: &CustodyItem, mine: bool) {
        let kept = Kept {
            size_bytes: item.size_bytes,
            demand_bytes: self.group.demand_bytes(item),
            mine,
        };
        if let Some(old) = self.items.insert(item.item, kept) {
            self.remove_from_totals(old);
        }
        self.add_to_totals(kept);
    }

    fn add_to_totals(&mut self, kept: Kept) {
        self.demand_bytes += u128::from(kept.demand_bytes);
        if kept.mine {
            self.my_items += 1;
            self.my_bytes += u128::from(kept.size_bytes);
        }
    }

    fn remove_from_totals(&mut self, kept: Kept) {
        self.demand_bytes -= u128::from(kept.demand_bytes);
        if kept.mine {
            self.my_items -= 1;
            self.my_bytes -= u128::from(kept.size_bytes);
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
        };
        let group = CustodyGroup::new(3, rule, [(me, 500), (other, 500)]);
        (group, me, other)
    }

    #[test]
    fn the_totals_follow_items_as_they_change() {
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
                load_factor: Some(0.31),
            }
        );

        // Item 1 grows, and item 4 arrives.
        view.update(&item(1, other, 300));
        view.update(&item(4, other, 5));
        assert_eq!(
            view.status(),
            CustodyStatus {
                items: 3,
                bytes: 315,
                load_factor: Some(0.72),
            }
        );
    }
}
