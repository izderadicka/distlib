//! This node's custody, as of the last settled membership.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap, HashSet},
};

use distlib_core::{CustodyGroup, CustodyItem, CustodyScore, ItemId, MemberId, within_pledge};
use serde::Serialize;

use crate::CustodyChange;

/// What `node.status` says about this node's custody.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct CustodyStatus {
    /// How many items this node keeps as custodian.
    pub items: usize,
    /// Their size.
    pub bytes: u64,
    /// The items it is custodian of but are over its pledge (D9): not
    /// fetched, and so a copy short.
    pub over_pledge: OverPledge,
    /// What every item asks of the group's pledges, over their total (D9).
    /// `None` when nobody has pledged anything that weighs.
    pub load_factor: Option<f64>,
}

/// Items over the pledge, and their size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct OverPledge {
    pub items: usize,
    pub bytes: u64,
}

/// One item's part in the totals.
#[derive(Debug, Clone, Copy)]
struct Part {
    size_bytes: u64,
    demand_bytes: u64,
    /// This node's score, when it is one of the item's custodians.
    score: Option<CustodyScore>,
}

/// What is kept of the assigned items when they are more than the pledge.
#[derive(Debug, Clone)]
struct Cut {
    kept: HashSet<ItemId>,
    kept_bytes: u64,
}

/// The settled group, and every item's part in this node's custody and in the
/// group's load. Totals are kept as items come and change, so a status is
/// read without a pass over every item.
#[derive(Debug, Clone)]
pub(crate) struct CustodyView {
    group: CustodyGroup,
    me: MemberId,
    pledge_bytes: u64,
    items: HashMap<ItemId, Part>,
    /// The items this node is custodian of, highest score first, with
    /// their sizes.
    assigned: BTreeMap<(Reverse<CustodyScore>, ItemId), u64>,
    assigned_bytes: u128,
    demand_bytes: u128,
    /// `None` while every assigned item fits in the pledge.
    cut: Option<Cut>,
}

impl CustodyView {
    /// Custody of every one of `items` under `group`: the whole pass.
    pub(crate) fn compute(group: CustodyGroup, me: MemberId, items: &[CustodyItem]) -> Self {
        let scores: HashMap<ItemId, CustodyScore> =
            group.custody_of(me, items).into_iter().collect();
        let mut view = Self {
            pledge_bytes: group.pledge_of(me),
            group,
            me,
            items: HashMap::with_capacity(items.len()),
            assigned: BTreeMap::new(),
            assigned_bytes: 0,
            demand_bytes: 0,
            cut: None,
        };
        for item in items {
            view.insert(item, scores.get(&item.item).copied());
        }
        view.cut_at_pledge();
        view
    }

    /// One item added or changed since the pass, and the changes to
    /// announce: always whether this node keeps it, since a file added to an
    /// item it keeps is one more blob to keep; then, if other items moved
    /// across the pledge with it, that every item is to be read again.
    pub(crate) fn update(&mut self, item: &CustodyItem) -> Vec<CustodyChange> {
        self.insert(item, self.group.custody_score(item, self.me));
        let cut_before = self.cut_at_pledge();
        let mut changes = vec![CustodyChange::Item {
            item: item.item,
            mine: self.is_mine(item.item),
        }];
        if self.others_moved(item.item, cut_before.as_ref()) {
            changes.push(CustodyChange::Recomputed);
        }
        changes
    }

    /// Whether this node keeps `item`.
    pub(crate) fn is_mine(&self, item: ItemId) -> bool {
        self.items
            .get(&item)
            .is_some_and(|part| part.score.is_some())
            && self.cut.as_ref().is_none_or(|cut| cut.kept.contains(&item))
    }

    /// The items this node keeps.
    pub(crate) fn my_items(&self) -> HashSet<ItemId> {
        match &self.cut {
            Some(cut) => cut.kept.clone(),
            None => self.assigned.keys().map(|(_, item)| *item).collect(),
        }
    }

    pub(crate) fn custodians(&self, item: &CustodyItem) -> Vec<MemberId> {
        self.group.custodians(item)
    }

    pub(crate) fn status(&self) -> CustodyStatus {
        let (items, bytes) = match &self.cut {
            Some(cut) => (cut.kept.len(), u128::from(cut.kept_bytes)),
            None => (self.assigned.len(), self.assigned_bytes),
        };
        CustodyStatus {
            items,
            bytes: saturated(bytes),
            over_pledge: OverPledge {
                items: self.assigned.len() - items,
                bytes: saturated(self.assigned_bytes - bytes),
            },
            load_factor: self.group.load_factor(self.demand_bytes),
        }
    }

    /// Cuts the assigned items at the pledge, if they are more than it, and
    /// returns the cut before.
    fn cut_at_pledge(&mut self) -> Option<Cut> {
        let cut = (self.assigned_bytes > u128::from(self.pledge_bytes)).then(|| {
            let highest_first = self.assigned.iter().map(|((_, item), size)| (*item, *size));
            let mut kept_bytes = 0;
            let kept = within_pledge(self.pledge_bytes, highest_first)
                .map(|(item, size_bytes)| {
                    kept_bytes += size_bytes;
                    item
                })
                .collect();
            Cut { kept, kept_bytes }
        });
        std::mem::replace(&mut self.cut, cut)
    }

    /// Whether an assigned item other than `changed` is kept now and was not
    /// under `cut_before`, or the other way round.
    fn others_moved(&self, changed: ItemId, cut_before: Option<&Cut>) -> bool {
        if cut_before.is_none() && self.cut.is_none() {
            return false;
        }
        self.assigned
            .keys()
            .map(|(_, item)| *item)
            .filter(|item| *item != changed)
            .any(|item| cut_before.is_none_or(|cut| cut.kept.contains(&item)) != self.is_mine(item))
    }

    fn insert(&mut self, item: &CustodyItem, score: Option<CustodyScore>) {
        let part = Part {
            size_bytes: item.size_bytes,
            demand_bytes: self.group.demand_bytes(item),
            score,
        };
        if let Some(old) = self.items.insert(item.item, part) {
            self.remove_from_totals(item.item, old);
        }
        self.add_to_totals(item.item, part);
    }

    fn add_to_totals(&mut self, item: ItemId, part: Part) {
        self.demand_bytes += u128::from(part.demand_bytes);
        if let Some(score) = part.score {
            self.assigned
                .insert((Reverse(score), item), part.size_bytes);
            self.assigned_bytes += u128::from(part.size_bytes);
        }
    }

    fn remove_from_totals(&mut self, item: ItemId, part: Part) {
        self.demand_bytes -= u128::from(part.demand_bytes);
        if let Some(score) = part.score {
            self.assigned.remove(&(Reverse(score), item));
            self.assigned_bytes -= u128::from(part.size_bytes);
        }
    }
}

fn saturated(bytes: u128) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
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
        pledging(500)
    }

    /// [`pair`], with `me` pledging `pledge_bytes`.
    fn pledging(pledge_bytes: u64) -> (CustodyGroup, MemberId, MemberId) {
        let (me, other) = (member(1), member(2));
        let rule = WeightRule {
            cap_bytes: 1_000,
            step_bytes: 1,
            min_bytes: 0,
        };
        let group = CustodyGroup::new(3, rule, [(me, pledge_bytes), (other, 500)]);
        (group, me, other)
    }

    /// Items 1 to `count`, as their ids, highest score for `me` first.
    fn highest_first(group: &CustodyGroup, me: MemberId, count: u8) -> Vec<ItemId> {
        let mut scored: Vec<(CustodyScore, ItemId)> = (1..=count)
            .map(|seed| {
                let item = item(seed, member(2), 0);
                let score = group.custody_score(&item, me).expect("custodian of all");
                (score, item.item)
            })
            .collect();
        scored.sort_unstable_by(|a, b| b.cmp(a));
        scored.into_iter().map(|(_, item)| item).collect()
    }

    /// The item `id`, added by `creators`, of `size_bytes`.
    fn sized(id: ItemId, creators: Vec<MemberId>, size_bytes: u64) -> CustodyItem {
        CustodyItem {
            item: id,
            creators,
            size_bytes,
        }
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
                over_pledge: OverPledge::default(),
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
                over_pledge: OverPledge::default(),
                load_factor: Some(0.72),
            }
        );
    }

    #[test]
    fn over_the_pledge_the_highest_scoring_items_that_fit_are_kept() {
        let (group, me, other) = pledging(100);
        let [high, middle, low] = highest_first(&group, me, 3)[..] else {
            unreachable!()
        };
        let items = [
            sized(high, vec![other], 60),
            sized(middle, vec![other], 50),
            sized(low, vec![other], 30),
        ];
        let view = CustodyView::compute(group, me, &items);
        // The middle one does not fit in the 40 left; the lowest does.
        assert_eq!(view.my_items(), HashSet::from([high, low]));
        assert!(!view.is_mine(middle));
        let status = view.status();
        assert_eq!((status.items, status.bytes), (2, 90));
        assert_eq!(
            status.over_pledge,
            OverPledge {
                items: 1,
                bytes: 50
            }
        );
    }

    #[test]
    fn an_item_moving_others_across_the_pledge_has_all_read_again() {
        let (group, me, other) = pledging(100);
        let [a, b, c, d] = highest_first(&group, me, 4)[..] else {
            unreachable!()
        };
        let mut view = CustodyView::compute(
            group,
            me,
            &[sized(b, vec![other], 50), sized(c, vec![other], 30)],
        );
        let updated = |item, mine, others_moved: bool| {
            let mut changes = vec![CustodyChange::Item { item, mine }];
            changes.extend(others_moved.then_some(CustodyChange::Recomputed));
            changes
        };

        // 105 of 100: d alone does not fit, and nothing else moves.
        let changes = view.update(&sized(d, vec![other], 25));
        assert_eq!(changes, updated(d, false, false));
        // 90 of 100: nothing cut.
        let changes = view.update(&sized(d, vec![other], 10));
        assert_eq!(changes, updated(d, true, false));

        // The highest, 60, pushes b out; c and d still fit after it.
        let changes = view.update(&sized(a, vec![other], 60));
        assert_eq!(changes, updated(a, true, true));
        assert_eq!(view.my_items(), HashSet::from([a, c, d]));

        // d grows past what is left, and moves nothing else.
        let changes = view.update(&sized(d, vec![other], 15));
        assert_eq!(changes, updated(d, false, false));

        // a is now partly this node's own: b and d fit again.
        let changes = view.update(&sized(a, vec![other, me], 60));
        assert_eq!(changes, updated(a, false, true));
        assert_eq!(view.my_items(), HashSet::from([b, c, d]));
        assert_eq!(view.status().over_pledge, OverPledge::default());
    }
}
