//! Rendezvous custody (phase 5's D1, D3): the properties the rest of the phase
//! relies on, over random groups.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{collections::BTreeSet, sync::LazyLock};

use distlib_core::{CustodyGroup, CustodyItem, ItemId, MemberId, WeightRule};
use iroh::SecretKey;
use proptest::prelude::*;

const RULE: WeightRule = WeightRule {
    cap_bytes: 1_000,
    step_bytes: 10,
    min_bytes: 0,
};
const REPLICAS: u8 = 3;

/// Enough members for any group here, made once: deriving a key is the slow
/// part of a case.
static POOL: LazyLock<Vec<MemberId>> = LazyLock::new(|| {
    (0..40_u8)
        .map(|n| MemberId::from(SecretKey::from_bytes(&[n; 32]).public()))
        .collect()
});

fn item(n: u32) -> ItemId {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&n.to_le_bytes());
    ItemId::from_bytes(bytes)
}

/// Items numbered `0..count`, each created by one of `creators` in turn.
fn items(count: u32, creators: &[MemberId]) -> Vec<CustodyItem> {
    (0..count)
        .map(|n| CustodyItem {
            item: item(n),
            creators: vec![creators[n as usize % creators.len()]],
            size_bytes: 0,
        })
        .collect()
}

/// Pledges for the first members of the pool, between nothing and past the cap.
fn pledges() -> impl Strategy<Value = Vec<(MemberId, u64)>> {
    prop::collection::vec(0..1_200_u64, 4..30).prop_map(|pledges| {
        POOL.iter()
            .copied()
            .zip(pledges)
            .collect::<Vec<(MemberId, u64)>>()
    })
}

fn group(pledges: &[(MemberId, u64)]) -> CustodyGroup {
    CustodyGroup::new(REPLICAS, RULE, pledges.iter().copied())
}

fn as_set(members: Vec<MemberId>) -> BTreeSet<MemberId> {
    members.into_iter().collect()
}

proptest! {
    #[test]
    fn the_same_group_in_any_order_gives_the_same_custodians(
        (pledges, shuffled) in pledges().prop_flat_map(|pledges| {
            let shuffled = Just(pledges.clone()).prop_shuffle();
            (Just(pledges), shuffled)
        }),
    ) {
        let members: Vec<MemberId> = pledges.iter().map(|(member, _)| *member).collect();
        for item in items(50, &members) {
            prop_assert_eq!(group(&pledges).custodians(&item), group(&shuffled).custodians(&item));
        }
    }

    #[test]
    fn a_member_joining_takes_one_slot_in_the_items_it_now_holds_and_moves_nothing_else(
        pledges in pledges(),
        newcomer_pledge in 10..1_200_u64,
    ) {
        let newcomer = POOL[pledges.len()];
        let before = group(&pledges);
        let mut joined = pledges.clone();
        joined.push((newcomer, newcomer_pledge));
        let after = group(&joined);
        let members: Vec<MemberId> = pledges.iter().map(|(member, _)| *member).collect();

        for item in items(50, &members) {
            let old = before.custodians(&item);
            let new = after.custodians(&item);
            if new.contains(&newcomer) {
                let lost: Vec<_> = as_set(old.clone()).difference(&as_set(new.clone())).copied().collect();
                prop_assert!(lost.len() <= 1, "{lost:?}");
                // What it displaced, if anything, was the lowest ranked.
                prop_assert!(lost.iter().all(|member| old.last() == Some(member)));
            } else {
                prop_assert_eq!(old, new);
            }
        }
    }

    #[test]
    fn a_member_leaving_moves_only_the_items_it_held(
        pledges in pledges(),
        leaving in 0..4_usize,
    ) {
        let leaver = pledges[leaving].0;
        let members: Vec<MemberId> = pledges.iter().map(|(member, _)| *member).collect();
        let remaining: Vec<_> = pledges.iter().copied().filter(|(member, _)| *member != leaver).collect();
        // Created by somebody who stays, so only custody is in question.
        let creators: Vec<MemberId> = members.iter().copied().filter(|member| *member != leaver).collect();

        for item in items(50, &creators) {
            let old = group(&pledges).custodians(&item);
            let new = group(&remaining).custodians(&item);
            if old.contains(&leaver) {
                prop_assert!(old.iter().filter(|member| **member != leaver).all(|member| new.contains(member)));
            } else {
                prop_assert_eq!(old, new);
            }
        }
    }

    #[test]
    fn a_creator_is_never_its_own_items_custodian_and_leaving_adds_one(pledges in pledges()) {
        let members: Vec<MemberId> = pledges.iter().map(|(member, _)| *member).collect();
        let weighted = pledges.iter().filter(|(_, pledge)| RULE.weight(*pledge) > 0).count();
        for item in items(50, &members) {
            let creator = item.creators[0];
            let old = group(&pledges).custodians(&item);
            prop_assert!(!old.contains(&creator));

            let remaining: Vec<_> = pledges.iter().copied().filter(|(member, _)| *member != creator).collect();
            let new = group(&remaining).custodians(&item);
            prop_assert!(old.iter().all(|member| new.contains(member)));
            let creator_weighs = RULE.weight(pledges.iter().find(|(m, _)| *m == creator).unwrap().1) > 0;
            let rankable = weighted - usize::from(creator_weighs);
            // The creator's copy is the first replica.
            prop_assert_eq!(old.len(), usize::from(REPLICAS - 1).min(rankable));
            prop_assert_eq!(new.len(), usize::from(REPLICAS).min(rankable));
        }
    }

    #[test]
    fn is_custodian_agrees_with_custodians(pledges in pledges()) {
        let group = group(&pledges);
        let members: Vec<MemberId> = pledges.iter().map(|(member, _)| *member).collect();
        for item in items(50, &members) {
            let custodians = group.custodians(&item);
            for member in &members {
                prop_assert_eq!(group.is_custodian(&item, *member), custodians.contains(member));
            }
        }
    }

    #[test]
    fn the_emergency_copy_goes_to_the_next_in_score_order_that_may_keep_it(
        pledges in pledges(),
    ) {
        let group = group(&pledges);
        // One slot more, and two: the same order, a member further.
        let one_more = CustodyGroup::new(REPLICAS + 1, RULE, pledges.iter().copied());
        let two_more = CustodyGroup::new(REPLICAS + 2, RULE, pledges.iter().copied());
        let members: Vec<MemberId> = pledges.iter().map(|(member, _)| *member).collect();
        for item in items(50, &members) {
            let custodians = group.custodians(&item);
            let first = group.first_after_custodians(&item, |_| true);
            prop_assert_eq!(first, one_more.custodians(&item).get(custodians.len()).copied());
            let second = group.first_after_custodians(&item, |member| Some(member) != first);
            prop_assert_eq!(second, two_more.custodians(&item).get(custodians.len() + 1).copied());
            if let Some(first) = first {
                prop_assert!(!custodians.contains(&first));
                prop_assert!(!item.creators.contains(&first));
            }
        }
    }

    #[test]
    fn a_pledge_changed_within_one_step_or_above_the_cap_weighs_the_same(
        pledge in 0..2_000_u64,
        change in 0..10_u64,
    ) {
        let within = pledge - pledge % RULE.step_bytes + change;
        prop_assert_eq!(RULE.weight(pledge), RULE.weight(within));
        prop_assert!(RULE.weight(pledge) <= RULE.cap_bytes);
        prop_assert_eq!(RULE.weight(RULE.cap_bytes + pledge), RULE.weight(RULE.cap_bytes));
    }
}

/// A member pledging less than one step weighs nothing, and is custodian of
/// nothing — even where the group has fewer members with a weight than an
/// item has slots.
#[test]
fn a_member_weighing_nothing_is_custodian_of_nothing() {
    let pledges = [(POOL[0], 500), (POOL[1], 500), (POOL[2], 0), (POOL[3], 9)];
    let group = group(&pledges);
    let all = items(20, &[POOL[39]]);
    for item in &all {
        assert_eq!(
            as_set(group.custodians(item)),
            as_set(vec![POOL[0], POOL[1]])
        );
    }
    for weightless in [POOL[2], POOL[3]] {
        assert!(group.custody_of(weightless, &all).is_empty());
        assert!(all.iter().all(|item| !group.is_custodian(item, weightless)));
    }
}

/// A pledge below the minimum weighs nothing even where it is over a step, and
/// one at the minimum weighs its step.
#[test]
fn a_pledge_below_the_minimum_weighs_nothing() {
    let rule = WeightRule {
        min_bytes: 25,
        ..RULE
    };
    assert_eq!(rule.weight(24), 0);
    assert_eq!(rule.weight(25), 20);
}

/// A pledge changed within a step weighs the same but is another group: the
/// change still settles, and custody says the new pledge.
#[test]
fn a_pledge_changed_within_a_step_is_another_group() {
    let [a, b] = [POOL[0], POOL[1]];
    let before = group(&[(a, 500), (b, 105)]);
    let after = group(&[(a, 500), (b, 108)]);
    assert_eq!(RULE.weight(105), RULE.weight(108));
    assert_ne!(before, after);
    assert_eq!((after.pledge_of(b), after.pledge_of(POOL[2])), (108, 0));
}

/// Over many items a member's share of the custodian slots follows its share
/// of the weight. Not exactly: a member is custodian of an item at most once,
/// so the heaviest fall a little short of their share and the lightest gain.
/// Within a tenth with no member over ~1/(2 × replicas) of the weight, which is
/// what the cap is for.
#[test]
fn custody_follows_weight() {
    let pledges: Vec<(MemberId, u64)> = POOL
        .iter()
        .copied()
        .zip((0..30_u64).map(|n| 100 + n * 10))
        .collect();
    let group = group(&pledges);
    let total: u64 = pledges.iter().map(|(_, pledge)| pledge).sum();
    // Created by nobody still a member, so every slot is ranked.
    let all = items(30_000, &[POOL[39]]);
    for (member, pledge) in &pledges {
        let held = group.custody_of(*member, &all).len() as f64;
        let expected = 30_000.0 * f64::from(REPLICAS) * *pledge as f64 / total as f64;
        assert!(
            (held / expected - 1.0).abs() < 0.1,
            "pledged {pledge}: held {held}, expected {expected:.0}"
        );
    }
}

/// [`CustodyGroup::custody_of`] says what [`CustodyGroup::is_custodian`]
/// says of every item, in order, whether it runs on one thread (a few items)
/// or on all of them (thousands).
#[test]
fn custody_of_is_is_custodian_over_every_item() {
    let pledges: Vec<(MemberId, u64)> = POOL
        .iter()
        .copied()
        .zip((0..20_u64).map(|n| n * 60))
        .collect();
    let group = group(&pledges);
    let members: Vec<MemberId> = pledges.iter().map(|(member, _)| *member).collect();
    for count in [100, 5_000] {
        let all = items(count, &members);
        for member in &members {
            let one_by_one: Vec<ItemId> = all
                .iter()
                .filter(|item| group.is_custodian(item, *member))
                .map(|item| item.item)
                .collect();
            assert_eq!(group.custody_of(*member, &all), one_by_one);
        }
    }
}

#[test]
fn the_load_factor_is_each_items_size_per_slot_over_the_total_weight() {
    let [a, b, c, outsider] = [POOL[0], POOL[1], POOL[2], POOL[3]];
    let group = CustodyGroup::new(REPLICAS, RULE, [(a, 500), (b, 305), (c, 0)]);
    // Its creator is a member, so two slots: 2 × 100.
    let by_a_member = CustodyItem {
        item: item(1),
        creators: vec![c],
        size_bytes: 100,
    };
    // Its creator is not, so three: 3 × 40.
    let by_an_outsider = CustodyItem {
        item: item(2),
        creators: vec![outsider],
        size_bytes: 40,
    };
    let demand = u128::from(group.demand_bytes(&by_a_member) + group.demand_bytes(&by_an_outsider));
    assert_eq!(demand, 320);
    // 500 and 305 weigh 500 and 300.
    assert_eq!(group.load_factor(demand), Some(0.4));

    let weightless = CustodyGroup::new(REPLICAS, RULE, [(a, 0)]);
    assert_eq!(weightless.load_factor(demand), None);
}
