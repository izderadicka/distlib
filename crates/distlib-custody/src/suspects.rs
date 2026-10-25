//! Suspects (phase 5's D12): members with probe failures that count, written
//! within the group's `failure_window_secs` by probers still in the group. A
//! member is red once at least two probers have one.

use std::{
    cmp::Reverse,
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

use distlib_consensus::MembershipState;
use distlib_core::MemberId;
use distlib_sync::RecordedFailure;

use crate::failed_copies::{failure_window, left_in_window};

/// How many probers' failures make a member red.
pub const RED_FROM_PROBERS: usize = 2;

/// One member's failures that count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suspect {
    pub member: MemberId,
    /// One per prober, the latest first.
    pub failures: Vec<RecordedFailure>,
}

impl Suspect {
    /// Whether enough probers failed it: a candidate for expulsion.
    pub fn is_red(&self) -> bool {
        self.failures.len() >= RED_FROM_PROBERS
    }

    /// Its failures in a row, over every prober.
    fn in_a_row(&self) -> u32 {
        self.failures
            .iter()
            .map(|recorded| recorded.failure.in_a_row)
            .sum()
    }
}

/// The members `failures` that count make suspects, as of `now`: the most
/// probers first, then the most failures in a row.
pub fn suspects(
    failures: Vec<RecordedFailure>,
    membership: &MembershipState,
    now: SystemTime,
) -> Vec<Suspect> {
    suspects_among(
        failures,
        |member| membership.is_member(member),
        failure_window(membership),
        now,
    )
}

/// [`suspects`], with who is a member and the window given.
fn suspects_among(
    failures: Vec<RecordedFailure>,
    is_member: impl Fn(&MemberId) -> bool,
    window: Duration,
    now: SystemTime,
) -> Vec<Suspect> {
    let mut by_member: BTreeMap<MemberId, Vec<RecordedFailure>> = BTreeMap::new();
    for recorded in failures.into_iter().filter(|recorded| {
        is_member(&recorded.custodian)
            && is_member(&recorded.prober)
            && left_in_window(recorded, window, now).is_some()
    }) {
        by_member
            .entry(recorded.custodian)
            .or_default()
            .push(recorded);
    }
    let mut suspects: Vec<Suspect> = by_member
        .into_iter()
        .map(|(member, mut failures)| {
            failures.sort_unstable_by_key(|recorded| Reverse(recorded.written));
            Suspect { member, failures }
        })
        .collect();
    suspects.sort_by_key(|suspect| Reverse((suspect.failures.len(), suspect.in_a_row())));
    suspects
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use std::time::UNIX_EPOCH;

    use distlib_core::{ContentHash, ItemId, ProbeFailure};

    use super::*;

    const WINDOW: Duration = Duration::from_secs(100);

    fn member(seed: u8) -> MemberId {
        MemberId::from(iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    /// `custodian` failed by `prober` `age` seconds before `now`.
    fn failed(
        custodian: MemberId,
        prober: MemberId,
        in_a_row: u32,
        age: u64,
        now: SystemTime,
    ) -> RecordedFailure {
        let written = (now - Duration::from_secs(age))
            .duration_since(UNIX_EPOCH)
            .unwrap();
        RecordedFailure {
            custodian,
            prober,
            failure: ProbeFailure {
                item: ItemId::from_bytes([1; 32]),
                blob: ContentHash::from_bytes([2; 32]),
                chunk: 0,
                in_a_row,
            },
            written: u64::try_from(written.as_micros()).unwrap(),
        }
    }

    #[test]
    fn two_probers_make_a_member_red_and_one_does_not() {
        let now = SystemTime::now();
        let (bob, carol, dave) = (member(1), member(2), member(3));
        let failures = vec![
            failed(bob, carol, 1, 20, now),
            failed(bob, dave, 1, 10, now),
            failed(carol, dave, 5, 10, now),
        ];
        let found = suspects_among(failures, |_| true, WINDOW, now);
        assert_eq!(
            found
                .iter()
                .map(|suspect| suspect.member)
                .collect::<Vec<_>>(),
            [bob, carol],
            "the most probers first, however many in a row"
        );
        assert!(found[0].is_red());
        assert_eq!(
            found[0]
                .failures
                .iter()
                .map(|recorded| recorded.prober)
                .collect::<Vec<_>>(),
            [dave, carol],
            "the latest first"
        );
        assert!(!found[1].is_red());
    }

    #[test]
    fn among_as_many_probers_the_most_in_a_row_come_first() {
        let now = SystemTime::now();
        let (bob, carol, dave) = (member(1), member(2), member(3));
        // Both ways round, so neither comes first by its id alone.
        for (fewer, more) in [(bob, carol), (carol, bob)] {
            let failures = vec![
                failed(fewer, dave, 1, 10, now),
                failed(more, dave, 3, 10, now),
            ];
            let found = suspects_among(failures, |_| true, WINDOW, now);
            assert_eq!(found[0].member, more);
        }
    }

    #[test]
    fn only_failures_within_the_window_between_members_count() {
        let now = SystemTime::now();
        let (bob, carol, dave, gone) = (member(1), member(2), member(3), member(4));
        let failures = vec![
            failed(bob, carol, 1, 10, now),
            failed(bob, dave, 1, 200, now),
            failed(bob, gone, 1, 10, now),
            failed(gone, carol, 1, 10, now),
        ];
        let found = suspects_among(failures, |member| *member != gone, WINDOW, now);
        assert_eq!(found.len(), 1, "nothing of a member who is gone");
        assert_eq!(found[0].failures.len(), 1);
        assert!(
            !found[0].is_red(),
            "one too old, one by a member who is gone"
        );
    }
}
