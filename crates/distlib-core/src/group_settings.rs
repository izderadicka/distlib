//! What a group decides once, when it is founded (phase 5's D2): written into
//! the founding entry, so every node reads the same values, and never changed
//! after — changing `replicas` or the cap would reshuffle every item at once.

use std::num::{NonZeroU8, NonZeroU32, NonZeroU64};

use serde::{Deserialize, Serialize};

use crate::WeightRule;

const GB: u64 = 1_000_000_000;
const HOUR: u32 = 60 * 60;
const DAY: u32 = 24 * HOUR;

/// How many times its pledge a member's requirement may be before adds and
/// pledge changes that would get it there are refused (D8). A constant until a
/// group needs another (Q9).
const FAR_UNDER_FACTOR: u64 = 2;

/// The group's settings. In a founder's config as `[group]`, read only when
/// founding; after that the log's copy is the one that counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GroupSettings {
    /// How many copies the group keeps of each item, the creator's included.
    pub replicas: NonZeroU8,
    /// The most a pledge weighs, so no one member is custodian of most items.
    pub cap_bytes: u64,
    /// Weights move in steps of this, so a small pledge edit moves nothing.
    pub weight_step_bytes: NonZeroU64,
    /// The least a member may pledge, and the least it is required to.
    pub min_pledge_bytes: u64,
    /// How many changes to the membership a proposal waits for its approvals
    /// before it is dropped. Counted in changes, not time: a quiet group keeps
    /// a proposal open for as long as nothing else happens.
    pub pending_expiry: NonZeroU64,
    /// How long the membership must stay unchanged before custody follows it.
    pub settle_secs: u32,
    /// The longest custody waits for the membership to settle.
    pub max_settle_secs: u32,
    /// How long an item may have only one live copy before another member
    /// fetches an emergency copy.
    pub emergency_after_secs: u32,
    /// The least a member may limit its downloads or its uploads to, in bytes
    /// per second: the same for both, since one link carries both.
    pub min_transfer_rate: u64,
    /// Whether members probe each other's custody.
    pub probes: bool,
    /// How often one member makes a probe.
    pub probe_every_secs: NonZeroU32,
    /// How long a recorded probe failure counts against a member.
    pub failure_window_secs: u32,
}

impl Default for GroupSettings {
    fn default() -> Self {
        Self {
            replicas: NonZeroU8::new(3).expect("three is not zero"),
            cap_bytes: 2_000 * GB,
            weight_step_bytes: NonZeroU64::new(10 * GB).expect("ten gigabytes is not zero"),
            min_pledge_bytes: 100 * GB,
            pending_expiry: NonZeroU64::new(128).expect("128 is not zero"),
            settle_secs: HOUR,
            max_settle_secs: DAY,
            emergency_after_secs: DAY,
            min_transfer_rate: 1_000_000,
            probes: true,
            probe_every_secs: NonZeroU32::new(5 * 60).expect("five minutes is not zero"),
            failure_window_secs: 7 * DAY,
        }
    }
}

impl GroupSettings {
    /// How a pledge becomes a custody weight under these settings.
    pub fn weight_rule(&self) -> WeightRule {
        WeightRule {
            cap_bytes: self.cap_bytes,
            step_bytes: self.weight_step_bytes.get(),
            min_bytes: self.min_pledge_bytes,
        }
    }

    /// A member's transfer limit, raised to the group's minimum: a member may
    /// limit its link, not opt out of its share.
    pub fn at_least_the_minimum(&self, limit: NonZeroU64) -> NonZeroU64 {
        limit.max(NonZeroU64::new(self.min_transfer_rate).unwrap_or(NonZeroU64::MIN))
    }

    /// What a member that added `added_bytes` of content has to pledge (D8):
    /// the other `replicas − 1` copies of what it added, and never less than
    /// the minimum.
    pub fn required_pledge(&self, added_bytes: u64) -> u64 {
        added_bytes
            .saturating_mul(u64::from(self.replicas.get()) - 1)
            .max(self.min_pledge_bytes)
    }

    /// Refuses settings that contradict each other, naming what is wrong.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.weight_step_bytes.get() > self.cap_bytes {
            return Err(
                "weight_step_bytes is larger than cap_bytes, so every member weighs nothing",
            );
        }
        if self.settle_secs > self.max_settle_secs {
            return Err("settle_secs is longer than max_settle_secs");
        }
        Ok(())
    }
}

/// How a member's pledge stands against what it is required to pledge (D8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PledgeStanding {
    Enough,
    /// Below the requirement: warned, not refused.
    Under,
    /// The requirement more than [`FAR_UNDER_FACTOR`] times the pledge: an
    /// add or a pledge change that gets here is refused.
    FarUnder,
}

impl PledgeStanding {
    pub fn of(pledge_bytes: u64, required_bytes: u64) -> Self {
        if required_bytes > pledge_bytes.saturating_mul(FAR_UNDER_FACTOR) {
            Self::FarUnder
        } else if required_bytes > pledge_bytes {
            Self::Under
        } else {
            Self::Enough
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_consistent() {
        assert_eq!(GroupSettings::default().check(), Ok(()));
    }

    #[test]
    fn a_step_above_the_cap_or_a_settle_past_its_limit_is_refused() {
        let step_above_cap = GroupSettings {
            cap_bytes: 5,
            weight_step_bytes: NonZeroU64::new(10).expect("ten is not zero"),
            ..GroupSettings::default()
        };
        assert!(step_above_cap.check().is_err());
        let settle_too_long = GroupSettings {
            settle_secs: DAY + 1,
            ..GroupSettings::default()
        };
        assert!(settle_too_long.check().is_err());
    }

    #[test]
    fn the_requirement_is_the_other_copies_of_what_was_added_and_at_least_the_minimum() {
        let settings = GroupSettings {
            min_pledge_bytes: 100,
            ..GroupSettings::default()
        };
        assert_eq!(settings.required_pledge(0), 100);
        assert_eq!(settings.required_pledge(60), 120);
        let five_copies = GroupSettings {
            replicas: NonZeroU8::new(5).expect("five is not zero"),
            ..settings
        };
        assert_eq!(five_copies.required_pledge(60), 240);
        assert_eq!(five_copies.required_pledge(u64::MAX), u64::MAX);
    }

    #[test]
    fn a_pledge_is_under_below_its_requirement_and_far_under_below_half() {
        assert_eq!(PledgeStanding::of(100, 100), PledgeStanding::Enough);
        assert_eq!(PledgeStanding::of(100, 101), PledgeStanding::Under);
        assert_eq!(PledgeStanding::of(100, 200), PledgeStanding::Under);
        assert_eq!(PledgeStanding::of(100, 201), PledgeStanding::FarUnder);
        assert_eq!(PledgeStanding::of(0, 1), PledgeStanding::FarUnder);
        assert_eq!(
            PledgeStanding::of(u64::MAX, u64::MAX),
            PledgeStanding::Enough
        );
    }

    #[test]
    fn a_limit_below_the_minimum_is_raised_to_it_and_one_above_is_kept() {
        let settings = GroupSettings {
            min_transfer_rate: 1_000,
            ..GroupSettings::default()
        };
        let rate = |bytes| NonZeroU64::new(bytes).expect("not zero");
        assert_eq!(settings.at_least_the_minimum(rate(10)), rate(1_000));
        assert_eq!(settings.at_least_the_minimum(rate(5_000)), rate(5_000));
    }
}
