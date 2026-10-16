//! What a group decides once, when it is founded (phase 5's D2): written into
//! the founding entry, so every node reads the same values, and never changed
//! after — changing `replicas` or the cap would reshuffle every item at once.

use std::num::{NonZeroU8, NonZeroU32, NonZeroU64};

use serde::{Deserialize, Serialize};

use crate::WeightRule;

const GB: u64 = 1_000_000_000;
const HOUR: u32 = 60 * 60;
const DAY: u32 = 24 * HOUR;

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
    /// The least a member may pledge.
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
    /// The least a member may throttle its custody fetches to, in bytes per
    /// second.
    pub min_fetch_rate: u64,
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
            min_fetch_rate: 1_000_000,
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
        }
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
}
