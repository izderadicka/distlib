//! What a hit and `library.item` say about who has an item (phase 4's 4b-5).

use std::collections::BTreeSet;

use distlib_core::{ItemId, MemberId};
use distlib_sync::{Availability, Holdings};
use serde_json::{Value, json};

/// Who is online, and whose holdings this node cannot tell yet — read once
/// per answer, so every hit on a page is counted against the same moment.
pub(crate) struct Seen<'a> {
    availability: &'a Availability,
    holdings: &'a Holdings,
    online: BTreeSet<MemberId>,
    /// Online members whose base list has not arrived, or did not add up.
    unknown: Vec<MemberId>,
}

impl<'a> Seen<'a> {
    pub(crate) fn now(availability: &'a Availability, holdings: &'a Holdings) -> Self {
        let online = availability.online().borrow().clone();
        let unknown = online
            .iter()
            .filter(|member| !availability.knows_holdings_of(member))
            .copied()
            .collect();
        Self {
            availability,
            holdings,
            online,
            unknown,
        }
    }

    /// Whether this node holds every content file of `item`.
    pub(crate) fn held(&self, item: &ItemId) -> bool {
        self.holdings.holds(*item)
    }

    /// How many online members hold `item` — `None` while any online
    /// member's holdings are unknown, since the count could then be short.
    pub(crate) fn providers(&self, item: &ItemId) -> Option<usize> {
        counted(&self.holders(item), &self.unknown)
    }

    /// Everything, as `library.item` shows it: also who the online holders
    /// are, and whose holdings are why `providers` is `null`.
    pub(crate) fn of(&self, item: &ItemId) -> Value {
        let holders = self.holders(item);
        json!({
            "held": self.held(item),
            "providers": counted(&holders, &self.unknown),
            "holders": holders,
            "unknown": self.unknown,
        })
    }

    /// The online members known to hold `item`, in order.
    fn holders(&self, item: &ItemId) -> Vec<MemberId> {
        let mut holders: Vec<MemberId> = self
            .availability
            .holders(item)
            .into_iter()
            .filter(|member| self.online.contains(member))
            .collect();
        holders.sort_unstable();
        holders
    }
}

/// How many `holders` — unless anyone's holdings are `unknown`, when the
/// count could be short.
fn counted(holders: &[MemberId], unknown: &[MemberId]) -> Option<usize> {
    unknown.is_empty().then_some(holders.len())
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    fn member() -> MemberId {
        MemberId::from(SecretKey::generate().public())
    }

    #[test]
    fn providers_are_not_counted_while_anyone_online_is_unknown() {
        let (bob, carol) = (member(), member());
        assert_eq!(counted(&[bob], &[]), Some(1));
        assert_eq!(counted(&[], &[]), Some(0), "nobody online has it");
        assert_eq!(counted(&[bob], &[carol]), None, "carol may have it too");
    }
}
