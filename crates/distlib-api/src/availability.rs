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

    /// Whom to ask for `item`'s files, best first (4b-5): the online members
    /// holding it, then the other online members, then everyone else — so an
    /// offline member is dialled, and its connect timeout waited out, only
    /// once nobody online could serve. Each tier is shuffled, so one holder
    /// is not asked for everything.
    pub(crate) fn ask_order(
        &self,
        item: &ItemId,
        members: impl IntoIterator<Item = MemberId>,
    ) -> Vec<MemberId> {
        tiers(&self.holders(item), &self.online, members)
            .into_iter()
            .flat_map(shuffled)
            .collect()
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

/// `members` in three tiers: among `holders`, `online`, and the rest.
fn tiers(
    holders: &[MemberId],
    online: &BTreeSet<MemberId>,
    members: impl IntoIterator<Item = MemberId>,
) -> [Vec<MemberId>; 3] {
    let (holding, others): (Vec<_>, Vec<_>) = members
        .into_iter()
        .partition(|member| holders.contains(member));
    let (online, offline) = others
        .into_iter()
        .partition(|member| online.contains(member));
    [holding, online, offline]
}

/// `members` in a random order. Randomness that fails — it does not, on any
/// platform this runs on — leaves them as they were: the order is a spread
/// of load, not a correctness matter.
fn shuffled(mut members: Vec<MemberId>) -> Vec<MemberId> {
    for last in (1..members.len()).rev() {
        let pick = getrandom::u32()
            .ok()
            .and_then(|draw| usize::try_from(draw).ok())
            .map_or(last, |draw| draw % (last + 1));
        members.swap(last, pick);
    }
    members
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

    /// Online holders first, offline members last — and nobody lost or asked
    /// twice on the way.
    #[test]
    fn members_are_asked_online_holders_first_and_offline_last() {
        let (bob, carol, dave, erin) = (member(), member(), member(), member());
        let online = BTreeSet::from([bob, carol]);

        let [holding, others_online, offline] = tiers(&[bob], &online, [erin, carol, dave, bob]);
        assert_eq!(holding, [bob]);
        assert_eq!(others_online, [carol]);
        assert_eq!(offline, [erin, dave]);
    }

    #[test]
    fn a_shuffle_keeps_every_member_once() {
        let members: Vec<MemberId> = (0..20).map(|_| member()).collect();
        let mut shuffled = shuffled(members.clone());
        assert_ne!(shuffled, members, "one order in 20! stays put");
        shuffled.sort_unstable();
        let mut sorted = members;
        sorted.sort_unstable();
        assert_eq!(shuffled, sorted);
    }

    #[test]
    fn providers_are_not_counted_while_anyone_online_is_unknown() {
        let (bob, carol) = (member(), member());
        assert_eq!(counted(&[bob], &[]), Some(1));
        assert_eq!(counted(&[], &[]), Some(0), "nobody online has it");
        assert_eq!(counted(&[bob], &[carol]), None, "carol may have it too");
    }
}
