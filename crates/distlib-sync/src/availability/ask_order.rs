//! Whom to ask for an item's files, best first (phase 4's 4b-5): what
//! `library.download` and the pin manager both fetch in.

use std::collections::BTreeSet;

use distlib_core::MemberId;

/// `members` in the order to ask them: online holders, the other online
/// members, offline holders, then everyone else — so an offline member is
/// dialled, and its connect timeout waited out, only once nobody online could
/// serve. Each tier is shuffled, so one holder is not asked for everything.
pub fn ask_order(
    holders: &[MemberId],
    online: &BTreeSet<MemberId>,
    members: impl IntoIterator<Item = MemberId>,
) -> Vec<MemberId> {
    tiers(holders, online, members)
        .into_iter()
        .flat_map(shuffled)
        .collect()
}

/// `members` in four tiers: online holders, other online members, offline
/// holders, other offline members.
fn tiers(
    holders: &[MemberId],
    online: &BTreeSet<MemberId>,
    members: impl IntoIterator<Item = MemberId>,
) -> [Vec<MemberId>; 4] {
    let holds = |member: &MemberId| holders.contains(member);
    let (online, offline): (Vec<_>, Vec<_>) = members
        .into_iter()
        .partition(|member| online.contains(member));
    let (online_holding, online_others) = online.into_iter().partition(holds);
    let (offline_holding, offline_others) = offline.into_iter().partition(holds);
    [
        online_holding,
        online_others,
        offline_holding,
        offline_others,
    ]
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

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    fn member() -> MemberId {
        MemberId::from(SecretKey::generate().public())
    }

    /// An offline holder comes after an online member that does not hold the
    /// item (#185 finding 13); one member per tier, so the order is exact.
    #[test]
    fn members_are_asked_online_first_and_holders_first_within_each() {
        let (bob, carol, dave, erin) = (member(), member(), member(), member());
        let online = BTreeSet::from([bob, carol]);

        let order = ask_order(&[bob, dave], &online, [erin, dave, carol, bob]);

        assert_eq!(order, [bob, carol, dave, erin]);
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
}
