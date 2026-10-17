//! Whom to ask for an item's files, best first (phase 4's 4b-5): what
//! `library.download` and the pin manager both fetch in.

use std::collections::BTreeSet;

use distlib_core::MemberId;

/// `members` in the order to ask them: those among `holders`, then the other
/// `online` members, then everyone else — so an offline member is dialled,
/// and its connect timeout waited out, only once nobody online could serve.
/// Each tier is shuffled, so one holder is not asked for everything.
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
}
