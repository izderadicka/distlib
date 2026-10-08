//! Who holds what, by the heartbeats this node has heard (phase 4's D3).
//!
//! The rules alone, like [`super::online`]: the service feeds this every beat
//! and every base list it fetches, and asks it which bases to fetch. Fetching
//! is the service's business; nothing here touches the network.
//!
//! **A beat's change replaces the last one; it never adds to it.** `added` and
//! `removed` are counted from the member's base, so an item added and then
//! lost again before the next base is simply in neither list. Applying each
//! beat's change on top of the last would keep it for ever. So the previous
//! change is undone before the new one is applied — which needs only the
//! previous change, not a copy of the base.

use std::collections::HashMap;

use distlib_core::{ContentHash, Holdings, ItemId, MemberId};

/// Every member's holdings this node knows, as one inverted map.
#[derive(Debug, Default)]
pub(super) struct Holders {
    index: Index,
    stated: HashMap<MemberId, Stated>,
}

/// Where this node is with one member's holdings.
#[derive(Debug)]
enum Stated {
    /// Its base is being fetched, or is to be by the next beat that names it.
    /// `latest` is the newest beat's statement, applied when the base comes.
    Unknown {
        base: ContentHash,
        latest: Holdings,
        fetching: bool,
    },
    /// In the map: `base`, then `added` and `removed`. `held` is how many
    /// items the map has for the member, so the count can be checked.
    Known {
        base: Option<ContentHash>,
        added: Vec<ItemId>,
        removed: Vec<ItemId>,
        held: u64,
    },
    /// Its statement did not add up against `base`. Fetching the same base
    /// again would give the same bytes, so nothing is fetched until a beat
    /// names another one.
    Broken { base: Option<ContentHash> },
}

impl Holders {
    /// Takes in one member's newest statement; the base to fetch, if this
    /// node needs it and is not already fetching it.
    pub(super) fn heard(&mut self, member: MemberId, holdings: &Holdings) -> Option<ContentHash> {
        match self.stated.get_mut(&member) {
            Some(Stated::Known { base, .. }) if *base == holdings.base => {
                self.restate(member, holdings);
                None
            }
            Some(Stated::Unknown {
                base,
                latest,
                fetching,
            }) if Some(*base) == holdings.base => {
                latest.clone_from(holdings);
                (!std::mem::replace(fetching, true)).then_some(*base)
            }
            Some(Stated::Broken { base }) if *base == holdings.base => None,
            // A member heard from for the first time, or with a new base.
            _ => {
                self.index.clear(member);
                if let Some(base) = holdings.base {
                    self.stated.insert(
                        member,
                        Stated::Unknown {
                            base,
                            latest: holdings.clone(),
                            fetching: true,
                        },
                    );
                    return Some(base);
                }
                self.known(member, None, 0, holdings);
                None
            }
        }
    }

    /// Takes in the base `member`'s beats name, fetched. Ignored if a newer
    /// beat has named another since.
    pub(super) fn arrived(
        &mut self,
        member: MemberId,
        base: ContentHash,
        items: impl Iterator<Item = ItemId>,
    ) {
        let latest = match self.stated.remove(&member) {
            Some(Stated::Unknown {
                base: wanted,
                latest,
                ..
            }) if wanted == base => latest,
            stale => {
                if let Some(stated) = stale {
                    self.stated.insert(member, stated);
                }
                return;
            }
        };
        let held = items
            .map(|item| u64::from(self.index.add(member, item)))
            .sum();
        self.known(member, Some(base), held, &latest);
    }

    /// The fetch of `member`'s `base` failed. If `retry`, the next beat that
    /// names it tries again; if not — the list itself was at fault, and the
    /// same hash is the same bytes — nothing is fetched until a beat names
    /// another.
    pub(super) fn failed(&mut self, member: MemberId, base: ContentHash, retry: bool) {
        let Some(stated) = self.stated.get_mut(&member) else {
            return;
        };
        let Stated::Unknown {
            base: wanted,
            fetching,
            ..
        } = stated
        else {
            return;
        };
        if *wanted != base {
            return;
        }
        if retry {
            *fetching = false;
        } else {
            *stated = Stated::Broken { base: Some(base) };
        }
    }

    /// Forgets whoever `is_member` no longer admits — one pass over the map.
    pub(super) fn retain_members(&mut self, is_member: impl Fn(&MemberId) -> bool) {
        self.stated.retain(|member, _| is_member(member));
        self.index.retain(is_member);
    }

    /// The members known to hold `item`.
    pub(super) fn holders(&self, item: &ItemId) -> &[MemberId] {
        self.index.holders(item)
    }

    /// Whether what `member` holds is known: heard from, its base in, and its
    /// statement adding up.
    pub(super) fn is_known(&self, member: &MemberId) -> bool {
        matches!(self.stated.get(member), Some(Stated::Known { .. }))
    }

    /// Records `member` as known from `base`, `held` items of it in the map
    /// already, and applies `holdings`' change on top.
    fn known(
        &mut self,
        member: MemberId,
        base: Option<ContentHash>,
        held: u64,
        holdings: &Holdings,
    ) {
        self.stated.insert(
            member,
            Stated::Known {
                base,
                added: Vec::new(),
                removed: Vec::new(),
                held,
            },
        );
        self.restate(member, holdings);
    }

    /// Replaces a known member's change with `holdings`', and checks the
    /// count: a statement that does not add up is dropped from the map.
    fn restate(&mut self, member: MemberId, holdings: &Holdings) {
        let Some(Stated::Known {
            base,
            added,
            removed,
            held,
        }) = self.stated.get_mut(&member)
        else {
            return;
        };
        // The previous change taken back — its removed come back, its added
        // go — then the new one applied.
        let undone = self.index.shift(member, *held, removed, added);
        *held = self
            .index
            .shift(member, undone, &holdings.added, &holdings.removed);
        if *held == holdings.count {
            added.clone_from(&holdings.added);
            removed.clone_from(&holdings.removed);
            return;
        }
        tracing::warn!(
            %member,
            stated = holdings.count,
            counted = *held,
            "a member's holdings do not add up; not counting them until its base changes"
        );
        let base = *base;
        self.index.clear(member);
        self.stated.insert(member, Stated::Broken { base });
    }
}

/// `item → members holding it`. Members by id, not D3's `u16` indexes: at the
/// target sizes the difference is a few megabytes (KISS, numbers decide).
///
/// A `Vec` per item rather than a set: most items settle on a handful of
/// holders, and scanning a few ids is as quick as hashing one. Only an item
/// everybody downloads — hot news — grows towards the size of the group, and
/// even a thousand ids is a scan of microseconds.
#[derive(Debug, Default)]
struct Index(HashMap<ItemId, Vec<MemberId>>);

impl Index {
    /// Whether it was new.
    fn add(&mut self, member: MemberId, item: ItemId) -> bool {
        let members = self.0.entry(item).or_default();
        let new = !members.contains(&member);
        if new {
            members.push(member);
        }
        new
    }

    /// Whether it was there.
    fn remove(&mut self, member: MemberId, item: &ItemId) -> bool {
        let Some(members) = self.0.get_mut(item) else {
            return false;
        };
        let Some(at) = members.iter().position(|held| *held == member) else {
            return false;
        };
        members.swap_remove(at);
        if members.is_empty() {
            self.0.remove(item);
        }
        true
    }

    /// Puts `member` on every item `coming` and takes it off every item
    /// `going`; how many items it holds after, given `held` before.
    fn shift(&mut self, member: MemberId, held: u64, coming: &[ItemId], going: &[ItemId]) -> u64 {
        let came: u64 = coming
            .iter()
            .map(|item| u64::from(self.add(member, *item)))
            .sum();
        let went: u64 = going
            .iter()
            .map(|item| u64::from(self.remove(member, item)))
            .sum();
        held + came - went
    }

    fn clear(&mut self, member: MemberId) {
        self.retain(|held| *held != member);
    }

    fn retain(&mut self, keep: impl Fn(&MemberId) -> bool) {
        self.0.retain(|_, members| {
            members.retain(&keep);
            !members.is_empty()
        });
    }

    fn holders(&self, item: &ItemId) -> &[MemberId] {
        self.0.get(item).map_or(&[], Vec::as_slice)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use iroh::SecretKey;

    use super::*;

    fn member() -> MemberId {
        MemberId::from(SecretKey::generate().public())
    }

    fn item(n: u8) -> ItemId {
        ItemId::from_bytes([n; 32])
    }

    fn base(n: u8) -> ContentHash {
        ContentHash::from_bytes([n; 32])
    }

    fn stated(base: Option<ContentHash>, count: u64, added: &[u8], removed: &[u8]) -> Holdings {
        Holdings {
            count,
            base,
            added: added.iter().copied().map(item).collect(),
            removed: removed.iter().copied().map(item).collect(),
        }
    }

    /// Who holds each of items 0 to 9.
    fn map(holders: &Holders, bob: MemberId) -> Vec<u8> {
        (0..10)
            .filter(|n| holders.holders(&item(*n)).contains(&bob))
            .collect()
    }

    #[test]
    fn without_a_base_the_change_is_everything_held() {
        let bob = member();
        let mut holders = Holders::default();

        assert_eq!(holders.heard(bob, &stated(None, 2, &[1, 2], &[])), None);
        assert!(holders.is_known(&bob));
        assert_eq!(map(&holders, bob), [1, 2]);
    }

    #[test]
    fn a_base_is_fetched_once_and_the_change_applied_on_top() {
        let bob = member();
        let mut holders = Holders::default();

        let first = stated(Some(base(9)), 3, &[4], &[2]);
        assert_eq!(holders.heard(bob, &first), Some(base(9)));
        assert!(!holders.is_known(&bob), "unknown until the base is in");
        let newer = stated(Some(base(9)), 2, &[], &[2]);
        assert_eq!(holders.heard(bob, &newer), None, "already being fetched");

        holders.arrived(bob, base(9), [1, 2, 3].map(item).into_iter());
        assert!(holders.is_known(&bob));
        assert_eq!(map(&holders, bob), [1, 3], "the newest beat's change");
    }

    /// The trap: an item added and lost again before the next base is in
    /// neither list, and must not be kept.
    #[test]
    fn each_change_replaces_the_last() {
        let bob = member();
        let mut holders = Holders::default();
        holders.heard(bob, &stated(Some(base(9)), 2, &[], &[]));
        holders.arrived(bob, base(9), [1, 2].map(item).into_iter());

        holders.heard(bob, &stated(Some(base(9)), 2, &[3], &[1]));
        assert_eq!(map(&holders, bob), [2, 3]);
        holders.heard(bob, &stated(Some(base(9)), 2, &[], &[]));
        assert_eq!(map(&holders, bob), [1, 2], "3 lost, 1 back");
        holders.heard(bob, &stated(Some(base(9)), 1, &[], &[2]));
        assert_eq!(map(&holders, bob), [1]);
    }

    /// Each beat is whole on its own: one gossip lost on the way is as if it
    /// had never been sent.
    #[test]
    fn a_missed_beat_changes_nothing() {
        let beats = [
            stated(Some(base(9)), 3, &[3], &[]),
            stated(Some(base(9)), 1, &[4], &[1, 2]),
            stated(Some(base(9)), 2, &[3], &[1]),
        ];
        let (bob, carol) = (member(), member());
        let mut holders = Holders::default();
        for (member, heard) in [
            (bob, &beats[..]),
            (carol, &[&beats[0], &beats[2]].map(Clone::clone)[..]),
        ] {
            holders.heard(member, &heard[0]);
            holders.arrived(member, base(9), [1, 2].map(item).into_iter());
            heard[1..].iter().for_each(|beat| {
                holders.heard(member, beat);
            });
        }
        assert_eq!(map(&holders, bob), [2, 3]);
        assert_eq!(map(&holders, carol), map(&holders, bob));
    }

    #[test]
    fn a_new_base_replaces_everything_the_old_one_said() {
        let bob = member();
        let mut holders = Holders::default();
        holders.heard(bob, &stated(Some(base(8)), 3, &[3], &[]));
        holders.arrived(bob, base(8), [1, 2].map(item).into_iter());

        assert_eq!(
            holders.heard(bob, &stated(Some(base(9)), 2, &[], &[])),
            Some(base(9))
        );
        assert!(!holders.is_known(&bob));
        assert!(
            map(&holders, bob).is_empty(),
            "not the old base's items meanwhile"
        );
        holders.arrived(bob, base(9), [3, 4].map(item).into_iter());
        assert_eq!(map(&holders, bob), [3, 4]);
    }

    #[test]
    fn a_base_that_is_no_longer_named_is_ignored() {
        let bob = member();
        let mut holders = Holders::default();
        holders.heard(bob, &stated(Some(base(8)), 1, &[], &[]));
        holders.heard(bob, &stated(Some(base(9)), 1, &[], &[]));

        holders.arrived(bob, base(8), [1].map(item).into_iter());
        assert!(!holders.is_known(&bob));
        holders.arrived(bob, base(9), [2].map(item).into_iter());
        assert_eq!(map(&holders, bob), [2]);
    }

    #[test]
    fn a_failed_fetch_is_tried_again_by_the_next_beat() {
        let bob = member();
        let mut holders = Holders::default();
        let beat = stated(Some(base(9)), 1, &[], &[]);
        holders.heard(bob, &beat);

        holders.failed(bob, base(8), true);
        assert_eq!(holders.heard(bob, &beat), None, "not that fetch");
        holders.failed(bob, base(9), true);
        assert_eq!(holders.heard(bob, &beat), Some(base(9)));
    }

    #[test]
    fn a_list_at_fault_is_not_fetched_again() {
        let bob = member();
        let mut holders = Holders::default();
        let beat = stated(Some(base(9)), 1, &[], &[]);
        holders.heard(bob, &beat);

        holders.failed(bob, base(9), false);
        assert_eq!(holders.heard(bob, &beat), None);
        assert!(!holders.is_known(&bob));
        assert_eq!(
            holders.heard(bob, &stated(Some(base(7)), 1, &[], &[])),
            Some(base(7))
        );
    }

    /// Fetching the same base again would give the same bytes, so a
    /// statement that does not add up waits for another base.
    #[test]
    fn a_statement_that_does_not_add_up_is_not_counted() {
        let bob = member();
        let mut holders = Holders::default();
        holders.heard(bob, &stated(Some(base(9)), 2, &[], &[]));
        holders.arrived(bob, base(9), [1, 2].map(item).into_iter());

        // 5 was never in the base, so removing it changes nothing.
        assert_eq!(
            holders.heard(bob, &stated(Some(base(9)), 1, &[], &[5])),
            None
        );
        assert!(!holders.is_known(&bob));
        assert!(map(&holders, bob).is_empty());
        assert_eq!(
            holders.heard(bob, &stated(Some(base(9)), 2, &[], &[])),
            None
        );
        assert!(
            !holders.is_known(&bob),
            "the same base is not fetched again"
        );

        assert_eq!(
            holders.heard(bob, &stated(Some(base(7)), 1, &[], &[])),
            Some(base(7))
        );
        holders.arrived(bob, base(7), [3].map(item).into_iter());
        assert_eq!(map(&holders, bob), [3]);
    }

    /// A member is a holder of an item once, however many times something
    /// says so.
    #[test]
    fn an_item_stated_twice_is_held_once() {
        let bob = member();
        let mut holders = Holders::default();
        holders.heard(bob, &stated(Some(base(9)), 1, &[1], &[]));
        holders.arrived(bob, base(9), [1, 1].map(item).into_iter());
        assert!(holders.is_known(&bob));
        assert_eq!(holders.holders(&item(1)), [bob]);
    }

    #[test]
    fn a_base_that_does_not_add_up_is_not_counted() {
        let bob = member();
        let mut holders = Holders::default();
        holders.heard(bob, &stated(Some(base(9)), 3, &[], &[]));

        holders.arrived(bob, base(9), [1, 2].map(item).into_iter());
        assert!(!holders.is_known(&bob));
        assert!(map(&holders, bob).is_empty());
    }

    #[test]
    fn members_are_counted_apart_and_forgotten_apart() {
        let (bob, carol) = (member(), member());
        let mut holders = Holders::default();
        holders.heard(bob, &stated(None, 2, &[1, 2], &[]));
        holders.heard(carol, &stated(None, 2, &[2, 3], &[]));
        assert_eq!(holders.holders(&item(2)).len(), 2);

        holders.retain_members(|member| *member == carol);
        assert!(!holders.is_known(&bob));
        assert!(holders.holders(&item(1)).is_empty());
        assert_eq!(holders.holders(&item(2)), [carol]);
        assert_eq!(map(&holders, carol), [2, 3]);
    }
}
