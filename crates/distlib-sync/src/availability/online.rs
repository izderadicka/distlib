//! Who is online, by the heartbeats this node has heard (phase 4's D4).
//!
//! The rules alone — no network, no clock of their own — so they are tested as
//! rules. The service feeds this every beat, every membership change and every
//! deadline, and publishes what [`Online::online`] says afterwards.

use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use distlib_core::{Heartbeat, MemberId};
use tokio::time::Instant;

/// How long a beat may ask to be believed for, whatever it says: a sender
/// cannot drop out between two packets, nor stay online an afternoon on one.
const TTL_MIN: Duration = Duration::from_secs(1);
const TTL_MAX: Duration = Duration::from_secs(60 * 60);

/// How long a beat from a member beating every `interval_secs` is believed:
/// three intervals, so one or two lost beats do not take it offline.
///
/// The sender's interval, not this node's, so two nodes configured
/// differently — or seeing the group at different sizes — still agree.
pub(super) fn ttl(interval_secs: u32) -> Duration {
    (Duration::from_secs(u64::from(interval_secs)) * 3).clamp(TTL_MIN, TTL_MAX)
}

/// The members heard from, and until when each counts as online.
#[derive(Debug)]
pub(super) struct Online {
    me: MemberId,
    members: BTreeSet<MemberId>,
    heard: HashMap<MemberId, Heard>,
}

/// The newest beat accepted from one member.
#[derive(Debug, Clone, Copy)]
struct Heard {
    epoch: u64,
    seq: u64,
    /// When the member stops counting as online; `None` once it has — it
    /// left, or its beats stopped. Kept rather than forgotten, so a late beat
    /// older than whatever took it offline cannot bring it back.
    until: Option<Instant>,
}

impl Online {
    pub(super) fn new(me: MemberId) -> Self {
        Self {
            me,
            members: BTreeSet::new(),
            heard: HashMap::new(),
        }
    }

    /// The group as it is now. Whoever is no longer in it is forgotten at
    /// once, rather than left online until their beat runs out.
    pub(super) fn members(&mut self, members: impl IntoIterator<Item = MemberId>) {
        let Self {
            members: now,
            heard,
            ..
        } = self;
        *now = members.into_iter().collect();
        heard.retain(|member, _| now.contains(member));
    }

    /// Takes in one beat, verified, heard at `now`; whether it was news.
    ///
    /// Refused: one from this node or from outside the group, and one no newer
    /// than what is held — a new `epoch` is a restart and always news, within
    /// an epoch only a higher `seq` is.
    pub(super) fn heard(&mut self, beat: &Heartbeat, now: Instant) -> bool {
        let member = beat.address.member();
        if member == self.me || !self.members.contains(&member) {
            return false;
        }
        if self
            .heard
            .get(&member)
            .is_some_and(|held| held.epoch == beat.epoch && beat.seq <= held.seq)
        {
            return false;
        }
        let until = (!beat.leaving).then(|| now + ttl(beat.interval_secs));
        self.heard.insert(
            member,
            Heard {
                epoch: beat.epoch,
                seq: beat.seq,
                until,
            },
        );
        true
    }

    /// Takes offline everyone whose beat ran out by `now`.
    pub(super) fn expire(&mut self, now: Instant) {
        for heard in self.heard.values_mut() {
            if heard.until.is_some_and(|until| until <= now) {
                heard.until = None;
            }
        }
    }

    /// When the next member's beat runs out, if anyone is online.
    pub(super) fn next_expiry(&self) -> Option<Instant> {
        self.heard.values().filter_map(|heard| heard.until).min()
    }

    /// Everyone online, this node aside.
    pub(super) fn online(&self) -> BTreeSet<MemberId> {
        self.heard
            .iter()
            .filter(|(_, heard)| heard.until.is_some())
            .map(|(member, _)| *member)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use distlib_core::{Holdings, NodeAddr, SignedAddress};
    use iroh::SecretKey;

    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    fn id(key: &SecretKey) -> MemberId {
        MemberId::from(key.public())
    }

    fn beat(key: &SecretKey, epoch: u64, seq: u64, interval_secs: u32) -> Heartbeat {
        Heartbeat {
            address: SignedAddress::sign(key, NodeAddr::default(), 0).unwrap(),
            epoch,
            seq,
            interval_secs,
            holdings: Holdings::default(),
            leaving: false,
        }
    }

    fn leaving(mut beat: Heartbeat) -> Heartbeat {
        beat.leaving = true;
        beat
    }

    /// A table of this node and `others`, all in the group.
    fn group(others: &[&SecretKey]) -> (Online, MemberId) {
        let me = MemberId::from(SecretKey::generate().public());
        let mut online = Online::new(me);
        online.members(others.iter().map(|key| id(key)).chain([me]));
        (online, me)
    }

    #[test]
    fn a_member_is_online_for_three_of_its_own_intervals() {
        let (bob, carol) = (SecretKey::generate(), SecretKey::generate());
        let (mut online, _) = group(&[&bob, &carol]);
        let start = Instant::now();

        assert!(online.heard(&beat(&bob, 1, 1, 10), start));
        assert!(online.heard(&beat(&carol, 1, 1, 1), start));
        assert_eq!(online.online(), BTreeSet::from([id(&bob), id(&carol)]));
        assert_eq!(online.next_expiry(), Some(start + 3 * SECOND));

        online.expire(start + 3 * SECOND - Duration::from_millis(1));
        assert_eq!(online.online().len(), 2, "not a moment early");
        online.expire(start + 3 * SECOND);
        assert_eq!(online.online(), BTreeSet::from([id(&bob)]));
        assert_eq!(online.next_expiry(), Some(start + 30 * SECOND));

        online.expire(start + 30 * SECOND);
        assert!(online.online().is_empty());
        assert_eq!(online.next_expiry(), None);
    }

    #[test]
    fn a_later_beat_moves_the_deadline() {
        let bob = SecretKey::generate();
        let (mut online, _) = group(&[&bob]);
        let start = Instant::now();

        online.heard(&beat(&bob, 1, 1, 1), start);
        assert!(online.heard(&beat(&bob, 1, 2, 1), start + 2 * SECOND));
        online.expire(start + 3 * SECOND);
        assert_eq!(online.online(), BTreeSet::from([id(&bob)]));
        assert_eq!(online.next_expiry(), Some(start + 5 * SECOND));
    }

    #[test]
    fn the_ttl_is_clamped_to_between_a_second_and_an_hour() {
        assert_eq!(ttl(0), SECOND);
        assert_eq!(ttl(1), 3 * SECOND);
        assert_eq!(ttl(1_200), 3_600 * SECOND);
        assert_eq!(ttl(u32::MAX), 3_600 * SECOND);
    }

    #[test]
    fn within_an_epoch_only_a_higher_seq_is_news() {
        let bob = SecretKey::generate();
        let (mut online, _) = group(&[&bob]);
        let start = Instant::now();

        assert!(online.heard(&beat(&bob, 1, 5, 1), start));
        assert!(!online.heard(&beat(&bob, 1, 5, 1), start + SECOND));
        assert!(!online.heard(&beat(&bob, 1, 4, 1), start + SECOND));
        assert_eq!(
            online.next_expiry(),
            Some(start + 3 * SECOND),
            "a beat that was not news moves nothing"
        );
        assert!(online.heard(&beat(&bob, 1, 6, 1), start + SECOND));
    }

    /// A restart starts counting again from wherever it likes.
    #[test]
    fn a_new_epoch_is_news_whatever_its_seq() {
        let bob = SecretKey::generate();
        let (mut online, _) = group(&[&bob]);
        let start = Instant::now();

        online.heard(&beat(&bob, 1, 5, 1), start);
        assert!(online.heard(&beat(&bob, 2, 1, 1), start));
    }

    #[test]
    fn a_leaving_beat_takes_a_member_offline_at_once() {
        let bob = SecretKey::generate();
        let (mut online, _) = group(&[&bob]);
        let start = Instant::now();

        online.heard(&beat(&bob, 1, 1, 60), start);
        assert!(online.heard(&leaving(beat(&bob, 1, 2, 60)), start));
        assert!(online.online().is_empty());
        assert_eq!(online.next_expiry(), None);
    }

    /// Gossip does not keep order, so a beat sent before the goodbye can
    /// arrive after it — and must not bring the member back for a whole TTL.
    #[test]
    fn a_beat_older_than_the_goodbye_does_not_bring_a_member_back() {
        let bob = SecretKey::generate();
        let (mut online, _) = group(&[&bob]);
        let start = Instant::now();

        online.heard(&leaving(beat(&bob, 1, 2, 60)), start);
        assert!(!online.heard(&beat(&bob, 1, 1, 60), start));
        assert!(online.online().is_empty());

        // And the same holds once a beat has merely run out.
        online.heard(&beat(&bob, 1, 3, 1), start);
        online.expire(start + 3 * SECOND);
        assert!(!online.heard(&beat(&bob, 1, 3, 1), start + 3 * SECOND));
        assert!(online.online().is_empty());
    }

    #[test]
    fn only_other_members_count() {
        let (bob, stranger) = (SecretKey::generate(), SecretKey::generate());
        let me = SecretKey::generate();
        let mut online = Online::new(id(&me));
        online.members([id(&me), id(&bob)]);
        let start = Instant::now();

        assert!(!online.heard(&beat(&stranger, 1, 1, 60), start));
        assert!(!online.heard(&beat(&me, 1, 1, 60), start));
        assert!(online.online().is_empty());
    }

    #[test]
    fn a_member_who_leaves_the_group_is_gone_at_once() {
        let (bob, carol) = (SecretKey::generate(), SecretKey::generate());
        let (mut online, me) = group(&[&bob, &carol]);
        let start = Instant::now();
        online.heard(&beat(&bob, 1, 1, 60), start);
        online.heard(&beat(&carol, 1, 1, 1), start);

        online.members([me, id(&bob)]);
        assert_eq!(online.online(), BTreeSet::from([id(&bob)]));
        assert_eq!(online.next_expiry(), Some(start + 180 * SECOND));
        assert!(!online.heard(&beat(&carol, 1, 2, 1), start));
    }
}
