//! Review evidence: passing assertions reproduce stale-epoch acceptance.
#![allow(clippy::unwrap_used)]

use super::*;
use distlib_consensus::{MemberProfile, MembershipEvent, SignedEvent, Timestamp};
use distlib_core::{Holdings as StatedHoldings, NodeAddr};

struct Observer {
    key: SecretKey,
    membership: MembershipState,
    online: Online,
    holders: Holders,
    directory: Directory,
    events: broadcast::Sender<Event>,
}

impl Observer {
    fn new() -> Self {
        let key = SecretKey::generate();
        let me = SecretKey::generate();
        let founders = [&key, &me]
            .into_iter()
            .map(|key| {
                (
                    MemberProfile {
                        member_id: MemberId::from(key.public()),
                        display_name: "review".into(),
                    },
                    NodeAddr::default(),
                )
            })
            .collect();
        let event = MembershipEvent::found(founders, Default::default(), Timestamp::from_millis(1))
            .unwrap();
        let signed = SignedEvent::sign(&key, event, Timestamp::from_millis(1), 0).unwrap();
        let mut membership = MembershipState::default();
        membership.apply(1, &signed).unwrap();
        Self {
            key,
            membership,
            online: Online::new(MemberId::from(me.public())),
            holders: Holders::default(),
            directory: Directory::default(),
            events: broadcast::channel(16).0,
        }
    }

    fn member(&self) -> MemberId {
        MemberId::from(self.key.public())
    }

    fn beat(&self, epoch: u64, seq: u64, item: Option<ItemId>, leaving: bool) -> Vec<u8> {
        let added: Vec<_> = item.into_iter().collect();
        let beat = Heartbeat {
            address: SignedAddress::sign(&self.key, NodeAddr::default(), 1).unwrap(),
            epoch,
            seq,
            interval_secs: 60,
            leaving,
            holdings: StatedHoldings {
                count: added.len() as u64,
                added,
                ..Default::default()
            },
        };
        SignedHeartbeat::sign(&self.key, &self.membership.group_id().unwrap(), beat)
            .unwrap()
            .encode()
            .unwrap()
    }

    fn deliver(&mut self, encoded: &[u8]) {
        hear(
            Hearing {
                online: &mut self.online,
                holders: &mut self.holders,
                directory: &self.directory,
                group: &self.membership.group_id().unwrap(),
                membership: &self.membership,
                events: &self.events,
            },
            encoded,
        )
        .unwrap();
    }
}

#[test]
fn proof_a_delayed_old_epoch_restores_holdings_removed_after_restart() {
    let mut observer = Observer::new();
    let item = ItemId::from_bytes([7; 32]);
    let first = observer.beat(10, 1, Some(item), false);
    let delayed = observer.beat(10, 2, Some(item), false);
    let restarted_empty = observer.beat(20, 1, None, false);
    observer.deliver(&first);
    assert_eq!(observer.holders.real_holders(&item), [observer.member()]);
    observer.deliver(&restarted_empty);
    assert!(observer.holders.real_holders(&item).is_empty());
    // A distinct, previously unseen message, delayed across the restart.
    observer.deliver(&delayed);
    assert!(observer.online.online().contains(&observer.member()));
    assert_eq!(observer.holders.real_holders(&item), [observer.member()]);
}

#[test]
fn proof_an_old_goodbye_takes_a_restarted_member_offline() {
    let mut observer = Observer::new();
    let first = observer.beat(10, 1, None, false);
    let goodbye = observer.beat(10, 2, None, true);
    let restarted = observer.beat(20, 1, None, false);
    observer.deliver(&first);
    observer.deliver(&restarted);
    assert!(observer.online.online().contains(&observer.member()));
    observer.deliver(&goodbye);
    assert!(!observer.online.online().contains(&observer.member()));
}
