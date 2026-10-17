//! The custody task on its own: the sources are channels this test drives.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipState, SignedEvent, Timestamp};
use distlib_core::{ContentHash, FileRecord, FileRole, Item, ItemId, MemberId, NodeAddr};
use distlib_custody::{Custody, CustodySources};
use distlib_store::{Store, StoredItem};
use iroh::SecretKey;
use tokio::sync::{broadcast, watch};

const GB: u64 = 1_000_000_000;

/// A group of `me` and `other`, founded by `other`, both pledging.
fn founded(me: MemberId, other: &SecretKey) -> MembershipState {
    let record = |member_id, name: &str| {
        (
            MemberRecord {
                member_id,
                display_name: name.to_owned(),
                pledge_bytes: 100 * GB,
            },
            NodeAddr::default(),
        )
    };
    let founders = vec![
        record(MemberId::from(other.public()), "other"),
        record(me, "me"),
    ];
    let event =
        MembershipEvent::found(founders, Default::default(), Timestamp::from_millis(1)).unwrap();
    let signed = SignedEvent::sign(other, event, Timestamp::from_millis(1), 0).unwrap();
    let mut state = MembershipState::new();
    state.apply(1, &signed).unwrap();
    state
}

/// One item of one file, added by `creator`.
fn an_item(creator: MemberId) -> StoredItem {
    let hash = ContentHash::from_bytes([1; 32]);
    StoredItem {
        item: Item {
            files: BTreeMap::from([(
                hash,
                FileRecord {
                    role: FileRole::Content,
                    format: "epub".to_owned(),
                    size: GB,
                    filename: "1.epub".to_owned(),
                    seq: None,
                    disc: None,
                    title: None,
                    duration: None,
                },
            )]),
            ..Item::new(ItemId::from_bytes([1; 32]))
        },
        ratings: BTreeMap::new(),
        reviews: BTreeMap::new(),
        file_authors: BTreeMap::from([(hash, BTreeSet::from([creator]))]),
        last_modified: 1,
    }
}

/// The read model's replay at a start can finish before custody sees the
/// group; custody's first pass must not be lost to it.
#[tokio::test]
async fn a_replay_finished_before_the_start_still_counts() {
    let other = SecretKey::generate();
    let me = MemberId::from(SecretKey::generate().public());
    let store = Store::open(None).await.unwrap();
    store
        .upsert_item(an_item(MemberId::from(other.public())))
        .await
        .unwrap();

    let (_membership_tx, membership) = watch::channel(founded(me, &other));
    let (_events_tx, events) = broadcast::channel(16);
    let (replays_tx, replays) = watch::channel(0);
    replays_tx.send_replace(1);

    let custody = Custody::start(
        me,
        CustodySources {
            membership,
            store,
            events,
            replays,
        },
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while custody.status().is_none_or(|status| status.items != 1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("custody of the one item, from the replay that had finished");
    custody.shutdown();
}
