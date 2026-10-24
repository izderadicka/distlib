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
    an_item_of(1, creator, GB)
}

/// Item `seed`, of one file of `size` bytes, added by `creator`.
fn an_item_of(seed: u8, creator: MemberId, size: u64) -> StoredItem {
    let hash = ContentHash::from_bytes([seed; 32]);
    StoredItem {
        item: Item {
            files: BTreeMap::from([(
                hash,
                FileRecord {
                    role: FileRole::Content,
                    format: "epub".to_owned(),
                    size,
                    filename: format!("{seed}.epub"),
                    seq: None,
                    disc: None,
                    title: None,
                    duration: None,
                },
            )]),
            ..Item::new(ItemId::from_bytes([seed; 32]))
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
            over_pledge_items: watch::Sender::new(0),
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

/// What the whole pass leaves over this node's pledge is said for the
/// heartbeat: two items of 60 GB, both this node's, against its 100 GB.
#[tokio::test]
async fn the_whole_pass_says_what_is_over_the_pledge() {
    let other = SecretKey::generate();
    let me = MemberId::from(SecretKey::generate().public());
    let store = Store::open(None).await.unwrap();
    for seed in [1, 2] {
        let item = an_item_of(seed, MemberId::from(other.public()), 60 * GB);
        store.upsert_item(item).await.unwrap();
    }
    let (_membership_tx, membership) = watch::channel(founded(me, &other));
    let (_events_tx, events) = broadcast::channel(16);
    let (replays_tx, replays) = watch::channel(0);
    replays_tx.send_replace(1);
    let over_pledge_items = watch::Sender::new(0);
    let mut told = over_pledge_items.subscribe();

    let custody = Custody::start(
        me,
        CustodySources {
            membership,
            store,
            events,
            replays,
            over_pledge_items,
        },
    );
    tokio::time::timeout(Duration::from_secs(5), told.wait_for(|items| *items == 1))
        .await
        .expect("one item said to be over the pledge")
        .unwrap();
    custody.shutdown();
}
