//! Evidence for docs/reviews/raft-membership-pledges-review.md.
//!
//! These probes assert that the reviewed defects EXIST. Passing demonstrates
//! the defect; these are not regression assertions of desired behavior.
//! Reverse the relevant assertion when implementing a fix. This evidence
//! branch changes no production code.
//!
//! Run: cargo nextest run -p distlib-consensus --test review_probes
//! No network sockets or timing-dependent concurrency are needed.
//!
//! The obsolete purge probe was removed after master disabled automatic purging.
//! The snapshot probe holds an old builder across reset, modeling detached work.

#![allow(clippy::unwrap_used)]

use distlib_consensus::{
    LogStore, MemberRecord, MembershipEvent, MembershipState, SignedEvent, StateMachineStore,
    Timestamp, TypeConfig,
};
use distlib_core::{MemberId, NodeAddr, RawMemberId};
use iroh::SecretKey;
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, storage::RaftStateMachine,
};
use std::sync::Arc;

fn record(key: &SecretKey, pledge_bytes: u64) -> MemberRecord {
    MemberRecord {
        member_id: MemberId::from(key.public()),
        display_name: "review".into(),
        pledge_bytes,
    }
}
fn sign(key: &SecretKey, event: MembershipEvent, seen: u64) -> SignedEvent {
    SignedEvent::sign(key, event, Timestamp::from_millis(1), seen).unwrap()
}
fn log_id(index: u64) -> LogId<RawMemberId> {
    LogId::new(CommittedLeaderId::new(1, RawMemberId::default()), index)
}
fn founded(keys: &[SecretKey]) -> MembershipState {
    let mut state = MembershipState::new();
    let event = MembershipEvent::found(
        keys.iter()
            .map(|k| (record(k, 0), NodeAddr::default()))
            .collect(),
        Default::default(),
        Timestamp::from_millis(1),
    )
    .unwrap();
    state.apply(1, &sign(&keys[0], event, 0)).unwrap();
    state
}

#[test]
fn admission_accepts_a_pledge_never_signed_by_its_owner() {
    let alice = SecretKey::generate();
    let bob = SecretKey::generate();
    let mut state = founded(std::slice::from_ref(&alice));
    state
        .apply(
            2,
            &sign(
                &alice,
                MembershipEvent::MemberAdded {
                    member: record(&bob, u64::MAX),
                },
                1,
            ),
        )
        .unwrap();
    assert_eq!(
        state
            .member(&MemberId::from(bob.public()))
            .unwrap()
            .pledge_bytes,
        u64::MAX
    );
}

#[test]
fn expired_proposal_can_be_approved_after_a_gap_of_rejected_or_raft_entries() {
    let keys = [
        SecretKey::generate(),
        SecretKey::generate(),
        SecretKey::generate(),
    ];
    let mut state = founded(&keys);
    state
        .apply(
            2,
            &sign(
                &keys[0],
                MembershipEvent::CoreGroupChanged {
                    core: keys[..2]
                        .iter()
                        .map(|k| (MemberId::from(k.public()), NodeAddr::default()))
                        .collect(),
                },
                1,
            ),
        )
        .unwrap();
    assert_eq!(state.pending().count(), 1);
    state
        .apply(
            1000,
            &sign(&keys[1], MembershipEvent::Approved { proposal: 2 }, 2),
        )
        .unwrap();
    assert_eq!(state.core().len(), 2);
}

#[tokio::test]
async fn pre_reset_snapshot_builder_can_restore_the_deleted_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(redb::Database::create(dir.path().join("raft.redb")).unwrap());
    let _log = LogStore::from_database(db.clone()).unwrap();
    let mut sm = StateMachineStore::from_database(db).unwrap();
    sm.apply(vec![Entry::<TypeConfig> {
        log_id: log_id(200),
        payload: EntryPayload::Blank,
    }])
    .await
    .unwrap();
    let mut old_builder = sm.get_snapshot_builder().await;
    sm.reset_for_promotion().await.unwrap();
    assert!(sm.get_current_snapshot().await.unwrap().is_none());
    old_builder.build_snapshot().await.unwrap();
    assert_eq!(
        sm.get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .meta
            .last_log_id
            .unwrap()
            .index,
        200
    );
    assert_eq!(sm.last_applied_index(), 0);
}
