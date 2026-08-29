//! The membership state machine.
//!
//! openraft's conformance suite (see `conformance.rs`) already covers the parts
//! of the contract openraft cares about. What is left, and what these cover, is
//! what *we* put on top: that committed events become the derived membership,
//! that the state survives a restart, and what happens to an event the rules
//! reject after Raft has already committed it.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::result_large_err)] // openraft's StorageError, in its own signatures

use std::sync::Arc;

use distlib_consensus::{
    MemberRecord, MembershipEvent, SignedEvent, StateMachineStore, Timestamp, TypeConfig,
};
use distlib_core::{MemberId, RawMemberId};
use iroh::SecretKey;
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, storage::RaftStateMachine,
};
use redb::Database;
use tempfile::TempDir;

/// A member who can sign events.
struct Signer {
    secret: SecretKey,
    id: MemberId,
}

impl Signer {
    fn generate() -> Self {
        let secret = SecretKey::generate();
        let id = MemberId::from(secret.public());
        Self { secret, id }
    }

    fn record(&self, name: &str) -> MemberRecord {
        MemberRecord {
            member_id: self.id,
            display_name: name.to_owned(),
            pledge_bytes: 0,
        }
    }

    fn sign(&self, event: MembershipEvent) -> SignedEvent {
        SignedEvent::sign(&self.secret, event, Timestamp::from_millis(1)).unwrap()
    }
}

fn new_store() -> (StateMachineStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = StateMachineStore::open(dir.path().join("sm.redb")).unwrap();
    (store, dir)
}

/// Closes the store and reopens the same file, standing in for a restart.
fn restart(store: StateMachineStore, dir: &TempDir) -> StateMachineStore {
    drop(store);
    StateMachineStore::open(dir.path().join("sm.redb")).unwrap()
}

fn log_id(index: u64) -> LogId<RawMemberId> {
    LogId::new(CommittedLeaderId::new(1, RawMemberId::default()), index)
}

fn entry(index: u64, event: SignedEvent) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(event),
    }
}

/// A founding event from `founder`, as log entry 1.
fn founding(founder: &Signer) -> Entry<TypeConfig> {
    entry(
        1,
        founder.sign(
            MembershipEvent::found(vec![founder.record("founder")], Timestamp::from_millis(1))
                .unwrap(),
        ),
    )
}

#[tokio::test]
async fn a_committed_event_becomes_the_derived_membership() {
    let (mut store, _dir) = new_store();
    let alice = Signer::generate();

    store.apply(vec![founding(&alice)]).await.unwrap();

    let membership = store.membership();
    assert!(membership.is_member(&alice.id));
    assert_eq!(membership.allowlist().collect::<Vec<_>>(), vec![alice.id]);
}

#[tokio::test]
async fn apply_returns_one_response_per_entry() {
    // openraft matches responses to entries positionally; returning a different
    // count silently misattributes results to clients.
    let (mut store, _dir) = new_store();
    let alice = Signer::generate();
    let blank = Entry {
        log_id: log_id(2),
        payload: EntryPayload::Blank,
    };

    let responses = store.apply(vec![founding(&alice), blank]).await.unwrap();

    assert_eq!(responses.len(), 2);
}

#[tokio::test]
async fn the_applied_state_survives_a_restart() {
    let (mut store, dir) = new_store();
    let alice = Signer::generate();
    store.apply(vec![founding(&alice)]).await.unwrap();

    let mut restarted = restart(store, &dir);

    assert!(restarted.membership().is_member(&alice.id));
    let (last_applied, _) = restarted.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1)));
}

#[tokio::test]
async fn a_rejected_event_is_skipped_rather_than_fatal() {
    // The design decision worth pinning. Raft has already committed this entry,
    // so every node sees it; returning an error would take down the whole group
    // over one malformed proposal. `MembershipState::apply` is deterministic and
    // leaves state untouched on error, so every node skips it identically and
    // they stay in agreement.
    let (mut store, _dir) = new_store();
    let alice = Signer::generate();
    let stranger = Signer::generate();

    // A non-member proposing into a group they do not belong to.
    let rejected = entry(
        2,
        stranger.sign(MembershipEvent::MemberAdded {
            member: stranger.record("uninvited"),
        }),
    );

    let responses = store
        .apply(vec![founding(&alice), rejected])
        .await
        .expect("a rejected event must not fail the apply");

    assert_eq!(responses.len(), 2, "the entry is still consumed");
    assert!(
        !store.membership().is_member(&stranger.id),
        "and it must not take effect"
    );
    let (last_applied, _) = store.applied_state().await.unwrap();
    assert_eq!(
        last_applied,
        Some(log_id(2)),
        "the log id still advances; the entry was applied, its content refused"
    );
}

#[tokio::test]
async fn a_snapshot_restores_the_same_state() {
    let (mut store, _dir) = new_store();
    let alice = Signer::generate();
    store.apply(vec![founding(&alice)]).await.unwrap();

    let snapshot = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();

    // Install it into a fresh, empty store.
    let (mut other, _other_dir) = new_store();
    assert!(other.membership().is_empty());
    other
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();

    assert!(other.membership().is_member(&alice.id));
    assert_eq!(
        other.applied_state().await.unwrap().0,
        Some(log_id(1)),
        "installing a snapshot also restores how far it had applied"
    );
}

#[tokio::test]
async fn an_installed_snapshot_is_the_current_one_and_survives_a_restart() {
    let (mut store, _dir) = new_store();
    let alice = Signer::generate();
    store.apply(vec![founding(&alice)]).await.unwrap();
    let built = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();

    let (mut other, other_dir) = new_store();
    other
        .install_snapshot(&built.meta, built.snapshot)
        .await
        .unwrap();

    let mut restarted = restart(other, &other_dir);
    let current = restarted.get_current_snapshot().await.unwrap();

    assert_eq!(
        current.map(|snapshot| snapshot.meta.snapshot_id),
        Some(built.meta.snapshot_id),
        "the trait requires get_current_snapshot to return the installed one"
    );
    assert!(restarted.membership().is_member(&alice.id));
}

#[tokio::test]
async fn a_builder_is_unaffected_by_later_applies() {
    // The trait asks for a view that subsequent changes do not disturb.
    let (mut store, _dir) = new_store();
    let alice = Signer::generate();
    let bob = Signer::generate();
    store.apply(vec![founding(&alice)]).await.unwrap();

    let mut builder = store.get_snapshot_builder().await;
    store
        .apply(vec![entry(
            2,
            alice.sign(MembershipEvent::MemberAdded {
                member: bob.record("bob"),
            }),
        )])
        .await
        .unwrap();

    let snapshot = builder.build_snapshot().await.unwrap();

    assert_eq!(
        snapshot.meta.last_log_id,
        Some(log_id(1)),
        "the snapshot reflects the state when the builder was taken"
    );
    assert!(store.membership().is_member(&bob.id), "the store moved on");
}

#[tokio::test]
async fn two_snapshots_at_the_same_log_id_get_distinct_ids() {
    // openraft identifies a transfer by snapshot id, so two builds of the same
    // state must not collide.
    let (mut store, _dir) = new_store();
    store
        .apply(vec![founding(&Signer::generate())])
        .await
        .unwrap();

    let first = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let second = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();

    assert_eq!(first.meta.last_log_id, second.meta.last_log_id);
    assert_ne!(first.meta.snapshot_id, second.meta.snapshot_id);
}

#[tokio::test]
async fn the_state_machine_shares_a_database_with_the_log() {
    // A node keeps one file for its whole Raft state.
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("raft.redb")).unwrap());

    let log = distlib_consensus::LogStore::from_database(Arc::clone(&db)).unwrap();
    let mut sm = StateMachineStore::from_database(db).unwrap();

    let alice = Signer::generate();
    sm.apply(vec![founding(&alice)]).await.unwrap();

    assert!(sm.membership().is_member(&alice.id));
    drop(log);
}
