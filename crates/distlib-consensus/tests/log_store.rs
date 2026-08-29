//! The redb Raft log.
//!
//! These target *our* storage layer rather than Raft itself, as §10 asks. The
//! recurring question is what survives a restart: openraft's contract says a
//! vote is durable once `save_vote` returns and entries are durable once the
//! append callback fires, and a store that answered from memory would pass a
//! naive test and lose an election after a crash. So the interesting cases here
//! all reopen the database.
//!
//! openraft's own `testing::Suite` covers the storage contract far more
//! thoroughly, but it exercises a `RaftLogStorage` and a `RaftStateMachine`
//! together and cannot run until the state machine lands in the next PR.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::sync::Arc;

use distlib_consensus::{
    LogStore, MemberRecord, MembershipEvent, SignedEvent, Timestamp, TypeConfig,
};
use distlib_core::{MemberId, RawMemberId};
use iroh::SecretKey;
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, Vote,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use redb::Database;
use tempfile::TempDir;

/// A store backed by a fresh database file, kept alive by the returned dir.
fn store() -> (LogStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = LogStore::open(dir.path().join("raft.redb")).unwrap();
    (store, dir)
}

/// Closes `store` and opens the same file again, standing in for a restart.
///
/// The drop is load-bearing: redb takes an exclusive lock, so the old handle has
/// to go before a new one can exist — which is exactly what a restart does.
fn restart(store: LogStore, dir: &TempDir) -> LogStore {
    drop(store);
    LogStore::open(dir.path().join("raft.redb")).unwrap()
}

fn a_member() -> MemberId {
    MemberId::from(SecretKey::generate().public())
}

fn log_id(term: u64, index: u64) -> LogId<RawMemberId> {
    LogId::new(CommittedLeaderId::new(term, RawMemberId::default()), index)
}

/// A real signed event, so entries carry the payload the log will actually hold.
fn entry(term: u64, index: u64) -> Entry<TypeConfig> {
    let secret = SecretKey::generate();
    let record = MemberRecord {
        member_id: MemberId::from(secret.public()),
        display_name: format!("member at {index}"),
        pledge_bytes: index,
    };
    let event = SignedEvent::sign(
        &secret,
        MembershipEvent::MemberAdded { member: record },
        Timestamp::from_millis(index),
    )
    .unwrap();

    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(event),
    }
}

async fn indices(store: &mut LogStore) -> Vec<u64> {
    use openraft::RaftLogReader as _;
    store
        .try_get_log_entries(..)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.log_id.index)
        .collect()
}

#[tokio::test]
async fn an_empty_log_has_no_state() {
    let (mut store, _dir) = store();

    let state = store.get_log_state().await.unwrap();

    assert_eq!(state.last_log_id, None);
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(store.read_vote().await.unwrap(), None);
}

#[tokio::test]
async fn entries_read_back_in_index_order() {
    let (mut store, _dir) = store();

    // Appended out of order on purpose: the log is a map keyed by index, and
    // reads must come back ordered regardless of arrival order.
    store
        .blocking_append(vec![entry(1, 3), entry(1, 1), entry(1, 2)])
        .await
        .unwrap();

    assert_eq!(indices(&mut store).await, vec![1, 2, 3]);
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(1, 3))
    );
}

#[tokio::test]
async fn a_range_read_excludes_its_upper_bound() {
    let (mut store, _dir) = store();
    store
        .blocking_append((1..=5).map(|index| entry(1, index)).collect::<Vec<_>>())
        .await
        .unwrap();

    use openraft::RaftLogReader as _;
    let read: Vec<u64> = store
        .try_get_log_entries(2..4)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.log_id.index)
        .collect();

    assert_eq!(read, vec![2, 3], "[start, stop) per the trait");
}

#[tokio::test]
async fn entries_survive_a_restart() {
    let (mut store, dir) = store();
    store
        .blocking_append(vec![entry(1, 1), entry(1, 2)])
        .await
        .unwrap();

    let mut restarted = restart(store, &dir);

    assert_eq!(indices(&mut restarted).await, vec![1, 2]);
    assert_eq!(
        restarted.get_log_state().await.unwrap().last_log_id,
        Some(log_id(1, 2))
    );
}

#[tokio::test]
async fn a_vote_survives_a_restart() {
    // The one that matters most: a vote answered from memory would let a node
    // vote twice in one term after a crash, which is the safety property Raft
    // rests on.
    let (mut store, dir) = store();
    let vote = Vote::new(7, RawMemberId::from(a_member()));

    store.save_vote(&vote).await.unwrap();

    assert_eq!(restart(store, &dir).read_vote().await.unwrap(), Some(vote));
}

#[tokio::test]
async fn the_committed_pointer_survives_a_restart() {
    let (mut store, dir) = store();

    store.save_committed(Some(log_id(2, 9))).await.unwrap();

    assert_eq!(
        restart(store, &dir).read_committed().await.unwrap(),
        Some(log_id(2, 9))
    );
}

#[tokio::test]
async fn truncate_removes_the_given_index_and_everything_after() {
    let (mut store, _dir) = store();
    store
        .blocking_append((1..=5).map(|index| entry(1, index)).collect::<Vec<_>>())
        .await
        .unwrap();

    store.truncate(log_id(1, 3)).await.unwrap();

    assert_eq!(
        indices(&mut store).await,
        vec![1, 2],
        "truncate is inclusive: index 3 conflicted and is being replaced"
    );
}

#[tokio::test]
async fn purge_removes_up_to_and_including_the_given_index() {
    let (mut store, _dir) = store();
    store
        .blocking_append((1..=5).map(|index| entry(1, index)).collect::<Vec<_>>())
        .await
        .unwrap();

    store.purge(log_id(1, 2)).await.unwrap();

    assert_eq!(indices(&mut store).await, vec![3, 4, 5]);
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 2)));
    assert_eq!(state.last_log_id, Some(log_id(1, 5)));
}

#[tokio::test]
async fn a_fully_purged_log_reports_the_purge_watermark_as_its_last_id() {
    // The subtle part of the trait's contract. With no entries left,
    // `last_log_id` must be the purge watermark rather than `None`, or Raft
    // concludes the log restarted from nothing and replays from index 0.
    let (mut store, dir) = store();
    store
        .blocking_append((1..=3).map(|index| entry(1, index)).collect::<Vec<_>>())
        .await
        .unwrap();

    store.purge(log_id(1, 3)).await.unwrap();

    // True immediately, and still true after a restart — the watermark is
    // persisted, not just remembered.
    let mut restarted = restart(store, &dir);
    let state = restarted.get_log_state().await.unwrap();

    assert!(indices(&mut restarted).await.is_empty());
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 3)));
    assert_eq!(
        state.last_log_id,
        Some(log_id(1, 3)),
        "an empty log still remembers how far it was purged"
    );
}

#[tokio::test]
async fn a_second_handle_to_the_same_file_is_refused() {
    // redb takes an exclusive lock, which is what we want: two LogStores on one
    // file would be two Raft logs disagreeing about the same node's state.
    let (_store, dir) = store();

    assert!(
        LogStore::open(dir.path().join("raft.redb")).is_err(),
        "a node\'s log must not be openable twice"
    );
}

#[tokio::test]
async fn a_reader_sees_entries_appended_after_it_was_taken() {
    // Replication tasks hold a reader while the leader keeps appending. A reader
    // pinned to one snapshot would silently stop replicating new entries.
    let (mut store, _dir) = store();
    let mut reader = store.get_log_reader().await;

    store.blocking_append(vec![entry(1, 1)]).await.unwrap();

    assert_eq!(indices(&mut reader).await, vec![1]);
}

#[tokio::test]
async fn two_stores_can_share_one_database() {
    // The log and, from the next PR, the state machine live in one file so a
    // node's whole raft state is a single thing to back up or move.
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("shared.redb")).unwrap());

    let mut writer = LogStore::from_database(Arc::clone(&db)).unwrap();
    let mut other = LogStore::from_database(db).unwrap();

    writer.blocking_append(vec![entry(1, 1)]).await.unwrap();

    assert_eq!(indices(&mut other).await, vec![1]);
}
