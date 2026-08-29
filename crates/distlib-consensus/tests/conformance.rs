//! openraft's own storage conformance suite, run against our redb stores.
//!
//! This is the bar the hand-written tests in `log_store.rs` and
//! `state_machine.rs` are not a substitute for. `Suite::test_store` runs 34
//! cases over the storage contract — log id bookkeeping, membership recovered
//! from log versus state machine, purge and truncate boundaries, vote
//! durability, snapshot install — written by the people who wrote the traits,
//! against the semantics they actually meant.
//!
//! It could not run in the log-store PR because it exercises a
//! `RaftLogStorage` and a `RaftStateMachine` together, and the state machine
//! did not exist yet.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point
#![allow(clippy::result_large_err)] // openraft's StorageError, in its own signatures

use std::sync::Arc;

use distlib_consensus::{LogStore, StateMachineStore, TypeConfig};
use distlib_core::RawMemberId;
use openraft::{
    StorageError,
    testing::{StoreBuilder, Suite},
};
use redb::Database;
use tempfile::TempDir;

/// Builds a fresh pair of stores per test case.
///
/// Both share one database file, which is how a real node runs — a node's whole
/// Raft state is one thing to back up or move — so the suite exercises that
/// arrangement rather than a convenient fiction.
struct RedbStores;

impl StoreBuilder<TypeConfig, LogStore, StateMachineStore, TempDir> for RedbStores {
    async fn build(
        &self,
    ) -> Result<(TempDir, LogStore, StateMachineStore), StorageError<RawMemberId>> {
        let dir = TempDir::new().expect("could not create a temporary directory");
        let db = Arc::new(
            Database::create(dir.path().join("raft.redb")).expect("could not create the database"),
        );

        let log = LogStore::from_database(Arc::clone(&db))?;
        let state_machine = StateMachineStore::from_database(db)?;

        // The TempDir is returned so it outlives the stores; dropping it would
        // delete the file out from under them mid-test.
        Ok((dir, log, state_machine))
    }
}

#[test]
fn openraft_storage_conformance_suite() {
    Suite::test_all(RedbStores).unwrap();
}
