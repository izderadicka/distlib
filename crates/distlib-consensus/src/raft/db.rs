//! Shared redb plumbing for the log store and the state machine.
//!
//! Both live in one database file, so a node's whole Raft state is a single
//! thing to back up or move, and both need the same two things: failures
//! reported against the right subsystem, and writes that keep redb's fsync off
//! the async workers running openraft's core loop.

// Signatures here are shaped by openraft's traits, and `StorageError` is its
// 280-byte type, which cannot be boxed without breaking the impls.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use openraft::{ErrorSubject, ErrorVerb, StorageError, StorageIOError};
use redb::{Database, WriteTransaction};

use crate::raft::types::TypeConfig;

pub(crate) type NodeId = <TypeConfig as openraft::RaftTypeConfig>::NodeId;
pub(crate) type StorageResult<T> = Result<T, StorageError<NodeId>>;

/// Reports a failure against the part of the store it came from.
///
/// The subject travels into the fatal error a node dies with, so a disk failure
/// while saving a vote has to say "vote" rather than pointing whoever debugs it
/// at the log.
fn failing(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
) -> impl Fn(&(dyn std::error::Error + 'static)) -> StorageError<NodeId> {
    // `from_dyn` rather than `new`: the concrete type is erased here so one
    // closure can report failures from several redb error types at a call site.
    move |source| {
        StorageIOError::new(
            subject.clone(),
            verb,
            anyerror::AnyError::from_dyn(source, None),
        )
        .into()
    }
}

pub(crate) fn writing(
    subject: ErrorSubject<NodeId>,
) -> impl Fn(&(dyn std::error::Error + 'static)) -> StorageError<NodeId> {
    failing(subject, ErrorVerb::Write)
}

pub(crate) fn reading(
    subject: ErrorSubject<NodeId>,
) -> impl Fn(&(dyn std::error::Error + 'static)) -> StorageError<NodeId> {
    failing(subject, ErrorVerb::Read)
}

/// Runs `f` in a write transaction on the blocking pool, then commits.
///
/// The commit is an fsync, and this is what keeps it off the async worker
/// running openraft's core loop. Everything `f` needs is moved in, so nothing
/// is borrowed across the await.
pub(crate) async fn write_txn<F>(
    db: &Arc<Database>,
    subject: ErrorSubject<NodeId>,
    f: F,
) -> StorageResult<()>
where
    F: FnOnce(&WriteTransaction) -> StorageResult<()> + Send + 'static,
{
    let db = Arc::clone(db);
    let joining = writing(subject.clone());

    tokio::task::spawn_blocking(move || {
        let fail = writing(subject);
        let txn = db.begin_write().map_err(|source| fail(&source))?;
        f(&txn)?;
        txn.commit().map_err(|source| fail(&source))
    })
    .await
    .map_err(|source| joining(&source))?
}
