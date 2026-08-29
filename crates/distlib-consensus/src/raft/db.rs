//! Shared redb plumbing for the log store and the state machine.
//!
//! Both live in one database file, so a node's whole Raft state is a single
//! thing to back up or move, and both need the same two things: failures
//! reported against the right subsystem, and writes that keep redb's fsync off
//! the async workers running openraft's core loop.
//!
//! One file has a cost worth naming: redb allows a single writer at a time, so
//! `begin_write` blocks while the other store is committing. openraft runs log
//! I/O and apply on separate tasks precisely so their fsyncs can overlap, and
//! sharing a database serialises them. That is the right trade here — the
//! membership log is small and changes rarely (§4.5 calls its snapshots
//! trivial), so commit latency is not the constraint — but it would not be for
//! a high-throughput log, and the fix then is a second database rather than
//! anything in this file.

// Signatures here are shaped by openraft's traits, and `StorageError` is its
// 280-byte type, which cannot be boxed without breaking the impls.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use openraft::{ErrorSubject, ErrorVerb, StorageError, StorageIOError};
use redb::{Database, ReadableDatabase, TableDefinition, WriteTransaction};

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

/// A table of postcard-encoded values under fixed string keys.
///
/// Both stores keep their bookkeeping in one of these — Raft's vote and log
/// pointers, the applied state and the current snapshot — so the read and write
/// helpers below serve all of it.
pub(crate) type KeyValueTable = TableDefinition<'static, &'static str, &'static [u8]>;

/// Creates whatever tables `open` touches, so later read transactions cannot
/// fail on a missing one — redb only creates a table when it is opened for
/// writing.
///
/// Takes a closure rather than a list because the two stores keep tables of
/// different key types; opening them is the only thing they need in common.
///
/// Synchronous: this runs at startup, not on the Raft path.
pub(crate) fn ensure_tables<F>(
    db: &Database,
    subject: ErrorSubject<NodeId>,
    open: F,
) -> StorageResult<()>
where
    F: FnOnce(&WriteTransaction) -> StorageResult<()>,
{
    let fail = writing(subject);
    let txn = db.begin_write().map_err(|source| fail(&source))?;
    open(&txn)?;
    txn.commit().map_err(|source| fail(&source))?;
    Ok(())
}

/// Reads and decodes one key, if present.
pub(crate) fn read_key<T>(
    db: &Database,
    table: KeyValueTable,
    key: &str,
    subject: ErrorSubject<NodeId>,
) -> StorageResult<Option<T>>
where
    T: serde::de::DeserializeOwned,
{
    let fail = reading(subject);
    let txn = db.begin_read().map_err(|source| fail(&source))?;
    let table = txn.open_table(table).map_err(|source| fail(&source))?;
    let Some(value) = table.get(key).map_err(|source| fail(&source))? else {
        return Ok(None);
    };
    Ok(Some(
        postcard::from_bytes(value.value()).map_err(|source| fail(&source))?,
    ))
}

/// Writes one already-encoded key, committing off the async workers.
pub(crate) async fn write_key(
    db: &Arc<Database>,
    table: KeyValueTable,
    key: &'static str,
    bytes: Vec<u8>,
    subject: ErrorSubject<NodeId>,
) -> StorageResult<()> {
    let insert_subject = subject.clone();
    write_txn(db, subject, move |txn| {
        let fail = writing(insert_subject);
        let mut table = txn.open_table(table).map_err(|source| fail(&source))?;
        table
            .insert(key, bytes.as_slice())
            .map_err(|source| fail(&source))?;
        Ok(())
    })
    .await
}

/// Encodes a value for storage.
pub(crate) fn encode<T: serde::Serialize>(
    value: &T,
    subject: ErrorSubject<NodeId>,
) -> StorageResult<Vec<u8>> {
    postcard::to_stdvec(value).map_err(|source| writing(subject)(&source))
}
