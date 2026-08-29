//! The Raft log, persisted in redb.
//!
//! Two tables and nothing clever. The log is a `u64 -> postcard(Entry)` map, and
//! everything else Raft needs to remember across a restart — its vote, the
//! committed pointer, the last purged id — lives in a small metadata table under
//! fixed keys.
//!
//! Durability is what this file exists for. openraft's contract is explicit that
//! a vote must be on disk before `save_vote` returns and that entries must be on
//! disk before the `append` callback fires; redb's `commit()` is the point where
//! that becomes true, so every method here commits before it returns or reports.

// Every signature here is fixed by openraft's traits, and `StorageError` is its
// type: 280 bytes, which clippy objects to and we cannot box without breaking
// the impls. Not ours to fix — the same call the `figment::Jail` closures got.
#![allow(clippy::result_large_err)]

use std::{fmt::Debug, ops::RangeBounds, sync::Arc};

use openraft::{
    LogId, LogState, RaftLogReader, StorageError, StorageIOError, Vote,
    storage::{LogFlushed, RaftLogStorage},
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::raft::types::TypeConfig;

/// Log entries, keyed by index.
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");

/// Everything else Raft persists, under the fixed keys below.
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const VOTE: &str = "vote";
const COMMITTED: &str = "committed";
const LAST_PURGED: &str = "last_purged";

type Entry = openraft::impls::Entry<TypeConfig>;
type NodeId = <TypeConfig as openraft::RaftTypeConfig>::NodeId;
type StorageResult<T> = Result<T, StorageError<NodeId>>;

/// A Raft log stored in a redb database.
///
/// Cheap to clone — clones share one database handle. openraft asks for a
/// separate reader via [`RaftLogStorage::get_log_reader`] and uses it from
/// replication tasks concurrently with writes, which redb's MVCC handles
/// directly, so the reader is just another clone rather than a second type.
#[derive(Debug, Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    /// Opens (or creates) the log at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> StorageResult<Self> {
        let db = Database::create(path).map_err(|source| StorageIOError::write_logs(&source))?;
        let store = Self { db: Arc::new(db) };
        store.ensure_tables()?;
        Ok(store)
    }

    /// Shares an already-open database.
    pub fn from_database(db: Arc<Database>) -> StorageResult<Self> {
        let store = Self { db };
        store.ensure_tables()?;
        Ok(store)
    }

    /// Creates both tables so later read transactions cannot fail on a missing
    /// one — redb only creates a table when it is opened for writing.
    fn ensure_tables(&self) -> StorageResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|source| StorageIOError::write_logs(&source))?;
        {
            txn.open_table(LOG)
                .map_err(|source| StorageIOError::write_logs(&source))?;
            txn.open_table(META)
                .map_err(|source| StorageIOError::write_logs(&source))?;
        }
        txn.commit()
            .map_err(|source| StorageIOError::write_logs(&source))?;
        Ok(())
    }

    fn read_meta<T>(&self, key: &str) -> StorageResult<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let txn = self
            .db
            .begin_read()
            .map_err(|source| StorageIOError::read_logs(&source))?;
        let table = txn
            .open_table(META)
            .map_err(|source| StorageIOError::read_logs(&source))?;
        let Some(value) = table
            .get(key)
            .map_err(|source| StorageIOError::read_logs(&source))?
        else {
            return Ok(None);
        };
        let decoded = postcard::from_bytes(value.value())
            .map_err(|source| StorageIOError::read_logs(&source))?;
        Ok(Some(decoded))
    }

    fn write_meta<T>(&self, key: &str, value: &T) -> StorageResult<()>
    where
        T: serde::Serialize,
    {
        let encoded =
            postcard::to_stdvec(value).map_err(|source| StorageIOError::write_logs(&source))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|source| StorageIOError::write_logs(&source))?;
        {
            let mut table = txn
                .open_table(META)
                .map_err(|source| StorageIOError::write_logs(&source))?;
            table
                .insert(key, encoded.as_slice())
                .map_err(|source| StorageIOError::write_logs(&source))?;
        }
        // The commit is the durability point openraft's contract turns on.
        txn.commit()
            .map_err(|source| StorageIOError::write_logs(&source))?;
        Ok(())
    }

    /// The id of the last entry still present, ignoring anything purged.
    fn last_log_id(&self) -> StorageResult<Option<LogId<NodeId>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|source| StorageIOError::read_logs(&source))?;
        let table = txn
            .open_table(LOG)
            .map_err(|source| StorageIOError::read_logs(&source))?;
        let last = table
            .last()
            .map_err(|source| StorageIOError::read_logs(&source))?;
        let Some((_, value)) = last else {
            return Ok(None);
        };
        let entry: Entry = postcard::from_bytes(value.value())
            .map_err(|source| StorageIOError::read_logs(&source))?;
        Ok(Some(entry.log_id))
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|source| StorageIOError::read_logs(&source))?;
        let table = txn
            .open_table(LOG)
            .map_err(|source| StorageIOError::read_logs(&source))?;

        table
            .range(range)
            .map_err(|source| StorageIOError::read_logs(&source))?
            .map(|row| {
                let (_, value) = row.map_err(|source| StorageIOError::read_logs(&source))?;
                postcard::from_bytes(value.value())
                    .map_err(|source| StorageIOError::read_logs(&source).into())
            })
            .collect()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last_purged_log_id: Option<LogId<NodeId>> = self.read_meta(LAST_PURGED)?.flatten();
        // Per the trait: with no entries present, `last_log_id` is the purge
        // watermark rather than `None`, or Raft would believe the log restarted
        // from nothing after a full purge.
        let last_log_id = self.last_log_id()?.or(last_purged_log_id);

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.write_meta(VOTE, &Some(vote))
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self.read_meta(VOTE)?.flatten())
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> StorageResult<()> {
        self.write_meta(COMMITTED, &committed)
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self.read_meta(COMMITTED)?.flatten())
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let txn = self
            .db
            .begin_write()
            .map_err(|source| StorageIOError::write_logs(&source))?;
        {
            let mut table = txn
                .open_table(LOG)
                .map_err(|source| StorageIOError::write_logs(&source))?;
            for entry in entries {
                let encoded = postcard::to_stdvec(&entry)
                    .map_err(|source| StorageIOError::write_logs(&source))?;
                table
                    .insert(entry.log_id.index, encoded.as_slice())
                    .map_err(|source| StorageIOError::write_logs(&source))?;
            }
        }
        txn.commit()
            .map_err(|source| StorageIOError::write_logs(&source))?;

        // Only now are the entries on disk, which is what the callback promises.
        // Signalling before the commit would let Raft treat unflushed entries as
        // durable and acknowledge a write it could still lose.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|source| StorageIOError::write_logs(&source))?;
        {
            let mut table = txn
                .open_table(LOG)
                .map_err(|source| StorageIOError::write_logs(&source))?;
            // Inclusive of `log_id`: the entry at that index is a conflicting
            // one being replaced, not one being kept.
            table
                .retain(|index, _| index < log_id.index)
                .map_err(|source| StorageIOError::write_logs(&source))?;
        }
        txn.commit()
            .map_err(|source| StorageIOError::write_logs(&source))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|source| StorageIOError::write_logs(&source))?;
        {
            let mut table = txn
                .open_table(LOG)
                .map_err(|source| StorageIOError::write_logs(&source))?;
            table
                .retain(|index, _| index > log_id.index)
                .map_err(|source| StorageIOError::write_logs(&source))?;
        }
        txn.commit()
            .map_err(|source| StorageIOError::write_logs(&source))?;

        // Recorded after the entries are gone, so a crash in between leaves the
        // watermark behind the data rather than ahead of it: re-purging is
        // harmless, whereas claiming to have purged what is still present is not.
        self.write_meta(LAST_PURGED, &Some(log_id))
    }
}
