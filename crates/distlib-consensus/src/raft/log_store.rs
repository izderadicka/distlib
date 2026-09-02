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
//! that becomes true, so every write commits before it returns or reports.
//!
//! That commit fsyncs, which is why the write paths run on the blocking pool
//! rather than inline. They are called from openraft's core loop, and an fsync
//! that takes tens of milliseconds on a loaded disk would stall Raft processing
//! for its duration — which the `append` docs ask implementations to avoid.

// Every signature here is fixed by openraft's traits, and `StorageError` is its
// type: 280 bytes, which clippy objects to and we cannot box without breaking
// the impls. Not ours to fix — the same call the `figment::Jail` closures got.
#![allow(clippy::result_large_err)]

use std::{fmt::Debug, ops::RangeBounds, sync::Arc};

use openraft::{
    ErrorSubject, LogId, LogState, RaftLogReader, Vote,
    entry::EntryPayload,
    storage::{LogFlushed, RaftLogStorage},
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::{
    raft::{
        db::{KeyValueTable, NodeId, StorageResult, ensure_tables, reading, write_txn, writing},
        types::TypeConfig,
    },
    signed::SignedEvent,
};

/// Log entries, keyed by index.
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");

/// Everything else Raft persists, under the fixed keys below.
const META: KeyValueTable = TableDefinition::new("raft_meta");

const VOTE: &str = "vote";
const COMMITTED: &str = "committed";
const LAST_PURGED: &str = "last_purged";

type Entry = openraft::impls::Entry<TypeConfig>;

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
        let fail = writing(ErrorSubject::Store);
        let db = Database::create(path).map_err(|source| fail(&source))?;
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
    /// one.
    fn ensure_tables(&self) -> StorageResult<()> {
        ensure_tables(&self.db, ErrorSubject::Store, |txn| {
            let fail = writing(ErrorSubject::Store);
            txn.open_table(LOG).map_err(|source| fail(&source))?;
            txn.open_table(META).map_err(|source| fail(&source))?;
            Ok(())
        })
    }

    fn read_meta<T>(&self, key: &str, subject: ErrorSubject<NodeId>) -> StorageResult<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let fail = reading(subject);
        let txn = self.db.begin_read().map_err(|source| fail(&source))?;
        let table = txn.open_table(META).map_err(|source| fail(&source))?;
        let Some(value) = table.get(key).map_err(|source| fail(&source))? else {
            return Ok(None);
        };
        Ok(Some(
            postcard::from_bytes(value.value()).map_err(|source| fail(&source))?,
        ))
    }

    async fn write_meta<T>(
        &self,
        key: &'static str,
        value: &T,
        subject: ErrorSubject<NodeId>,
    ) -> StorageResult<()>
    where
        T: serde::Serialize,
    {
        let fail = writing(subject.clone());
        let encoded = postcard::to_stdvec(value).map_err(|source| fail(&source))?;

        write_txn(&self.db, subject.clone(), move |txn| {
            let fail = writing(subject);
            let mut table = txn.open_table(META).map_err(|source| fail(&source))?;
            table
                .insert(key, encoded.as_slice())
                .map_err(|source| fail(&source))?;
            Ok(())
        })
        .await
    }

    /// The id of the last entry still present, ignoring anything purged.
    /// The membership events in `(after, up_to]`, with their log indices.
    ///
    /// The serving half of `distlib/memberlog/0`. Raft's blank entries are left
    /// out — they carry no membership event, and a follower folds events rather
    /// than replaying Raft — which is why the caller is given `up_to`
    /// separately rather than inferring a cursor from the last index returned.
    ///
    /// `up_to` is the caller's business, not this method's: only entries the
    /// state machine has *applied* may be served, and the log does not know
    /// what has been applied.
    pub fn events_after(&self, after: u64, up_to: u64) -> StorageResult<Vec<(u64, SignedEvent)>> {
        let fail = reading(ErrorSubject::Logs);
        let txn = self.db.begin_read().map_err(|source| fail(&source))?;
        let table = txn.open_table(LOG).map_err(|source| fail(&source))?;

        table
            .range(after.saturating_add(1)..=up_to)
            .map_err(|source| fail(&source))?
            .map(|row| {
                let (index, value) = row.map_err(|source| fail(&source))?;
                let entry: Entry =
                    postcard::from_bytes(value.value()).map_err(|source| fail(&source))?;
                Ok(match entry.payload {
                    EntryPayload::Normal(event) => Some((index.value(), event)),
                    _ => None,
                })
            })
            .filter_map(Result::transpose)
            .collect()
    }

    /// The lowest index this log can still serve.
    ///
    /// Everything below has been purged after a snapshot, so a follower asking
    /// from further back cannot be caught up from entries alone.
    pub fn first_available(&self) -> StorageResult<u64> {
        let purged: Option<LogId<NodeId>> =
            self.read_meta(LAST_PURGED, ErrorSubject::Logs)?.flatten();
        Ok(purged.map_or(1, |log_id| log_id.index.saturating_add(1)))
    }

    fn last_log_id(&self) -> StorageResult<Option<LogId<NodeId>>> {
        let fail = reading(ErrorSubject::Logs);
        let txn = self.db.begin_read().map_err(|source| fail(&source))?;
        let table = txn.open_table(LOG).map_err(|source| fail(&source))?;
        let Some((_, value)) = table.last().map_err(|source| fail(&source))? else {
            return Ok(None);
        };
        let entry: Entry = postcard::from_bytes(value.value()).map_err(|source| fail(&source))?;
        Ok(Some(entry.log_id))
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry>> {
        let fail = reading(ErrorSubject::Logs);
        let txn = self.db.begin_read().map_err(|source| fail(&source))?;
        let table = txn.open_table(LOG).map_err(|source| fail(&source))?;

        table
            .range(range)
            .map_err(|source| fail(&source))?
            .map(|row| {
                let (_, value) = row.map_err(|source| fail(&source))?;
                postcard::from_bytes(value.value()).map_err(|source| fail(&source))
            })
            .collect()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last_purged_log_id: Option<LogId<NodeId>> =
            self.read_meta(LAST_PURGED, ErrorSubject::Logs)?.flatten();
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
        // Stored bare: a missing key in META already means "no vote saved", so
        // wrapping it in an `Option` would only add a layer to peel back off.
        self.write_meta(VOTE, vote, ErrorSubject::Vote).await
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        self.read_meta(VOTE, ErrorSubject::Vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> StorageResult<()> {
        // Here the `Option` is the caller's own, so it is stored as given. The
        // subject stays `Logs`: this is a pointer into the log, not a vote.
        self.write_meta(COMMITTED, &committed, ErrorSubject::Logs)
            .await
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self.read_meta(COMMITTED, ErrorSubject::Logs)?.flatten())
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let fail = writing(ErrorSubject::Logs);
        // Encoded up front so only the I/O crosses onto the blocking pool, and
        // so a borrowed iterator does not have to outlive this call.
        let encoded: Vec<(u64, Vec<u8>)> = entries
            .into_iter()
            .map(|entry| {
                let bytes = postcard::to_stdvec(&entry).map_err(|source| fail(&source))?;
                Ok((entry.log_id.index, bytes))
            })
            .collect::<StorageResult<_>>()?;

        write_txn(&self.db, ErrorSubject::Logs, move |txn| {
            let fail = writing(ErrorSubject::Logs);
            let mut table = txn.open_table(LOG).map_err(|source| fail(&source))?;
            for (index, bytes) in encoded {
                table
                    .insert(index, bytes.as_slice())
                    .map_err(|source| fail(&source))?;
            }
            Ok(())
        })
        .await?;

        // Only now are the entries on disk, which is what the callback promises.
        // Signalling before the commit would let Raft treat unflushed entries as
        // durable and acknowledge a write it could still lose.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        write_txn(&self.db, ErrorSubject::Logs, move |txn| {
            let fail = writing(ErrorSubject::Logs);
            let mut table = txn.open_table(LOG).map_err(|source| fail(&source))?;
            // Inclusive of `log_id`: the entry at that index is a conflicting one
            // being replaced, not one being kept. Bounded to the affected range,
            // because plain `retain` walks the whole table to delete a handful.
            table
                .retain_in(log_id.index.., |_, _| false)
                .map_err(|source| fail(&source))?;
            Ok(())
        })
        .await
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        let fail = writing(ErrorSubject::Logs);
        let watermark = postcard::to_stdvec(&Some(log_id)).map_err(|source| fail(&source))?;

        // Entries and watermark in one transaction. Split across two commits, a
        // crash in between would leave the entries gone but the watermark still
        // behind them, and `get_log_state` would then advertise a range with a
        // hole in it — the one thing the trait says must never happen. One
        // commit is also one fsync rather than two, and purge runs after every
        // snapshot.
        write_txn(&self.db, ErrorSubject::Logs, move |txn| {
            let fail = writing(ErrorSubject::Logs);
            let mut table = txn.open_table(LOG).map_err(|source| fail(&source))?;
            table
                .retain_in(..=log_id.index, |_, _| false)
                .map_err(|source| fail(&source))?;

            let mut meta = txn.open_table(META).map_err(|source| fail(&source))?;
            meta.insert(LAST_PURGED, watermark.as_slice())
                .map_err(|source| fail(&source))?;
            Ok(())
        })
        .await
    }
}
