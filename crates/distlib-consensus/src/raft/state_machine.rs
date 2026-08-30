//! The Raft state machine: committed events folded into the membership.
//!
//! Thin by design. All the rules live in [`MembershipState`], which is a pure
//! function of the log; this file is about persisting the result and satisfying
//! openraft's contract around snapshots.
//!
//! It takes the "persistent state machine" option openraft offers: `apply`
//! writes the new state to disk before returning, so a snapshot is a
//! convenience for catching peers up rather than the thing recovery depends on.
//! For a membership table that is small (§4.5 calls snapshots trivial), paying
//! one commit per apply is cheaper to reason about than replaying a log from
//! the last snapshot on every restart.

// Signatures here are shaped by openraft's traits, and `StorageError` is its
// 280-byte type, which cannot be boxed without breaking the impls.
#![allow(clippy::result_large_err)]

use std::{
    io::Cursor,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use openraft::{
    EntryPayload, ErrorSubject, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StoredMembership, storage::RaftStateMachine,
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{
    error::ConsensusError,
    raft::{
        db::{
            KeyValueTable, NodeId, StorageResult, encode, ensure_tables, read_key, reading,
            write_key, write_txn, writing,
        },
        types::{NodeAddr, TypeConfig},
    },
    state::MembershipState,
};

/// State machine storage, keyed by the constants below.
const SM: KeyValueTable = TableDefinition::new("raft_state_machine");

const APPLIED: &str = "applied";
const SNAPSHOT: &str = "snapshot";

type Entry = openraft::impls::Entry<TypeConfig>;

/// Everything one node has applied.
///
/// Serialised as a unit, so the membership, the projection and the log id they
/// correspond to can never be written out of step with each other.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Applied {
    last_applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, NodeAddr>,
    state: MembershipState,
}

/// A snapshot as stored: its metadata and the bytes openraft streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, NodeAddr>,
    data: Vec<u8>,
}

/// Everything a state machine handle shares with its clones.
///
/// openraft clones the store freely, so one `Arc` around the lot means a clone
/// is a single refcount bump and adding shared state later does not add another
/// pointer to keep in step.
#[derive(Debug)]
struct Inner {
    /// Nested rather than flattened, because the database outlives this store:
    /// it is handed in already shared with [`crate::LogStore`], and cloned out
    /// again for each [`SnapshotBuilder`]. The other two fields belong to the
    /// state machine alone.
    db: Arc<Database>,
    applied: Mutex<Applied>,
    /// Distinguishes snapshots built at the same log id.
    snapshot_seq: AtomicU64,
    /// Announces the membership after every change.
    ///
    /// The point of the crate: a committed event has to reach the connection
    /// allowlist, and a watch channel is how that happens without the state
    /// machine knowing anything about the network. Kept here rather than
    /// published by the caller, so nothing can apply an entry and forget to say
    /// so.
    membership: watch::Sender<MembershipState>,
}

/// The membership state machine, persisted in redb.
#[derive(Debug, Clone)]
pub struct StateMachineStore {
    inner: Arc<Inner>,
}

impl StateMachineStore {
    /// Opens (or creates) the state machine in `db`, restoring what was applied.
    pub fn from_database(db: Arc<Database>) -> StorageResult<Self> {
        ensure_tables(&db, ErrorSubject::StateMachine, |txn| {
            let fail = writing(ErrorSubject::StateMachine);
            txn.open_table(SM).map_err(|source| fail(&source))?;
            Ok(())
        })?;

        let store = Self {
            inner: Arc::new(Inner {
                db,
                applied: Mutex::new(Applied::default()),
                snapshot_seq: AtomicU64::new(0),
                membership: watch::channel(MembershipState::new()).0,
            }),
        };
        if let Some(applied) =
            read_key::<Applied>(&store.inner.db, SM, APPLIED, ErrorSubject::StateMachine)?
        {
            store.inner.membership.send_replace(applied.state.clone());
            *store.lock() = applied;
        }
        Ok(store)
    }

    /// Opens (or creates) the state machine at `path`, in its own database.
    pub fn open(path: impl AsRef<std::path::Path>) -> StorageResult<Self> {
        let fail = writing(ErrorSubject::StateMachine);
        let db = Database::create(path).map_err(|source| fail(&source))?;
        Self::from_database(Arc::new(db))
    }

    /// The snapshot currently stored, if any.
    fn stored_snapshot(&self) -> StorageResult<Option<StoredSnapshot>> {
        read_key(&self.inner.db, SM, SNAPSHOT, ErrorSubject::Snapshot(None))
    }

    /// The membership derived from everything applied so far.
    ///
    /// The point of the whole crate: this is what feeds the connection
    /// allowlist, rather than anything in a config file.
    pub fn membership(&self) -> MembershipState {
        self.lock().state.clone()
    }

    /// Watches the membership, for whoever has to react to it changing.
    ///
    /// Each subscriber gets its own cursor, so a caller cannot miss a change by
    /// sharing a receiver with somebody else.
    pub fn subscribe(&self) -> watch::Receiver<MembershipState> {
        self.inner.membership.subscribe()
    }

    /// Publishes the membership if applying changed it.
    ///
    /// Compared rather than sent unconditionally, because most entries do not
    /// touch the membership — a `Blank` from a new leader, or Raft's own voter
    /// configuration — and a spurious notification would make every observer
    /// re-derive an allowlist that has not moved.
    fn announce(&self, state: &MembershipState) {
        if *self.inner.membership.borrow() != *state {
            self.inner.membership.send_replace(state.clone());
        }
    }

    /// A poison-tolerant lock.
    ///
    /// Nothing here panics while holding it — the guard covers in-memory
    /// mutation only, never I/O — so a poisoned lock means an unrelated panic
    /// elsewhere, and refusing to serve the state afterwards would turn that
    /// into a second failure.
    fn lock(&self) -> std::sync::MutexGuard<'_, Applied> {
        self.inner
            .applied
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> StorageResult<(Option<LogId<NodeId>>, StoredMembership<NodeId, NodeAddr>)> {
        let applied = self.lock();
        Ok((applied.last_applied, applied.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> StorageResult<Vec<Result<(), ConsensusError>>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        // The lock covers in-memory mutation and encoding only; the commit
        // happens after it is dropped, so no lock is ever held across an await.
        let (encoded, derived) = {
            let mut applied = self.lock();
            for entry in entries {
                applied.last_applied = Some(entry.log_id);

                match entry.payload {
                    // A no-op a new leader commits to establish its term.
                    EntryPayload::Blank => responses.push(Ok(())),

                    EntryPayload::Normal(event) => {
                        let verdict = applied.state.apply(&event);
                        if let Err(error) = &verdict {
                            // Raft has already committed this entry, so every
                            // node sees it and every node rejects it the same
                            // way — `MembershipState::apply` is deterministic
                            // and leaves the state untouched on error, so
                            // skipping keeps the cluster in agreement.
                            //
                            // Returning an error here instead would be a fatal
                            // storage failure on *every* node at once: one
                            // malformed proposal would take down the group.
                            // Proposals are validated before they are submitted;
                            // this is the backstop for one that should not have
                            // got through.
                            tracing::error!(
                                %error,
                                log_id = %entry.log_id,
                                "committed membership event rejected; skipping it"
                            );
                        }
                        // Reported back to whoever proposed it, so they learn
                        // the entry committed but did not take effect.
                        responses.push(verdict);
                    }

                    // Raft's own voter configuration. The trait asks only that
                    // it be stored, so it is stored.
                    EntryPayload::Membership(membership) => {
                        applied.membership = StoredMembership::new(Some(entry.log_id), membership);
                        responses.push(Ok(()));
                    }
                }
            }
            let encoded = encode(&*applied, ErrorSubject::StateMachine)?;
            (encoded, applied.state.clone())
        };

        write_key(
            &self.inner.db,
            SM,
            APPLIED,
            encoded,
            ErrorSubject::StateMachine,
        )
        .await?;

        // After the commit: an observer told about a membership the node could
        // still lose to a crash would be enforcing one that never happened.
        self.announce(&derived);
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // A copy taken now, so later applies cannot change what this builder
        // produces — which is what the trait asks for.
        SnapshotBuilder {
            db: Arc::clone(&self.inner.db),
            applied: self.lock().clone(),
            seq: self.inner.snapshot_seq.fetch_add(1, Ordering::Relaxed),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, NodeAddr>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StorageResult<()> {
        let subject = ErrorSubject::Snapshot(Some(meta.signature()));
        let data = snapshot.into_inner();
        let applied: Applied =
            postcard::from_bytes(&data).map_err(|source| reading(subject.clone())(&source))?;

        let stored = encode(
            &StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            },
            subject.clone(),
        )?;

        let membership = applied.state.clone();
        *self.lock() = applied;

        // Both keys in one transaction. The applied state and the snapshot it
        // came from cannot then disagree after a crash, and it is one fsync
        // rather than two. `data` is reused verbatim for APPLIED rather than
        // re-encoded: it decoded into `Applied` above, so the bytes already are
        // that value, and storing them twice from one source keeps the two keys
        // literally identical.
        let insert_subject = subject.clone();
        write_txn(&self.inner.db, subject, move |txn| {
            let fail = writing(insert_subject);
            let mut table = txn.open_table(SM).map_err(|source| fail(&source))?;
            table
                .insert(APPLIED, data.as_slice())
                .map_err(|source| fail(&source))?;
            table
                .insert(SNAPSHOT, stored.as_slice())
                .map_err(|source| fail(&source))?;
            Ok(())
        })
        .await?;

        // A snapshot replaces the membership wholesale, so observers need
        // telling just as much as after an apply — more so, because this is how
        // a node that fell behind far enough to be caught up by snapshot learns
        // about an expulsion. Without this the allowlist would keep admitting a
        // member the group removed while that node was away, until some later
        // entry happened to change the membership again.
        self.announce(&membership);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<TypeConfig>>> {
        Ok(self.stored_snapshot()?.map(|stored| Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        }))
    }
}

/// Builds a snapshot from a copy of the state taken when it was created.
#[derive(Debug)]
pub struct SnapshotBuilder {
    db: Arc<Database>,
    applied: Applied,
    seq: u64,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<TypeConfig>> {
        // Two snapshots can share a `last_log_id`, so the sequence number is
        // what keeps their ids distinct during a transfer.
        let snapshot_id = match self.applied.last_applied {
            Some(log_id) => format!("{}-{}-{}", log_id.leader_id, log_id.index, self.seq),
            None => format!("--{}", self.seq),
        };
        let meta = SnapshotMeta {
            last_log_id: self.applied.last_applied,
            last_membership: self.applied.membership.clone(),
            snapshot_id,
        };

        let subject = ErrorSubject::Snapshot(Some(meta.signature()));
        let data = encode(&self.applied, subject.clone())?;
        let stored = encode(
            &StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            },
            subject.clone(),
        )?;

        let ours = self.applied.last_applied;
        let insert_subject = subject.clone();
        write_txn(&self.db, subject, move |txn| {
            let fail = writing(insert_subject);
            let mut table = txn.open_table(SM).map_err(|source| fail(&source))?;

            // openraft spawns `build_snapshot` onto its own task while the state
            // machine worker keeps running, so a builder started at an older log
            // id can still be in flight when a newer snapshot is installed from
            // the leader. Overwriting blindly would move `get_current_snapshot`
            // *backwards* — and since openraft purges the log up to an installed
            // snapshot, the entries needed to bridge the gap are already gone,
            // leaving this node unable to catch anyone up.
            let newer_exists = {
                let existing = table.get(SNAPSHOT).map_err(|source| fail(&source))?;
                match existing {
                    Some(value) => {
                        let stored: StoredSnapshot =
                            postcard::from_bytes(value.value()).map_err(|source| fail(&source))?;
                        stored.meta.last_log_id > ours
                    }
                    None => false,
                }
            };
            if newer_exists {
                return Ok(());
            }

            table
                .insert(SNAPSHOT, stored.as_slice())
                .map_err(|source| fail(&source))?;
            Ok(())
        })
        .await?;

        // Returned regardless: openraft asked this builder for a snapshot and
        // gets the one it built. Only the *stored* current snapshot is held
        // back from going backwards.
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}
