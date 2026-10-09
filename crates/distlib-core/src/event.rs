//! What a node tells whoever is watching it: §7.2's event stream.
//!
//! **Ids, never values** (phase 3's D2). An event says *what* changed, and a
//! page that cares asks the API for the new state. That is one extra round
//! trip against a loopback listener, and it buys three things: a new event
//! type cannot break a page written before it, a page that missed events
//! recovers by the same refetch as everything else, and nothing here has to
//! decide how much of a record is worth sending.
//!
//! Here rather than in `distlib-api` because the producers are elsewhere — the
//! read model's projection will publish catalogue events, and it sits below the
//! API in the dependency graph.

use serde::{Deserialize, Serialize};

use crate::{ItemId, MemberId};

/// One thing a watcher may want to refetch.
///
/// Serialised with its name as `type`, so an event's data reads on its own
/// without the SSE frame around it — and read back by `distlib download`,
/// which watches the stream for its download's ending.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    /// The membership changed: a member came or went, a proposal was made,
    /// approved or withdrawn, a pledge or the core group moved.
    ///
    /// No payload. The membership is one thing, so there is nothing to name —
    /// `group.members` and `group.pending` are what to ask.
    #[serde(rename = "membership.changed")]
    MembershipChanged,

    /// The catalogue's swarm changed: a neighbour came or went, or a sync round
    /// with a peer finished (C14).
    ///
    /// No payload, for the same reason — `node.status` says what it looks like
    /// now. Fires with every sync round, so a quiet group still sends one now
    /// and then.
    #[serde(rename = "sync.status")]
    SyncStatus,

    /// An item this node's read model did not hold is now in it — searchable
    /// and readable, since it is published only after both are committed.
    #[serde(rename = "catalogue.item_added")]
    ItemAdded { item_id: ItemId },

    /// An item this node already held was re-read and written again: a field
    /// changed, a file was contributed, or content it was waiting for arrived.
    ///
    /// Occasionally about a write that changed nothing a page shows. The read
    /// model re-reads whole items rather than diffing them, and a spare
    /// refetch is the whole cost of that.
    #[serde(rename = "catalogue.item_changed")]
    ItemChanged { item_id: ItemId },

    /// What this node can say about one member's availability changed: it
    /// came online or went offline, or what it holds changed, or became known.
    ///
    /// The member, not the items: one member going offline touches every item
    /// it holds, thousands of ids in one event, so a page refetches what it
    /// shows instead (phase 4's D11).
    #[serde(rename = "availability.changed")]
    AvailabilityChanged { member_id: MemberId },

    /// How far a download has got, in bytes and in files, across all of the
    /// files it is fetching.
    ///
    /// **The one event that carries values**, against the rule above. Progress
    /// *is* the news, and a page that refetched on every tick would be asking
    /// several times a second for what the event could simply have said. The
    /// byte count is not monotonic — a provider failover starts a file again —
    /// so a page keeps its own high-water mark.
    #[serde(rename = "download.progress")]
    DownloadProgress {
        task_id: TaskId,
        item_id: ItemId,
        #[serde(flatten)]
        progress: Progress,
    },

    /// A download finished: every file is written. `library.task` says where.
    #[serde(rename = "download.finished")]
    DownloadFinished { task_id: TaskId, item_id: ItemId },

    /// A download failed. `library.task` says why.
    #[serde(rename = "download.failed")]
    DownloadFailed { task_id: TaskId, item_id: ItemId },
}

/// How far a download has got.
///
/// Files as well as bytes, because they answer different questions: bytes
/// say how long is left, files say how much can already be opened — a
/// written file is complete, where half the bytes of an audiobook may be no
/// chapter at all.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub bytes_done: u64,
    pub bytes_total: u64,
    /// Files written to their destination.
    pub files_done: u64,
    pub files_total: u64,
}

/// Names one piece of work in progress on this node — a download, today.
///
/// Counted from one when the node starts, and meaningful only to the node
/// that handed it out: nothing outlives a restart, so neither does the id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub u64);

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Event {
    /// The name §7.2 gives it, which is also its SSE `event:` field.
    pub fn name(&self) -> &'static str {
        match self {
            Self::MembershipChanged => "membership.changed",
            Self::SyncStatus => "sync.status",
            Self::ItemAdded { .. } => "catalogue.item_added",
            Self::ItemChanged { .. } => "catalogue.item_changed",
            Self::AvailabilityChanged { .. } => "availability.changed",
            Self::DownloadProgress { .. } => "download.progress",
            Self::DownloadFinished { .. } => "download.finished",
            Self::DownloadFailed { .. } => "download.failed",
        }
    }
}
