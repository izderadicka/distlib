//! How openraft is parameterised for the membership log.

// `declare_raft_types!` expands its default `SnapshotData = Cursor<Vec<u8>>`
// unqualified, so the type has to be in scope here even though we never name it.
use std::io::Cursor;

use distlib_core::{NodeAddr, RawMemberId};

use crate::{error::ConsensusError, signed::SignedEvent};

openraft::declare_raft_types!(
    /// The membership log's Raft configuration.
    ///
    /// `D = SignedEvent` — the log's payload is one signed membership event, so
    /// the thing Raft replicates and the thing the projection folds are the same
    /// value, with no wrapper in between.
    ///
    /// `R = Result<(), ConsensusError>` — the verdict the state machine reached.
    /// Committing an entry and applying it are different things here: a
    /// committed event whose rules do not hold is skipped rather than fatal
    /// (P1-8), so a write that returns `Ok` says only that the entry is in the
    /// log. Carrying the verdict is what lets a proposer learn that the thing
    /// they asked for did not actually happen.
    ///
    /// `NodeId = RawMemberId` rather than `MemberId`, because openraft requires
    /// `Default`. See [`RawMemberId`] for why that is not solved by giving
    /// `MemberId` one.
    ///
    /// `Entry` and `SnapshotData` are left at their defaults —
    /// `openraft::impls::Entry<Self>` and `Cursor<Vec<u8>>`. The cursor is
    /// exactly right here: §4.5 calls snapshots trivial because the state is the
    /// whole membership table, which is small enough to hold in memory, and it
    /// already satisfies the `AsyncRead + AsyncWrite + AsyncSeek` bounds.
    pub TypeConfig:
        D = SignedEvent,
        R = Result<(), ConsensusError>,
        NodeId = RawMemberId,
        Node = NodeAddr,
);
