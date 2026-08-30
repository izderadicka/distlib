//! How openraft is parameterised for the membership log.

// `declare_raft_types!` expands its default `SnapshotData = Cursor<Vec<u8>>`
// unqualified, so the type has to be in scope here even though we never name it.
use std::collections::BTreeSet;
use std::io::Cursor;
use std::net::SocketAddr;

use distlib_core::RawMemberId;
use serde::{Deserialize, Serialize};

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

/// Where to reach a node, carried in Raft's membership so the network layer has
/// something to dial.
///
/// Empty by default, which is a meaningful value rather than a placeholder: it
/// means "dial by member id alone and let iroh's address lookup find them",
/// which is what works when relays are enabled. The explicit addresses matter
/// for the cases where lookup is unavailable — a LAN, or `relay_mode =
/// "disabled"`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAddr {
    /// Relay to reach this node through, as a URL.
    ///
    /// A `String` rather than an `iroh::RelayUrl` because this is persisted in
    /// the Raft log and sent over the wire: it should deserialise into something
    /// inspectable even if it no longer parses, rather than failing the whole
    /// entry. Parsing happens where it is dialled.
    pub relay: Option<String>,

    /// Socket addresses to try directly.
    ///
    /// A set, not a list, for three reasons that all point the same way. These
    /// values are persisted in the Raft log and compared across nodes, and
    /// openraft's `Node` bound requires `Eq`; with a `Vec`, the same two
    /// addresses listed in a different order would compare unequal and encode
    /// to different bytes, so openraft would see a membership change where
    /// nothing changed. A set also removes duplicates for free. Nothing is
    /// lost: iroh races the paths it is given rather than treating them as a
    /// preference order.
    pub direct: BTreeSet<SocketAddr>,
}

impl NodeAddr {
    /// A node reachable only through address lookup.
    pub fn lookup_only() -> Self {
        Self::default()
    }

    /// Adds a relay URL.
    pub fn with_relay(mut self, relay: impl Into<String>) -> Self {
        self.relay = Some(relay.into());
        self
    }

    /// Adds a directly dialable socket address.
    pub fn with_direct(mut self, addr: SocketAddr) -> Self {
        self.direct.insert(addr);
        self
    }

    /// Whether this carries no addressing at all.
    pub fn is_empty(&self) -> bool {
        self.relay.is_none() && self.direct.is_empty()
    }
}
