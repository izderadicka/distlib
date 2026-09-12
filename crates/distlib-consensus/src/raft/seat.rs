//! Whether this node has a Raft, asked at the moment somebody needs one.
//!
//! Phase 1 settled a node's role at startup: a voter built a `Raft` and served
//! `distlib/raft/0`, everyone else did neither, and which of the two you were
//! was fixed for the life of the process. Promotion (2.3-2) ends that, and the
//! reason it needs a type of its own is iroh: `Router::spawn` fixes the set of
//! protocols, and `Router::shutdown` closes the *endpoint* — so a running node
//! cannot start serving a protocol it was not already serving.
//!
//! So every node serves the consensus protocols from startup and the handlers
//! ask this whether there is anything behind them. An empty seat is refused
//! with a close code, which is what a non-voter got before anyway (P1-22); the
//! difference is that it is now decided per connection rather than by what the
//! endpoint advertises, and that is what lets the answer change without a
//! restart.
//!
//! **Nothing here is a permission.** The seat says whether this node *has* a
//! Raft, never whether a peer may speak to it — that is
//! [`crate::raft::network::RaftProtocol`]'s own check against the voter set,
//! and it still runs.

use std::sync::{Arc, Mutex, PoisonError};

use openraft::Raft;

use crate::raft::types::TypeConfig;

/// The Raft this node votes with, if it votes.
///
/// Cheap to clone, and every clone sees the same seat — the shape
/// [`distlib_net::Connections`] and [`distlib_net::AddressBook`] already use,
/// for the same reason: the protocols are built before the answer is known and
/// must see it change.
#[derive(Clone, Default)]
pub(crate) struct Seat(Arc<Mutex<Option<Raft<TypeConfig>>>>);

impl Seat {
    /// An empty seat: this node does not vote.
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// A seat already filled, for a node that starts up as a voter.
    pub(crate) fn holding(raft: Raft<TypeConfig>) -> Self {
        Self(Arc::new(Mutex::new(Some(raft))))
    }

    /// The Raft, cloned out.
    ///
    /// Cloned rather than borrowed on purpose: a `Raft` handle is an `Arc`
    /// inside, and every caller here goes on to await on it. Handing back a
    /// guard would either hold a blocking lock across an await — which cannot
    /// compile in a `Send` future, and is the mistake
    /// [`distlib_net::Connections`] documents itself against — or force an
    /// async mutex that would serialise every RPC this node answers.
    pub(crate) fn raft(&self) -> Option<Raft<TypeConfig>> {
        self.lock().clone()
    }

    /// Whether this node votes.
    pub(crate) fn is_taken(&self) -> bool {
        self.lock().is_some()
    }

    /// Sits down: this node votes from now on.
    ///
    /// Replaces whatever was there, which is unreachable today — a node is
    /// promoted once and nothing demotes it in place (that gap is recorded in
    /// the phase-2 register) — and is the only sane meaning if it ever is not.
    pub(crate) fn take(&self, raft: Raft<TypeConfig>) {
        *self.lock() = Some(raft);
    }

    /// A poison-tolerant lock.
    ///
    /// Nothing here panics while holding it: the guard covers a clone of an
    /// `Arc` and nothing else. A poisoned lock would mean an unrelated panic,
    /// and refusing to answer afterwards would turn that into a node that has
    /// silently stopped taking part in consensus.
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Raft<TypeConfig>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// `Raft` does not implement `Debug`, and `ProtocolHandler` requires it of the
// handlers that hold one of these. Whether the seat is taken is the whole of
// what is worth printing; the rest is Raft's own metrics.
impl std::fmt::Debug for Seat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Seat").field(&self.is_taken()).finish()
    }
}
