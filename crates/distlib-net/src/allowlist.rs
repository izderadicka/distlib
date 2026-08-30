//! Who this node is willing to talk to.
//!
//! In phase 0 the set comes from configuration and changes only when the
//! process restarts. From phase 1 it is derived from the committed membership
//! log, and expulsion must take effect on a running node — §4.4 requires open
//! connections to a removed member to be closed, not merely refused next time.
//!
//! That last requirement is why the set sits behind a [`tokio::sync::watch`]
//! channel rather than a lock: closing live connections needs a *notification*,
//! which a lock cannot provide. Phase 1 replaces only the producer — the Raft
//! state machine drives [`AllowlistWriter`] — and the reading side is unchanged.

use std::collections::HashSet;

use distlib_core::MemberId;
use tokio::sync::watch;

/// The set of members this node will talk to.
///
/// Cheap to clone: every clone observes the same set and sees updates from
/// [`AllowlistWriter`] immediately. Cloneable is a requirement, not a
/// convenience — `Builder::hooks` takes `impl EndpointHooks + 'static` and the
/// hook must own its copy.
///
/// **Never hold the `borrow()` guard across an `.await`.** It is a read guard
/// on the channel's internal lock, so keeping it alive across a suspension
/// point would stall any concurrent write. Every method below borrows and drops
/// within a single expression.
#[derive(Debug, Clone)]
pub struct Allowlist {
    members: watch::Receiver<HashSet<MemberId>>,
    self_id: MemberId,
}

/// The write half. Whoever holds this decides membership.
#[derive(Debug)]
pub struct AllowlistWriter {
    members: watch::Sender<HashSet<MemberId>>,
}

/// Creates an allowlist containing `members`, readable by any number of clones.
///
/// The writer is returned separately so the authority over membership is
/// explicit: a component holding only an [`Allowlist`] can enforce the policy
/// but cannot change it.
pub fn allowlist(
    self_id: MemberId,
    members: impl IntoIterator<Item = MemberId>,
) -> (AllowlistWriter, Allowlist) {
    let (tx, rx) = watch::channel(members.into_iter().collect());
    (
        AllowlistWriter { members: tx },
        Allowlist {
            members: rx,
            self_id,
        },
    )
}

impl Allowlist {
    /// Whether `id` may connect to us, or we to it.
    ///
    /// This node is always allowed, whatever the set says. A node that could
    /// exclude itself would be unable to reach its own services, and during a
    /// phase 1 expulsion the removed node must still be able to observe the log
    /// entry that removed it.
    pub fn is_allowed(&self, id: &MemberId) -> bool {
        *id == self.self_id || self.members.borrow().contains(id)
    }

    /// How many members are listed. Does not count this node.
    pub fn len(&self) -> usize {
        self.members.borrow().len()
    }

    /// Whether no other member is listed.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Waits until the set changes.
    ///
    /// The reason this is a `watch` channel rather than a lock. §4.4 requires
    /// that expelling a member close connections that are *already open*, not
    /// merely refuse the next attempt, and that needs a signal rather than a
    /// value someone happens to read again.
    ///
    /// Returns an error once the writer is gone, which means the set can never
    /// change again — for a caller looping on this, that is the signal to stop
    /// rather than a failure.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.members.changed().await
    }
}

impl AllowlistWriter {
    /// Replaces the whole set.
    ///
    /// Wholesale replacement rather than add/remove because phase 1 projects
    /// the set from committed log state: the log is the truth, and applying a
    /// snapshot of it cannot drift the way a sequence of deltas can.
    pub fn replace(&self, members: impl IntoIterator<Item = MemberId>) {
        // `send_replace` rather than `send`: it cannot fail when every reader
        // has gone away, so there is no `Result` to discard. The previous set
        // is returned and dropped.
        self.members.send_replace(members.into_iter().collect());
    }
}
