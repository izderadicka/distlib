//! Who this node is willing to talk to.
//!
//! In phase 0 the set comes from configuration and changes only when the
//! process restarts. From phase 1 it is derived from the committed membership
//! log, and expulsion must take effect on a running node — §4.4 requires open
//! connections to a removed member to be closed, not merely refused next time.
//!
//! So the set is behind a [`tokio::sync::watch`] channel from the start rather
//! than a frozen `HashSet`. Phase 1 replaces the *producer* — the Raft state
//! machine drives [`AllowlistWriter`] — and nothing on the reading side has to
//! change.

use std::{collections::HashSet, sync::Arc};

use distlib_core::MemberId;
use tokio::sync::watch;

/// The set of members this node will talk to.
///
/// Cheap to clone: every clone observes the same set and sees updates from
/// [`AllowlistWriter`] immediately. Cloneable is a requirement, not a
/// convenience — `Builder::hooks` takes `impl EndpointHooks + 'static` and the
/// hook must own its copy.
#[derive(Debug, Clone)]
pub struct Allowlist {
    members: watch::Receiver<Arc<HashSet<MemberId>>>,
    self_id: MemberId,
}

/// The write half. Whoever holds this decides membership.
#[derive(Debug)]
pub struct AllowlistWriter {
    members: watch::Sender<Arc<HashSet<MemberId>>>,
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
    let (tx, rx) = watch::channel(Arc::new(members.into_iter().collect()));
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

    /// The current set, without this node's own implicit membership.
    pub fn snapshot(&self) -> Arc<HashSet<MemberId>> {
        Arc::clone(&self.members.borrow())
    }

    /// How many members are listed. Does not count this node.
    pub fn len(&self) -> usize {
        self.members.borrow().len()
    }

    /// Whether no other member is listed.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// This node's own identity.
    pub fn self_id(&self) -> MemberId {
        self.self_id
    }
}

impl AllowlistWriter {
    /// Replaces the whole set.
    ///
    /// Wholesale replacement rather than add/remove because phase 1 projects
    /// the set from committed log state: the log is the truth, and applying a
    /// snapshot of it cannot drift the way a sequence of deltas can.
    pub fn replace(&self, members: impl IntoIterator<Item = MemberId>) {
        let members: HashSet<_> = members.into_iter().collect();
        // `send` fails only when every reader is gone, which means nothing is
        // enforcing the policy any more — there is no one left to tell.
        let _ = self.members.send(Arc::new(members));
    }
}
