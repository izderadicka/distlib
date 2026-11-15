//! A voter that hears from no leader reads the log as a follower would
//! (#194 finding 1, 5e-9).
//!
//! A voter away while a member was admitted and promoted comes back refusing
//! that member as a leader: it decides who may speak Raft, and who may connect
//! at all, from its own view, which predates the newcomer. Only the leader
//! replicates, so it would never receive the entries that make the newcomer
//! legitimate to it. So a seated voter that has heard from no leader for
//! [`LEADERLESS_AFTER`] fetches the signed log past what it applied from the
//! voters it knows, folds it onto a copy of its membership, and lets whoever
//! that names as core speak Raft to it, until its own state machine has
//! applied that far.
//!
//! **Authority stays with the signed log.** The fold is the one every node
//! runs, so a source can withhold entries, which leaves this node as stuck as
//! before, but cannot forge them. The fetched entries never reach the Raft log
//! or the state machine: those still take entries only from a leader.

use std::{
    collections::{BTreeSet, VecDeque},
    time::Duration,
};

use distlib_core::{MemberId, NodeAddr, RawMemberId};
use openraft::{Raft, metrics::RaftServerMetrics};
use tokio::sync::watch;

use crate::{
    MembershipState,
    raft::{
        follower::{self, SharedSources},
        memberlog::{Fetched, MemberlogClient},
        state_machine::StateMachineStore,
        types::TypeConfig,
    },
    state::PROTOCOL_MAX,
};

/// How long a seated voter goes without a leader before it reads the log
/// itself: a few election timeouts, so an ordinary election never does.
pub(crate) const LEADERLESS_AFTER: Duration =
    Duration::from_millis(3 * crate::node::ELECTION_TIMEOUT_MAX);

/// The core the signed log names past what this node has applied, and how
/// far that log reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatchUpView {
    core: BTreeSet<MemberId>,
    at: u64,
}

impl CatchUpView {
    /// Whether `peer` may speak Raft to a node that has applied up to
    /// `applied`: only while it is behind the view, so that once it has caught
    /// up its own log decides again, and a member the log later removes is not
    /// still admitted by an old view.
    pub(crate) fn admits(&self, peer: &MemberId, applied: u64) -> bool {
        applied < self.at && self.core.contains(peer)
    }
}

/// The catch-up view of this node, shared by what decides who may speak Raft.
#[derive(Debug, Clone)]
pub(crate) struct CatchUp(watch::Sender<Option<CatchUpView>>);

impl Default for CatchUp {
    fn default() -> Self {
        Self(watch::channel(None).0)
    }
}

impl CatchUp {
    /// Whether the view admits `peer` to a node that has applied up to
    /// `applied`.
    pub(crate) fn admits(&self, peer: &MemberId, applied: u64) -> bool {
        self.0
            .borrow()
            .as_ref()
            .is_some_and(|view| view.admits(peer, applied))
    }

    /// The members the view admits to a node that has applied up to `applied`.
    pub(crate) fn admitted(&self, applied: u64) -> Vec<MemberId> {
        self.0.borrow().as_ref().map_or_else(Vec::new, |view| {
            view.core
                .iter()
                .filter(|peer| view.admits(peer, applied))
                .copied()
                .collect()
        })
    }

    /// Each new view, for whoever turns it into a transport exception.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Option<CatchUpView>> {
        self.0.subscribe()
    }

    fn clear(&self) {
        self.0.send_if_modified(|current| current.take().is_some());
    }

    fn set(&self, view: CatchUpView) {
        self.0.send_if_modified(|current| {
            let changed = current.as_ref() != Some(&view);
            *current = Some(view);
            changed
        });
    }
}

/// What reading the log for a leaderless voter needs.
pub(crate) struct Catching {
    pub(crate) me: MemberId,
    pub(crate) raft: Raft<TypeConfig>,
    pub(crate) state_machine: StateMachineStore,
    pub(crate) client: MemberlogClient,
    pub(crate) sources: SharedSources,
    pub(crate) catch_up: CatchUp,
}

/// For as long as this node has `raft`: each time it has heard from no
/// leader for [`LEADERLESS_AFTER`], reads the log past what it applied and
/// publishes the core it names.
pub(crate) async fn catch_up_when_leaderless(catching: Catching) {
    // A view from an earlier seat would outlive it: a promotion replays the
    // log from the start, so this node would be behind it again.
    catching.catch_up.clear();
    let mut server = catching.raft.server_metrics();
    while until_leaderless(&mut server).await {
        if let Some(view) = read_ahead(&catching).await {
            tracing::info!(
                at = view.at,
                core = view.core.len(),
                "no leader heard; letting the core the log names speak Raft"
            );
            catching.catch_up.set(view);
        }
    }
}

/// Waits until no leader has been heard for [`LEADERLESS_AFTER`]. `false`
/// once the Raft is gone.
async fn until_leaderless(
    server: &mut watch::Receiver<RaftServerMetrics<RawMemberId, NodeAddr>>,
) -> bool {
    loop {
        if server
            .wait_for(|seen| seen.current_leader.is_none())
            .await
            .is_err()
        {
            return false;
        }
        match tokio::time::timeout(
            LEADERLESS_AFTER,
            server.wait_for(|seen| seen.current_leader.is_some()),
        )
        .await
        {
            Err(_) => return true,
            Ok(Err(_)) => return false,
            Ok(Ok(_)) => {}
        }
    }
}

/// The core the signed log names past what this node applied, from the first
/// voter it knows that has more; `None` if none has.
async fn read_ahead(catching: &Catching) -> Option<CatchUpView> {
    let Catching {
        me,
        state_machine,
        client,
        sources,
        ..
    } = catching;
    let applied = state_machine.last_applied_index();
    let mut candidates: VecDeque<(MemberId, NodeAddr)> =
        follower::read(sources).candidates(*me).into();
    candidates.extend(
        state_machine
            .membership()
            .core()
            .iter()
            .filter(|(id, _)| *id != me)
            .map(|(id, addr)| (*id, addr.clone())),
    );
    let mut asked = BTreeSet::new();
    while let Some((member, addr)) = candidates.pop_front() {
        if !asked.insert(member) {
            continue;
        }
        let mut membership = state_machine.membership();
        let at = fold_from(client, member, &addr, applied, &mut membership).await;
        if at > applied {
            return Some(CatchUpView {
                core: membership.core().keys().copied().collect(),
                at,
            });
        }
    }
    None
}

/// Folds onto `membership` what `member` serves past `cursor`, every page of
/// it, and answers the last index folded.
async fn fold_from(
    client: &MemberlogClient,
    member: MemberId,
    addr: &NodeAddr,
    mut cursor: u64,
    membership: &mut MembershipState,
) -> u64 {
    loop {
        let fetched = match client.fetch(member, addr, cursor).await {
            Ok(fetched) => fetched,
            Err(error) => {
                tracing::debug!(%error, "could not read the log from a voter to catch up; trying another");
                return cursor;
            }
        };
        let Fetched::Entries {
            up_to,
            events,
            more,
            ..
        } = fetched
        else {
            return cursor;
        };
        for (index, event) in &events {
            // Past what this build speaks, the core it would name is unknown.
            if membership.protocol() > PROTOCOL_MAX {
                return cursor;
            }
            if let Err(error) = membership.apply(*index, event) {
                tracing::debug!(%index, %error, "the fold refuses an event read to catch up; skipping it");
            }
            cursor = *index;
        }
        cursor = cursor.max(up_to);
        if !more {
            return cursor;
        }
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    fn member(seed: u8) -> MemberId {
        MemberId::from(SecretKey::from_bytes(&[seed; 32]).public())
    }

    #[test]
    fn a_view_admits_its_core_only_while_this_node_is_behind_it() {
        let view = CatchUpView {
            core: BTreeSet::from([member(1), member(2)]),
            at: 10,
        };
        assert!(view.admits(&member(1), 9));
        assert!(!view.admits(&member(3), 9), "not in the core it names");
        assert!(
            !view.admits(&member(1), 10),
            "caught up: its own log decides"
        );
    }

    #[test]
    fn the_admitted_are_none_once_caught_up_or_cleared() {
        let catch_up = CatchUp::default();
        assert!(catch_up.admitted(0).is_empty());
        catch_up.set(CatchUpView {
            core: BTreeSet::from([member(1)]),
            at: 5,
        });
        assert_eq!(catch_up.admitted(4), [member(1)]);
        assert!(catch_up.admits(&member(1), 4));
        assert!(catch_up.admitted(5).is_empty());
        catch_up.set(CatchUpView {
            core: BTreeSet::from([member(1)]),
            at: 9,
        });
        catch_up.clear();
        assert!(catch_up.admitted(0).is_empty(), "cleared");
    }
}
