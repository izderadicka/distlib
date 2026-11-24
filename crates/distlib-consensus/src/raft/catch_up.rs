//! A voter that hears from no leader reads the log as a follower would
//! (#194 finding 1, 5e-9).
//!
//! A voter away while a member was admitted and promoted comes back refusing
//! that member as a leader: it decides who may speak Raft, and who may connect
//! at all, from its own view, which predates the newcomer. Only the leader
//! replicates, so it would never receive the entries that make the newcomer
//! legitimate to it. A node in the middle of its own promotion refuses a
//! leader promoted after it sat down the same way, as a learner that goes on
//! naming its old leader. So a seated node that has accepted no
//! `AppendEntries` for [`UNHEARD_AFTER`], and does not lead, fetches the signed
//! log past what it applied from every voter it knows or is pointed to, folds
//! it onto a copy of its membership, and lets whoever that names as core speak
//! Raft to it, until its own state machine has applied that far.
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
use openraft::Raft;
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

/// How long a seated node goes without an `AppendEntries` before it reads the
/// log itself: a few election timeouts, so an ordinary election never does.
pub(crate) const UNHEARD_AFTER: Duration =
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

/// The catch-up view of this node, shared by what decides who may speak Raft,
/// and word of each `AppendEntries` it accepts.
#[derive(Debug, Clone)]
pub(crate) struct CatchUp {
    view: watch::Sender<Option<CatchUpView>>,
    heard: watch::Sender<()>,
}

impl Default for CatchUp {
    fn default() -> Self {
        Self {
            view: watch::channel(None).0,
            heard: watch::channel(()).0,
        }
    }
}

impl CatchUp {
    /// Whether the view admits `peer` to a node that has applied up to
    /// `applied`.
    pub(crate) fn admits(&self, peer: &MemberId, applied: u64) -> bool {
        self.view
            .borrow()
            .as_ref()
            .is_some_and(|view| view.admits(peer, applied))
    }

    /// The members the view admits to a node that has applied up to `applied`.
    pub(crate) fn admitted(&self, applied: u64) -> Vec<MemberId> {
        self.view.borrow().as_ref().map_or_else(Vec::new, |view| {
            view.core
                .iter()
                .filter(|peer| view.admits(peer, applied))
                .copied()
                .collect()
        })
    }

    /// Each new view, for whoever turns it into a transport exception.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Option<CatchUpView>> {
        self.view.subscribe()
    }

    /// A leader's `AppendEntries` was accepted: someone this node admits
    /// leads, and is replicating to it.
    pub(crate) fn heard_from_leader(&self) {
        self.heard.send_replace(());
    }

    fn clear(&self) {
        self.view
            .send_if_modified(|current| current.take().is_some());
    }

    fn set(&self, view: CatchUpView) {
        self.view.send_if_modified(|current| {
            let changed = current.as_ref() != Some(&view);
            *current = Some(view);
            changed
        });
    }
}

/// What reading the log for an unheard voter needs.
pub(crate) struct Catching {
    pub(crate) me: MemberId,
    pub(crate) raft: Raft<TypeConfig>,
    pub(crate) state_machine: StateMachineStore,
    pub(crate) client: MemberlogClient,
    pub(crate) sources: SharedSources,
    pub(crate) catch_up: CatchUp,
}

/// For as long as this node is seated: each time it has accepted no
/// `AppendEntries` for [`UNHEARD_AFTER`] and does not lead, reads the log past
/// what it applied and publishes the core it names.
pub(crate) async fn catch_up_when_unheard(catching: Catching) {
    // A view from an earlier seat would outlive it: a promotion replays the
    // log from the start, so this node would be behind it again.
    catching.catch_up.clear();
    let mut heard = catching.catch_up.heard.subscribe();
    loop {
        match tokio::time::timeout(UNHEARD_AFTER, heard.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) => return,
            Err(_) => {}
        }
        let me = RawMemberId::from(catching.me);
        if catching.raft.server_metrics().borrow().current_leader == Some(me) {
            continue;
        }
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

/// The signed log past what this node applied, folded onto its membership.
struct Reading {
    membership: MembershipState,
    cursor: u64,
}

/// The core the signed log names past what this node applied, read from every
/// voter it knows and every voter they point to; `None` if none has more.
///
/// One fold across all of them, so a retired voter's prefix and its
/// successors' entries add up, as a follower's reads do.
async fn read_ahead(catching: &Catching) -> Option<CatchUpView> {
    let Catching {
        me,
        state_machine,
        client,
        sources,
        ..
    } = catching;
    let applied = state_machine.last_applied_index();
    let mut reading = Reading {
        membership: state_machine.membership(),
        cursor: applied,
    };
    let mut candidates: VecDeque<(MemberId, NodeAddr)> =
        follower::read(sources).candidates(*me).into();
    candidates.extend(core_of(&reading.membership));
    let mut asked = BTreeSet::from([*me]);
    while let Some((member, addr)) = candidates.pop_front() {
        if !asked.insert(member) {
            continue;
        }
        candidates.extend(reading.fold_from(client, member, &addr).await);
        candidates.extend(core_of(&reading.membership));
    }
    (reading.cursor > applied).then(|| CatchUpView {
        core: reading.membership.core().keys().copied().collect(),
        at: reading.cursor,
    })
}

fn core_of(membership: &MembershipState) -> Vec<(MemberId, NodeAddr)> {
    membership
        .core()
        .iter()
        .map(|(member, addr)| (*member, addr.clone()))
        .collect()
}

impl Reading {
    /// Folds what `member` serves past the cursor, every page of it, and
    /// answers the voters it points to.
    async fn fold_from(
        &mut self,
        client: &MemberlogClient,
        member: MemberId,
        addr: &NodeAddr,
    ) -> Vec<(MemberId, NodeAddr)> {
        loop {
            let fetched = match client.fetch(member, addr, self.cursor).await {
                Ok(fetched) => fetched,
                Err(error) => {
                    tracing::debug!(%error, "could not read the log from a voter to catch up; trying another");
                    return Vec::new();
                }
            };
            let (up_to, events, more, source) = match fetched {
                Fetched::Entries {
                    up_to,
                    events,
                    more,
                    source,
                } => (up_to, events, more, source),
                Fetched::Redirect(source) => return source.core,
                Fetched::NoGroup => return Vec::new(),
            };
            for (index, event) in &events {
                // Past what this build speaks, the core it would name is unknown.
                if self.membership.protocol() > PROTOCOL_MAX {
                    return Vec::new();
                }
                if let Err(error) = self.membership.apply(*index, event) {
                    tracing::debug!(%index, %error, "the fold refuses an event read to catch up; skipping it");
                }
                self.cursor = *index;
            }
            self.cursor = self.cursor.max(up_to);
            if !more {
                return source.core;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
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

    /// The transport exception follows the view, so it is told of each new
    /// one, and only of a new one.
    #[test]
    fn a_view_is_announced_only_when_it_changes() {
        let catch_up = CatchUp::default();
        let mut views = catch_up.subscribe();
        let view = CatchUpView {
            core: BTreeSet::from([member(1)]),
            at: 5,
        };
        catch_up.set(view.clone());
        assert!(views.has_changed().unwrap());
        views.borrow_and_update();
        catch_up.set(view);
        assert!(!views.has_changed().unwrap(), "the same view again");
    }
}
