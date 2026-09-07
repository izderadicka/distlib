//! Making Raft's voter set match the one the log says the group has.
//!
//! `CoreGroupChanged` commits like any other event and updates the projection
//! ([`crate::MembershipState::core`]). That is a *statement*; this is what makes
//! it true. Until it existed the two could disagree for ever — P1-23 — and the
//! disagreement was invisible, because everything user-facing reads the
//! projection while consensus reads openraft's own membership.
//!
//! **A diff, not a command.** Nothing here reacts to a particular event: it
//! compares what the log says the core group is against what openraft thinks,
//! and closes the gap. That is what makes it recover on its own. A change
//! interrupted by a lost election, a crash, or a restart is simply a gap the
//! next pass finds, with nothing extra to persist and no half-finished state to
//! resume. It also means expulsion is covered for free:
//! [`crate::MembershipEvent::MemberExpelled`] already drops a member from the
//! projection's core, and before this the expelled node stayed an openraft voter
//! for ever — a vote the group could never collect.
//!
//! **Only the leader may.** `change_membership` is a write, so this does
//! nothing on a follower; it wakes when leadership moves and picks up whatever
//! the previous leader did not finish.
//!
//! **Promotion is not here.** A node only starts serving `distlib/raft/0` when
//! it starts up as a voter (P1-30), so promoting one now would create a voter
//! that counts toward quorum and can never answer. The state machine refuses
//! such an event outright, so this never sees one; the loud log below is a
//! backstop for the day that rule changes without this one.

use std::{collections::BTreeMap, time::Duration};

use distlib_core::{MemberId, NodeAddr, RawMemberId};
use openraft::{ChangeMembers, Raft, ServerState};

use crate::raft::{state_machine::StateMachineStore, types::TypeConfig};

/// How long to wait before trying again after a change that did not take.
///
/// The common reason is losing leadership mid-change, and the node that took
/// over reconciles anyway — so this is a backstop rather than the mechanism,
/// and it is sized to stay quiet rather than to be prompt.
const RETRY: Duration = Duration::from_secs(2);

/// What one pass found to do.
#[derive(Debug, PartialEq, Eq)]
enum Pass {
    /// Raft already agrees with the log.
    Converged,
    /// Something was submitted; look again, in case there is more.
    Changed,
}

/// Keeps openraft's voter set in step with the log's, for as long as this node
/// runs.
///
/// Never returns. Started only on a core node — a follower has no Raft to
/// change — and aborted at shutdown.
pub(crate) async fn enact(raft: Raft<TypeConfig>, state_machine: StateMachineStore) {
    let mut published = state_machine.subscribe();
    // `server_metrics` rather than `metrics`: the latter changes on every
    // heartbeat, and the only thing worth waking for is this node's own
    // leadership or the membership openraft holds — which is exactly what this
    // one carries.
    let mut server = raft.server_metrics();

    loop {
        match pass(&raft, &state_machine).await {
            // Something moved; the next pass may find the rest of it. Removals
            // and address changes are separate joint-consensus rounds, so more
            // than one pass is the ordinary case rather than an error path.
            Ok(Pass::Changed) => continue,
            Ok(Pass::Converged) => {}
            Err(error) => {
                tracing::warn!(%error, "could not put the core group into effect; will retry");
                tokio::time::sleep(RETRY).await;
                continue;
            }
        }

        // Wake on either side of the comparison moving: the log saying the core
        // group is something else, or this node becoming the one that can act
        // on it. Without the second, a node that becomes leader *after* the
        // event applied would never look again, and the group would stay split
        // until the next membership change — which is to say, the recovery this
        // module promises would not exist.
        //
        // **Argued, not tested.** Removing the second arm fails nothing in the
        // suite, and no deterministic test for it was found: creating a gap
        // requires a committed change, and committing one requires a leader,
        // whose own loop then closes the gap before any other node could be
        // asked to. A test would have to race the two, and a detector that
        // fires half the time is worse than an honest note. The case that
        // deserves one is a leader removing *itself*: openraft leaves the
        // cluster in a joint config if leadership is lost before the uniform
        // config commits, and finishing that is exactly this arm's job.
        tokio::select! {
            changed = published.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            changed = server.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// Compares once, and submits at most one change.
///
/// One change per pass because [`ChangeMembers`] describes one kind of change
/// and each call is its own joint-consensus round. Addresses first: a node being
/// removed does not need its address corrected, and doing the harmless half
/// first means a failure partway leaves the group with the *right* addressing
/// and a stale voter, rather than the reverse.
async fn pass(
    raft: &Raft<TypeConfig>,
    state_machine: &StateMachineStore,
) -> Result<Pass, Box<dyn std::error::Error + Send + Sync>> {
    let server = raft.server_metrics().borrow().clone();
    if server.state != ServerState::Leader {
        return Ok(Pass::Converged);
    }

    let wanted = state_machine.membership();
    let wanted = wanted.core();
    if wanted.is_empty() {
        // Before founding. Raft has no membership to reconcile against either.
        return Ok(Pass::Converged);
    }

    let voters: BTreeMap<MemberId, NodeAddr> = server
        .membership_config
        .nodes()
        .filter_map(|(id, addr)| Some((MemberId::try_from(*id).ok()?, addr.clone())))
        .filter(|(member, _)| {
            server
                .membership_config
                .voter_ids()
                .any(|voter| MemberId::try_from(voter).is_ok_and(|voter| voter == *member))
        })
        .collect();

    // Addresses that moved, for voters that stay.
    let moved: BTreeMap<RawMemberId, NodeAddr> = wanted
        .iter()
        .filter(|(member, addr)| voters.get(member).is_some_and(|current| current != *addr))
        .map(|(member, addr)| (RawMemberId::from(*member), addr.clone()))
        .collect();
    if !moved.is_empty() {
        tracing::info!(
            count = moved.len(),
            "the log gives a core node a new address"
        );
        // `SetNodes` carries a warning in openraft's own documentation, because
        // pointing a voter's address at a different node lets two quorums form.
        // That cannot happen here: `RaftClient` dials
        // `EndpointAddr::new(member.endpoint_id())`, so the address is a hint
        // the QUIC handshake overrides — a wrong one reaches nobody rather than
        // the wrong somebody. openraft names that as the `RaftNetwork`'s
        // responsibility in the same document, and iroh discharges it.
        raft.change_membership(ChangeMembers::SetNodes(moved), false)
            .await?;
        return Ok(Pass::Changed);
    }

    // Voters the log no longer lists.
    let departed: std::collections::BTreeSet<RawMemberId> = voters
        .keys()
        .filter(|member| !wanted.contains_key(member))
        .map(|member| RawMemberId::from(*member))
        .collect();
    if !departed.is_empty() {
        tracing::info!(count = departed.len(), "the log drops a core node");
        // `retain: false` — not kept on as learners. P1-22 refuses
        // `distlib/raft/0` to anyone who is not a current voter, so retaining
        // them would mean replicating to a node this group also refuses to talk
        // to, which is a standing failure rather than a graceful demotion.
        raft.change_membership(ChangeMembers::RemoveVoters(departed), false)
            .await?;
        return Ok(Pass::Changed);
    }

    // Anything the log lists that does not vote yet. Unreachable while the state
    // machine refuses a promoting `CoreGroupChanged`, and loud rather than
    // silent because the two rules have to move together.
    let promoted: Vec<MemberId> = wanted
        .keys()
        .filter(|member| !voters.contains_key(member))
        .copied()
        .collect();
    if !promoted.is_empty() {
        tracing::error!(
            ?promoted,
            "the log names core nodes that do not vote, and promotion is not implemented; \
             they will not take part in consensus"
        );
    }

    Ok(Pass::Converged)
}
