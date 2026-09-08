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

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

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

    /// This node is not the leader, so any difference is not its to close.
    ///
    /// A separate answer from [`Self::Converged`] even though the loop waits
    /// after either, because they are different facts and conflating them reads
    /// as though the other voters are being skipped. They are not: openraft
    /// writes a membership change as log entries, so every voter receives it by
    /// ordinary replication. There is nothing for a non-leader to *do* here,
    /// and nothing it *could* do — `change_membership` is a write, and a
    /// non-leader gets `ForwardToLeader` for its trouble.
    NotOurs,

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
            Ok(Pass::Converged | Pass::NotOurs) => {}
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

/// The one change to submit next, or `None` when Raft already agrees.
#[derive(Debug, PartialEq, Eq)]
enum Change {
    /// Voters whose recorded address the log has moved.
    Readdress(BTreeMap<RawMemberId, NodeAddr>),
    /// Voters the log no longer lists.
    Remove(BTreeSet<RawMemberId>),
    /// The log names voters Raft does not have. Not a change — nothing here can
    /// promote — so this is something to say rather than something to do.
    CannotPromote(Vec<MemberId>),
}

/// What one comparison says to do, given what the log wants and what Raft has.
///
/// Pure, and separated from [`pass`] for exactly the reason this module is easy
/// to get wrong: the interesting part is a set comparison, and a set comparison
/// that needs a three-node cluster to exercise is one nobody will argue with.
/// The tests at the bottom of this file are the argument.
///
/// **One change, not a plan.** [`ChangeMembers`] describes a single kind of
/// change and each call is its own joint-consensus round, so a pass that finds
/// two things to do submits the first and is called again. Addresses come
/// before removals: a departing voter is not in `wanted`, so its address is
/// never in `Readdress` anyway, and taking the half that cannot reduce the
/// voter set first means a failure in between leaves a group that is fully
/// addressable rather than one short.
fn next_change(
    wanted: &BTreeMap<MemberId, NodeAddr>,
    voters: &BTreeMap<MemberId, NodeAddr>,
) -> Option<Change> {
    // An empty `wanted` is not "remove every voter". It means either that no
    // group has been founded, or that expulsions have emptied the projection's
    // core group — which they can, today, because `MemberExpelled` drops a
    // member from `core` with no floor beneath it. Neither is an instruction:
    // openraft refuses an empty membership outright, and a group with no voters
    // could never commit the event that would restore them. So this leaves Raft
    // alone and the group keeps whatever voters it had. That is a bad state to
    // be in, but a stable one, and the fix belongs where the hole is — in the
    // rule that let the core group be emptied.
    if wanted.is_empty() {
        return None;
    }

    let moved: BTreeMap<RawMemberId, NodeAddr> = wanted
        .iter()
        .filter(|(member, addr)| voters.get(member).is_some_and(|current| current != *addr))
        .map(|(member, addr)| (RawMemberId::from(*member), addr.clone()))
        .collect();
    if !moved.is_empty() {
        return Some(Change::Readdress(moved));
    }

    let departed: BTreeSet<RawMemberId> = voters
        .keys()
        .filter(|member| !wanted.contains_key(member))
        .map(|member| RawMemberId::from(*member))
        .collect();
    if !departed.is_empty() {
        return Some(Change::Remove(departed));
    }

    let promoted: Vec<MemberId> = wanted
        .keys()
        .filter(|member| !voters.contains_key(member))
        .copied()
        .collect();
    if !promoted.is_empty() {
        return Some(Change::CannotPromote(promoted));
    }

    None
}

/// The voters openraft currently has, each with the address it would dial.
///
/// `None` if that picture cannot be read — every id here was written by this
/// codebase from a [`MemberId`], and every voter is guaranteed a node entry by
/// openraft's own validity check, so either failing is an invariant violation.
/// Refusing to act on a partial picture is the point: a voter dropped quietly
/// here would be one this module could never remove and never notice.
fn current_voters(
    membership: &openraft::Membership<RawMemberId, NodeAddr>,
) -> Option<BTreeMap<MemberId, NodeAddr>> {
    membership
        .voter_ids()
        .map(|id| {
            let member = MemberId::try_from(id).ok()?;
            let addr = membership.get_node(&id)?;
            Some((member, addr.clone()))
        })
        .collect()
}

/// Compares once, and submits at most one change.
async fn pass(
    raft: &Raft<TypeConfig>,
    state_machine: &StateMachineStore,
) -> Result<Pass, Box<dyn std::error::Error + Send + Sync>> {
    let server = raft.server_metrics().borrow().clone();
    if server.state != ServerState::Leader {
        return Ok(Pass::NotOurs);
    }

    let Some(voters) = current_voters(server.membership_config.membership()) else {
        tracing::error!(
            membership = ?server.membership_config,
            "raft's voter set cannot be read as member ids; not reconciling it"
        );
        return Ok(Pass::Converged);
    };

    let published = state_machine.membership();
    let Some(change) = next_change(published.core(), &voters) else {
        return Ok(Pass::Converged);
    };

    match change {
        Change::Readdress(moved) => {
            tracing::info!(
                count = moved.len(),
                "the log gives a core node a new address"
            );
            // `SetNodes` carries a warning in openraft's own documentation,
            // because pointing a voter's address at a different node lets two
            // quorums form. That cannot happen here: `RaftClient` dials
            // `EndpointAddr::new(member.endpoint_id())`, so the address is a
            // hint the QUIC handshake overrides — a wrong one reaches nobody
            // rather than the wrong somebody. openraft names that as the
            // `RaftNetwork`'s responsibility in the same document, and iroh
            // discharges it.
            raft.change_membership(ChangeMembers::SetNodes(moved), false)
                .await?;
            Ok(Pass::Changed)
        }
        Change::Remove(departed) => {
            tracing::info!(count = departed.len(), "the log drops a core node");
            // `retain: false` — not kept on as learners. P1-22 refuses
            // `distlib/raft/0` to anyone who is not a current voter, so
            // retaining them would mean replicating to a node this group also
            // refuses to talk to: a standing failure rather than a graceful
            // demotion.
            raft.change_membership(ChangeMembers::RemoveVoters(departed), false)
                .await?;
            Ok(Pass::Changed)
        }
        Change::CannotPromote(promoted) => {
            tracing::error!(
                ?promoted,
                "the log names core nodes that do not vote, and promotion is not implemented; \
                 they will not take part in consensus"
            );
            Ok(Pass::Converged)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use super::*;

    fn member(seed: u8) -> MemberId {
        MemberId::from(iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    fn at(port: u16) -> NodeAddr {
        NodeAddr::default().with_direct(([127, 0, 0, 1], port).into())
    }

    /// The core group as the log and as Raft, from the same shorthand.
    fn view(entries: &[(u8, u16)]) -> BTreeMap<MemberId, NodeAddr> {
        entries
            .iter()
            .map(|(seed, port)| (member(*seed), at(*port)))
            .collect()
    }

    fn raw(seed: u8) -> RawMemberId {
        RawMemberId::from(member(seed))
    }

    #[test]
    fn agreement_asks_for_nothing() {
        let both = view(&[(1, 11), (2, 12), (3, 13)]);
        assert_eq!(next_change(&both, &both), None);
    }

    #[test]
    fn a_moved_address_is_set_and_only_for_the_one_that_moved() {
        let wanted = view(&[(1, 11), (2, 12), (3, 99)]);
        let voters = view(&[(1, 11), (2, 12), (3, 13)]);

        assert_eq!(
            next_change(&wanted, &voters),
            Some(Change::Readdress(BTreeMap::from([(raw(3), at(99))])))
        );
    }

    #[test]
    fn a_voter_the_log_no_longer_lists_is_removed() {
        let wanted = view(&[(1, 11), (2, 12)]);
        let voters = view(&[(1, 11), (2, 12), (3, 13)]);

        assert_eq!(
            next_change(&wanted, &voters),
            Some(Change::Remove(BTreeSet::from([raw(3)])))
        );
    }

    #[test]
    fn addressing_is_done_before_removing() {
        // Both are outstanding. One change per pass, and this is the order:
        // a failure in between leaves a group that is fully addressable rather
        // than one voter short.
        let wanted = view(&[(1, 11), (2, 99)]);
        let voters = view(&[(1, 11), (2, 12), (3, 13)]);

        assert_eq!(
            next_change(&wanted, &voters),
            Some(Change::Readdress(BTreeMap::from([(raw(2), at(99))])))
        );

        // The pass after the addresses have landed.
        let voters = view(&[(1, 11), (2, 99), (3, 13)]);
        assert_eq!(
            next_change(&wanted, &voters),
            Some(Change::Remove(BTreeSet::from([raw(3)])))
        );
        assert_eq!(next_change(&wanted, &wanted), None, "and then it is done");
    }

    #[test]
    fn a_departing_voter_is_never_readdressed_on_the_way_out() {
        // It is not in `wanted`, so it cannot be in `Readdress` — which is what
        // makes the ordering above cost nothing.
        let wanted = view(&[(1, 11)]);
        let voters = view(&[(1, 11), (2, 12)]);

        assert_eq!(
            next_change(&wanted, &voters),
            Some(Change::Remove(BTreeSet::from([raw(2)])))
        );
    }

    #[test]
    fn a_voter_the_log_has_gained_is_reported_not_added() {
        // Promotion is refused by the state machine, so this is unreachable —
        // and says so loudly rather than quietly doing nothing, because the two
        // rules have to move together.
        let wanted = view(&[(1, 11), (2, 12)]);
        let voters = view(&[(1, 11)]);

        assert_eq!(
            next_change(&wanted, &voters),
            Some(Change::CannotPromote(vec![member(2)]))
        );
    }

    #[test]
    fn an_empty_core_group_is_not_an_instruction_to_remove_everyone() {
        // Two ways to get here: no group founded yet, or expulsions have emptied
        // the projection's core — which they can, since `MemberExpelled` drops a
        // member from `core` with no floor. Neither means "remove every voter":
        // openraft refuses an empty membership, and a group with no voters could
        // never commit the event that would restore them.
        let voters = view(&[(1, 11), (2, 12)]);

        assert_eq!(next_change(&BTreeMap::new(), &voters), None);
    }
}
