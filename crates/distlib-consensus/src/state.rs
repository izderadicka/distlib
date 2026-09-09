//! The projection: committed events folded into who the group currently is.
//!
//! This is a pure function of the log, and deliberately knows nothing about
//! Raft, storage or the network. Everything a node enforces — the connection
//! allowlist, the pledge table, the set of Raft voters — is *derived* here
//! rather than configured anywhere, which is the whole point of Phase 1.
//!
//! Ordered collections throughout: iteration order is part of the value once
//! this gets snapshotted and compared across nodes, so `BTreeMap`/`BTreeSet`
//! rather than the hashed equivalents.

use std::collections::{BTreeMap, BTreeSet};

use distlib_core::{GroupId, MemberId, NodeAddr};
use serde::{Deserialize, Serialize};

use crate::{
    error::{ConsensusError, Result},
    event::{MemberRecord, MembershipEvent, check_founders},
    signed::SignedEvent,
};

/// Who the group is, as of the events applied so far.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipState {
    group: Option<GroupId>,
    members: BTreeMap<MemberId, MemberRecord>,
    /// The Raft voters, each with the address the group records for it.
    ///
    /// A map rather than a set because the address belongs *to* the voter and
    /// nothing else in the log carries it. Keeping the two apart would be two
    /// values that have to agree, and the one thing this projection exists to
    /// prevent is two answers to the same question.
    core: BTreeMap<MemberId, NodeAddr>,
    /// Proposals waiting for approvals, by the log index they were made at.
    ///
    /// §4.4 step 1 lets any member submit a change and step 2 gives the
    /// decision to a quorum of core nodes. This is the gap between the two.
    /// Keyed by log index because that is what [`MembershipEvent::Approved`]
    /// names, and because it is already unique and already monotonic.
    pending: BTreeMap<u64, Proposal>,
    /// Log index of the last entry that changed this state.
    ///
    /// The log's own index rather than a counter of our own: one monotonic
    /// number, and one that means something to a reader. Only successful
    /// applies move it — not Raft's blank entries, and not refused proposals —
    /// so a leader election does not invalidate proposals in flight.
    changed_at: u64,
}

/// A submitted change that has not yet collected the approvals it needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    event: MembershipEvent,
    proposer: MemberId,
    /// Who has approved so far, the proposer included when they were core.
    ///
    /// Not every one of these necessarily counts: the threshold is measured
    /// against the core group as it stands when an approval lands, and so are
    /// the approvals themselves — see [`MembershipState::enough`].
    approvals: BTreeSet<MemberId>,
}

impl Proposal {
    /// What was proposed.
    pub fn event(&self) -> &MembershipEvent {
        &self.event
    }

    /// The member who submitted it.
    pub fn proposer(&self) -> MemberId {
        self.proposer
    }

    /// Who has approved it so far.
    pub fn approvals(&self) -> impl Iterator<Item = MemberId> + '_ {
        self.approvals.iter().copied()
    }
}

/// The member an event is *about*, where their own approval must not count.
///
/// Expulsion only. Removing somebody is the one decision where the person
/// concerned is on the other side of it; a demotion they vote for is a
/// resignation, and an admission is not a vote about anyone already inside.
fn target(event: &MembershipEvent) -> Option<MemberId> {
    match event {
        MembershipEvent::MemberExpelled { member, .. } => Some(*member),
        _ => None,
    }
}

/// The member an event concerns, for deciding which pending proposals a change
/// has made stale. Wider than [`target`]: an admission is about somebody too.
fn subject(event: &MembershipEvent) -> Option<MemberId> {
    match event {
        MembershipEvent::MemberAdded { member } => Some(member.member_id),
        MembershipEvent::MemberExpelled { member, .. } => Some(*member),
        _ => None,
    }
}

impl MembershipState {
    /// An empty state, before any event has been applied.
    pub fn new() -> Self {
        Self::default()
    }

    /// Verifies `signed` and folds it in, as the entry at `index`.
    ///
    /// Verification happens here rather than at the point the entry arrived, so
    /// there is no path into the state that skips it. Every rule lives here for
    /// the same reason: this runs identically on every node, core or follower,
    /// so every node reaches the same verdict about the same entry. A check
    /// anywhere else — the API, the protocol handler — would be decoration, and
    /// two nodes disagreeing about whether an entry applied would be a split
    /// membership.
    ///
    /// On error the state is left untouched: every rule is checked before
    /// anything is mutated, so a rejected event cannot half-apply.
    pub fn apply(&mut self, index: u64, signed: &SignedEvent) -> Result<()> {
        // Reaching the event verifies it; there is no accessor that does not.
        let event = signed.event()?;
        let proposer = signed.proposer();

        // The membership this was proposed against must still be the current
        // one. This is what stops a node acting on a view of the group that has
        // moved on — a follower behind on the log finds out here, rather than
        // silently proposing against a group that no longer exists.
        if signed.changed_at() != self.changed_at {
            return Err(ConsensusError::StaleProposal {
                seen: signed.changed_at(),
                current: self.changed_at,
            });
        }

        match event {
            MembershipEvent::GroupFounded { group_id, founders } => {
                self.found(*group_id, founders, proposer)?;
            }

            event => {
                // Everything other than founding requires an established group
                // and a proposer who is currently inside it. This is what keeps
                // the log closed: only members change the membership.
                let group_exists = self.group.is_some();
                if !group_exists {
                    return Err(ConsensusError::NotFounded);
                }
                if !self.members.contains_key(&proposer) {
                    return Err(ConsensusError::ProposerNotAMember { proposer });
                }
                self.authorise(proposer, event)?;
                match event {
                    MembershipEvent::Approved { proposal } => self.approve(*proposal, proposer)?,
                    event => self.submit(index, proposer, event)?,
                }
            }
        }

        // Only a successful apply moves it, which is what makes it a usable
        // comparand for the next proposal.
        self.changed_at = index;
        Ok(())
    }

    /// Whether this proposer may *submit* this event.
    ///
    /// Being a member is enough to submit an admission or an expulsion — §4.4
    /// step 1 says "any member", and step 2 is where the ceremony lives; see
    /// [`Self::approvals_needed`]. Three events are not like that:
    ///
    /// * a pledge is a promise about the proposer's *own* storage, and §5.5
    ///   makes custodian assignment depend on it, so letting anyone rewrite
    ///   anyone else's would let one member move everybody's data;
    /// * the core group is the set of Raft voters, and a non-voter rewriting it
    ///   could remove every voter but themselves;
    /// * an approval *is* §4.4 step 2, so only a core node casts one.
    fn authorise(&self, proposer: MemberId, event: &MembershipEvent) -> Result<()> {
        match event {
            MembershipEvent::PledgeChanged { member, .. } if *member != proposer => {
                Err(ConsensusError::PledgeNotOwn {
                    proposer,
                    member: *member,
                })
            }
            MembershipEvent::CoreGroupChanged { .. } if !self.core.contains_key(&proposer) => {
                Err(ConsensusError::NotCoreMember { proposer })
            }
            MembershipEvent::Approved { .. } if !self.core.contains_key(&proposer) => {
                Err(ConsensusError::ApproverNotCore { approver: proposer })
            }
            _ => Ok(()),
        }
    }

    /// How many core approvals `event` needs before it takes effect.
    ///
    /// One rule: **changing who votes takes a majority of voters, everything
    /// else takes one.** The majority is computed rather than configured, which
    /// §4.4's "configurable quorum" is not — the fold has to reach the same
    /// verdict on every node, so a per-node setting would split the membership.
    /// Configurable means *in the log*, and that needs a policy event the log
    /// does not have; §5.5's weight cap needs exactly the same machinery, so it
    /// gets built once, there.
    ///
    /// Measured against the core group as it stands **now**, not as it stood
    /// when the proposal was made: promoting a fourth voter into a group of
    /// three needs two approvals, not three. Joint consensus makes it arguable
    /// either way, so it is written down.
    pub fn approvals_needed(&self, event: &MembershipEvent) -> usize {
        match event {
            // A pledge is the member's own (P1-20). Nobody else has standing to
            // agree to it, so requiring an approval would mean requiring one
            // from a bystander.
            MembershipEvent::PledgeChanged { .. } => 0,
            event if self.changes_the_voters(event) => self.core.len() / 2 + 1,
            _ => 1,
        }
    }

    /// Whether `approvals` are enough for `event` to take effect.
    ///
    /// **The only place a threshold is decided**, so a proposal that arrives
    /// already approved and one that gets there five entries later are answered
    /// by the same rule rather than by two that have to agree.
    ///
    /// Both sides of the comparison are measured against the core group as it
    /// stands *now*: how many approvals are needed, and which of the ones held
    /// still count. An approval from somebody since demoted or expelled is not
    /// a voter's agreement, and counting it would let a shrinking core carry
    /// decisions on the word of people who have left it.
    fn decided(&self, event: &MembershipEvent, approvals: &BTreeSet<MemberId>) -> bool {
        let voting = approvals
            .iter()
            .filter(|member| self.is_core(member))
            .count();
        voting >= self.approvals_needed(event)
    }

    /// Whether `event` would move the set of Raft voters — as opposed to their
    /// addresses, or the membership around them.
    ///
    /// Written symmetrically: a core group that differs from the current one in
    /// *either* direction changes the voters. Promotion cannot commit yet
    /// (`PromotionUnsupported`), but the rule that will govern it should not
    /// arrive with it.
    fn changes_the_voters(&self, event: &MembershipEvent) -> bool {
        match event {
            MembershipEvent::MemberExpelled { member, .. } => self.is_core(member),
            MembershipEvent::CoreGroupChanged { core } => {
                core.len() != self.core.len()
                    || core.iter().any(|(id, _)| !self.core.contains_key(id))
            }
            _ => false,
        }
    }

    /// Records a submitted change, applying it at once if the approvals it
    /// arrives with are already every approval it needs.
    ///
    /// It arrives with at most one — the proposer's own, and only when they are
    /// a core member, since a follower's agreement does not count toward a
    /// quorum of core nodes. So the two cases this decides between are "takes
    /// one approval, and here it is" and "needs more than it has". Everything
    /// that needs more waits in [`Self::pending`] for the rest to arrive as log
    /// entries of their own, and [`Self::approve`] is what puts them there.
    ///
    /// That a core proposer's own proposal counts as their approval is what
    /// keeps a core operator's `distlib admit` and `distlib expel` a single
    /// step, as they are today: what changes is a *follower* submitting one,
    /// and a core member being removed.
    fn submit(&mut self, index: u64, proposer: MemberId, event: &MembershipEvent) -> Result<()> {
        let mut approvals = BTreeSet::new();
        if self.is_core(&proposer) && target(event) != Some(proposer) {
            approvals.insert(proposer);
        }

        if self.decided(event, &approvals) {
            return self.enact(event);
        }

        // Deliberately not refused as a duplicate of an identical proposal
        // already pending. Two members expelling the same peer at the same time
        // would then split their approvals and neither would reach its
        // threshold — a wart, but a self-healing one, since one more approval
        // on either decides it. Refusing instead would leave a stuck slot that
        // nothing can clear, and clearing it needs the withdrawal event that
        // arrives with its surface in 2.2-2.
        self.pending.insert(
            index,
            Proposal {
                event: event.clone(),
                proposer,
                approvals,
            },
        );
        Ok(())
    }

    /// Records `approver`'s agreement, applying the proposal if that was the
    /// last approval it needed.
    fn approve(&mut self, proposal: u64, approver: MemberId) -> Result<()> {
        // Taken out for the duration. Every path either applies it — in which
        // case it stops being pending — or puts it back exactly as it was,
        // which is what keeps [`Self::apply`]'s promise that a refused entry
        // changes nothing.
        let Some(mut entry) = self.pending.remove(&proposal) else {
            return Err(ConsensusError::UnknownProposal { proposal });
        };

        if let Err(error) = self.still_allowed(&entry, approver) {
            self.pending.insert(proposal, entry);
            return Err(error);
        }

        // Built beside the stored set rather than into it, so the error path
        // below puts back exactly what it took out. The set holds at most one
        // entry per core member, so this is a handful of ids.
        //
        // Inserting is idempotent rather than refused as a repeat, because a
        // proposal can become sufficient with nobody approving it — the core
        // group shrinking lowers the threshold under one already sitting there
        // — and nothing re-examines a pending proposal on its own. An existing
        // approver saying so again is what makes that reachable.
        let mut approvals = entry.approvals.clone();
        approvals.insert(approver);

        if !self.decided(&entry.event, &approvals) {
            entry.approvals = approvals;
            self.pending.insert(proposal, entry);
            return Ok(());
        }

        match self.enact(&entry.event) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.pending.insert(proposal, entry);
                Err(error)
            }
        }
    }

    /// Whether the rules still allow `approver` to approve `entry`.
    ///
    /// Re-checked here rather than only when the proposal was made, because the
    /// group can move in between and that gap is precisely where this kind of
    /// thing goes wrong: the proposer must still be a member, and must still be
    /// allowed to have proposed what they did.
    fn still_allowed(&self, entry: &Proposal, approver: MemberId) -> Result<()> {
        if target(&entry.event) == Some(approver) {
            return Err(ConsensusError::SelfApproval { member: approver });
        }
        if !self.members.contains_key(&entry.proposer) {
            return Err(ConsensusError::ProposerNotAMember {
                proposer: entry.proposer,
            });
        }
        self.authorise(entry.proposer, &entry.event)
    }

    /// Puts a decided change into the state, and drops the pending proposals it
    /// has made stale.
    ///
    /// A proposal about somebody whose membership just changed is answering a
    /// question that has moved: a pending expulsion of a member who has since
    /// been expelled has nothing left to do, and one of a member who has since
    /// been re-admitted was decided against a group they were not in.
    fn enact(&mut self, event: &MembershipEvent) -> Result<()> {
        self.apply_to_founded_group(event)?;
        if let Some(member) = subject(event) {
            self.pending
                .retain(|_, pending| subject(&pending.event) != Some(member));
        }
        Ok(())
    }

    /// The members this node will talk to — the derived allowlist.
    ///
    /// Shaped to feed `distlib_net::allowlist` directly.
    pub fn allowlist(&self) -> impl Iterator<Item = MemberId> + '_ {
        self.members.keys().copied()
    }

    /// The group's identity, once founded.
    pub fn group_id(&self) -> Option<GroupId> {
        self.group
    }

    /// The current Raft voters, each with the address the group records for it.
    pub fn core(&self) -> &BTreeMap<MemberId, NodeAddr> {
        &self.core
    }

    /// Whether `member` is currently a Raft voter.
    pub fn is_core(&self, member: &MemberId) -> bool {
        self.core.contains_key(member)
    }

    /// Changes waiting for approvals, each with the log index that
    /// [`MembershipEvent::Approved`] names it by. Ordered by index, so the
    /// oldest is first.
    pub fn pending(&self) -> impl Iterator<Item = (u64, &Proposal)> + '_ {
        self.pending.iter().map(|(index, entry)| (*index, entry))
    }

    /// The log index this membership last changed at.
    ///
    /// What a proposal is made against: a proposer states the value they saw,
    /// and [`Self::apply`] refuses anything proposed against a superseded one.
    pub fn changed_at(&self) -> u64 {
        self.changed_at
    }

    /// Whether `member` currently belongs to the group.
    pub fn is_member(&self, member: &MemberId) -> bool {
        self.members.contains_key(member)
    }

    /// What is known about one member.
    pub fn member(&self, member: &MemberId) -> Option<&MemberRecord> {
        self.members.get(member)
    }

    /// Every member, ordered by id.
    pub fn members(&self) -> impl Iterator<Item = &MemberRecord> + '_ {
        self.members.values()
    }

    /// How many members there are.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Whether the group has no members — true only before founding.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    fn found(
        &mut self,
        group_id: GroupId,
        founders: &[(MemberRecord, NodeAddr)],
        proposer: MemberId,
    ) -> Result<()> {
        if self.group.is_some() {
            return Err(ConsensusError::AlreadyFounded);
        }
        // Non-empty and duplicate-free. Re-checked here rather than trusted from
        // the constructor, because this event may have arrived from another node.
        check_founders(founders)?;
        // A founder who is not in their own founding set would create a group
        // they are not a member of, and could never propose anything to it.
        if !founders
            .iter()
            .any(|(founder, _)| founder.member_id == proposer)
        {
            return Err(ConsensusError::FounderNotIncluded { proposer });
        }

        self.group = Some(group_id);
        self.members = founders
            .iter()
            .map(|(founder, _)| (founder.member_id, founder.clone()))
            .collect();
        // Founders are the initial voters, at the addresses they founded with;
        // `CoreGroupChanged` moves it from here.
        self.core = founders
            .iter()
            .map(|(founder, addr)| (founder.member_id, addr.clone()))
            .collect();
        Ok(())
    }

    fn apply_to_founded_group(&mut self, event: &MembershipEvent) -> Result<()> {
        match event {
            MembershipEvent::GroupFounded { .. } => Err(ConsensusError::AlreadyFounded),

            MembershipEvent::MemberAdded { member } => {
                // Insert rather than reject-if-present: this is also how an
                // expelled member is re-admitted, and how a record is corrected.
                self.members.insert(member.member_id, member.clone());
                Ok(())
            }

            MembershipEvent::MemberExpelled { member, .. } => {
                if self.members.remove(member).is_none() {
                    return Err(ConsensusError::UnknownMember { member: *member });
                }
                // A non-member cannot be a voter. Leaving them in `core` would
                // leave Raft expecting a vote from someone no longer allowed to
                // connect.
                self.core.remove(member);
                Ok(())
            }

            MembershipEvent::PledgeChanged {
                member,
                pledge_bytes,
            } => {
                let record = self
                    .members
                    .get_mut(member)
                    .ok_or(ConsensusError::UnknownMember { member: *member })?;
                record.pledge_bytes = *pledge_bytes;
                Ok(())
            }

            MembershipEvent::CoreGroupChanged { core } => {
                // Non-empty, and everybody named is a member. A group with no
                // voters can never commit anything again — including the event
                // that would restore its voters — and a voter who is not a
                // member is a vote Raft waits for and can never collect.
                if core.is_empty() || !core.iter().all(|(id, _)| self.members.contains_key(id)) {
                    return Err(ConsensusError::InvalidCoreGroup);
                }
                // A member named twice with two addresses has no single answer,
                // and folding into a map would silently pick one of them.
                let folded: BTreeMap<MemberId, NodeAddr> = core.iter().cloned().collect();
                if folded.len() != core.len() {
                    return Err(ConsensusError::InvalidCoreGroup);
                }
                // Removals and address changes only, for now. Raft's voter set
                // is put into step with this projection by
                // [`crate::raft::core_group`], and it cannot promote: a node
                // serves `distlib/raft/0` only if it started as a voter
                // (P1-30), so a promoted one would count toward quorum and
                // never answer. Refusing the event is what keeps the two from
                // disagreeing — an addition simply never commits.
                if let Some(member) = folded.keys().find(|id| !self.core.contains_key(id)) {
                    return Err(ConsensusError::PromotionUnsupported { member: *member });
                }
                self.core = folded;
                Ok(())
            }

            // Unreachable: `apply` dispatches approvals, and an approval never
            // becomes a pending proposal, so none ever reaches here. Spelled out
            // rather than caught by a wildcard so that adding an event to the
            // enum stays a compile error.
            MembershipEvent::Approved { .. } => Err(ConsensusError::ApprovalIsNotAChange),
        }
    }
}
