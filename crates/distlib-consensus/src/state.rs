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

use distlib_core::{GroupId, MemberId, Namespace, NamespaceSecret, NodeAddr};
use serde::{Deserialize, Serialize};

use crate::{
    error::{ConsensusError, Result},
    event::{MemberRecord, MembershipEvent, check_founders},
    signed::SignedEvent,
};

/// How far the log may advance past a proposal before it stops waiting.
///
/// Counted in log entries rather than in time. Not because a timestamp could
/// not be folded — `at` is signed, so every node reads the same bytes and would
/// reach the same verdict — but because nothing *verifies* it (P1-3). Expiring
/// on time needs a "now", and the fold has none: the only candidate is the
/// timestamp of whatever entry is being applied, which is equally self-reported.
/// So one member whose clock is a year fast would sweep the whole pending set
/// the moment they committed anything. That is not an attack — §2 assumes
/// members do not attack the protocol — it is a misconfiguration, and
/// misconfigured clocks are ordinary. Log position carries no such blast radius,
/// and compaction neither renumbers nor reuses it.
///
/// **What this does and does not fix.** It bounds the map — which is re-encoded
/// into redb on every apply, so unbounded growth is write amplification on the
/// hot path, not just memory — and it clears a slot whose proposer has gone away
/// without withdrawing it. It does *not* make a proposal expire after any
/// amount of *time*: in a quiet group a year-old proposal may be three entries
/// old and will still be waiting. Nothing available fixes that, and pretending
/// otherwise would be worse than saying so.
///
/// A hundred and twenty-eight, because a contested expulsion in a five-voter
/// group is four entries, so this is thirty-odd governance decisions. A proposal
/// that has watched that many go past is not under discussion any more.
///
/// **One number for every group is known to be wrong, and wrong in opposite
/// directions at the two ends.** A group with heavy membership churn burns this
/// quickly, so a genuine deliberation can be swept while it is still being had;
/// a settled group of three friends may never reach it, so the abandoned slot
/// this exists to clear is never cleared for them. Deferred rather than guessed
/// at, and recorded against P2-6 under "Carried out of Phase 2" in
/// `docs/plan-phases/phase-2-catalogue.md` — nobody has yet watched a real
/// group's membership-event rate, so choosing a better number now would be
/// guessing with extra steps.
///
/// Not refreshed by approvals. A proposal that cannot gather its threshold
/// within this much group activity is not going to, and a rule that could be
/// held open indefinitely by one member approving periodically would be a
/// slower version of the thing being fixed.
pub const PENDING_EXPIRY: u64 = 128;

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
    /// The document namespaces the group has, and the key to each.
    ///
    /// Part of the membership projection rather than a store of its own
    /// because it arrives the same way membership does — committed to the log,
    /// folded identically on every node — and because the same rule decides
    /// who may read it: whoever may read the log. `distlib-sync` opens a
    /// replica from what is here; nothing else reads it.
    namespaces: BTreeMap<Namespace, NamespaceSecret>,
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

    /// Who has approved it so far — everyone who ever did.
    ///
    /// Not all of these necessarily count: see
    /// [`MembershipState::approvals_counting`] for the ones that decide it.
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

/// What an event is *about*, for deciding which pending proposals conflict with
/// it and which a change has made stale.
///
/// Wider than [`target`] in two directions: an admission is about somebody too,
/// and the core group is a subject in its own right even though it is not a
/// member. That second one is the whole of what 2.2-3 needed — before it, a
/// pending `CoreGroupChanged` was the one kind of proposal nothing could ever
/// conflict with and nothing could ever clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Subject {
    Member(MemberId),
    CoreGroup,
}

fn subject(event: &MembershipEvent) -> Option<Subject> {
    match event {
        MembershipEvent::MemberAdded { member } => Some(Subject::Member(member.member_id)),
        MembershipEvent::MemberExpelled { member, .. } => Some(Subject::Member(*member)),
        MembershipEvent::CoreGroupChanged { .. } => Some(Subject::CoreGroup),
        // A pledge needs no approvals and so never waits; founding, approvals
        // and withdrawals are not proposals at all.
        _ => None,
    }
}

/// Whether the core group just enacted leaves `proposed` unsafe to approve.
///
/// A pending `CoreGroupChanged` carries the *whole* desired core group rather
/// than a delta (P1-23), so approving it later writes its map wholesale. That
/// is only safe where the map still agrees with the change just made:
/// anywhere it does not, approving it would silently revert that change. With
/// `relay_mode = "disabled"` the worst case is putting a moved core node's
/// dead address back, carrying a majority's signature — the failure 2.1
/// existed to close. So staleness wins ties, and the caller says out loud
/// which proposal it dropped.
///
/// **The rule: a map is stale iff it names a member the change moved, at
/// anything other than where the change put them.** Judged only over the
/// members the change actually moved, because the rest of the map is nobody's
/// business here.
///
/// Absence is the case worth spelling out, because it is what the blunt rule
/// this replaced got wrong. A map that does not name somebody is proposing to
/// *demote* them, and that is compatible with any change to where they are —
/// approving it does not revert the move, it makes the move moot. So omission
/// never counts against a map, and a demotion keeps the approvals it has
/// gathered while the member it is about changes address underneath it.
fn superseded_by(
    proposed: &[(MemberId, NodeAddr)],
    before: &BTreeMap<MemberId, NodeAddr>,
    now: &BTreeMap<MemberId, NodeAddr>,
) -> bool {
    let proposed: BTreeMap<_, _> = proposed.iter().map(|(id, addr)| (id, addr)).collect();
    before.keys().chain(now.keys()).any(|member| {
        before.get(member) != now.get(member)
            && proposed
                .get(member)
                .is_some_and(|wants| Some(*wants) != now.get(member))
    })
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
                    MembershipEvent::Withdrawn { proposal } => {
                        self.withdraw(*proposal, proposer)?
                    }
                    event => self.submit(index, proposer, event)?,
                }
            }
        }

        // Only a successful apply moves it, which is what makes it a usable
        // comparand for the next proposal.
        self.changed_at = index;

        // Then drop whatever the log has now left behind. After the dispatch
        // above rather than before it, so a refused entry changes nothing; the
        // consequence is that the last index at which a proposal can still be
        // approved is exactly `proposal + PENDING_EXPIRY` — an approval landing
        // there is dispatched, and only then is the proposal swept.
        self.pending
            .retain(|proposal, _| index.saturating_sub(*proposal) < PENDING_EXPIRY);
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
            // The same rule as the core group's, for the same reason: this is
            // the group deciding something about itself rather than about one
            // member, and the core group is who decides those.
            MembershipEvent::NamespaceCreated { .. } if !self.core.contains_key(&proposer) => {
                Err(ConsensusError::NamespaceNotCore { proposer })
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
        self.counting(approvals).count() >= self.approvals_needed(event)
    }

    /// The approvals on `proposal` that still count toward its threshold.
    ///
    /// The same set [`Self::decided`] measures, exposed because anything
    /// showing an operator "1 of 2" has to show the same 1. Reporting the raw
    /// set beside a threshold computed from the current core group produces
    /// sentences like "5 of 2, still waiting", which reads as a broken group
    /// rather than as four approvals from people who have since left it.
    ///
    /// While a proposal is pending this is always fewer than
    /// [`Self::approvals_needed`] — a proposal with enough is not pending.
    pub fn approvals_counting<'a>(
        &'a self,
        proposal: &'a Proposal,
    ) -> impl Iterator<Item = MemberId> + 'a {
        self.counting(&proposal.approvals)
    }

    /// Those of `approvals` given by members who are core *now*.
    fn counting<'a>(
        &'a self,
        approvals: &'a BTreeSet<MemberId>,
    ) -> impl Iterator<Item = MemberId> + 'a {
        approvals
            .iter()
            .copied()
            .filter(move |member| self.is_core(member))
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

        // One waiting proposal per subject. Two about the same thing split the
        // approvals they need and neither reaches its threshold, and the shape
        // that really bites is one member asking over and over: each attempt
        // makes a fresh entry and spreads the approvals thinner.
        //
        // Refused rather than superseded, because superseding would let anybody
        // discard the approvals a proposal had gathered by proposing again.
        // That leaves a slot only its proposer can clear — which is what
        // `Withdrawn` is for, and what [`PENDING_EXPIRY`] is for when the
        // proposer has gone away.
        if let Some(proposed_about) = subject(event)
            && let Some((waiting, _)) = self
                .pending
                .iter()
                .find(|(_, pending)| subject(&pending.event) == Some(proposed_about))
        {
            return Err(ConsensusError::AlreadyPending { proposal: *waiting });
        }

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

    /// Takes back a proposal, at the request of the member who made it.
    ///
    /// **The authorisation lives here rather than in [`Self::authorise`]**, and
    /// deliberately: the rule is "you proposed it", which needs the pending map
    /// to check, and `authorise` sees only the event. Leaving it to
    /// `authorise`'s catch-all would let any member withdraw anybody's
    /// proposal — the veto that [`MembershipEvent::Withdrawn`] exists *not* to
    /// be.
    fn withdraw(&mut self, proposal: u64, member: MemberId) -> Result<()> {
        let Some(entry) = self.pending.get(&proposal) else {
            return Err(ConsensusError::UnknownProposal { proposal });
        };
        if entry.proposer != member {
            return Err(ConsensusError::NotTheProposer { member, proposal });
        }
        self.pending.remove(&proposal);
        Ok(())
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
    ///
    /// The core group is judged differently, by [`superseded_by`]: it is the
    /// one subject where two proposals routinely coexist, because a
    /// voter-changing map waits for a majority while an address-only one
    /// enacts on a single approval and sails straight past it. Dropping every
    /// pending core proposal whenever the map moved at all discarded approvals
    /// nobody had withdrawn, so the pending map is now compared against the
    /// change rather than assumed stale by it.
    fn enact(&mut self, event: &MembershipEvent) -> Result<()> {
        // Kept across the change: what superseded a pending core map is the
        // difference between these two, not either one alone.
        let before = self.core.clone();
        self.apply_to_founded_group(event)?;

        let about = subject(event);
        let (core, mut dropped) = (&self.core, Vec::new());
        self.pending.retain(|index, pending| {
            let keep = match &pending.event {
                MembershipEvent::CoreGroupChanged { core: proposed } => {
                    !superseded_by(proposed, &before, core)
                }
                // Same subject: answering a question that has moved.
                other => subject(other) != about,
            };
            if !keep {
                dropped.push((*index, pending.proposer));
            }
            keep
        });
        for (proposal, proposer) in dropped {
            tracing::info!(
                proposal,
                %proposer,
                "the change just made supersedes a pending proposal; dropping it"
            );
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

    /// How many further changes the proposal at `proposal` will still be
    /// waiting after.
    ///
    /// Zero means the next entry sweeps it. Deliberately a count of *changes*
    /// rather than of anything time-like: it only moves when the group commits
    /// something, so a proposal can sit at the same number for a month and then
    /// go in an afternoon. Anything displaying it should say so.
    pub fn expires_after(&self, proposal: u64) -> u64 {
        PENDING_EXPIRY.saturating_sub(self.changed_at.saturating_sub(proposal) + 1)
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
    /// The key to one of the group's namespaces, once it has that one.
    ///
    /// `None` before the namespace is created — which is every group founded
    /// before this event existed, and any founding that stopped between the
    /// two entries it writes. A core node is what fixes that, by proposing
    /// one; nothing here can, because a fold may not invent a secret.
    pub fn namespace(&self, kind: Namespace) -> Option<&NamespaceSecret> {
        self.namespaces.get(&kind)
    }

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
                //
                // The pledge survives that, because it is the one field of the
                // record its own member owns: `PledgeChanged` is self-only
                // (P1-20) precisely so nobody sets somebody else's, and
                // overwriting it from a `MemberAdded` — which always carries
                // the zero `propose_add` fills in — is that same write by
                // another door. Correcting a display name would otherwise
                // retract a storage promise §5.5's custodian assignment reads.
                // Re-admitting an expelled member finds no record and so
                // starts them at zero, which is the intent there.
                let record = MemberRecord {
                    pledge_bytes: self
                        .members
                        .get(&member.member_id)
                        .map_or(member.pledge_bytes, |had| had.pledge_bytes),
                    ..member.clone()
                };
                self.members.insert(member.member_id, record);
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
                // Additions allowed since 2.3-2. Until then this refused them
                // with `PromotionUnsupported`, because a node served
                // `distlib/raft/0` only if it had started as a voter (P1-30) —
                // so a promoted one would have counted toward quorum and never
                // answered. Both halves of that have moved: every node serves
                // the protocol now, and [`crate::raft::core_group`] adds a new
                // voter as a learner first and promotes it only once it has
                // caught up.
                self.core = folded;
                Ok(())
            }

            MembershipEvent::NamespaceCreated { kind, secret } => {
                // First one wins, rather than the last. Approving a second
                // secret for the same kind would not replace the namespace so
                // much as abandon it: every node would open a fresh, empty
                // replica and whatever the group had written would still exist
                // and no longer be anybody's catalogue.
                if self.namespaces.contains_key(kind) {
                    return Err(ConsensusError::NamespaceExists { kind: *kind });
                }
                self.namespaces.insert(*kind, secret.clone());
                Ok(())
            }

            // Unreachable: `apply` dispatches both, and neither ever becomes a
            // pending proposal, so neither reaches here. Spelled out rather than
            // caught by a wildcard so that adding an event to the enum stays a
            // compile error.
            MembershipEvent::Approved { .. } | MembershipEvent::Withdrawn { .. } => {
                Err(ConsensusError::NotAMembershipChange)
            }
        }
    }
}
