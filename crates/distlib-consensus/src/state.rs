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

use distlib_core::{GroupId, MemberId};
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
    core: BTreeSet<MemberId>,
}

impl MembershipState {
    /// An empty state, before any event has been applied.
    pub fn new() -> Self {
        Self::default()
    }

    /// Verifies `signed` and folds it in.
    ///
    /// Verification happens here rather than at the point the entry arrived, so
    /// there is no path into the state that skips it.
    ///
    /// On error the state is left untouched: every rule is checked before
    /// anything is mutated, so a rejected event cannot half-apply.
    pub fn apply(&mut self, signed: &SignedEvent) -> Result<()> {
        // Reaching the event verifies it; there is no accessor that does not.
        let event = signed.event()?;
        let proposer = signed.proposer();

        match event {
            MembershipEvent::GroupFounded { group_id, founders } => {
                self.found(*group_id, founders, proposer)
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
                self.apply_to_founded_group(event)
            }
        }
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

    /// The current Raft voters.
    pub fn core(&self) -> &BTreeSet<MemberId> {
        &self.core
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
        founders: &[MemberRecord],
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
        if !founders.iter().any(|founder| founder.member_id == proposer) {
            return Err(ConsensusError::FounderNotIncluded { proposer });
        }

        self.group = Some(group_id);
        self.members = founders
            .iter()
            .map(|founder| (founder.member_id, founder.clone()))
            .collect();
        // Founders are the initial voters; `CoreGroupChanged` moves it from here.
        self.core = self.members.keys().copied().collect();
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
                if core.is_empty() || !core.iter().all(|id| self.members.contains_key(id)) {
                    return Err(ConsensusError::InvalidCoreGroup);
                }
                self.core = core.iter().copied().collect();
                Ok(())
            }
        }
    }
}
