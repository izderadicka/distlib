//! What each JSON-RPC method does.
//!
//! Method names come from §7.1 verbatim, so phase 3 extends this set rather
//! than renaming it: `library.*` and the SSE stream land beside these, and a
//! caller written against `group.members` today keeps working.

use std::{collections::BTreeMap, sync::Arc};

use distlib_consensus::{MemberRecord, MembershipEvent, MembershipNode, MembershipState};
use distlib_core::{MemberId, NetConfig, NodeAddr, Ticket};
use iroh::SecretKey;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::rpc::Error;

/// Everything the methods need: the running node and the key it signs with.
///
/// The node is shared rather than owned: whoever started it keeps serving the
/// group with it while this answers questions about it.
pub struct Api {
    pub node: Arc<MembershipNode>,
    pub secret: SecretKey,
    /// How this node reaches the network.
    ///
    /// Needed for `group.ticket`: a joiner has to reach the group the way this
    /// node does, so the directions have to carry it.
    pub net: NetConfig,
}

impl Api {
    /// Dispatches one call.
    pub async fn call(&self, method: &str, params: Option<Value>) -> Result<Value, Error> {
        match method {
            "node.status" => self.status(),
            "group.members" => self.members(),
            "group.propose_add" => self.propose_add(parse(params)?).await,
            "group.propose_expel" => self.propose_expel(parse(params)?).await,
            "group.propose_core" => self.propose_core(parse(params)?).await,
            "group.pending" => self.pending(),
            "group.approve" => self.approve(parse(params)?).await,
            "group.withdraw" => self.withdraw(parse(params)?).await,
            "group.pledge_set" => self.pledge_set(parse(params)?).await,
            "group.ticket" => self.ticket(),
            other => Err(Error::method_not_found(other)),
        }
    }

    /// `node.status` — who this node is and where it stands in its group.
    fn status(&self) -> Result<Value, Error> {
        let membership = self.node.membership();
        let me = self.node.id();

        // Only a voter has a Raft to report on. A follower answers `null` for
        // both rather than inventing a state, since "this node is following"
        // is a different thing from "this node is a follower of a term".
        let (raft, leader) = match self.node.raft() {
            Some(raft) => {
                let metrics = raft.metrics();
                let metrics = metrics.borrow();
                (
                    Some(format!("{:?}", metrics.state)),
                    metrics
                        .current_leader
                        .and_then(|id| MemberId::try_from(id).ok()),
                )
            }
            None => (None, None),
        };

        Ok(json!({
            "member": me,
            "group": membership.group_id(),
            // Derived, not configured — the log decides who votes.
            "core": membership.is_core(&me),
            "members": membership.len(),
            "core_group": membership.core().keys().collect::<Vec<_>>(),
            // The log index this membership last changed at: what a proposal is
            // checked against, so a caller can see whether it is looking at a
            // current view.
            "changed_at": membership.changed_at(),
            "raft": raft,
            "leader": leader,
            // How far a follower has read the log. Null on a voter, which gets
            // the log pushed to it rather than fetching it.
            "followed_upto": (!self.node.is_core()).then(|| self.node.followed_upto()),
            // A count, not a listing. Status is a summary and every other field
            // in it is one line; `group.pending` is where the detail lives.
            "pending": membership.pending().count(),
        }))
    }

    /// `group.ticket` — directions for somebody who has been admitted (§4.3).
    ///
    /// Built here rather than by the caller because the addresses come from
    /// Raft's own membership, which only a running node holds. The relay
    /// settings come from this node's configuration: a joiner has to reach the
    /// group the same way this node does.
    ///
    /// Not a credential. Anyone may ask for one, and holding it grants nothing
    /// — admission is a committed `MemberAdded`, and until that exists a
    /// ticket-holder is refused at the allowlist like anybody else.
    fn ticket(&self) -> Result<Value, Error> {
        let membership = self.node.membership();
        let group = membership
            .group_id()
            .ok_or_else(|| Error::failed("this node is in no group yet"))?;

        let ticket = Ticket {
            group,
            core: self.node.core_addresses(),
            relay_mode: self.net.relay_mode,
            relay_urls: self.net.relay_urls.clone(),
        };

        Ok(json!({ "ticket": ticket.to_string(), "group": group }))
    }

    /// `group.members` — the membership as this node has it.
    fn members(&self) -> Result<Value, Error> {
        let membership = self.node.membership();
        let members: Vec<Value> = membership
            .members()
            .map(|record| {
                json!({
                    "member": record.member_id,
                    "name": record.display_name,
                    "pledge_bytes": record.pledge_bytes,
                    "core": membership.is_core(&record.member_id),
                })
            })
            .collect();

        Ok(json!({
            "group": membership.group_id(),
            "changed_at": membership.changed_at(),
            "members": members,
        }))
    }

    /// `group.pending` — the changes waiting for approvals (§4.4 step 2).
    ///
    /// `needed` and `approvals` are both measured against the core group as it
    /// stands now — `approvals` counts only those from members who are still
    /// voters — which is what the fold will do when the next approval lands.
    /// So a proposal can be one approval away today and two away tomorrow, and
    /// this reports what is true when asked rather than what was true when it
    /// was proposed.
    fn pending(&self) -> Result<Value, Error> {
        let membership = self.node.membership();
        let pending: Vec<Value> = membership
            .pending()
            .map(|(proposal, entry)| {
                json!({
                    "proposal": proposal,
                    "proposer": entry.proposer(),
                    "what": describe(entry.event(), &membership),
                    "approvals": membership.approvals_counting(entry).collect::<Vec<_>>(),
                    "needed": membership.approvals_needed(entry.event()),
                    // A count of further *changes*, not a duration: it only
                    // moves when the group commits something. Named so a
                    // caller cannot read it as time.
                    "expires_after_changes": membership.expires_after(proposal),
                })
            })
            .collect();

        Ok(json!({
            "changed_at": membership.changed_at(),
            "pending": pending,
        }))
    }

    /// `group.approve` — agree to a pending proposal (§4.4 step 2).
    ///
    /// **Answers about the proposal, not about the approval.** An approval is
    /// never itself a proposal — the fold dispatches it rather than holding it —
    /// so asking after the approval's own log index would always answer
    /// "applied", which is true of the approval and says nothing about the
    /// thing it was cast on. The caller wants to know whether the change has
    /// happened yet, and that is a question about the proposal's index.
    async fn approve(&self, params: Proposal) -> Result<Value, Error> {
        self.commit(MembershipEvent::Approved {
            proposal: params.proposal,
        })
        .await?;
        Ok(self.outcome(params.proposal))
    }

    /// `group.withdraw` — take back a proposal of your own.
    ///
    /// No member parameter, for the same reason `group.pledge_set` has none:
    /// only the proposer may withdraw, so accepting one would only produce
    /// proposals the group refuses.
    async fn withdraw(&self, params: Proposal) -> Result<Value, Error> {
        self.commit(MembershipEvent::Withdrawn {
            proposal: params.proposal,
        })
        .await?;
        // Not `outcome`: a withdrawn proposal is not pending, and reporting
        // that as "applied" would say the change had taken effect when the
        // point of withdrawing was that it will not.
        Ok(json!({
            "changed_at": self.node.membership().changed_at(),
            "proposal": params.proposal,
            "withdrawn": true,
        }))
    }

    /// `group.propose_add` — admit a member (§4.3).
    async fn propose_add(&self, params: ProposeAdd) -> Result<Value, Error> {
        self.propose(MembershipEvent::MemberAdded {
            member: MemberRecord {
                member_id: params.member,
                display_name: params.name.unwrap_or_default(),
                // Theirs to set, not ours: §5.5 makes custodian assignment
                // depend on it, and `PledgeChanged` is self-only for that
                // reason. Admitting somebody does not speak for their storage.
                pledge_bytes: 0,
            },
        })
        .await
    }

    /// `group.propose_expel` — remove a member (§4.4).
    async fn propose_expel(&self, params: ProposeExpel) -> Result<Value, Error> {
        self.propose(MembershipEvent::MemberExpelled {
            member: params.member,
            reason: params.reason,
        })
        .await
    }

    /// `group.propose_core` — move, add or drop one core node (§4.2).
    ///
    /// **A delta, though the event is not.** [`MembershipEvent::CoreGroupChanged`]
    /// carries the whole desired core group, because a node map is what openraft
    /// is handed; but building that map is a read-modify-write, and where it
    /// happens decides whether it is safe. Done here, the read and the signature
    /// are the same node's, one await apart, and [`ConsensusError::StaleProposal`]
    /// covers the gap — the event is signed against the `changed_at` the map was
    /// read at, so a change that landed in between refuses this one rather than
    /// silently reverting it. A caller that assembled the map itself would sign
    /// against a `changed_at` newer than its own read, and that guard would pass
    /// while the map put back an address, a voter, or a demotion the group had
    /// just decided.
    ///
    /// So the caller names one member and says which of two things should
    /// become of them — `"change": "set"` with an address, or
    /// `"change": "remove"` — and never has to know where the others are.
    ///
    /// An address given replaces whatever the log holds rather than adding to
    /// it: a node that moved is not at both addresses, and a stale one left
    /// behind is a path every peer keeps trying. Removal is a demotion, not an
    /// expulsion; they stay a member.
    ///
    /// [`ConsensusError::StaleProposal`]: distlib_consensus::ConsensusError::StaleProposal
    async fn propose_core(&self, params: ProposeCore) -> Result<Value, Error> {
        let membership = self.node.membership();
        let mut core = membership.core().clone();

        // Either arm refuses a request that would change nothing, rather than
        // committing it as a no-op. The event would apply — the map is valid —
        // and applying it moves `changed_at`, which invalidates every proposal
        // in flight. Nothing should be able to do that by asking for something
        // that was already true.
        match params {
            ProposeCore::Set { member, addr } => {
                if core.get(&member) == Some(&addr) {
                    return Err(Error::invalid_params(format!(
                        "{member} is already a core node at that address"
                    )));
                }
                core.insert(member, addr);
            }
            ProposeCore::Remove { member } => {
                if core.remove(&member).is_none() {
                    return Err(Error::invalid_params(format!(
                        "{member} is not a core node"
                    )));
                }
            }
        }

        self.propose(MembershipEvent::CoreGroupChanged {
            core: core.into_iter().collect(),
        })
        .await
    }

    /// `group.pledge_set` — set *this* node's storage pledge.
    ///
    /// No member parameter, and that is the rule rather than a simplification:
    /// a pledge may only be set by the member it belongs to, so accepting one
    /// would only produce proposals the group refuses.
    async fn pledge_set(&self, params: PledgeSet) -> Result<Value, Error> {
        self.propose(MembershipEvent::PledgeChanged {
            member: self.node.id(),
            pledge_bytes: params.bytes,
        })
        .await
    }

    /// Commits an event, and reports what became of it.
    ///
    /// **`applied` is the field that matters**, and the reason this returns
    /// more than it used to: since 2.2-1 a committed proposal may be waiting
    /// for core approvals rather than in effect, and a caller told only
    /// "committed" would report success for something that has not happened
    /// yet. The entry's own log index answers it — a proposal still in
    /// `pending` under that index is waiting — which is exact where matching on
    /// the event's content would not be, since two proposals can say the same
    /// thing.
    ///
    /// `changed_at` stays for the caller about to propose again: it is the view
    /// their next proposal will be checked against.
    async fn propose(&self, event: MembershipEvent) -> Result<Value, Error> {
        let proposal = self.commit(event).await?;
        Ok(self.outcome(proposal))
    }

    /// Commits an event, answering with the log index it was applied at.
    async fn commit(&self, event: MembershipEvent) -> Result<u64, Error> {
        self.node
            .propose(event, &self.secret)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Where the proposal at `proposal` now stands.
    ///
    /// One function for both the caller who just made it and the caller who
    /// just approved it, because they are asking the same question — has this
    /// change happened yet, and if not what is it waiting for — and two
    /// implementations of it would be free to disagree.
    fn outcome(&self, proposal: u64) -> Value {
        let membership = self.node.membership();
        let waiting = membership
            .pending()
            .find(|(index, _)| *index == proposal)
            .map(|(_, entry)| {
                json!({
                    "approvals": membership.approvals_counting(entry).count(),
                    "needed": membership.approvals_needed(entry.event()),
                })
            });

        json!({
            "changed_at": membership.changed_at(),
            "proposal": proposal,
            "applied": waiting.is_none(),
            "waiting": waiting,
        })
    }
}

/// A one-line description of a proposal, for an operator deciding about it.
///
/// Rendered here rather than at the CLI because the API is the surface a
/// caller writes against, and a caller that had to match on the event shape to
/// print it would be reimplementing this.
///
/// Takes the membership because one event cannot be read without it.
/// [`MembershipEvent::CoreGroupChanged`] carries the whole desired core group
/// rather than a delta (P1-23), so printing its contents tells an approver who
/// would be left and not who would go — and a demotion is the only core-group
/// change that ever waits for approval, which makes that the one case this has
/// to get right. Diffed against the core group *as it stands now*, which is
/// also what the fold will compare against if the next approval decides it.
fn describe(event: &MembershipEvent, membership: &MembershipState) -> String {
    match event {
        MembershipEvent::MemberAdded { member } => {
            format!("admit {} ({})", member.member_id, member.display_name)
        }
        MembershipEvent::MemberExpelled { member, reason } => {
            format!("expel {member}: {reason}")
        }
        MembershipEvent::CoreGroupChanged { core } => describe_core(core, membership),
        MembershipEvent::PledgeChanged {
            member,
            pledge_bytes,
        } => format!("set the pledge of {member} to {pledge_bytes} bytes"),
        // Neither is ever a pending proposal — both are dispatched by the fold
        // rather than held — so this is unreachable rather than a real case.
        MembershipEvent::GroupFounded { group_id, .. } => format!("found group {group_id}"),
        MembershipEvent::Approved { proposal } => format!("approve {proposal}"),
        MembershipEvent::Withdrawn { proposal } => format!("withdraw {proposal}"),
        // The kind, never the secret. This string is rendered to anyone
        // holding the API token and printed by `distlib pending`; the key to
        // the group's catalogue belongs in neither.
        MembershipEvent::NamespaceCreated { kind, .. } => format!("create the {kind} namespace"),
    }
}

/// What a proposed core group would change, said as changes.
///
/// Three kinds, and all three are listed rather than only the first, because a
/// map can say more than one thing at once and an approver agreeing to "drop
/// bob" should not find they also agreed to move carol. A move carries the
/// address it would move them to for the same reason: it is the whole content
/// of that change, and a line saying only "move carol" asks somebody to agree
/// to something it has not told them.
///
/// Falls back to naming the whole group when it would change nothing — which is
/// not reachable through `group.propose_core`, since that refuses a no-op, but
/// is reachable by anybody proposing the event directly, and "" is not an
/// answer.
fn describe_core(proposed: &[(MemberId, NodeAddr)], membership: &MembershipState) -> String {
    let now = membership.core();
    let wanted: BTreeMap<MemberId, &NodeAddr> = proposed
        .iter()
        .map(|(member, addr)| (*member, addr))
        .collect();

    let mut changes: Vec<String> = Vec::new();
    changes.extend(
        now.keys()
            .filter(|member| !wanted.contains_key(member))
            .map(|member| format!("drop {member} from the core group")),
    );
    changes.extend(
        wanted
            .iter()
            .filter_map(|(member, addr)| match now.get(member) {
                None => Some(format!("add {member} to the core group")),
                Some(was) if was != *addr => Some(format!(
                    "move {member} to {}",
                    addr.direct
                        .iter()
                        .map(ToString::to_string)
                        .chain(addr.relay.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
                Some(_) => None,
            }),
    );

    if changes.is_empty() {
        return format!("leave the core group at its {} members", now.len());
    }
    changes.join(", ")
}

/// `deny_unknown_fields` throughout: a caller passing a parameter a method does
/// not have has misunderstood something, and silence would let them believe it
/// took effect. `group.pledge_set` is the sharp case — it takes no `member`,
/// because a pledge belongs to whoever sets it, and quietly ignoring one would
/// look exactly like setting somebody else's.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProposeAdd {
    member: MemberId,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProposeExpel {
    member: MemberId,
    reason: String,
}

/// What `group.propose_core` is asked to do: one member, and which of the two
/// things should become of them.
///
/// **Tagged, rather than inferred from whether an address was given.** The
/// shape this replaced was a single `addr: Option<NodeAddr>` where `null` meant
/// "drop them" — one field answering two unrelated questions, *where are they*
/// and *should they vote*, so the difference between moving a node and demoting
/// it was a value rather than a word. It also needed a custom deserialiser to
/// stop serde reading a **missing** `addr` as `None`, which is to say: a
/// forgotten field would have demoted a voter, and the only thing standing in
/// the way was a helper somebody had to remember to keep. A tag makes that
/// unrepresentable instead of guarded against, and it reads the same way the
/// two CLI verbs do.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "change", rename_all = "snake_case", deny_unknown_fields)]
enum ProposeCore {
    /// Where to reach `member` once they vote — moving them if they are
    /// already a core node, adding them if they are not.
    Set { member: MemberId, addr: NodeAddr },

    /// Stop counting `member` as a voter. They stay a member.
    Remove { member: MemberId },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PledgeSet {
    bytes: u64,
}

/// What `group.approve` and `group.withdraw` name: the log index a proposal was
/// made at, which is what `group.pending` reports and what the log itself
/// speaks in.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Proposal {
    proposal: u64,
}

/// Reads the params a method expects, or says what was wrong with them.
fn parse<T: for<'de> Deserialize<'de>>(params: Option<Value>) -> Result<T, Error> {
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|error| Error::invalid_params(error.to_string()))
}
