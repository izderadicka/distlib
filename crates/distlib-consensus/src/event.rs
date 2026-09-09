//! The vocabulary of the membership log.
//!
//! Every change to who belongs to the group is one of these events. They are
//! the only Raft state in the system (§4.2); the catalogue and everything else
//! sync by other means.

use distlib_core::{GroupId, MemberId, NodeAddr};
use serde::{Deserialize, Serialize};

use crate::error::{ConsensusError, Result};

/// Domain tag for deriving a group id, so it cannot collide with an item id or
/// any other BLAKE3 output in the system.
const GROUP_ID_TAG: &[u8] = b"distlib.group.v1";

/// Milliseconds since the Unix epoch.
///
/// **Informational only.** This is the proposing member's clock, which nothing
/// verifies and nothing keeps in step. The order of the log is authoritative
/// for anything that needs ordering — never compare timestamps to decide what
/// happened first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Wraps a count of milliseconds since the Unix epoch.
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Milliseconds since the Unix epoch.
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Reads the local clock.
    ///
    /// Saturates at the epoch if the clock is set before 1970, rather than
    /// panicking: a nonsensical clock should not take a node down, and this
    /// value carries no authority anyway.
    pub fn now() -> Self {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                elapsed.as_millis().min(u64::MAX as u128) as u64
            });
        Self(millis)
    }
}

/// What is known about one member.
///
/// Per delta P0-2 there is no separate `node_id`: v1 defines member and
/// endpoint identity as equal, and Phase 2 introduces `DeviceId` at the point
/// multi-device makes it mean something.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberRecord {
    /// The member's identity, and the key their connections are authenticated by.
    pub member_id: MemberId,

    /// Display name. Metadata, not identity — two members may share one.
    pub display_name: String,

    /// Storage this member commits to providing.
    ///
    /// Lives in the log rather than in gossip because custodian assignment
    /// (§5.5) requires every peer to agree on identical weights; a value that
    /// drifted between peers would give them different custodians for the same
    /// item.
    pub pledge_bytes: u64,
}

/// A change to the group's membership.
///
/// Note what is *not* here: neither `MemberAdded` nor `MemberExpelled` carries
/// the member who proposed it. §4.2 sketched `invited_by` and `proposed_by`
/// fields, but the signing envelope ([`crate::SignedEvent`]) already carries an
/// authenticated `proposer`. Keeping both would let them disagree, with no rule
/// for which one wins — and only the signed one means anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipEvent {
    /// The first entry in every log. Establishes the group and its founders,
    /// who become both the initial members and the initial core group.
    ///
    /// Each founder is paired with the address it is reachable at. This is the
    /// only place the log records an address, and it is not decoration: every
    /// other member is found by id, because gossip and the address book supply
    /// the rest (P1-39) — but the core group is what a node holding no log yet
    /// has to dial in order to *get* one, so those addresses cannot come from
    /// the log's own contents by any other route.
    GroupFounded {
        group_id: GroupId,
        founders: Vec<(MemberRecord, NodeAddr)>,
    },

    /// A new member joins.
    ///
    /// Also the way a previously expelled member is re-admitted; the latest
    /// event wins, and the log keeps the whole history either way.
    MemberAdded { member: MemberRecord },

    /// A member is removed. Every peer drops them from the allowlist and closes
    /// connections already open to them (§4.4).
    MemberExpelled { member: MemberId, reason: String },

    /// A member revises their storage commitment.
    PledgeChanged { member: MemberId, pledge_bytes: u64 },

    /// The set of Raft voters changes, or one of them moves.
    ///
    /// The whole desired core group, addresses included — not a delta. A core
    /// node that changes IP or port is submitted the same way one is added or
    /// removed, because it is the same map either way, and machines get
    /// renumbered far more often than founders get replaced (P1-23).
    CoreGroupChanged { core: Vec<(MemberId, NodeAddr)> },

    /// A core member agrees to a proposal that is waiting for approvals (§4.4).
    ///
    /// `proposal` is the **log index** the proposal was applied at, not its
    /// subject. Two pending proposals about the same person therefore stay
    /// distinguishable, and the index is already the currency here — it is what
    /// [`crate::MembershipState::changed_at`] speaks in.
    ///
    /// Appended, not inserted — see the note at the end of this enum.
    Approved { proposal: u64 },

    /// The member who made a proposal takes it back.
    ///
    /// The deliberate way to clear a pending proposal, as against the automatic
    /// one 2.2-3 adds. **Its proposer alone may withdraw it**, and that is the
    /// whole rule: letting any core member withdraw would hand one of them a
    /// veto over a decision a majority of the others were reaching, which is
    /// precisely what the thresholds exist to prevent. Nobody else has anything
    /// to take back.
    ///
    /// New variants go **here, at the end, and nowhere else.** postcard encodes
    /// an enum variant by its declaration index, so inserting one anywhere above
    /// would renumber every variant after it: entries already written would
    /// deserialise as a different event, and every signature's pre-image would
    /// change. Appending leaves both alone, which is why `SIGNING_DOMAIN` has
    /// not had to move for either of these.
    Withdrawn { proposal: u64 },
}

impl MembershipEvent {
    /// Builds the founding event, deriving the group id from the founders.
    ///
    /// Derived rather than random so it needs no RNG and can be recomputed by
    /// anyone reading the log: `BLAKE3(tag || n || sorted[ member_id ] || at)`.
    /// The founder set is sorted for the same reason `ItemId` sorts its hashes —
    /// so the value does not depend on the order they were listed in — and `at`
    /// separates two groups founded by the same people.
    ///
    /// Fallible because the derivation is only well defined for a set. A
    /// repeated founder would put `n` and the hashed sequence out of step with
    /// the membership the event actually establishes — the state folds founders
    /// into a map, so `[a, a, b]` yields two members but an id derived from
    /// three entries. Two different events would then describe the same group
    /// under different ids.
    pub fn found(founders: Vec<(MemberRecord, NodeAddr)>, at: Timestamp) -> Result<Self> {
        // Checking and sorting are the same pass, and this needs both.
        let ids = checked_ids(&founders)?;

        let mut hasher = blake3::Hasher::new();
        hasher.update(GROUP_ID_TAG);
        hasher.update(&(ids.len() as u64).to_le_bytes());
        for id in &ids {
            // Ids only. An address is not identity: a group re-founded on a
            // different port is the same group, and hashing the address in
            // would say otherwise.
            hasher.update(id.as_bytes());
        }
        hasher.update(&at.as_millis().to_le_bytes());

        Ok(Self::GroupFounded {
            group_id: GroupId::from_bytes(*hasher.finalize().as_bytes()),
            founders,
        })
    }
}

/// The founders' ids, sorted — if the set is a valid one: non-empty, and no
/// member twice.
///
/// One function for both because they are one pass, and because the two callers
/// want the same list for related reasons. [`MembershipEvent::found`] hashes it,
/// so the order is part of the group id; the duplicate rule is a scan of
/// adjacent pairs, which only means anything on a sorted list.
///
/// Sorting `MemberId` sorts by the key's bytes — `iroh::PublicKey` compares
/// `as_bytes()`, and the newtype's derived `Ord` delegates to it — which is what
/// lets one list serve a byte-oriented hash and an equality scan alike.
fn checked_ids(founders: &[(MemberRecord, NodeAddr)]) -> Result<Vec<MemberId>> {
    if founders.is_empty() {
        return Err(ConsensusError::NoFounders);
    }

    let mut ids: Vec<MemberId> = founders
        .iter()
        .map(|(record, _)| record.member_id)
        .collect();
    ids.sort_unstable();
    if let Some(pair) = ids.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(ConsensusError::DuplicateFounder { member: pair[0] });
    }
    Ok(ids)
}

/// The rule a founder set must satisfy, for the caller that wants only the
/// verdict.
///
/// Checked in two places on purpose — [`MembershipEvent::found`] so a locally
/// built event cannot be malformed, and again on apply, because an event
/// arriving from another node is not ours to trust.
pub(crate) fn check_founders(founders: &[(MemberRecord, NodeAddr)]) -> Result<()> {
    checked_ids(founders).map(|_| ())
}
