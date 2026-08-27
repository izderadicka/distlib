//! The vocabulary of the membership log.
//!
//! Every change to who belongs to the group is one of these events. They are
//! the only Raft state in the system (§4.2); the catalogue and everything else
//! sync by other means.

use distlib_core::{GroupId, MemberId};
use serde::{Deserialize, Serialize};

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
    GroupFounded {
        group_id: GroupId,
        founders: Vec<MemberRecord>,
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

    /// The set of Raft voters changes.
    CoreGroupChanged { core: Vec<MemberId> },
}

impl MembershipEvent {
    /// Builds the founding event, deriving the group id from the founders.
    ///
    /// Derived rather than random so it needs no RNG and can be recomputed by
    /// anyone reading the log: `BLAKE3(tag || n || sorted[ member_id ] || at)`.
    /// The founder set is sorted for the same reason `ItemId` sorts its hashes —
    /// so the value does not depend on the order they were listed in — and `at`
    /// separates two groups founded by the same people.
    pub fn found(founders: Vec<MemberRecord>, at: Timestamp) -> Self {
        let mut ids: Vec<[u8; 32]> = founders
            .iter()
            .map(|record| *record.member_id.as_bytes())
            .collect();
        ids.sort_unstable();

        let mut hasher = blake3::Hasher::new();
        hasher.update(GROUP_ID_TAG);
        hasher.update(&(ids.len() as u64).to_le_bytes());
        for id in &ids {
            hasher.update(id);
        }
        hasher.update(&at.as_millis().to_le_bytes());

        Self::GroupFounded {
            group_id: GroupId::from_bytes(*hasher.finalize().as_bytes()),
            founders,
        }
    }
}
