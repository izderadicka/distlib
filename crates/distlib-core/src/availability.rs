//! What a member says about itself every minute: that it is up, where it is,
//! and which items it holds — phase 4's heartbeat (D1–D3).
//!
//! Only the pure parts: the signed statement, its size guard, and the list of
//! held items it points at. Sending, receiving and the clocks around them are
//! `distlib-sync`'s, the same split [`SignedAddress`] and its announcer have.
//!
//! **Availability is never replicated.** Nothing here is written to the
//! document, the read model or the log; a receiver works it out in memory and
//! forgets it when it stops (§5.6).

use iroh::{SecretKey, Signature};
use serde::{Deserialize, Serialize};

use crate::{
    addr::SignedAddress,
    error::{CoreError, Result},
    id::{ContentHash, GroupId, ItemId, MemberId},
};

/// The largest message any gossip topic in the workspace carries, framing
/// included.
///
/// **A wire-compatibility setting, not a tuning knob** (phase 4, ground truth
/// 3). A receiver drops the connection on a frame above its own limit, and the
/// disconnect reaches every topic on it — membership and the catalogue, not
/// just heartbeats. So every `Gossip` in the process is built with this, and a
/// raise after a release has to reach every receiver before any sender uses it.
/// iroh-gossip's default is 4 KiB; this is raised while nothing is released.
pub const GOSSIP_MAX_MESSAGE: usize = 16 * 1024;

/// The most an encoded heartbeat may be.
///
/// A gossip frame wraps it in the topic, the message id and a few enum tags —
/// about a hundred bytes — and this leaves several times that, so a heartbeat
/// that passes here is never the frame that breaks a connection.
pub const HEARTBEAT_MAX: usize = GOSSIP_MAX_MESSAGE - 512;

/// How many ids `added` and `removed` may hold together: 12 KiB of them.
///
/// The rest of a heartbeat is about 720 B, with room in the cap for an address
/// of sixty-odd sockets. A node whose next change would pass this publishes a
/// new base list first (D5), which empties the delta.
pub const DELTA_MAX: usize = 12 * 1024 / 32;

/// The most ids a base list may hold — 320 MB of them, the 10M end of D3.
pub const BASE_MAX: u64 = 10_000_000;

/// Domain separation for heartbeat signatures, versioned as
/// [`SignedAddress`]'s is.
const SIGNING_DOMAIN: &[u8] = b"distlib.heartbeat.v1";

/// The first byte of every base list.
///
/// Kept so a different encoding — id prefixes, or the Bloom filter D3 set aside
/// — can follow as a compatible change rather than a new protocol.
const BASE_FORMAT: u8 = 1;

/// One member's statement about itself, before it is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Where the member is, signed on its own (C5). Its signer is the
    /// heartbeat's signer, which is how a receiver knows who sent this.
    pub address: SignedAddress,
    /// Random per start, so a restart is told apart from a replay: a new
    /// epoch is an appearance.
    pub epoch: u64,
    /// One more on every beat, within an epoch.
    ///
    /// Also what keeps beats apart at all: gossip drops a message identical to
    /// one it saw in the last 90 s (ground truth 2).
    pub seq: u64,
    /// The sender's current interval. A receiver trusts a beat for three of
    /// these, so two nodes with different settings still agree (D4).
    pub interval_secs: u32,
    pub holdings: Holdings,
    /// Said once, on a graceful stop, so the member is gone at once rather
    /// than after its TTL.
    pub leaving: bool,
}

/// Which items the member holds, as a list it published and a change to it.
///
/// **The change is counted from the base, not from the previous beat** (D3),
/// so every beat is complete on its own and a receiver that missed one loses
/// nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Holdings {
    /// How many items the member holds now.
    ///
    /// The one check a receiver can make for free:
    /// `base + added − removed == count`.
    pub count: u64,
    /// The member's last published list — see [`encode_base`] — or `None`
    /// before it has published one.
    pub base: Option<ContentHash>,
    /// Held now and not in `base`.
    #[serde(with = "raw_ids")]
    pub added: Vec<ItemId>,
    /// In `base` and no longer held.
    #[serde(with = "raw_ids")]
    pub removed: Vec<ItemId>,
}

/// A [`Heartbeat`] signed by the member it is about, for one group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedHeartbeat {
    heartbeat: Heartbeat,
    signature: Signature,
}

impl SignedHeartbeat {
    /// Signs `heartbeat` for `group` as the holder of `secret_key`.
    ///
    /// The address inside must be this key's own: a heartbeat speaks for its
    /// signer, and carrying somebody else's address is a mistake, not a
    /// statement.
    pub fn sign(secret_key: &SecretKey, group: &GroupId, heartbeat: Heartbeat) -> Result<Self> {
        let member = MemberId::from(secret_key.public());
        if heartbeat.address.member() != member {
            return Err(CoreError::BadHeartbeatSignature { member });
        }
        let payload = signing_payload(group, &heartbeat)?;
        Ok(Self {
            signature: secret_key.sign(&payload),
            heartbeat,
        })
    }

    /// The bytes that go on the wire, if they fit (D2).
    ///
    /// **An oversize heartbeat is refused here rather than sent**: gossip
    /// checks the size only as it writes the frame, after `broadcast` has
    /// returned, and what it does about it is drop the connection.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = postcard::to_stdvec(self).map_err(CoreError::HeartbeatEncoding)?;
        if bytes.len() > HEARTBEAT_MAX {
            return Err(CoreError::HeartbeatTooLarge {
                size: bytes.len(),
                max: HEARTBEAT_MAX,
            });
        }
        Ok(bytes)
    }

    /// Reads one off the wire. The size is checked before anything is parsed.
    ///
    /// Not yet checked for anything else — see [`Self::open`].
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > HEARTBEAT_MAX {
            return Err(CoreError::HeartbeatTooLarge {
                size: bytes.len(),
                max: HEARTBEAT_MAX,
            });
        }
        postcard::from_bytes(bytes).map_err(CoreError::HeartbeatEncoding)
    }

    /// The heartbeat, once both signatures check out for `group`.
    ///
    /// The address's own, and the heartbeat's by the member that address
    /// names — so neither part can be lifted into a statement by somebody
    /// else, nor replayed into another group, whose id is signed but never
    /// sent. The only way to reach the contents, as with
    /// [`SignedAddress::addr`].
    pub fn open(&self, group: &GroupId) -> Result<&Heartbeat> {
        let member = self.member();
        self.heartbeat.address.verify()?;
        let payload = signing_payload(group, &self.heartbeat)?;
        member
            .as_public_key()
            .verify(&payload, &self.signature)
            .map_err(|_| CoreError::BadHeartbeatSignature { member })?;
        Ok(&self.heartbeat)
    }

    /// Who this claims to be from. Only meaningful once [`Self::open`]ed.
    pub fn member(&self) -> MemberId {
        self.heartbeat.address.member()
    }
}

/// The exact bytes a heartbeat signature covers: the domain, the group, and
/// the heartbeat as postcard writes it, which is canonical for a given value.
fn signing_payload(group: &GroupId, heartbeat: &Heartbeat) -> Result<Vec<u8>> {
    let mut payload = SIGNING_DOMAIN.to_vec();
    payload.extend_from_slice(group.as_bytes());
    postcard::to_extend(heartbeat, payload).map_err(CoreError::HeartbeatEncoding)
}

/// A member's full list of held items, as published (D3, D5).
///
/// A format byte, then the ids as raw bytes, sorted and without repeats —
/// sorted only so that one set always makes the same bytes, and so the same
/// hash, whatever order it was gathered in. Nothing ever searches it.
pub fn encode_base(items: impl IntoIterator<Item = ItemId>) -> Vec<u8> {
    let mut ids: Vec<ItemId> = items.into_iter().collect();
    ids.sort_unstable();
    ids.dedup();
    let mut bytes = Vec::with_capacity(1 + ids.len() * 32);
    bytes.push(BASE_FORMAT);
    ids.iter()
        .for_each(|id| bytes.extend_from_slice(id.as_bytes()));
    bytes
}

/// Reads a base list, fetched whole.
///
/// Whole rather than streamed — Ivan's call at review: at the "100k easily"
/// target a list is 3.2 MB and at a million items 32 MB, held only while it is
/// read into the receiver's map. Refuses a list in another format, one that
/// ends partway through an id, and one longer than [`BASE_MAX`].
pub fn decode_base(bytes: &[u8]) -> Result<impl ExactSizeIterator<Item = ItemId> + '_> {
    let Some((&format, ids)) = bytes.split_first() else {
        return Err(CoreError::BadBaseList {
            reason: "it is empty",
        });
    };
    if format != BASE_FORMAT {
        return Err(CoreError::BadBaseList {
            reason: "it is in a format this build does not read",
        });
    }
    let (ids, rest) = ids.as_chunks::<32>();
    if !rest.is_empty() {
        return Err(CoreError::BadBaseList {
            reason: "it ends partway through an id",
        });
    }
    if ids.len() as u64 > BASE_MAX {
        return Err(CoreError::BadBaseList {
            reason: "it holds more ids than any list may",
        });
    }
    Ok(ids.iter().copied().map(ItemId::from_bytes))
}

/// Serde for item ids as raw bytes, where everywhere else they are hex.
///
/// A heartbeat carries up to [`DELTA_MAX`] of them, and as hex each would cost
/// 65 bytes rather than 32 — the difference between a delta that fits the
/// frame and one that does not.
mod raw_ids {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::id::ItemId;

    pub(super) fn serialize<S: Serializer>(
        ids: &[ItemId],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(ids.iter().map(ItemId::as_bytes))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<ItemId>, D::Error> {
        Ok(Vec::<[u8; 32]>::deserialize(deserializer)?
            .into_iter()
            .map(ItemId::from_bytes)
            .collect())
    }
}
