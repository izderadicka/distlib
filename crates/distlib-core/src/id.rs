//! The identifiers the whole system is keyed by.

use std::{fmt, str::FromStr};

use iroh::PublicKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::error::CoreError;

/// Domain separation tag for item fingerprints.
///
/// Changing this value changes the identity of every item in every catalogue,
/// so it is versioned rather than edited.
const ITEM_FINGERPRINT_TAG: &[u8] = b"distlib.item.v1";

/// A member of the group.
///
/// A member *is* an ed25519 public key — the same key iroh uses as its
/// endpoint identity — so there is nothing to steal server-side and nothing to
/// revoke except group membership itself.
///
/// Note that `iroh::EndpointId` is a type alias for `iroh::PublicKey`, not a
/// separate type, so [`Self::endpoint_id`] buys readability at the call site
/// and nothing more; the compiler cannot tell the two roles apart. The newtype
/// here is what actually keeps a member id from being mistaken for any other
/// key, and v1 runs one node per member, so the two roles coincide anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberId(PublicKey);

impl MemberId {
    /// The iroh endpoint identity this member connects with.
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.0
    }

    /// The same key, named for its cryptographic role rather than its transport
    /// one — for verifying a signature this member produced.
    ///
    /// `iroh::EndpointId` is an alias of `PublicKey`, so this returns exactly
    /// what [`Self::endpoint_id`] does. Two names because the two uses read
    /// differently at a call site, which is the same reason iroh keeps the
    /// alias at all.
    pub fn as_public_key(&self) -> &PublicKey {
        &self.0
    }

    /// The raw 32-byte public key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// A short prefix for logs. Not unique — never use it to look a member up.
    pub fn fmt_short(&self) -> impl std::fmt::Display {
        self.0.fmt_short()
    }
}

impl From<PublicKey> for MemberId {
    fn from(key: PublicKey) -> Self {
        Self(key)
    }
}

impl From<MemberId> for PublicKey {
    fn from(id: MemberId) -> Self {
        id.0
    }
}

impl fmt::Display for MemberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for MemberId {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        PublicKey::from_str(s)
            .map(Self)
            .map_err(|_| CoreError::InvalidId {
                kind: "member id",
                value: s.to_owned(),
            })
    }
}

// Serialised through `Display`/`FromStr` rather than delegating to `PublicKey`,
// so a member id is a readable string in config files and JSON regardless of
// how iroh chooses to encode keys.
impl Serialize for MemberId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MemberId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::from_str(&raw).map_err(D::Error::custom)
    }
}

/// Hex `Display`, `FromStr` and `from_bytes` for the 32-byte identifiers.
///
/// Defined before its uses because `macro_rules!` is textually scoped.
macro_rules! hex_id {
    ($ty:ty, $kind:literal) => {
        impl $ty {
            /// Wraps raw bytes without interpreting them.
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", blake3::Hash::from_bytes(self.0).to_hex())
            }
        }

        impl FromStr for $ty {
            type Err = CoreError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                blake3::Hash::from_hex(s)
                    .map(|hash| Self(*hash.as_bytes()))
                    .map_err(|_| CoreError::InvalidId {
                        kind: $kind,
                        value: s.to_owned(),
                    })
            }
        }
    };
}

/// A member id as plain bytes, before anyone has checked it is a real key.
///
/// Exists for framework boundaries that will not accept [`MemberId`]. openraft's
/// `NodeId` is the first: it requires `Default`, which an ed25519 public key
/// cannot sensibly have — `PublicKey` validates that its bytes decompress to a
/// point on the curve, so there is no "default key" to return.
///
/// Deliberately *not* solved by giving `MemberId` a `Default`. A `MemberId` is
/// somebody, and a type whose default value is a member is a hazard in a system
/// where membership is the security boundary: `#[serde(default)]` on any config
/// or wire struct would then silently produce a member instead of an error.
/// openraft needs the bound only so its own storage conformance suite can build
/// placeholder log ids, which is not a reason to weaken the domain vocabulary.
///
/// This is the workspace's one unvalidated representation, and
/// [`MemberId::try_from`] is its single validation point — so bytes arriving
/// from any framework or off any wire are checked in exactly one place.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct RawMemberId([u8; 32]);

impl RawMemberId {
    /// The raw bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Builds a `RawMemberId` from a small integer, for test harnesses only.
///
/// openraft's storage conformance suite requires `NodeId: From<u64>` so it can
/// mint node ids in its fixtures. That is meaningless in this domain — an
/// integer is not a member — so it is behind a feature and never exists in a
/// production build.
///
/// Contrast the `Default` question, where a feature flag would *not* have been
/// enough: `MemberId` refuses `Default` because `#[serde(default)]` could then
/// silently invent a member in real code. This impl carries no such risk,
/// because it cannot be reached unless something explicitly turns it on.
#[cfg(feature = "testing")]
impl From<u64> for RawMemberId {
    fn from(id: u64) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&id.to_le_bytes());
        Self(bytes)
    }
}

impl From<MemberId> for RawMemberId {
    fn from(id: MemberId) -> Self {
        Self(*id.as_bytes())
    }
}

impl TryFrom<RawMemberId> for MemberId {
    type Error = CoreError;

    /// Checks the bytes really are an ed25519 public key.
    ///
    /// Fallible on purpose: this is the boundary where unvalidated bytes become
    /// an identity, and roughly half of all 32-byte values do not decompress to
    /// a point on the curve.
    ///
    /// The all-zeros [`Default`] is *not* one of them — it is a valid low-order
    /// point and converts happily. That is harmless for a different reason: no
    /// usable secret key exists for it, so it can never sign a membership event
    /// and therefore can never propose its way into a log or be admitted by a
    /// valid one. It only ever arises where openraft asks for a `Default`, which
    /// is inside openraft's own test suite.
    fn try_from(raw: RawMemberId) -> Result<Self, Self::Error> {
        PublicKey::from_bytes(&raw.0)
            .map(Self)
            .map_err(|_| CoreError::InvalidId {
                kind: "member id",
                value: blake3::Hash::from_bytes(raw.0).to_hex().to_string(),
            })
    }
}

/// Identifies one group. Derived from the founding event in the membership log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GroupId(#[serde(with = "hex32")] [u8; 32]);

/// Identifies one catalogue item by the set of content files it was born with.
///
/// Computed once at creation and then frozen: adding a missing chapter or a
/// second format later must not change the id, because ratings, reviews,
/// bookmarks and custodianship all key off it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ItemId(#[serde(with = "hex32")] [u8; 32]);

impl ItemId {
    /// Fingerprints the set of `role: content` blob hashes an item is made of.
    ///
    /// ```text
    /// BLAKE3( "distlib.item.v1" || n || sorted[ h_i ] )
    /// ```
    ///
    /// * **Sorted** so filename and insertion order cannot affect identity.
    /// * **Counted** so the pre-image describes its own shape.
    /// * **Domain separated** so an item id can never equal a blob hash.
    ///
    /// §5.2 of the design plan also length-prefixes each element. Every `h_i`
    /// is a 32-byte BLAKE3 hash, so the concatenation is already unambiguous
    /// and the prefix cannot distinguish anything. A future scheme admitting
    /// variable-length elements would carry its own tag, which is what
    /// prevents cross-version collisions — the length prefix never was.
    /// Recorded as P0-7 in `docs/plan-deltas.md`.
    ///
    /// Cover art and subtitles are excluded by the caller — only content files
    /// take part, so adding a cover does not create a different item.
    ///
    /// Callers pass a set: the catalogue keys files by blob hash, so duplicates
    /// cannot occur. Duplicates are hashed as given rather than silently
    /// collapsed, since a caller with duplicates has a bug worth surfacing.
    pub fn from_content_hashes(hashes: &[[u8; 32]]) -> Self {
        let mut sorted: Vec<&[u8; 32]> = hashes.iter().collect();
        sorted.sort_unstable();

        let mut hasher = blake3::Hasher::new();
        hasher.update(ITEM_FINGERPRINT_TAG);
        hasher.update(&(sorted.len() as u64).to_le_bytes());
        for hash in sorted {
            hasher.update(hash);
        }
        Self(*hasher.finalize().as_bytes())
    }

    /// The raw 32-byte fingerprint.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

hex_id!(GroupId, "group id");
hex_id!(ItemId, "item id");
hex_id!(RawMemberId, "raw member id");

/// Serde for `[u8; 32]` as a hex string, so ids stay readable in TOML and JSON.
mod hex32 {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub(super) fn serialize<S: Serializer>(
        bytes: &[u8; 32],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&blake3::Hash::from_bytes(*bytes).to_hex())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<[u8; 32], D::Error> {
        let raw = String::deserialize(deserializer)?;
        blake3::Hash::from_hex(&raw)
            .map(|hash| *hash.as_bytes())
            .map_err(D::Error::custom)
    }
}
