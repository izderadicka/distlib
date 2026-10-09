//! What members say about items, each in keys of their own (phase 4's D8).
//!
//! **A community key names the member who wrote it**, and that is what makes
//! it conflict-free: two members rating one item at once write two keys, not
//! one key twice. It is also what makes it forgeable. iroh-docs checks only
//! that an entry is signed by its author, not that the author is the member
//! the key names, so a reader counts an entry only when the two agree (D9) —
//! a check the catalogue makes, since only it sees who wrote an entry.
//!
//! Apart from [`Key`](crate::Key), which stays item-only: the pump and the
//! replay read `item/…` as an item's own fields, and these are not.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{CoreError, ItemId, MemberId};

const RATING: &str = "rating";
const REVIEW: &str = "review";

/// The longest review there may be, in bytes of UTF-8: 16 KiB.
pub const REVIEW_MAX_BYTES: usize = 16 * 1024;

/// A key a member writes about themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommunityKey {
    /// `rating/{item_id}/{member_id}`
    Rating { item: ItemId, member: MemberId },
    /// `review/{item_id}/{member_id}`
    Review { item: ItemId, member: MemberId },
}

impl CommunityKey {
    /// The key as it is written in the document.
    pub fn encode(&self) -> String {
        match self {
            Self::Rating { item, member } => format!("{RATING}/{item}/{member}"),
            Self::Review { item, member } => format!("{REVIEW}/{item}/{member}"),
        }
    }

    /// Reads a key back, or `None` if this build does not recognise it.
    ///
    /// **Only in the one spelling [`Self::encode`] writes.** An id parses from
    /// more than one — hex in either case, for one — and a key read from
    /// another would be a second key for the same member and item. Written by
    /// that member, it would pass the author check too: a second rating beside
    /// the first. So a key counts only if it encodes back to itself.
    pub fn parse(key: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(key).ok()?;
        let mut parts = text.split('/');
        let kind = parts.next()?;
        let item = ItemId::from_str(parts.next()?).ok()?;
        let member = MemberId::from_str(parts.next()?).ok()?;
        let parsed = match kind {
            RATING => Self::Rating { item, member },
            REVIEW => Self::Review { item, member },
            _ => return None,
        };
        (parsed.encode().as_bytes() == key).then_some(parsed)
    }

    /// The item this key is about.
    pub const fn item(&self) -> ItemId {
        match self {
            Self::Rating { item, .. } | Self::Review { item, .. } => *item,
        }
    }

    /// The member who says it — and so the only author whose entry at this
    /// key counts (D9).
    pub const fn member(&self) -> MemberId {
        match self {
            Self::Rating { member, .. } | Self::Review { member, .. } => *member,
        }
    }

    /// Every member's rating of `item`, as a prefix to read.
    pub fn ratings_of(item: ItemId) -> String {
        format!("{RATING}/{item}/")
    }

    /// Every member's review of `item`, as a prefix to read.
    pub fn reviews_of(item: ItemId) -> String {
        format!("{REVIEW}/{item}/")
    }
}

/// A member's rating of an item: one to five.
///
/// Checked when read as well as when made, so an entry holding anything else
/// — written by a raw put, or by another build — is not a rating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct Rating(u8);

impl Rating {
    /// The rating as a number, one to five.
    pub const fn get(self) -> u8 {
        self.0
    }

    /// The value as it is written in the document: a JSON number.
    pub fn encode(self) -> Vec<u8> {
        serde_json::to_vec(&self).expect("a number always encodes as JSON")
    }

    /// Reads a value back, or `None` if it is not a rating.
    pub fn decode(value: &[u8]) -> Option<Self> {
        serde_json::from_slice(value).ok()
    }
}

impl TryFrom<u8> for Rating {
    type Error = CoreError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        (1..=5)
            .contains(&value)
            .then_some(Self(value))
            .ok_or(CoreError::InvalidRating { value })
    }
}

impl From<Rating> for u8 {
    fn from(rating: Rating) -> Self {
        rating.0
    }
}

/// A member's review of an item: text of at most [`REVIEW_MAX_BYTES`].
///
/// Checked when read as well as when made, like [`Rating`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Review(String);

impl Review {
    /// The review's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The value as it is written in the document: a JSON string.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a string always encodes as JSON")
    }

    /// Reads a value back, or `None` if it is not a review.
    pub fn decode(value: &[u8]) -> Option<Self> {
        serde_json::from_slice(value).ok()
    }
}

impl TryFrom<String> for Review {
    type Error = CoreError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        if text.len() > REVIEW_MAX_BYTES {
            return Err(CoreError::ReviewTooLong {
                len: text.len(),
                max: REVIEW_MAX_BYTES,
            });
        }
        Ok(Self(text))
    }
}

impl From<Review> for String {
    fn from(review: Review) -> Self {
        review.0
    }
}
