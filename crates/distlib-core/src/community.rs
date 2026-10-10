//! What members say about items, each in keys of their own (phase 4's D8).
//!
//! **A community key names the member who wrote it**, and that is what makes
//! it conflict-free: two members rating one item at once write two keys, not
//! one key twice. It is also what makes it forgeable. iroh-docs checks only
//! that an entry is signed by its author, not that the author is the member
//! the key names, so a reader counts an entry only when the two agree (D9) —
//! a check the catalogue makes, since only it sees who wrote an entry.
//!
//! **No key is a prefix of another by the same author** (ground truth 10):
//! writing a key prunes that author's longer keys beneath it. Every key here
//! ends in a fixed-length id, and nothing writes a bare prefix.
//!
//! Apart from [`Key`](crate::Key), which stays item-only: the pump and the
//! replay read `item/…` as an item's own fields, and these are not.

use std::{fmt, str::FromStr};

use data_encoding::HEXLOWER;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{CoreError, ItemId, MemberId};

const RATING: &str = "rating";
const REVIEW: &str = "review";
const BOOKMARK: &str = "bookmark";

/// The longest review there may be, in bytes of UTF-8: 16 KiB.
pub const REVIEW_MAX_BYTES: usize = 16 * 1024;

/// The longest note a bookmark may carry, in bytes of UTF-8: 4 KiB.
pub const NOTE_MAX_BYTES: usize = 4 * 1024;

/// The longest position a bookmark may name, in bytes of UTF-8: a page, a
/// chapter or a time, not prose.
pub const POSITION_MAX_BYTES: usize = 256;

/// A key a member writes about themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommunityKey {
    /// `rating/{item_id}/{member_id}`
    Rating { item: ItemId, member: MemberId },
    /// `review/{item_id}/{member_id}`
    Review { item: ItemId, member: MemberId },
    /// `bookmark/{item_id}/{member_id}/{bookmark_id}`
    Bookmark {
        item: ItemId,
        member: MemberId,
        bookmark: BookmarkId,
    },
}

impl CommunityKey {
    /// The key as it is written in the document.
    pub fn encode(&self) -> String {
        match self {
            Self::Rating { item, member } => format!("{RATING}/{item}/{member}"),
            Self::Review { item, member } => format!("{REVIEW}/{item}/{member}"),
            Self::Bookmark {
                item,
                member,
                bookmark,
            } => format!("{BOOKMARK}/{item}/{member}/{bookmark}"),
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
            BOOKMARK => Self::Bookmark {
                item,
                member,
                bookmark: BookmarkId::from_str(parts.next()?).ok()?,
            },
            _ => return None,
        };
        (parsed.encode().as_bytes() == key).then_some(parsed)
    }

    /// The item this key is about.
    pub const fn item(&self) -> ItemId {
        match self {
            Self::Rating { item, .. } | Self::Review { item, .. } | Self::Bookmark { item, .. } => {
                *item
            }
        }
    }

    /// The member who says it — and so the only author whose entry at this
    /// key counts (D9).
    pub const fn member(&self) -> MemberId {
        match self {
            Self::Rating { member, .. }
            | Self::Review { member, .. }
            | Self::Bookmark { member, .. } => *member,
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

    /// Every member's bookmarks on `item`, as a prefix to read.
    pub fn bookmarks_of(item: ItemId) -> String {
        format!("{BOOKMARK}/{item}/")
    }
}

/// One of a member's bookmarks on an item: 16 random bytes, written in hex.
///
/// A member may leave several on one item (D8), so each has an id of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BookmarkId([u8; 16]);

impl BookmarkId {
    /// Wraps raw bytes without interpreting them.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for BookmarkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&HEXLOWER.encode(&self.0))
    }
}

impl FromStr for BookmarkId {
    type Err = CoreError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        HEXLOWER
            .decode(text.as_bytes())
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .map(Self)
            .ok_or_else(|| CoreError::InvalidId {
                kind: "bookmark id",
                value: text.to_owned(),
            })
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
        to_json(&self)
    }

    /// Reads a value back, or `None` if it is not a rating.
    pub fn decode(value: &[u8]) -> Option<Self> {
        from_json(value)
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
        to_json(self)
    }

    /// Reads a value back, or `None` if it is not a review.
    pub fn decode(value: &[u8]) -> Option<Self> {
        from_json(value)
    }
}

impl TryFrom<String> for Review {
    type Error = CoreError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        capped("review", text, REVIEW_MAX_BYTES).map(Self)
    }
}

impl From<Review> for String {
    fn from(review: Review) -> Self {
        review.0
    }
}

/// A member's bookmark on an item (D8): a shared pointer into it, with a note.
///
/// **When it last changed is not here.** The document already records when
/// each entry was written, and a field would be a second answer able to
/// disagree — the catalogue reads it off the entry, as it does an item's
/// `last_modified`. When it was *made* is here, since an edit rewrites the
/// entry.
///
/// Checked when read as well as when made, like [`Rating`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "BookmarkFields")]
pub struct Bookmark {
    position: String,
    note: String,
    created_at: u64,
}

impl Bookmark {
    /// A bookmark at `position` — a page, a chapter, `01:23:45` — made at
    /// `created_at`, in microseconds since the epoch.
    pub fn new(position: String, note: String, created_at: u64) -> Result<Self, CoreError> {
        Ok(Self {
            position: capped("bookmark position", position, POSITION_MAX_BYTES)?,
            note: capped("bookmark note", note, NOTE_MAX_BYTES)?,
            created_at,
        })
    }

    /// Where in the item it points: free text.
    pub fn position(&self) -> &str {
        &self.position
    }

    /// What its member said about the place.
    pub fn note(&self) -> &str {
        &self.note
    }

    /// When it was made, in microseconds since the epoch.
    pub const fn created_at(&self) -> u64 {
        self.created_at
    }

    /// The value as it is written in the document: a JSON object.
    pub fn encode(&self) -> Vec<u8> {
        to_json(self)
    }

    /// Reads a value back, or `None` if it is not a bookmark.
    pub fn decode(value: &[u8]) -> Option<Self> {
        from_json(value)
    }
}

/// A bookmark as it arrives, before [`Bookmark::new`] has checked it.
#[derive(Deserialize)]
struct BookmarkFields {
    position: String,
    note: String,
    created_at: u64,
}

impl TryFrom<BookmarkFields> for Bookmark {
    type Error = CoreError;

    fn try_from(fields: BookmarkFields) -> Result<Self, Self::Error> {
        Self::new(fields.position, fields.note, fields.created_at)
    }
}

/// `text`, if it is at most `max` bytes of UTF-8.
fn capped(what: &'static str, text: String, max: usize) -> Result<String, CoreError> {
    if text.len() > max {
        return Err(CoreError::TextTooLong {
            what,
            len: text.len(),
            max,
        });
    }
    Ok(text)
}

/// A community value as the document holds it: JSON, as an item's are.
fn to_json(value: &impl Serialize) -> Vec<u8> {
    serde_json::to_vec(value).expect("numbers, strings and their structs always encode as JSON")
}

/// A community value read back, or `None` if it is not one.
fn from_json<T: DeserializeOwned>(value: &[u8]) -> Option<T> {
    serde_json::from_slice(value).ok()
}
