//! Why this node keeps a blob, spelled as the name of the tag that keeps it
//! (phase 5's "Tags" section).
//!
//! A blob store keeps a blob while any tag points at it, and garbage
//! collection deletes the rest. Each reason is its own tag, per item and blob,
//! so a blob kept for two reasons, or by two items, stays until every one of
//! them is released.

use std::{collections::HashMap, str::FromStr};

use crate::{ContentHash, ItemId};

/// Why a node keeps one of an item's blobs.
///
/// Ordered strongest first, so a blob kept for two reasons counts under the
/// lesser of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KeepReason {
    /// This node added the item: kept for as long as it is a member.
    Own,
    /// This node is one of the item's custodians: kept while it is.
    Custody,
    /// This node downloaded the item, or was its custodian: kept while there
    /// is room.
    Cache,
}

impl KeepReason {
    /// Every reason, strongest first.
    pub const ALL: [Self; 3] = [Self::Own, Self::Custody, Self::Cache];

    /// Whether what is kept for this reason stays when space is short: own
    /// and custody do, the cache does not.
    pub fn is_mandatory(self) -> bool {
        self != Self::Cache
    }

    /// The tag that keeps `blob` of `item` for this reason:
    /// `<reason>/<item>/<blob>`.
    pub fn tag(self, item: ItemId, blob: ContentHash) -> String {
        format!("{}{item}/{blob}", self.prefix())
    }

    /// What every tag for this reason starts with.
    pub fn prefix(self) -> &'static str {
        match self {
            Self::Own => "own/",
            Self::Custody => "custody/",
            Self::Cache => "cache/",
        }
    }

    /// What every tag for this reason and `item` starts with.
    pub fn item_prefix(self, item: ItemId) -> String {
        format!("{}{item}/", self.prefix())
    }

    /// The item and blob a tag for this reason names; `None` for a tag that
    /// is not one.
    pub fn parse(self, tag: &str) -> Option<(ItemId, ContentHash)> {
        let (item, blob) = tag.strip_prefix(self.prefix())?.split_once('/')?;
        Some((
            ItemId::from_str(item).ok()?,
            ContentHash::from_str(blob).ok()?,
        ))
    }
}

/// Each blob this node keeps, once, under the strongest reason it is kept for,
/// with its size.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeptBlobs(HashMap<ContentHash, (KeepReason, u64)>);

impl KeptBlobs {
    /// Adds `blob`, of `bytes`, kept for `reason`. A blob already here keeps
    /// the stronger of its two reasons.
    pub fn add(&mut self, reason: KeepReason, blob: ContentHash, bytes: u64) {
        self.0
            .entry(blob)
            .and_modify(|(kept_for, _)| *kept_for = (*kept_for).min(reason))
            .or_insert((reason, bytes));
    }

    /// The bytes kept for each reason.
    pub fn usage(&self) -> StorageUse {
        let mut usage = StorageUse::default();
        for &(reason, bytes) in self.0.values() {
            *match reason {
                KeepReason::Own => &mut usage.own_bytes,
                KeepReason::Custody => &mut usage.custody_bytes,
                KeepReason::Cache => &mut usage.cache_bytes,
            } += bytes;
        }
        usage
    }

    /// How many bytes keeping `blobs` as this node's own adds to what stays
    /// when space is short: the blobs not kept as own or custody already.
    /// Each blob is named once, as in an item's files.
    pub fn mandatory_growth(&self, blobs: impl IntoIterator<Item = (ContentHash, u64)>) -> u64 {
        blobs
            .into_iter()
            .filter(|(blob, _)| {
                !self
                    .0
                    .get(blob)
                    .is_some_and(|(reason, _)| reason.is_mandatory())
            })
            .map(|(_, bytes)| bytes)
            .sum()
    }
}

/// The bytes a node keeps, by the strongest reason each blob is kept for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageUse {
    pub own_bytes: u64,
    pub custody_bytes: u64,
    pub cache_bytes: u64,
}

impl StorageUse {
    /// What stays when space is short: own and custody.
    pub fn mandatory_bytes(self) -> u64 {
        self.own_bytes + self.custody_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tag_names_the_reason_the_item_and_the_blob() {
        let item = ItemId::from_bytes([1; 32]);
        let blob = ContentHash::from_bytes([2; 32]);
        assert_eq!(
            KeepReason::Own.tag(item, blob),
            format!("own/{item}/{blob}")
        );
        assert_eq!(
            KeepReason::Custody.tag(item, blob),
            format!("custody/{item}/{blob}")
        );
        assert_eq!(
            KeepReason::Cache.tag(item, blob),
            format!("cache/{item}/{blob}")
        );
    }

    #[test]
    fn a_tag_reads_back_as_its_item_and_blob() {
        let item = ItemId::from_bytes([1; 32]);
        let blob = ContentHash::from_bytes([2; 32]);
        let tag = KeepReason::Custody.tag(item, blob);
        assert!(tag.starts_with(&KeepReason::Custody.item_prefix(item)));
        assert_eq!(KeepReason::Custody.parse(&tag), Some((item, blob)));
        assert_eq!(KeepReason::Cache.parse(&tag), None, "another reason's tag");
        assert_eq!(KeepReason::Custody.parse("custody/not-an-item/x"), None);
    }

    fn blob(byte: u8) -> ContentHash {
        ContentHash::from_bytes([byte; 32])
    }

    #[test]
    fn a_blob_kept_for_two_reasons_counts_once_under_the_stronger() {
        let mut kept = KeptBlobs::default();
        kept.add(KeepReason::Cache, blob(1), 100);
        kept.add(KeepReason::Own, blob(1), 100);
        kept.add(KeepReason::Custody, blob(2), 20);
        kept.add(KeepReason::Cache, blob(2), 20);
        kept.add(KeepReason::Cache, blob(3), 3);
        assert_eq!(
            kept.usage(),
            StorageUse {
                own_bytes: 100,
                custody_bytes: 20,
                cache_bytes: 3,
            }
        );
        assert_eq!(kept.usage().mandatory_bytes(), 120);
    }

    #[test]
    fn only_blobs_not_yet_own_or_custody_grow_what_is_mandatory() {
        let mut kept = KeptBlobs::default();
        kept.add(KeepReason::Own, blob(1), 100);
        kept.add(KeepReason::Custody, blob(2), 20);
        kept.add(KeepReason::Cache, blob(3), 3);
        assert_eq!(kept.mandatory_growth([(blob(1), 100), (blob(2), 20)]), 0);
        assert_eq!(
            kept.mandatory_growth([(blob(1), 100), (blob(3), 3), (blob(4), 4)]),
            7,
            "a cached blob becomes mandatory, a new one is new"
        );
    }
}
