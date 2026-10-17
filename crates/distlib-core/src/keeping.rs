//! Why this node keeps a blob, spelled as the name of the tag that keeps it
//! (phase 5's "Tags" section).
//!
//! A blob store keeps a blob while any tag points at it, and garbage
//! collection deletes the rest. Each reason is its own tag, per item and blob,
//! so a blob kept for two reasons, or by two items, stays until every one of
//! them is released.

use std::str::FromStr;

use crate::{ContentHash, ItemId};

/// Why a node keeps one of an item's blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        assert_eq!(KeepReason::Custody.parse(&tag), Some((item, blob)));
        assert_eq!(KeepReason::Cache.parse(&tag), None, "another reason's tag");
        assert_eq!(KeepReason::Custody.parse("custody/not-an-item/x"), None);
    }
}
