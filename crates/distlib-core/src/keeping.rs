//! Why this node keeps a blob, spelled as the name of the tag that keeps it
//! (phase 5's "Tags" section).
//!
//! A blob store keeps a blob while any tag points at it, and garbage
//! collection deletes the rest. Each reason is its own tag, per item and blob,
//! so a blob kept for two reasons, or by two items, stays until every one of
//! them is released.

use crate::{ContentHash, ItemId};

/// Why a node keeps one of an item's blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepReason {
    /// This node added the item: kept for as long as it is a member.
    Own,
    /// This node downloaded the item: kept while there is room.
    Cache,
}

impl KeepReason {
    /// The tag that keeps `blob` of `item` for this reason:
    /// `<reason>/<item>/<blob>`.
    pub fn tag(self, item: ItemId, blob: ContentHash) -> String {
        format!("{}/{item}/{blob}", self.prefix())
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Own => "own",
            Self::Cache => "cache",
        }
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
            KeepReason::Cache.tag(item, blob),
            format!("cache/{item}/{blob}")
        );
    }
}
