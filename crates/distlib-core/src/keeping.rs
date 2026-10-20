//! Why this node keeps a blob, spelled as the name of the tag that keeps it
//! (phase 5's "Tags" section).
//!
//! A blob store keeps a blob while any tag points at it, and garbage
//! collection deletes the rest. Each reason is its own tag, per item and blob,
//! so a blob kept for two reasons, or by two items, stays until every one of
//! them is released.

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::SystemTime,
};

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

/// Each blob this node keeps, with its size and every tag that keeps it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeptBlobs(HashMap<ContentHash, KeptBlob>);

#[derive(Debug, Clone, PartialEq)]
struct KeptBlob {
    bytes: u64,
    /// Why, and for which item, each of its tags keeps it.
    tags: Vec<(KeepReason, ItemId)>,
}

impl KeptBlob {
    fn strongest_reason(&self) -> Option<KeepReason> {
        self.tags.iter().map(|(reason, _)| *reason).min()
    }
}

/// The cached items to let go of, and whether that makes room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evictions {
    /// Least recently used first.
    pub items: Vec<ItemId>,
    /// How many bytes are still over the limit once they are gone.
    pub short_by: u64,
}

impl KeptBlobs {
    /// Adds the tag that keeps `blob`, of `bytes`, for `reason` and `item`.
    pub fn add(&mut self, reason: KeepReason, item: ItemId, blob: ContentHash, bytes: u64) {
        self.0
            .entry(blob)
            .or_insert(KeptBlob {
                bytes,
                tags: Vec::new(),
            })
            .tags
            .push((reason, item));
    }

    /// The bytes kept for each reason, each blob counted once under the
    /// strongest reason it is kept for.
    pub fn usage(&self) -> StorageUse {
        let mut usage = StorageUse::default();
        for kept in self.0.values() {
            if let Some(reason) = kept.strongest_reason() {
                *match reason {
                    KeepReason::Own => &mut usage.own_bytes,
                    KeepReason::Custody => &mut usage.custody_bytes,
                    KeepReason::Cache => &mut usage.cache_bytes,
                } += kept.bytes;
            }
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
                    .and_then(KeptBlob::strongest_reason)
                    .is_some_and(KeepReason::is_mandatory)
            })
            .map(|(_, bytes)| bytes)
            .sum()
    }

    /// How many bytes of `blobs` are not counted yet: those not kept, or kept
    /// before any of their bytes arrived. Each blob is named once.
    pub fn uncounted_bytes(&self, blobs: impl IntoIterator<Item = (ContentHash, u64)>) -> u64 {
        blobs
            .into_iter()
            .filter(|(blob, _)| !self.0.get(blob).is_some_and(|kept| kept.bytes > 0))
            .map(|(_, bytes)| bytes)
            .sum()
    }

    /// The cached items to let go of for `needed` more bytes to fit under
    /// `limit`, least recently used first, and no more than that takes.
    ///
    /// Never `keeping`, the item room is made for, nor an item also kept as
    /// own or custody, which frees nothing. An item `last_used` has no time
    /// for counts as the oldest; ties go by item id. A blob two items share
    /// is freed only with the last of them.
    pub fn evictions(
        &self,
        keeping: ItemId,
        needed: u64,
        limit: u64,
        last_used: impl Fn(ItemId) -> Option<SystemTime>,
    ) -> Evictions {
        let mut cached: HashMap<ItemId, Vec<ContentHash>> = HashMap::new();
        let mut mandatory: HashSet<ItemId> = HashSet::new();
        for (blob, kept) in &self.0 {
            for &(reason, item) in &kept.tags {
                if reason.is_mandatory() {
                    mandatory.insert(item);
                } else {
                    cached.entry(item).or_default().push(*blob);
                }
            }
        }
        let mut candidates: Vec<(Option<SystemTime>, ItemId)> = cached
            .keys()
            .filter(|item| **item != keeping && !mandatory.contains(item))
            .map(|item| (last_used(*item), *item))
            .collect();
        candidates.sort_unstable();

        let mut tags_left: HashMap<ContentHash, usize> = self
            .0
            .iter()
            .map(|(blob, kept)| (*blob, kept.tags.len()))
            .collect();
        let mut total = self.usage().total_bytes() + needed;
        let mut items = Vec::new();
        for (_, item) in candidates {
            if total <= limit {
                break;
            }
            for blob in &cached[&item] {
                let left = tags_left
                    .get_mut(blob)
                    .expect("every cached blob is one of the kept");
                *left -= 1;
                if *left == 0 {
                    total -= self.0[blob].bytes;
                }
            }
            items.push(item);
        }
        Evictions {
            items,
            short_by: total.saturating_sub(limit),
        }
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

    /// Everything kept.
    pub fn total_bytes(self) -> u64 {
        self.mandatory_bytes() + self.cache_bytes
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

    fn item(byte: u8) -> ItemId {
        ItemId::from_bytes([byte; 32])
    }

    #[test]
    fn a_blob_kept_for_two_reasons_counts_once_under_the_stronger() {
        let mut kept = KeptBlobs::default();
        kept.add(KeepReason::Cache, item(1), blob(1), 100);
        kept.add(KeepReason::Own, item(2), blob(1), 100);
        kept.add(KeepReason::Custody, item(3), blob(2), 20);
        kept.add(KeepReason::Cache, item(3), blob(2), 20);
        kept.add(KeepReason::Cache, item(4), blob(3), 3);
        assert_eq!(
            kept.usage(),
            StorageUse {
                own_bytes: 100,
                custody_bytes: 20,
                cache_bytes: 3,
            }
        );
        assert_eq!(kept.usage().mandatory_bytes(), 120);
        assert_eq!(kept.usage().total_bytes(), 123);
    }

    #[test]
    fn only_blobs_not_yet_own_or_custody_grow_what_is_mandatory() {
        let mut kept = KeptBlobs::default();
        kept.add(KeepReason::Own, item(1), blob(1), 100);
        kept.add(KeepReason::Custody, item(2), blob(2), 20);
        kept.add(KeepReason::Cache, item(3), blob(3), 3);
        assert_eq!(kept.mandatory_growth([(blob(1), 100), (blob(2), 20)]), 0);
        assert_eq!(
            kept.mandatory_growth([(blob(1), 100), (blob(3), 3), (blob(4), 4)]),
            7,
            "a cached blob becomes mandatory, a new one is new"
        );
    }

    #[test]
    fn only_blobs_not_kept_or_still_to_arrive_are_uncounted() {
        let mut kept = KeptBlobs::default();
        kept.add(KeepReason::Cache, item(1), blob(1), 100);
        kept.add(KeepReason::Custody, item(2), blob(2), 0);
        assert_eq!(
            kept.uncounted_bytes([(blob(1), 100), (blob(2), 20), (blob(3), 3)]),
            23
        );
    }

    fn at(secs: u64) -> Option<SystemTime> {
        Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs))
    }

    #[test]
    fn the_least_recently_used_go_first_and_no_more_than_needed() {
        let mut kept = KeptBlobs::default();
        for byte in 1..=4 {
            kept.add(KeepReason::Cache, item(byte), blob(byte), 10);
        }
        // 1 and 2 never stamped, 2 before 1 by id; then 4, then 3.
        let last_used = |id: ItemId| match id.as_bytes()[0] {
            3 => at(200),
            4 => at(100),
            _ => None,
        };
        let plan = kept.evictions(item(9), 0, 25, last_used);
        assert_eq!(
            plan,
            Evictions {
                items: vec![item(1), item(2)],
                short_by: 0,
            }
        );
        let plan = kept.evictions(item(9), 10, 25, last_used);
        assert_eq!(plan.items, [item(1), item(2), item(4)]);
        assert_eq!(kept.evictions(item(9), 0, 40, last_used).items, []);
    }

    #[test]
    fn what_is_kept_otherwise_or_made_room_for_is_never_evicted() {
        let mut kept = KeptBlobs::default();
        kept.add(KeepReason::Own, item(1), blob(1), 10);
        kept.add(KeepReason::Cache, item(2), blob(2), 10);
        kept.add(KeepReason::Custody, item(2), blob(2), 10);
        kept.add(KeepReason::Cache, item(3), blob(3), 10);
        kept.add(KeepReason::Cache, item(4), blob(4), 10);
        let plan = kept.evictions(item(3), 15, 20, |_| None);
        assert_eq!(
            plan,
            Evictions {
                items: vec![item(4)],
                short_by: 25,
            },
            "only 4 may go, and it is not enough"
        );
    }

    #[test]
    fn a_blob_two_items_share_is_freed_only_with_the_last() {
        let mut kept = KeptBlobs::default();
        kept.add(KeepReason::Cache, item(1), blob(1), 10);
        kept.add(KeepReason::Cache, item(2), blob(1), 10);
        kept.add(KeepReason::Cache, item(3), blob(3), 10);
        let plan = kept.evictions(item(9), 0, 10, |id| at(u64::from(id.as_bytes()[0])));
        assert_eq!(plan.items, [item(1), item(2)], "item 1 alone frees nothing");
    }
}
