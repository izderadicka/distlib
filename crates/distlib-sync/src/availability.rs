//! Who is online, and which items this node holds — what its heartbeat says
//! (phase 4's D4 and D6).
//!
//! The heartbeat is [`Availability`]; the rest of this file is the set its
//! holdings are read from.
//!
//! **A set this node keeps, not a question asked per row.** Whether an item is
//! held changes only when its content files do — a download lands, a member
//! adds a file this node lacks — and each of those passes through something
//! that can say so: the projection, which reads every item the document
//! changes, and `library.download`, which knows when a fetch is done. Asking
//! the blob store on every read instead would be one lookup per file per row
//! of every listing.
//!
//! In memory only, like everything about availability (§5.6): the projection's
//! replay at every start fills it again.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex, PoisonError},
};

use distlib_core::{FileRole, Item, ItemId, KeepReason};
use futures_lite::stream::StreamExt as _;
use iroh_blobs::{Hash, api::Store as BlobStore, api::proto::BlobStatus};
use tokio::{sync::watch, time::Instant};

use crate::error::{Result, SyncError};

mod ask_order;
mod heartbeat;
mod holders;
mod online;
mod statement;

pub use ask_order::ask_order;
pub use heartbeat::{Availability, Sources, topic_for};

/// The items every one of whose content files is complete in this node's
/// store.
///
/// Cheap to clone; every clone is the same set.
#[derive(Debug, Clone)]
pub struct Holdings {
    blobs: BlobStore,
    held: Arc<Mutex<HashSet<ItemId>>>,
    /// When the set last changed — what the heartbeat waits on to say so,
    /// and how long it has been quiet (D5).
    changed: Arc<watch::Sender<Instant>>,
}

impl Holdings {
    /// An empty set, for the items whose content is in `blobs`.
    ///
    /// The catalogue makes the one a node uses; public for tests that need a
    /// set without one.
    pub fn new(blobs: BlobStore) -> Self {
        Self {
            blobs,
            held: Arc::default(),
            changed: Arc::new(watch::Sender::new(Instant::now())),
        }
    }

    /// Works out again whether `item` is held, and records the answer.
    ///
    /// Held means **every `role: content` file is complete here and kept for
    /// the item** — covers and other roles do not count, as they do not count
    /// towards an item's identity. **Every one the item was born with**: its
    /// content files must fingerprint to its id, so an item whose records are
    /// still arriving is not held, however complete the ones here are (#185).
    ///
    /// **Kept, not only here**: a file whose tags are released, an evicted
    /// item's, stays complete until the next collection deletes it, and is
    /// not held meanwhile. A file kept only by another item's tag counts as
    /// not held either, which under-claims rather than over-claims.
    pub async fn recheck(&self, item: &Item) -> Result<bool> {
        // Collected first: an iterator of closures held across the await
        // below would make this future not `Send`.
        let content: Vec<Hash> = item
            .files
            .iter()
            .filter(|(_, record)| record.role == FileRole::Content)
            .map(|(hash, _)| Hash::from_bytes(*hash.as_bytes()))
            .collect();
        let mut held = item.fingerprint() == Some(item.id) && {
            let kept = self.kept_for(item.id).await?;
            content.iter().all(|hash| kept.contains(hash))
        };
        for hash in content.into_iter().take_while(|_| held) {
            let status = self
                .blobs
                .blobs()
                .status(hash)
                .await
                .map_err(SyncError::content("asked whether an item's file is here"))?;
            if !matches!(status, BlobStatus::Complete { .. }) {
                held = false;
                break;
            }
        }

        // One insert or remove: a panic elsewhere cannot leave this half done.
        let mut set = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let changed = if held {
            set.insert(item.id)
        } else {
            set.remove(&item.id)
        };
        drop(set);
        if changed {
            self.changed.send_replace(Instant::now());
        }
        Ok(held)
    }

    /// The blobs some tag keeps for `item`, whatever the reason.
    async fn kept_for(&self, item: ItemId) -> Result<HashSet<Hash>> {
        const DOING: &str = "listed what is kept for an item";
        let mut kept = HashSet::new();
        for reason in KeepReason::ALL {
            let mut tags = self
                .blobs
                .tags()
                .list_prefix(reason.item_prefix(item))
                .await
                .map_err(SyncError::content(DOING))?;
            while let Some(tag) = tags.next().await {
                kept.insert(tag.map_err(SyncError::content(DOING))?.hash);
            }
        }
        Ok(kept)
    }

    /// Records that `item` is no longer held: its blobs are let go of, and
    /// go at the next collection.
    pub fn forget(&self, item: ItemId) {
        let removed = self
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&item);
        if removed {
            self.changed.send_replace(Instant::now());
        }
    }

    /// Whether this node holds `item`, as last worked out.
    pub fn holds(&self, item: ItemId) -> bool {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&item)
    }

    /// Every item held, as last worked out.
    pub fn held(&self) -> HashSet<ItemId> {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// When the set last changed; wakes whoever waits on it at each change,
    /// and only at a change — a recheck that finds what was known is none.
    pub fn changes(&self) -> watch::Receiver<Instant> {
        self.changed.subscribe()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use distlib_core::{ContentHash, FileRecord};
    use iroh_blobs::store::mem::MemStore;

    use super::*;

    /// The heartbeat beats on a change, so a recheck that finds what was
    /// known — every item of a replay — must not look like one. Held is
    /// complete and kept.
    #[tokio::test]
    async fn only_a_change_to_the_set_is_signalled() {
        let store = MemStore::new();
        let holdings = Holdings::new((*store).clone());
        let mut changes = holdings.changes();
        let content = store.add_bytes(b"dune".to_vec()).await.unwrap().hash;
        let hash = ContentHash::from_bytes(*content.as_bytes());
        let mut item = Item::new(ItemId::from_content_hashes(&[*hash.as_bytes()]));
        item.files.insert(hash, content_record("dune.epub"));

        assert!(
            !holdings.recheck(&item).await.unwrap(),
            "complete, but kept by nothing"
        );
        assert!(!changes.has_changed().unwrap(), "not held, as before: none");
        let tag = KeepReason::Cache.tag(item.id, *item.files.keys().next().unwrap());
        store.tags().set(tag, content).await.unwrap();

        assert!(holdings.recheck(&item).await.unwrap());
        assert!(changes.has_changed().unwrap(), "held: a change");
        changes.borrow_and_update();
        assert!(holdings.recheck(&item).await.unwrap());
        assert!(!changes.has_changed().unwrap(), "held still: none");
        assert_eq!(holdings.held(), HashSet::from([item.id]));

        // Forgotten while its bytes are still here, as an eviction leaves it.
        holdings.forget(item.id);
        assert!(!holdings.holds(item.id));
        assert!(changes.has_changed().unwrap(), "forgotten: a change");
        changes.borrow_and_update();
        holdings.forget(item.id);
        assert!(!changes.has_changed().unwrap(), "forgotten again: none");
    }

    /// #185 finding 1: an item of two files, only one of whose records has
    /// arrived, is not held, though that file is complete and kept; it is
    /// once the second record lands and its file is kept.
    #[tokio::test]
    async fn an_item_whose_records_are_still_arriving_is_not_held() {
        let store = MemStore::new();
        let holdings = Holdings::new((*store).clone());
        let mut hashes = Vec::new();
        for bytes in [b"part one".to_vec(), b"part two".to_vec()] {
            let added = store.add_bytes(bytes).await.unwrap().hash;
            hashes.push(ContentHash::from_bytes(*added.as_bytes()));
        }
        let born = hashes
            .iter()
            .map(|hash| *hash.as_bytes())
            .collect::<Vec<_>>();
        let mut item = Item::new(ItemId::from_content_hashes(&born));
        for hash in &hashes {
            let tag = KeepReason::Cache.tag(item.id, *hash);
            let blob = Hash::from_bytes(*hash.as_bytes());
            store.tags().set(tag, blob).await.unwrap();
        }

        item.files.insert(hashes[0], content_record("one.mp3"));
        assert!(!holdings.recheck(&item).await.unwrap(), "one record of two");
        item.files.insert(hashes[1], content_record("two.mp3"));
        assert!(holdings.recheck(&item).await.unwrap(), "both");
    }

    /// **Pins a defect (review after #185, finding 4); reverse it with the
    /// fix.** Every file the item was born with is complete and kept, and
    /// one more `role: content` record, which any member can write, makes
    /// the item not held: its files no longer fingerprint to its id.
    #[tokio::test]
    async fn one_more_content_record_makes_a_whole_item_not_held() {
        let store = MemStore::new();
        let holdings = Holdings::new((*store).clone());
        let content = store.add_bytes(b"dune".to_vec()).await.unwrap().hash;
        let hash = ContentHash::from_bytes(*content.as_bytes());
        let mut item = Item::new(ItemId::from_content_hashes(&[*hash.as_bytes()]));
        item.files.insert(hash, content_record("dune.epub"));
        let tag = KeepReason::Custody.tag(item.id, hash);
        store.tags().set(tag, content).await.unwrap();
        assert!(holdings.recheck(&item).await.unwrap());

        // A file nobody has, named by a record somebody wrote.
        let stray = ContentHash::from_bytes([9; 32]);
        item.files.insert(stray, content_record("stray.epub"));
        assert!(
            !holdings.recheck(&item).await.unwrap(),
            "the born file is still complete and kept"
        );
        assert!(!holdings.holds(item.id));
    }

    fn content_record(filename: &str) -> FileRecord {
        FileRecord {
            role: FileRole::Content,
            format: "epub".to_owned(),
            size: 4,
            filename: filename.to_owned(),
            seq: None,
            disc: None,
            title: None,
            duration: None,
        }
    }
}
