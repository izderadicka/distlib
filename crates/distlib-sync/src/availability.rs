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

use distlib_core::{FileRole, Item, ItemId};
use iroh_blobs::{Hash, api::Store as BlobStore, api::proto::BlobStatus};
use tokio::{sync::watch, time::Instant};

use crate::error::{Result, SyncError};

mod heartbeat;
mod holders;
mod online;
mod statement;

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
    /// Held means **every `role: content` file is complete here** — covers and
    /// other roles do not count, as they do not count towards an item's
    /// identity. An item with no content file yet is one still arriving, and
    /// is not held.
    pub async fn recheck(&self, item: &Item) -> Result<bool> {
        // Collected first: an iterator of closures held across the await
        // below would make this future not `Send`.
        let content: Vec<Hash> = item
            .files
            .iter()
            .filter(|(_, record)| record.role == FileRole::Content)
            .map(|(hash, _)| Hash::from_bytes(*hash.as_bytes()))
            .collect();
        let mut held = !content.is_empty();
        for hash in content {
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
    /// known — every item of a replay — must not look like one.
    #[tokio::test]
    async fn only_a_change_to_the_set_is_signalled() {
        let store = MemStore::new();
        let holdings = Holdings::new((*store).clone());
        let mut changes = holdings.changes();
        let content = store.add_bytes(b"dune".to_vec()).await.unwrap().hash;
        let mut item = Item::new(ItemId::from_bytes([1; 32]));
        item.files.insert(
            ContentHash::from_bytes(*content.as_bytes()),
            FileRecord {
                role: FileRole::Content,
                format: "epub".to_owned(),
                size: 4,
                filename: "dune.epub".to_owned(),
                seq: None,
                disc: None,
                title: None,
                duration: None,
            },
        );

        assert!(holdings.recheck(&item).await.unwrap());
        assert!(changes.has_changed().unwrap(), "held: a change");
        changes.borrow_and_update();
        assert!(holdings.recheck(&item).await.unwrap());
        assert!(!changes.has_changed().unwrap(), "held still: none");
        assert_eq!(holdings.held(), HashSet::from([item.id]));
    }
}
