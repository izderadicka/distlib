//! Who is online, and which items this node holds — what its heartbeat says
//! (phase 4's D4 and D6).
//!
//! The heartbeat is [`Availability`]; the rest of this file is the set its
//! holdings will be read from.
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

use crate::error::{Result, SyncError};

mod heartbeat;
mod online;

pub use heartbeat::{Availability, topic_for};

/// The items every one of whose content files is complete in this node's
/// store.
///
/// Cheap to clone; every clone is the same set.
#[derive(Debug, Clone)]
pub struct Holdings {
    blobs: BlobStore,
    held: Arc<Mutex<HashSet<ItemId>>>,
}

impl Holdings {
    pub(crate) fn new(blobs: BlobStore) -> Self {
        Self {
            blobs,
            held: Arc::default(),
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
        if held {
            set.insert(item.id);
        } else {
            set.remove(&item.id);
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
}
