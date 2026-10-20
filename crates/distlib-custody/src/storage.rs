//! The storage limit, kept by evicting the cache (phase 5's "Why a node keeps
//! an item"): before a custody fetch, a download or an add, the cached items
//! used longest ago are let go of until what is about to be kept fits.

use std::{num::NonZeroU64, sync::Arc};

use distlib_core::{ContentHash, ItemId, KeepReason};
use distlib_net::{Blobs, NetError};
use distlib_store::{Store, StoreError};
use distlib_sync::Holdings;
use tokio::sync::Mutex;

/// What this node's blobs may take, and the cache's eviction to stay within it.
///
/// Cheap to clone: every clone makes room under the same lock, so two callers
/// do not both evict for the same space.
#[derive(Debug, Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    limit: Option<NonZeroU64>,
    blobs: Blobs,
    store: Store,
    holdings: Holdings,
    making_room: Mutex<()>,
}

/// What to do when even the whole cache is not room enough.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IfShort {
    /// Evict nothing: what wanted the room is refused, and the cache stays.
    KeepTheCache,
    /// Evict all that can go: what wanted the room is kept anyway.
    EvictAnyway,
}

/// Whether what is about to be kept fits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Room {
    Made,
    /// Over the limit by this many bytes, with what [`IfShort`] allowed
    /// evicted.
    Short(u64),
}

/// Why room could not be made.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    Blobs(#[from] NetError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl Storage {
    /// Keeps the blobs in `blobs` under `limit`, with last-used times from
    /// `store`, telling `holdings` what is evicted. No limit when `None`.
    pub fn new(limit: Option<NonZeroU64>, blobs: Blobs, store: Store, holdings: Holdings) -> Self {
        Self {
            inner: Arc::new(Inner {
                limit,
                blobs,
                store,
                holdings,
                making_room: Mutex::new(()),
            }),
        }
    }

    /// The most the blobs this node keeps may take; no limit when `None`.
    pub fn limit(&self) -> Option<NonZeroU64> {
        self.inner.limit
    }

    /// Evicts cached items, least recently used first, until `files` of
    /// `item` fit under the limit alongside everything kept. Never `item`
    /// itself, nor anything kept as own or custody.
    pub async fn make_room(
        &self,
        item: ItemId,
        files: impl IntoIterator<Item = (ContentHash, u64)>,
        if_short: IfShort,
    ) -> Result<Room, StorageError> {
        let Some(limit) = self.inner.limit else {
            return Ok(Room::Made);
        };
        let _making_room = self.inner.making_room.lock().await;
        let kept = self.inner.blobs.kept_blobs().await?;
        let last_used = self.inner.store.last_used().await?;
        let plan = kept.evictions(item, kept.uncounted_bytes(files), limit.get(), |item| {
            last_used.get(&item).copied()
        });
        if plan.short_by > 0 && if_short == IfShort::KeepTheCache {
            return Ok(Room::Short(plan.short_by));
        }
        for evicted in plan.items {
            self.evict(evicted).await?;
        }
        Ok(match plan.short_by {
            0 => Room::Made,
            short_by => Room::Short(short_by),
        })
    }

    /// Lets go of `item`'s cached blobs; they go at the next collection.
    async fn evict(&self, item: ItemId) -> Result<(), StorageError> {
        let blobs = &self.inner.blobs;
        for blob in blobs.kept_for(KeepReason::Cache, item).await? {
            blobs.release(KeepReason::Cache, item, blob).await?;
        }
        self.inner.holdings.forget(item);
        tracing::info!(%item, "evicted from the cache to stay within the storage limit");
        Ok(())
    }
}
