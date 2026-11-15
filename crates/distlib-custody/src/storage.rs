//! The storage limit, kept by evicting the cache (phase 5's "Why a node keeps
//! an item"): before a custody fetch, a download or an add, cached items are
//! let go of until what is about to be kept fits, those with the most copies
//! elsewhere first, then those used longest ago (5e-8). A custody fetch that
//! would not fit even so waits (P5-26).

use std::{cmp::Reverse, collections::BTreeSet, fmt, num::NonZeroU64, sync::Arc, time::SystemTime};

use distlib_consensus::MembershipState;
use distlib_core::{ContentHash, ItemId, KeepReason, MemberId};
use distlib_net::{Blobs, NetError};
use distlib_store::{Store, StoreError};
use distlib_sync::{Availability, Holdings};
use tokio::sync::{Mutex, watch};

/// What this node's blobs may take, and the cache's eviction to stay within it.
///
/// Cheap to clone: every clone makes room under the same lock, so two callers
/// do not both evict for the same space.
#[derive(Debug, Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

struct Inner {
    limit: Option<NonZeroU64>,
    sources: StorageSources,
    making_room: Mutex<()>,
}

/// The limit alone: availability holds the node's secret key, never printed.
impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Storage")
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// What the storage limit is kept with.
pub struct StorageSources {
    /// This node, whose own copy is not one elsewhere.
    pub me: MemberId,
    pub blobs: Blobs,
    /// The last-used times.
    pub store: Store,
    /// Told what is evicted.
    pub holdings: Holdings,
    /// Who is online, and what they hold.
    pub availability: Availability,
    /// For the group's `replicas`.
    pub membership: watch::Receiver<MembershipState>,
}

/// Whether what is about to be kept fits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Room {
    Made,
    /// Over the limit by this many bytes, even with all that could go
    /// evicted.
    Short(u64),
}

/// What to evict when even the whole cache is not room enough.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WhenShort {
    EvictAnyway,
    EvictNothing,
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
    /// Keeps the blobs in `sources` under `limit`. No limit when `None`.
    pub fn new(limit: Option<NonZeroU64>, sources: StorageSources) -> Self {
        Self {
            inner: Arc::new(Inner {
                limit,
                sources,
                making_room: Mutex::new(()),
            }),
        }
    }

    /// The most the blobs this node keeps may take; no limit when `None`.
    pub fn limit(&self) -> Option<NonZeroU64> {
        self.inner.limit
    }

    /// Evicts cached items, in [`eviction_order`], until `files` of `item`
    /// fit under the limit alongside everything kept. Never `item`
    /// itself, nor anything kept for a mandatory reason. When even that is not
    /// enough, all that can go is evicted anyway: whoever wanted the room is
    /// likely to want it again.
    pub async fn make_room(
        &self,
        item: ItemId,
        files: impl IntoIterator<Item = (ContentHash, u64)>,
    ) -> Result<Room, StorageError> {
        self.evict_for(item, files, WhenShort::EvictAnyway).await
    }

    /// [`Self::make_room`], but when even the whole cache is not enough,
    /// nothing is evicted: for custody, which then waits rather than goes
    /// past the limit.
    pub async fn make_room_if_it_fits(
        &self,
        item: ItemId,
        files: impl IntoIterator<Item = (ContentHash, u64)>,
    ) -> Result<Room, StorageError> {
        self.evict_for(item, files, WhenShort::EvictNothing).await
    }

    async fn evict_for(
        &self,
        item: ItemId,
        files: impl IntoIterator<Item = (ContentHash, u64)>,
        when_short: WhenShort,
    ) -> Result<Room, StorageError> {
        let Some(limit) = self.inner.limit else {
            return Ok(Room::Made);
        };
        let _making_room = self.inner.making_room.lock().await;
        let sources = &self.inner.sources;
        let kept = sources.blobs.kept_blobs().await?;
        let last_used = sources.store.last_used().await?;
        let online = sources.availability.online().borrow().clone();
        let replicas = usize::from(sources.membership.borrow().settings().replicas.get());
        let plan = kept.evictions(item, kept.uncounted_bytes(files), limit.get(), |item| {
            eviction_order(
                copies_elsewhere(&sources.availability, sources.me, item, &online),
                replicas,
                last_used.get(&item).copied(),
            )
        });
        if plan.short_by > 0 && when_short == WhenShort::EvictNothing {
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
        let blobs = &self.inner.sources.blobs;
        for blob in blobs.kept_for(KeepReason::Cache, item).await? {
            blobs.release(KeepReason::Cache, item, blob).await?;
        }
        self.inner.sources.holdings.forget(item);
        tracing::info!(%item, "evicted from the cache to stay within the storage limit");
        Ok(())
    }
}

/// How many members in `online`, other than `me`, hold a real copy of `item`.
pub(crate) fn copies_elsewhere(
    availability: &Availability,
    me: MemberId,
    item: ItemId,
    online: &BTreeSet<MemberId>,
) -> usize {
    availability
        .real_holders(&item)
        .iter()
        .filter(|member| **member != me && online.contains(member))
        .count()
}

/// Which cached item goes first, the lowest first (5e-8): the one with the
/// most `copies_elsewhere`, counted up to `replicas`, since past that an item
/// is safe; then the one used longest ago, never used counting as oldest. An
/// item short of copies goes last, never not at all.
fn eviction_order(
    copies_elsewhere: usize,
    replicas: usize,
    last_used: Option<SystemTime>,
) -> (Reverse<usize>, Option<SystemTime>) {
    (Reverse(copies_elsewhere.min(replicas)), last_used)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn at(secs: u64) -> Option<SystemTime> {
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
    }

    #[test]
    fn more_copies_elsewhere_go_first_though_used_more_recently() {
        assert!(eviction_order(2, 3, at(200)) < eviction_order(1, 3, at(100)));
        assert!(eviction_order(1, 3, None) < eviction_order(0, 3, None));
    }

    #[test]
    fn with_as_many_copies_the_older_goes_first() {
        assert!(eviction_order(1, 3, at(100)) < eviction_order(1, 3, at(200)));
        assert!(eviction_order(1, 3, None) < eviction_order(1, 3, at(100)));
    }

    #[test]
    fn copies_past_replicas_count_as_replicas() {
        assert!(eviction_order(3, 3, at(100)) < eviction_order(5, 3, at(200)));
    }
}
