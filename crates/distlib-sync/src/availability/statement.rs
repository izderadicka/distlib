//! What a beat says about the items this node holds: the last list it
//! published, and what has changed since (phase 4's D3 and D5).
//!
//! The list — the base — is a blob in this node's own store, which receivers
//! fetch once per base; the change since — the delta — rides in every beat,
//! counted from the base rather than from the beat before, so a beat that is
//! lost costs nothing. A new base is published when the delta would no longer
//! fit, and once the set has been quiet a while with a delta to fold in.

use std::{collections::HashSet, time::Duration};

use distlib_core::{
    ContentHash, Holdings, ItemId,
    availability::{DELTA_MAX, encode_base},
};
use iroh_blobs::api::Store as BlobStore;
use tokio::time::Instant;

use crate::error::{Result, SyncError};

/// The one name the newest base is kept under, so publishing the next one
/// unprotects the last rather than keeping every list for ever (D5; the old
/// ones wait for phase 5's GC, C26).
pub(super) const BASE_TAG: &str = "distlib/availability/base";

/// How long the set has to stay unchanged before a delta is folded into a
/// new base — so a burst of downloads costs one base fetch per receiver, at
/// its end, and the beats after it are small again (D5).
const QUIET: Duration = Duration::from_secs(10 * 60);

/// The last base this node published, and the holdings stated against it.
#[derive(Debug)]
pub(super) struct Statement {
    blobs: BlobStore,
    base: Option<Base>,
}

/// A published list: its hash, and the set it lists.
#[derive(Debug)]
struct Base {
    hash: ContentHash,
    items: HashSet<ItemId>,
}

impl Statement {
    /// Nothing published yet: until a base is, the delta is counted from an
    /// empty list.
    pub(super) fn new(blobs: BlobStore) -> Self {
        Self { blobs, base: None }
    }

    /// The holdings a beat sent at `now` states for `held`, a set unchanged
    /// since `changed_at`.
    ///
    /// Publishes `held` as the new base first when the delta would overflow
    /// the beat, or when it is not empty and the set has been quiet for ten
    /// minutes. A failure to publish is returned rather than worked around:
    /// with no base to count from, an overflowing delta has nothing true to
    /// say.
    pub(super) async fn holdings(
        &mut self,
        held: &HashSet<ItemId>,
        changed_at: Instant,
        now: Instant,
    ) -> Result<Holdings> {
        let mut delta = self.delta(held);
        let size = delta.0.len() + delta.1.len();
        if size > DELTA_MAX || (size > 0 && now.saturating_duration_since(changed_at) >= QUIET) {
            self.publish(held).await?;
            delta = (Vec::new(), Vec::new());
        }
        let (added, removed) = delta;
        Ok(Holdings {
            count: u64::try_from(held.len()).unwrap_or(u64::MAX),
            base: self.base.as_ref().map(|base| base.hash),
            added,
            removed,
        })
    }

    /// What `held` adds to the base, and what it no longer has of it.
    fn delta(&self, held: &HashSet<ItemId>) -> (Vec<ItemId>, Vec<ItemId>) {
        let Some(base) = &self.base else {
            return (held.iter().copied().collect(), Vec::new());
        };
        (
            held.difference(&base.items).copied().collect(),
            base.items.difference(held).copied().collect(),
        )
    }

    async fn publish(&mut self, held: &HashSet<ItemId>) -> Result<()> {
        let tagged = self
            .blobs
            .add_bytes(encode_base(held.iter().copied()))
            .with_named_tag(BASE_TAG)
            .await
            .map_err(SyncError::content("published the list of items held here"))?;
        tracing::debug!(hash = %tagged.hash, count = held.len(), "published a new base");
        self.base = Some(Base {
            hash: ContentHash::from_bytes(*tagged.hash.as_bytes()),
            items: held.clone(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use distlib_core::availability::decode_base;
    use iroh_blobs::{Hash, store::mem::MemStore};

    use super::*;

    fn ids(range: std::ops::Range<u16>) -> HashSet<ItemId> {
        range
            .map(|n| {
                let mut bytes = [0; 32];
                bytes[..2].copy_from_slice(&n.to_be_bytes());
                ItemId::from_bytes(bytes)
            })
            .collect()
    }

    fn sorted(mut ids: Vec<ItemId>) -> Vec<ItemId> {
        ids.sort_unstable();
        ids
    }

    /// The list a base names, read back out of the store, and the hash the
    /// tag points at.
    async fn published(store: &MemStore, base: ContentHash) -> (HashSet<ItemId>, ContentHash) {
        let bytes = store
            .blobs()
            .get_bytes(Hash::from_bytes(*base.as_bytes()))
            .await
            .unwrap();
        let tagged = store.tags().get(BASE_TAG).await.unwrap().unwrap().hash;
        (
            decode_base(&bytes).unwrap().collect(),
            ContentHash::from_bytes(*tagged.as_bytes()),
        )
    }

    #[tokio::test]
    async fn before_any_base_everything_held_is_the_delta() {
        let store = MemStore::new();
        let mut statement = Statement::new((*store).clone());
        let now = Instant::now();

        let nothing = statement.holdings(&HashSet::new(), now, now).await.unwrap();
        assert_eq!(nothing, Holdings::default());
        let quiet = statement
            .holdings(&HashSet::new(), now, now + 2 * QUIET)
            .await
            .unwrap();
        assert_eq!(
            quiet,
            Holdings::default(),
            "no delta, no base: nothing to fold in"
        );

        let held = ids(0..3);
        let some = statement.holdings(&held, now, now).await.unwrap();
        assert_eq!(some.count, 3);
        assert_eq!(some.base, None);
        assert_eq!(sorted(some.added), sorted(held.into_iter().collect()));
        assert!(some.removed.is_empty());
        assert!(
            store.tags().get(BASE_TAG).await.unwrap().is_none(),
            "nothing published"
        );
    }

    #[tokio::test]
    async fn a_delta_that_would_overflow_is_folded_into_a_new_base() {
        let store = MemStore::new();
        let mut statement = Statement::new((*store).clone());
        let now = Instant::now();

        let just_fits = ids(0..DELTA_MAX as u16);
        let fits = statement.holdings(&just_fits, now, now).await.unwrap();
        assert_eq!((fits.base, fits.added.len()), (None, DELTA_MAX));

        let held = ids(0..DELTA_MAX as u16 + 1);
        let stated = statement.holdings(&held, now, now).await.unwrap();
        let base = stated.base.expect("a base is published");
        assert_eq!(stated.count, held.len() as u64);
        assert!(stated.added.is_empty() && stated.removed.is_empty());
        assert_eq!(published(&store, base).await, (held, base));
    }

    /// After a base, the delta is counted from it — both ways.
    #[tokio::test]
    async fn the_delta_is_what_changed_since_the_base() {
        let store = MemStore::new();
        let mut statement = Statement::new((*store).clone());
        let now = Instant::now();
        let base = statement
            .holdings(&ids(0..500), now, now)
            .await
            .unwrap()
            .base
            .unwrap();

        let held = ids(1..502);
        let stated = statement.holdings(&held, now, now).await.unwrap();
        assert_eq!(stated.base, Some(base));
        assert_eq!(stated.count, 501);
        assert_eq!(
            sorted(stated.added),
            sorted(ids(500..502).into_iter().collect())
        );
        assert_eq!(stated.removed, ids(0..1).into_iter().collect::<Vec<_>>());
    }

    /// Ten quiet minutes fold a delta into a new base, and the tag moves to
    /// it; nine do not, and neither does an empty delta.
    #[tokio::test]
    async fn a_quiet_delta_is_folded_into_a_new_base() {
        let store = MemStore::new();
        let mut statement = Statement::new((*store).clone());
        let changed = Instant::now();
        let first = statement
            .holdings(&ids(0..500), changed, changed)
            .await
            .unwrap()
            .base
            .unwrap();

        let held = ids(0..501);
        let not_yet = statement
            .holdings(&held, changed, changed + QUIET - Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!((not_yet.base, not_yet.added.len()), (Some(first), 1));

        let quiet = statement
            .holdings(&held, changed, changed + QUIET)
            .await
            .unwrap();
        let second = quiet.base.unwrap();
        assert_ne!(second, first);
        assert!(quiet.added.is_empty());
        assert_eq!(published(&store, second).await, (held.clone(), second));

        let again = statement
            .holdings(&held, changed, changed + 2 * QUIET)
            .await
            .unwrap();
        assert_eq!(
            again.base,
            Some(second),
            "nothing to fold in, nothing published"
        );
    }
}
