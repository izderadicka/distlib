//! Probes (phase 5's D11 and D12): every `probe_every_secs`, a random chunk of
//! an item this node holds is asked of one of its other custodians that
//! announces it. A failure is recorded in the catalogue; this node's next
//! pass of that custodian clears it.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError, Weak},
    time::Duration,
};

use distlib_consensus::MembershipState;
use distlib_core::{ContentHash, FileRole, ItemId, MemberId, ProbeFailure};
use distlib_net::{Blobs, NetError, blobs::Probed};
use distlib_store::{Store, StoreError};
use distlib_sync::{Availability, Catalogue, Holdings, SyncError};
use serde::Serialize;
use tokio::{sync::watch, task::JoinHandle};

use crate::Custody;

/// How long a probe waits to connect, and then for its chunk.
pub const PROBE_WITHIN: Duration = Duration::from_secs(30);

/// How the probes this node made since it started came out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProbeCounts {
    pub passed: u64,
    pub failed: u64,
    /// The custodian could not be reached.
    pub skipped: u64,
}

/// What probes are chosen and made from.
pub struct ProbeSources {
    pub me: MemberId,
    /// Who the custodians of each item are.
    pub custody: Custody,
    /// The group's settings, and who pledges the minimum.
    pub membership: watch::Receiver<MembershipState>,
    /// An item's files.
    pub store: Store,
    /// This node's copies, and the probe itself.
    pub blobs: Blobs,
    /// Who announces what.
    pub availability: Availability,
    /// What this node holds.
    pub holdings: Holdings,
    /// Where failures are recorded.
    pub catalogue: Catalogue,
}

/// This node's probes, made in the background.
#[derive(Debug, Clone)]
pub struct Probes {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    counts: Mutex<ProbeCounts>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().unwrap_or_else(PoisonError::into_inner) {
            task.abort();
        }
    }
}

impl Probes {
    /// Starts probing, every `probe_every_secs` while the group probes.
    pub fn start(sources: ProbeSources) -> Self {
        let inner = Arc::new(Inner::default());
        let task = tokio::spawn(run(sources, Arc::downgrade(&inner)));
        *lock(&inner.task) = Some(task);
        Self { inner }
    }

    pub fn counts(&self) -> ProbeCounts {
        *lock(&self.inner.counts)
    }

    /// Stops the task. Dropping the last handle does the same.
    pub fn shutdown(&self) {
        if let Some(task) = lock(&self.inner.task).take() {
            task.abort();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Why a probe could not be chosen or its outcome recorded.
#[derive(Debug, thiserror::Error)]
enum ProbeError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Net(#[from] NetError),
    #[error(transparent)]
    Sync(#[from] SyncError),
    #[error("no random number to choose with")]
    Random(#[from] getrandom::Error),
}

async fn run(sources: ProbeSources, inner: Weak<Inner>) {
    // The settings come with the group, and never change after.
    let mut membership = sources.membership.clone();
    let every = match membership
        .wait_for(|state| state.group_id().is_some())
        .await
    {
        Ok(group) => Duration::from_secs(group.settings().probe_every_secs.get().into()),
        Err(_) => {
            tracing::error!("the log is gone before there was a group; no probes are made");
            return;
        }
    };
    // This node's own failures recorded, by custodian: how many in a row.
    // Read from the catalogue at the first probe that has an outcome.
    let mut mine: Option<HashMap<MemberId, u32>> = None;
    loop {
        tokio::time::sleep(every).await;
        let probed = probe_once(&sources, &mut mine).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let mut counts = lock(&inner.counts);
        match probed {
            Ok(Some(Probed::Passed)) => counts.passed += 1,
            Ok(Some(Probed::Failed)) => counts.failed += 1,
            Ok(Some(Probed::Unreachable)) => counts.skipped += 1,
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, "could not make a probe; trying again at the next");
            }
        }
    }
}

/// One probe, if the group probes, this node pledges the minimum (a member
/// below it is refused content, which would read as failures), and it holds
/// an item another custodian announces.
async fn probe_once(
    sources: &ProbeSources,
    mine: &mut Option<HashMap<MemberId, u32>>,
) -> Result<Option<Probed>, ProbeError> {
    let ProbeSources {
        me,
        custody,
        membership,
        store,
        blobs,
        availability,
        holdings,
        catalogue,
    } = sources;
    {
        let membership = membership.borrow();
        if !membership.settings().probes || !membership.pledges_the_minimum(me) {
            return Ok(None);
        }
    }
    let held: Vec<ItemId> = holdings.held().into_iter().collect();
    let Some(item) = pick(&held)? else {
        return Ok(None);
    };
    let (Some(stored), Some(group)) = (store.item(item).await?, custody.group()) else {
        return Ok(None);
    };
    let announcers = availability.holders(&item);
    let custodians: Vec<MemberId> = group
        .custodians(&stored.custody_item())
        .into_iter()
        .filter(|custodian| custodian != me && announcers.contains(custodian))
        .collect();
    let contents: Vec<ContentHash> = stored
        .item
        .files
        .iter()
        .filter(|(_, file)| file.role == FileRole::Content)
        .map(|(blob, _)| *blob)
        .collect();
    let (Some(custodian), Some(blob)) = (pick(&custodians)?, pick(&contents)?) else {
        return Ok(None);
    };
    // This node's own copy says how many chunks there are, not the catalogue.
    let Some(chunks) = blobs.chunks(blob).await? else {
        return Ok(None);
    };
    let chunk = below(chunks)?;
    let probed = blobs.probe(custodian, blob, chunk, PROBE_WITHIN).await;

    let mine = match mine {
        Some(mine) => mine,
        None => mine.insert(my_failures(catalogue, *me).await?),
    };
    match probed {
        Probed::Passed => {
            if mine.remove(&custodian).is_some() {
                catalogue.clear_probe_failure(custodian).await?;
            }
        }
        Probed::Failed => {
            let in_a_row = mine.get(&custodian).map_or(1, |before| before + 1);
            tracing::warn!(custodian = %custodian.fmt_short(), %item, %blob, chunk, in_a_row, "a custodian failed a probe");
            let failure = ProbeFailure {
                item,
                blob,
                chunk,
                in_a_row,
            };
            catalogue.record_probe_failure(custodian, &failure).await?;
            mine.insert(custodian, in_a_row);
        }
        Probed::Unreachable => {
            tracing::debug!(custodian = %custodian.fmt_short(), "could not reach a custodian to probe; skipped");
        }
    }
    Ok(Some(probed))
}

/// The failures `me` has recorded, by custodian.
async fn my_failures(
    catalogue: &Catalogue,
    me: MemberId,
) -> Result<HashMap<MemberId, u32>, SyncError> {
    Ok(catalogue
        .probe_failures()
        .await?
        .into_iter()
        .filter(|recorded| recorded.prober == me)
        .map(|recorded| (recorded.custodian, recorded.failure.in_a_row))
        .collect())
}

/// One of `choices` at random, or `None` if there are none.
fn pick<T: Copy>(choices: &[T]) -> Result<Option<T>, getrandom::Error> {
    if choices.is_empty() {
        return Ok(None);
    }
    let index = below(choices.len() as u64)?;
    Ok(choices
        .get(usize::try_from(index).unwrap_or(usize::MAX))
        .copied())
}

/// A random number below `bound`, which is not zero. Its slight lean
/// towards smaller numbers, under one in 2^40 for any bound here, does not
/// matter to sampling.
fn below(bound: u64) -> Result<u64, getrandom::Error> {
    Ok(getrandom::u64()? % bound)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use super::*;

    #[test]
    fn a_pick_is_one_of_the_choices_and_none_of_nothing() {
        assert_eq!(pick::<u8>(&[]).unwrap(), None);
        assert_eq!(pick(&[7]).unwrap(), Some(7));
        let picked = pick(&[1, 2, 3]).unwrap().unwrap();
        assert!([1, 2, 3].contains(&picked));
    }

    #[test]
    fn every_number_below_a_bound_comes_up() {
        let mut seen = [false; 4];
        for _ in 0..400 {
            seen[usize::try_from(below(4).unwrap()).unwrap()] = true;
        }
        assert_eq!(seen, [true; 4]);
    }
}
