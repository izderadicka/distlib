//! Probes (phase 5's D11 and D12): every `probe_every_secs`, a random chunk of
//! an item this node holds is asked of one of its other custodians that
//! announces it. One probe in four re-checks a suspect instead. A failure is
//! recorded in the catalogue; this node's next pass of that custodian clears
//! it.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError, Weak},
    time::{Duration, SystemTime},
};

use distlib_consensus::MembershipState;
use distlib_core::{ContentHash, FileRole, ItemId, MemberId, ProbeFailure};
use distlib_net::{Blobs, NetError, blobs::Probed};
use distlib_store::{Store, StoreError};
use distlib_sync::{Availability, Catalogue, Holdings, RecordedFailure, SyncError};
use serde::Serialize;
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    Coverage, Custody,
    failed_copies::{failure_window, left_in_window},
};

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
    /// Who has low coverage: a suspect.
    pub coverage: Coverage,
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
    let Ok(every) = membership
        .wait_for(|state| state.group_id().is_some())
        .await
        .map(|group| Duration::from_secs(group.settings().probe_every_secs.get().into()))
    else {
        tracing::error!("the log is gone before there was a group; no probes are made");
        return;
    };
    // This node's own failures recorded, by custodian. Read from the
    // catalogue at the first probe.
    let mut mine: Option<HashMap<MemberId, ProbeFailure>> = None;
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

/// One probe in this many re-checks a suspect (D11).
const SUSPECT_ONE_IN: u64 = 4;

/// What one probe asks `custodian` for: `chunk` of `blob`, a content file of
/// `item`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Target {
    custodian: MemberId,
    item: ItemId,
    blob: ContentHash,
    chunk: u64,
}

impl Target {
    /// The chunk a failure of `custodian` was recorded at.
    fn recorded(custodian: MemberId, failure: &ProbeFailure) -> Self {
        Self {
            custodian,
            item: failure.item,
            blob: failure.blob,
            chunk: failure.chunk,
        }
    }
}

/// One probe, if the group probes, this node pledges the minimum (a member
/// below it is refused content, which would read as failures), and there is
/// a custodian to probe: one in four a suspect, otherwise, or with no
/// suspect, an item this node holds that another custodian announces.
async fn probe_once(
    sources: &ProbeSources,
    mine: &mut Option<HashMap<MemberId, ProbeFailure>>,
) -> Result<Option<Probed>, ProbeError> {
    {
        let membership = sources.membership.borrow();
        if !membership.settings().probes || !membership.pledges_the_minimum(&sources.me) {
            return Ok(None);
        }
    }
    let mine = match mine {
        Some(mine) => mine,
        None => mine.insert(my_failures(&sources.catalogue, sources.me).await?),
    };
    let suspect = if below(SUSPECT_ONE_IN)? == 0 {
        suspect_target(sources, mine).await?
    } else {
        None
    };
    let target = match suspect {
        Some(target) => target,
        None => match random_target(sources, None).await? {
            Some(target) => target,
            None => return Ok(None),
        },
    };
    let probed = sources
        .blobs
        .probe(target.custodian, target.blob, target.chunk, PROBE_WITHIN)
        .await;
    record(&sources.catalogue, mine, target, probed).await?;
    Ok(Some(probed))
}

/// A suspect to re-check (D11), of the custodians that still announce the
/// item: this node's own failures first, to confirm or clear them, then
/// those others recorded within the window, both at the recorded chunk; then
/// a member with low coverage (D10), at a random chunk of an item it
/// announces.
async fn suspect_target(
    sources: &ProbeSources,
    mine: &HashMap<MemberId, ProbeFailure>,
) -> Result<Option<Target>, ProbeError> {
    let ProbeSources {
        me,
        membership,
        availability,
        catalogue,
        coverage,
        ..
    } = sources;
    let own: Vec<Target> = mine
        .iter()
        .map(|(custodian, failure)| Target::recorded(*custodian, failure))
        .collect();
    let window = failure_window(&membership.borrow());
    let others = others_failures(
        &catalogue.probe_failures().await?,
        *me,
        window,
        SystemTime::now(),
    );
    let announces = |target: &Target| {
        availability
            .holders(&target.item)
            .contains(&target.custodian)
    };
    match suspect(&own, &others, &coverage.low(), announces)? {
        Some(Suspect::Recorded(target)) => Ok(Some(target)),
        Some(Suspect::LowCoverage(member)) => random_target(sources, Some(member)).await,
        None => Ok(None),
    }
}

/// The failures others recorded of custodians other than `me`, within
/// `window` of `now`.
fn others_failures(
    failures: &[RecordedFailure],
    me: MemberId,
    window: Duration,
    now: SystemTime,
) -> Vec<Target> {
    failures
        .iter()
        .filter(|recorded| {
            recorded.prober != me
                && recorded.custodian != me
                && left_in_window(recorded, window, now).is_some()
        })
        .map(|recorded| Target::recorded(recorded.custodian, &recorded.failure))
        .collect()
}

/// Who a suspect probe re-checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Suspect {
    /// A recorded failure, at its chunk.
    Recorded(Target),
    /// A member with low coverage.
    LowCoverage(MemberId),
}

/// One of `own` that `announces` admits, else one of `others`, else one of
/// `low`.
fn suspect(
    own: &[Target],
    others: &[Target],
    low: &[MemberId],
    announces: impl Fn(&Target) -> bool,
) -> Result<Option<Suspect>, getrandom::Error> {
    for recorded in [own, others] {
        let announced: Vec<Target> = recorded.iter().copied().filter(&announces).collect();
        if let Some(target) = pick(&announced)? {
            return Ok(Some(Suspect::Recorded(target)));
        }
    }
    Ok(pick(low)?.map(Suspect::LowCoverage))
}

/// A random chunk of a random content file of a random item this node holds,
/// of one of its other custodians that announces it: `only`, if given.
async fn random_target(
    sources: &ProbeSources,
    only: Option<MemberId>,
) -> Result<Option<Target>, ProbeError> {
    let ProbeSources {
        me,
        custody,
        store,
        blobs,
        availability,
        holdings,
        ..
    } = sources;
    let held: Vec<ItemId> = holdings
        .held()
        .into_iter()
        .filter(|item| only.is_none_or(|member| availability.holders(item).contains(&member)))
        .collect();
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
        .filter(|custodian| may_probe(*custodian, *me, &announcers, only))
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
    Ok(Some(Target {
        custodian,
        item,
        blob,
        chunk: below(chunks)?,
    }))
}

/// Whether `me` may probe `custodian` for an item `announcers` announce: not
/// itself, one that announces it, and `only`, if given.
fn may_probe(
    custodian: MemberId,
    me: MemberId,
    announcers: &[MemberId],
    only: Option<MemberId>,
) -> bool {
    custodian != me
        && announcers.contains(&custodian)
        && only.is_none_or(|member| member == custodian)
}

/// Records how probing `target` came out: a failure written, counting those
/// in a row, or this node's failure of that custodian cleared by a pass.
async fn record(
    catalogue: &Catalogue,
    mine: &mut HashMap<MemberId, ProbeFailure>,
    target: Target,
    probed: Probed,
) -> Result<(), SyncError> {
    let Target {
        custodian,
        item,
        blob,
        chunk,
    } = target;
    match probed {
        Probed::Passed => {
            if mine.remove(&custodian).is_some() {
                catalogue.clear_probe_failure(custodian).await?;
            }
        }
        Probed::Failed => {
            let in_a_row = mine.get(&custodian).map_or(1, |before| before.in_a_row + 1);
            tracing::warn!(custodian = %custodian.fmt_short(), %item, %blob, chunk, in_a_row, "a custodian failed a probe");
            let failure = ProbeFailure {
                item,
                blob,
                chunk,
                in_a_row,
            };
            catalogue.record_probe_failure(custodian, &failure).await?;
            mine.insert(custodian, failure);
        }
        Probed::Unreachable => {
            tracing::debug!(custodian = %custodian.fmt_short(), "could not reach a custodian to probe; skipped");
        }
    }
    Ok(())
}

/// The failures `me` has recorded, by custodian.
async fn my_failures(
    catalogue: &Catalogue,
    me: MemberId,
) -> Result<HashMap<MemberId, ProbeFailure>, SyncError> {
    Ok(catalogue
        .probe_failures()
        .await?
        .into_iter()
        .filter(|recorded| recorded.prober == me)
        .map(|recorded| (recorded.custodian, recorded.failure))
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

    use std::collections::HashSet;

    use super::*;

    #[test]
    fn a_pick_is_any_of_the_choices_and_none_of_nothing() {
        assert_eq!(pick::<u8>(&[]).unwrap(), None);
        let picked: HashSet<u8> = (0..300)
            .map(|_| pick(&[1, 2, 3]).unwrap().unwrap())
            .collect();
        assert_eq!(picked, HashSet::from([1, 2, 3]));
    }

    fn member(seed: u8) -> MemberId {
        MemberId::from(iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    fn target(custodian: MemberId, chunk: u64) -> Target {
        Target {
            custodian,
            item: ItemId::from_bytes([1; 32]),
            blob: ContentHash::from_bytes([2; 32]),
            chunk,
        }
    }

    #[test]
    fn a_suspect_is_an_own_failure_then_anothers_then_low_coverage() {
        let (bob, carol, dave) = (member(1), member(2), member(3));
        let (own, others, low) = ([target(bob, 1)], [target(carol, 2)], [dave]);
        let everyone = |_: &Target| true;
        for _ in 0..20 {
            assert_eq!(
                suspect(&own, &others, &low, everyone).unwrap(),
                Some(Suspect::Recorded(target(bob, 1)))
            );
        }
        assert_eq!(
            suspect(&[], &others, &low, everyone).unwrap(),
            Some(Suspect::Recorded(target(carol, 2)))
        );
        assert_eq!(
            suspect(&[], &[], &low, everyone).unwrap(),
            Some(Suspect::LowCoverage(dave))
        );
        assert_eq!(suspect(&[], &[], &[], everyone).unwrap(), None);
    }

    #[test]
    fn a_failure_whose_custodian_no_longer_announces_the_item_is_passed_over() {
        let (bob, carol) = (member(1), member(2));
        let own = [target(bob, 1), target(carol, 2)];
        let not_bob = |target: &Target| target.custodian != bob;
        for _ in 0..20 {
            assert_eq!(
                suspect(&own, &[], &[], not_bob).unwrap(),
                Some(Suspect::Recorded(target(carol, 2)))
            );
        }
        assert_eq!(
            suspect(&own[..1], &[target(bob, 3)], &[carol], not_bob).unwrap(),
            Some(Suspect::LowCoverage(carol)),
            "others' failures are passed over the same way"
        );
    }

    #[test]
    fn anothers_failure_counts_within_the_window_and_not_of_me() {
        let (me, bob, carol) = (member(1), member(2), member(3));
        let now = SystemTime::now();
        let window = Duration::from_secs(100);
        let recorded = |custodian, prober, chunk, age: u64| RecordedFailure {
            custodian,
            prober,
            failure: ProbeFailure {
                item: ItemId::from_bytes([1; 32]),
                blob: ContentHash::from_bytes([2; 32]),
                chunk,
                in_a_row: 1,
            },
            written: u64::try_from(
                (now - Duration::from_secs(age))
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros(),
            )
            .unwrap(),
        };
        let failures = [
            recorded(bob, carol, 1, 10),
            recorded(bob, me, 2, 10),
            recorded(me, carol, 3, 10),
            recorded(bob, carol, 4, 200),
        ];
        assert_eq!(
            others_failures(&failures, me, window, now),
            [target(bob, 1)],
            "not my own, not of me, not out of the window"
        );
    }

    #[test]
    fn only_another_custodian_that_announces_the_item_is_probed_and_only_the_one_asked_for() {
        let (me, bob, carol) = (member(1), member(2), member(3));
        let announcers = [me, bob];
        assert!(may_probe(bob, me, &announcers, None));
        assert!(!may_probe(me, me, &announcers, None), "not itself");
        assert!(!may_probe(carol, me, &announcers, None), "not announced");
        assert!(may_probe(bob, me, &announcers, Some(bob)));
        assert!(!may_probe(bob, me, &announcers, Some(carol)));
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
