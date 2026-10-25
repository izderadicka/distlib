//! Coverage (phase 5's D10): of the items this node holds, the share each of
//! their other custodians announces holding. This node's own view, from the
//! heartbeats it hears; nothing is sent for it.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use distlib_consensus::MembershipState;
use distlib_core::{CustodyGroup, CustodyItem, ItemId, MemberId};
use distlib_store::{Store, StoreError};
use distlib_sync::{Availability, Holdings};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
    time::Instant,
};

use crate::Custody;

/// How often coverage is worked out again, unless beats come more often.
pub const COVERAGE_EVERY: Duration = Duration::from_secs(30);

/// One member's coverage: of the items this node holds that the member
/// should hold too, how many it announces.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberCoverage {
    pub expected: usize,
    pub announced: usize,
}

impl MemberCoverage {
    /// Whether the member announces less than it should hold: a suspect.
    pub fn is_low(&self) -> bool {
        self.announced < self.expected
    }
}

/// What coverage is worked out from.
pub struct CoverageSources {
    /// Who the custodians of each item are, and since when.
    pub custody: Custody,
    /// For `max_settle_secs`.
    pub membership: watch::Receiver<MembershipState>,
    /// An item's creators and size.
    pub store: Store,
    /// Who is online, and what they hold.
    pub availability: Availability,
    /// What this node holds.
    pub holdings: Holdings,
}

/// Every judged member's coverage, worked out again in the background.
#[derive(Debug, Clone)]
pub struct Coverage {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    counted: Mutex<HashMap<MemberId, MemberCoverage>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().unwrap_or_else(PoisonError::into_inner) {
            task.abort();
        }
    }
}

impl Coverage {
    /// Starts working out coverage every `every`.
    pub fn start(sources: CoverageSources, every: Duration) -> Self {
        let inner = Arc::new(Inner::default());
        let task = tokio::spawn(run(sources, every, Arc::downgrade(&inner)));
        *lock(&inner.task) = Some(task);
        Self { inner }
    }

    /// `member`'s coverage as last worked out. `None` while it is not
    /// judged: offline, what it holds not known yet, or before the first
    /// count.
    pub fn of(&self, member: &MemberId) -> Option<MemberCoverage> {
        lock(&self.inner.counted).get(member).copied()
    }

    /// The members whose coverage, as last worked out, is low.
    pub(crate) fn low(&self) -> Vec<MemberId> {
        lock(&self.inner.counted)
            .iter()
            .filter(|(_, coverage)| coverage.is_low())
            .map(|(member, _)| *member)
            .collect()
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

/// Why coverage could not be worked out this time.
#[derive(Debug, thiserror::Error)]
enum CoverageError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the count failed")]
    Count(#[from] JoinError),
}

async fn run(sources: CoverageSources, every: Duration, inner: std::sync::Weak<Inner>) {
    let mut ticks = tokio::time::interval(every);
    loop {
        ticks.tick().await;
        let counted = match count_now(&sources).await {
            Ok(counted) => counted,
            Err(error) => {
                tracing::warn!(%error, "could not work out coverage; trying again later");
                continue;
            }
        };
        let Some(inner) = inner.upgrade() else {
            return;
        };
        *lock(&inner.counted) = counted;
    }
}

/// Coverage now, of every member online whose holdings are known.
async fn count_now(
    sources: &CoverageSources,
) -> Result<HashMap<MemberId, MemberCoverage>, CoverageError> {
    let CoverageSources {
        custody,
        membership,
        store,
        availability,
        holdings,
    } = sources;
    let Some((group, since)) = custody.assignments(holdings.held()) else {
        return Ok(HashMap::new());
    };
    let max_settle = Duration::from_secs(membership.borrow().settings().max_settle_secs.into());
    let online = availability.online().borrow().clone();
    let judged = to_judge(online, |member| availability.knows_holdings_of(member));

    let mut held = Vec::with_capacity(since.len());
    for (item, since) in since {
        if let Some(stored) = store.item(item).await? {
            held.push((stored.custody_item(), since));
        }
    }

    let availability = availability.clone();
    let now = Instant::now();
    let counted = tokio::task::spawn_blocking(move || {
        count(
            &group,
            &held,
            &judged,
            |item| availability.holders(item),
            |since| now.saturating_duration_since(since) >= max_settle,
        )
    })
    .await?;
    Ok(counted)
}

/// The members of `online` whose holdings are known.
fn to_judge(
    online: impl IntoIterator<Item = MemberId>,
    knows_holdings_of: impl Fn(&MemberId) -> bool,
) -> HashSet<MemberId> {
    online
        .into_iter()
        .filter(|member| knows_holdings_of(member))
        .collect()
}

/// Each `judged` member's coverage of `held`, each item with when its
/// custodians were worked out: only the items assigned long enough ago, by
/// `old_enough`, count, so a member still fetching is not blamed. A judged
/// member with nothing to hold counts nothing.
fn count(
    group: &CustodyGroup,
    held: &[(CustodyItem, Instant)],
    judged: &HashSet<MemberId>,
    holders: impl Fn(&ItemId) -> Vec<MemberId>,
    old_enough: impl Fn(Instant) -> bool,
) -> HashMap<MemberId, MemberCoverage> {
    let mut counted: HashMap<MemberId, MemberCoverage> = judged
        .iter()
        .map(|member| (*member, MemberCoverage::default()))
        .collect();
    for (item, _) in held.iter().filter(|(_, since)| old_enough(*since)) {
        let announced_by = holders(&item.item);
        for custodian in group
            .custodians(item)
            .into_iter()
            .filter(|custodian| judged.contains(custodian))
        {
            let coverage = counted.entry(custodian).or_default();
            coverage.expected += 1;
            if announced_by.contains(&custodian) {
                coverage.announced += 1;
            }
        }
    }
    counted
}

#[cfg(test)]
mod tests {
    use distlib_core::WeightRule;

    use super::*;

    fn member(seed: u8) -> MemberId {
        MemberId::from(iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    /// Me, who added every item, and two others, each custodian of all of
    /// them: three replicas, one of them mine.
    fn group() -> (CustodyGroup, MemberId, MemberId, MemberId) {
        let (me, bob, carol) = (member(1), member(2), member(3));
        let rule = WeightRule {
            cap_bytes: 1_000,
            step_bytes: 1,
            min_bytes: 0,
        };
        let group = CustodyGroup::new(3, rule, [(me, 500), (bob, 500), (carol, 500)]);
        (group, me, bob, carol)
    }

    fn item(seed: u8, creator: MemberId) -> CustodyItem {
        CustodyItem {
            item: ItemId::from_bytes([seed; 32]),
            creators: vec![creator],
            size_bytes: 10,
        }
    }

    fn covered(expected: usize, announced: usize) -> MemberCoverage {
        MemberCoverage {
            expected,
            announced,
        }
    }

    #[test]
    fn each_custodian_is_judged_by_what_it_announces_of_what_it_should_hold() {
        let (group, me, bob, carol) = group();
        let start = Instant::now();
        let held = [(item(1, me), start), (item(2, me), start)];
        let judged = HashSet::from([bob, carol]);
        // Bob announces both, Carol only the first.
        let holders = |item: &ItemId| {
            if *item == ItemId::from_bytes([1; 32]) {
                vec![bob, carol]
            } else {
                vec![bob]
            }
        };
        let counted = count(&group, &held, &judged, holders, |_| true);
        assert_eq!(counted[&bob], covered(2, 2));
        assert_eq!(counted[&carol], covered(2, 1));
    }

    #[test]
    fn a_young_assignment_and_an_unjudged_member_are_not_counted() {
        let (group, me, bob, carol) = group();
        let start = Instant::now();
        let young = start + Duration::from_secs(1);
        let held = [(item(1, me), start), (item(2, me), young)];
        // Carol is not judged.
        let judged = HashSet::from([bob]);
        let counted = count(
            &group,
            &held,
            &judged,
            |_| Vec::new(),
            |since| since == start,
        );
        assert_eq!(counted, HashMap::from([(bob, covered(1, 0))]));
        assert!(!counted.contains_key(&carol));
    }

    #[test]
    fn a_judged_member_with_nothing_to_hold_counts_nothing() {
        let (group, _, bob, _) = group();
        let judged = HashSet::from([bob]);
        let counted = count(&group, &[], &judged, |_| Vec::new(), |_| true);
        assert_eq!(counted, HashMap::from([(bob, covered(0, 0))]));
    }

    #[test]
    fn coverage_is_low_when_less_is_announced_than_expected() {
        assert!(covered(2, 1).is_low());
        assert!(!covered(2, 2).is_low());
        assert!(!covered(0, 0).is_low());
    }

    #[test]
    fn a_member_is_judged_only_once_what_it_holds_is_known() {
        let (_, _, bob, carol) = group();
        let judged = to_judge([bob, carol], |member| *member == bob);
        assert_eq!(judged, HashSet::from([bob]));
    }
}
