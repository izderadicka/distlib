//! The copies probes found missing (phase 5's D11), kept in availability: a
//! custodian that failed a probe of an item within the group's
//! `failure_window_secs` does not hold a real copy of it, for the emergency
//! copy or for letting go of custody.

use std::{
    collections::HashSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use distlib_consensus::MembershipState;
use distlib_core::{ItemId, MemberId};
use distlib_sync::{Availability, Catalogue, RecordedFailure};
use tokio::sync::watch;

/// Keeps `availability`'s failed copies to the failures in `catalogue`: read
/// again at each change, and when the oldest leaves the window.
pub(crate) async fn follow(
    catalogue: Catalogue,
    availability: Availability,
    membership: watch::Receiver<MembershipState>,
) {
    catalogue.ready().await;
    // Taken before the first read, so a change during it is not missed.
    let mut changed = catalogue.probe_failures_changed();
    loop {
        let window = failure_window(&membership.borrow());
        let next_out = match catalogue.probe_failures().await {
            Ok(failures) => {
                let (failed_copies, next_out) = failed_within(&failures, window, SystemTime::now());
                availability.set_failed_copies(failed_copies);
                next_out
            }
            Err(error) => {
                tracing::warn!(%error, "could not read the probe failures; read again at the next change");
                None
            }
        };
        tokio::select! {
            changed = changed.changed() => if changed.is_err() {
                tracing::error!("the catalogue is gone; failed copies are no longer followed");
                return;
            },
            () = tokio::time::sleep(next_out.unwrap_or_default()), if next_out.is_some() => {}
        }
    }
}

/// The group's `failure_window_secs`: how long a failure counts.
pub(crate) fn failure_window(membership: &MembershipState) -> Duration {
    Duration::from_secs(membership.settings().failure_window_secs.into())
}

/// How long `recorded` has left in `window` at `now`; `None` once it is out.
pub(crate) fn left_in_window(
    recorded: &RecordedFailure,
    window: Duration,
    now: SystemTime,
) -> Option<Duration> {
    let now = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let age = now.saturating_sub(Duration::from_micros(recorded.written));
    window.checked_sub(age).filter(|left| !left.is_zero())
}

/// The copies `failures` written within `window` of `now` say are missing,
/// and how long until the first of them is out of the window.
fn failed_within(
    failures: &[RecordedFailure],
    window: Duration,
    now: SystemTime,
) -> (HashSet<(MemberId, ItemId)>, Option<Duration>) {
    failures
        .iter()
        .filter_map(|recorded| {
            let left = left_in_window(recorded, window, now)?;
            Some(((recorded.custodian, recorded.failure.item), left))
        })
        .fold(
            (HashSet::new(), None),
            |(mut failed_copies, next_out), (copy, left)| {
                failed_copies.insert(copy);
                (
                    failed_copies,
                    Some(next_out.map_or(left, |next| left.min(next))),
                )
            },
        )
}

#[cfg(test)]
mod tests {
    use distlib_core::{ContentHash, ProbeFailure};
    use iroh::SecretKey;

    use super::*;

    const WINDOW: Duration = Duration::from_secs(100);

    fn recorded(custodian: MemberId, item: ItemId, written: SystemTime) -> RecordedFailure {
        RecordedFailure {
            custodian,
            prober: MemberId::from(SecretKey::generate().public()),
            failure: ProbeFailure {
                item,
                blob: ContentHash::from_bytes([1; 32]),
                chunk: 0,
                in_a_row: 1,
            },
            written: u64::try_from(
                written
                    .duration_since(UNIX_EPOCH)
                    .expect("after the epoch")
                    .as_micros(),
            )
            .expect("within u64's microseconds"),
        }
    }

    #[test]
    fn only_failures_within_the_window_are_failed_copies_until_the_first_leaves() {
        // Whole seconds, as an entry's microseconds hold it exactly.
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let (bob, carol) = (
            MemberId::from(SecretKey::generate().public()),
            MemberId::from(SecretKey::generate().public()),
        );
        let (book, film) = (ItemId::from_bytes([1; 32]), ItemId::from_bytes([2; 32]));
        let failures = [
            recorded(bob, book, now - Duration::from_secs(30)),
            recorded(bob, film, now - Duration::from_secs(60)),
            recorded(carol, book, now - Duration::from_secs(100)),
        ];

        assert_eq!(
            failed_within(&failures, WINDOW, now),
            (
                HashSet::from([(bob, book), (bob, film)]),
                Some(Duration::from_secs(40))
            ),
            "carol's is as old as the window"
        );
        assert_eq!(failed_within(&[], WINDOW, now), (HashSet::new(), None));
    }
}
