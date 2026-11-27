//! A failed copy is tried again on a clock (5i-4): a minute after the first
//! failure, doubling with each one in a row, up to an hour.

use std::time::Duration;

use tokio::time::Instant;

const FIRST_RETRY_AFTER: Duration = Duration::from_secs(60);
const LAST_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);

/// How many times in a row an item's copy has failed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Failures(u32);

impl Failures {
    /// One more failure at `now`, and when to try again after it.
    pub(crate) fn one_more(self, now: Instant) -> (Self, Instant) {
        let failures = Self(self.0.saturating_add(1));
        (failures, now + failures.retry_after())
    }

    /// How long to wait after the last of these failures.
    fn retry_after(self) -> Duration {
        let doublings = self.0.saturating_sub(1);
        FIRST_RETRY_AFTER
            .saturating_mul(2_u32.saturating_pow(doublings))
            .min(LAST_RETRY_AFTER)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retry_waits_a_minute_doubling_to_an_hour() {
        let now = Instant::now();
        let waits: Vec<Duration> = (0..9)
            .scan(Failures::default(), |failures, _| {
                let (more, retry_at) = failures.one_more(now);
                *failures = more;
                Some(retry_at - now)
            })
            .collect();
        let minutes = |minutes: u64| Duration::from_secs(minutes * 60);
        assert_eq!(
            waits,
            [1, 2, 4, 8, 16, 32, 60, 60, 60].map(minutes),
            "and stays at an hour"
        );
        assert_eq!(Failures(u32::MAX).one_more(now).1 - now, minutes(60));
    }
}
