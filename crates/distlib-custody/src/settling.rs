//! Waiting for the membership to settle before custody follows it (D4).

use std::time::Duration;

use tokio::time::Instant;

/// A value that is adopted only once it has stayed the same for `settle`, or
/// at the latest `max_settle` after it first moved away from the adopted one.
#[derive(Debug)]
pub(crate) struct Settling<T> {
    settle: Duration,
    max_settle: Duration,
    adopted: T,
    waiting: Option<Waiting<T>>,
}

/// A value seen but not adopted yet.
#[derive(Debug)]
struct Waiting<T> {
    value: T,
    first_change: Instant,
    last_change: Instant,
}

impl<T: PartialEq> Settling<T> {
    /// Starts from `adopted`, taken as settled already.
    pub(crate) fn new(adopted: T, settle: Duration, max_settle: Duration) -> Self {
        Self {
            settle,
            max_settle,
            adopted,
            waiting: None,
        }
    }

    pub(crate) fn adopted(&self) -> &T {
        &self.adopted
    }

    /// The value is now `value`. The same value again changes nothing; a
    /// different one restarts the wait, but not the `max_settle` limit.
    pub(crate) fn change(&mut self, value: T, now: Instant) {
        if value == self.adopted {
            self.waiting = None;
            return;
        }
        match &mut self.waiting {
            Some(waiting) if waiting.value == value => {}
            Some(waiting) => {
                waiting.value = value;
                waiting.last_change = now;
            }
            None => {
                self.waiting = Some(Waiting {
                    value,
                    first_change: now,
                    last_change: now,
                });
            }
        }
    }

    /// When the value waiting is adopted, if one is.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.waiting.as_ref().map(|waiting| {
            (waiting.last_change + self.settle).min(waiting.first_change + self.max_settle)
        })
    }

    /// Adopts the value waiting if its deadline has passed, and returns it.
    pub(crate) fn settle(&mut self, now: Instant) -> Option<&T> {
        if self.deadline()? > now {
            return None;
        }
        self.adopted = self.waiting.take()?.value;
        Some(&self.adopted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SETTLE: Duration = Duration::from_secs(30);
    const MAX_SETTLE: Duration = Duration::from_secs(100);

    fn secs(start: Instant, secs: u64) -> Instant {
        start + Duration::from_secs(secs)
    }

    #[test]
    fn a_burst_of_changes_is_adopted_once_settle_after_the_last() {
        let start = Instant::now();
        let mut settling = Settling::new(0, SETTLE, MAX_SETTLE);
        for (at, value) in [(0, 1), (10, 2), (20, 3)] {
            settling.change(value, secs(start, at));
            assert_eq!(settling.settle(secs(start, at)), None);
        }
        assert_eq!(settling.deadline(), Some(secs(start, 50)));
        assert_eq!(settling.settle(secs(start, 49)), None);
        assert_eq!(settling.settle(secs(start, 50)), Some(&3));
        assert_eq!(settling.deadline(), None);
        assert_eq!(settling.settle(secs(start, 90)), None);
    }

    #[test]
    fn a_value_that_never_settles_is_adopted_at_max_settle() {
        let start = Instant::now();
        let mut settling = Settling::new(0, SETTLE, MAX_SETTLE);
        for at in (0..=90).step_by(10) {
            settling.change(at + 1, secs(start, at));
        }
        assert_eq!(settling.deadline(), Some(secs(start, 100)));
        assert_eq!(settling.settle(secs(start, 100)), Some(&91));
    }

    #[test]
    fn the_same_value_again_does_not_restart_the_wait() {
        let start = Instant::now();
        let mut settling = Settling::new(0, SETTLE, MAX_SETTLE);
        settling.change(1, secs(start, 0));
        settling.change(1, secs(start, 20));
        assert_eq!(settling.deadline(), Some(secs(start, 30)));
    }

    #[test]
    fn going_back_to_the_adopted_value_stops_waiting() {
        let start = Instant::now();
        let mut settling = Settling::new(0, SETTLE, MAX_SETTLE);
        settling.change(1, secs(start, 0));
        settling.change(0, secs(start, 10));
        assert_eq!(settling.deadline(), None);
        assert_eq!(settling.settle(secs(start, 200)), None);
        assert_eq!(settling.adopted(), &0);
    }

    #[test]
    fn with_no_settle_period_a_change_is_adopted_at_once() {
        let start = Instant::now();
        let mut settling = Settling::new(0, Duration::ZERO, Duration::ZERO);
        settling.change(1, start);
        assert_eq!(settling.settle(start), Some(&1));
        assert_eq!(settling.deadline(), None);
    }
}
