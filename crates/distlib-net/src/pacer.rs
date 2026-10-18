//! Holding bytes to a rate. A caller that has moved some bytes waits here
//! until the rate allows them, before it moves any more: on a stream with
//! backpressure, waiting before reading on slows the sender too.

use std::{
    num::NonZeroU64,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use tokio::time::{Instant, sleep_until};

/// How far ahead of its rate a pacer lets bytes through after a pause: enough
/// that a timer waking late costs no throughput, too little to matter as a
/// burst. Time idle beyond it earns nothing.
const BURST: Duration = Duration::from_millis(100);

/// A rate in bytes per second, shared by everyone pacing through it: two
/// transfers at once get the rate between them, not each.
#[derive(Debug)]
pub struct BytePacer {
    state: Mutex<PacerState>,
}

#[derive(Debug)]
struct PacerState {
    rate: Option<NonZeroU64>,
    /// When the bytes let through so far are all within the rate.
    caught_up_at: Instant,
}

impl BytePacer {
    /// A pacer at `rate` bytes per second, or with no limit.
    pub fn new(rate: Option<NonZeroU64>) -> Self {
        Self {
            state: Mutex::new(PacerState {
                rate,
                caught_up_at: Instant::now(),
            }),
        }
    }

    /// Changes the rate, for whatever is paced from now on.
    pub fn set_rate(&self, rate: Option<NonZeroU64>) {
        self.lock().rate = rate;
    }

    /// Waits until `bytes` more are within the rate. Returns at once with no
    /// limit.
    pub async fn pace(&self, bytes: u64) {
        let caught_up_at = {
            let mut state = self.lock();
            let Some(rate) = state.rate else {
                return;
            };
            let now = Instant::now();
            let earliest = now.checked_sub(BURST).unwrap_or(now);
            state.caught_up_at = state.caught_up_at.max(earliest) + time_for(bytes, rate);
            state.caught_up_at
        };
        sleep_until(caught_up_at).await;
    }

    fn lock(&self) -> MutexGuard<'_, PacerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// How long `bytes` take at `rate` bytes per second.
fn time_for(bytes: u64, rate: NonZeroU64) -> Duration {
    let nanos = u128::from(bytes) * 1_000_000_000 / u128::from(rate.get());
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn rate(bytes_per_sec: u64) -> Option<NonZeroU64> {
        NonZeroU64::new(bytes_per_sec)
    }

    /// Paces `chunks` chunks of `bytes` each and says how long it took.
    async fn time_to_pace(pacer: &BytePacer, chunks: u32, bytes: u64) -> Duration {
        let started = Instant::now();
        for _ in 0..chunks {
            pacer.pace(bytes).await;
        }
        started.elapsed()
    }

    #[tokio::test(start_paused = true)]
    async fn bytes_take_their_size_over_the_rate() {
        let pacer = BytePacer::new(rate(1_000));
        let took = time_to_pace(&pacer, 100, 100).await;
        assert_eq!(took, Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn two_at_once_share_the_rate() {
        let pacer = Arc::new(BytePacer::new(rate(1_000)));
        let started = Instant::now();
        let other = tokio::spawn({
            let pacer = Arc::clone(&pacer);
            async move { time_to_pace(&pacer, 50, 100).await }
        });
        time_to_pace(&pacer, 50, 100).await;
        other.await.expect("the other pacing does not panic");
        assert_eq!(started.elapsed(), Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn time_idle_earns_no_more_than_the_burst() {
        let pacer = BytePacer::new(rate(1_000));
        tokio::time::sleep(Duration::from_secs(60)).await;
        let took = time_to_pace(&pacer, 10, 100).await;
        assert_eq!(took, Duration::from_secs(1) - BURST);
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_rate_nothing_waits_and_a_new_rate_applies_at_once() {
        let pacer = BytePacer::new(None);
        assert_eq!(time_to_pace(&pacer, 100, 1_000_000).await, Duration::ZERO);
        pacer.set_rate(rate(1_000));
        let took = time_to_pace(&pacer, 10, 100).await;
        assert_eq!(took, Duration::from_secs(1));
    }
}
