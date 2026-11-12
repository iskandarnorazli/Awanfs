//! Token bucket for background I/O (#28, owner decision 24): compaction copies at most `rate`
//! bytes per second, so it does not crowd out requests on filesystems without copy-on-write
//! cloning.

use std::time::Duration;

/// How long to wait so that `spent` bytes over `elapsed` stay within `rate` bytes per second
/// (with up to one second of burst). Zero when under budget or when `rate` is 0 (no limit).
pub fn delay_for(spent: u64, elapsed: Duration, rate: u64) -> Duration {
    if rate == 0 {
        return Duration::ZERO;
    }
    let allowed_at = Duration::from_secs_f64(spent.saturating_sub(rate) as f64 / rate as f64);
    allowed_at.saturating_sub(elapsed)
}

/// Paces one background job.
pub struct Throttle {
    rate: u64,
    spent: u64,
    started: std::time::Instant,
}

impl Throttle {
    pub fn new(rate: u64) -> Self {
        Throttle { rate, spent: 0, started: std::time::Instant::now() }
    }

    /// Records `bytes` and sleeps if the job is ahead of its rate.
    pub async fn spend(&mut self, bytes: u64) {
        self.spent += bytes;
        let d = delay_for(self.spent, self.started.elapsed(), self.rate);
        if !d.is_zero() {
            tokio::time::sleep(d).await;
        }
    }
}
