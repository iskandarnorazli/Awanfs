//! #28: the compaction token bucket. Pure arithmetic, no clock: delay_for says how long to
//! wait so that bytes spent stay within the rate (with one second of burst).

use s3_server::throttle::delay_for;
use std::time::Duration;

#[test]
fn the_bucket_allows_one_second_of_burst_then_paces_to_the_rate() {
    let mb = 1_000_000u64;
    // 50 MB/s: the first 50 MB go at once.
    assert_eq!(delay_for(50 * mb, Duration::ZERO, 50 * mb), Duration::ZERO);
    // 100 MB at once: wait 1 s; after 0.4 s, 0.6 s more.
    assert_eq!(delay_for(100 * mb, Duration::ZERO, 50 * mb), Duration::from_secs(1));
    assert_eq!(delay_for(100 * mb, Duration::from_millis(400), 50 * mb), Duration::from_millis(600));
    // On or behind schedule: no wait.
    assert_eq!(delay_for(100 * mb, Duration::from_secs(2), 50 * mb), Duration::ZERO);
    // Copying the gate's 100 MB volume at 50 MB/s takes at least 1 s beyond the burst.
    let total: Duration = (1..=100u64).map(|i| delay_for(i * mb, Duration::ZERO, 50 * mb)).max().unwrap();
    assert_eq!(total, Duration::from_secs(1));
    // Rate 0 means no limit.
    assert_eq!(delay_for(u64::MAX, Duration::ZERO, 0), Duration::ZERO);
}
