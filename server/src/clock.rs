//! One clock for the whole server (#158).
//!
//! Production code reads the wall clock only through [`Clock`]; tests inject
//! [`ManualClock`] so time-dependent behaviour never sleeps.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Source of wall-clock nanoseconds since the Unix epoch.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> u64;
}

/// Real system wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

/// Deterministic clock controlled by tests.
#[derive(Debug)]
pub struct ManualClock {
    now_ns: AtomicU64,
}

impl ManualClock {
    pub fn new(initial_ns: u64) -> Self {
        Self {
            now_ns: AtomicU64::new(initial_ns),
        }
    }

    pub fn advance(&self, delta_ns: u64) -> u64 {
        self.now_ns.fetch_add(delta_ns, Ordering::SeqCst) + delta_ns
    }

    pub fn set(&self, ns: u64) {
        self.now_ns.store(ns, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ns(&self) -> u64 {
        self.now_ns.load(Ordering::SeqCst)
    }
}

const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Formats `ns` since the Unix epoch as an RFC 7231 IMF-fixdate HTTP-date
/// (`"Tue, 10 Nov 2026 00:00:00 GMT"`).
pub fn format_http_date(ns: u64) -> String {
    let secs = ns / 1_000_000_000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // 1970-01-01 was a Thursday (index 4).
    let wday = ((days + 4).rem_euclid(7)) as usize;

    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };

    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        WEEKDAYS[wday],
        d,
        MONTHS[(m - 1) as usize],
        y,
        rem / 3600,
        (rem / 60) % 60,
        rem % 60
    )
}

/// Formats milliseconds since the Unix epoch as an HTTP-date.
pub fn format_http_date_ms(ms: u64) -> String {
    format_http_date(ms.saturating_mul(1_000_000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_date_matches_known_instants() {
        assert_eq!(format_http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        // 1994-11-06 08:49:37 GMT = 784111777 s
        assert_eq!(
            format_http_date(784_111_777 * 1_000_000_000),
            "Sun, 06 Nov 1994 08:49:37 GMT"
        );
        // 2000-02-29 00:00:00 GMT = 951782400 s
        assert_eq!(
            format_http_date(951_782_400 * 1_000_000_000),
            "Tue, 29 Feb 2000 00:00:00 GMT"
        );
    }
}
