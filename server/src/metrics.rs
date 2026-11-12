//! Request metrics for `/_awanfs/metrics` in Prometheus text format (#42).

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

/// S3 operations, as the metrics label them.
pub const OPERATIONS: &[&str] = &[
    "ListBuckets",
    "CreateBucket",
    "DeleteBucket",
    "HeadBucket",
    "ListObjects",
    "PutBucketLifecycle",
    "GetBucketLifecycle",
    "DeleteObjects",
    "PutObject",
    "CopyObject",
    "GetObject",
    "HeadObject",
    "DeleteObject",
    "CreateMultipartUpload",
    "UploadPart",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "Other",
];

/// Upper bounds of the latency histogram buckets, in seconds.
pub const LATENCY_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

const CLASSES: [&str; 4] = ["2xx", "3xx", "4xx", "5xx"];

#[derive(Default)]
struct OpStats {
    /// By status class 2xx, 3xx, 4xx, 5xx.
    by_class: [AtomicU64; 4],
    /// Cumulative counts per `LATENCY_BUCKETS` bound; the last slot is +Inf.
    buckets: [AtomicU64; 13],
    /// Sum of durations in microseconds.
    sum_us: AtomicU64,
}

pub struct Metrics {
    ops: Vec<OpStats>,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics { ops: OPERATIONS.iter().map(|_| OpStats::default()).collect(), bytes_in: AtomicU64::new(0), bytes_out: AtomicU64::new(0) }
    }
}

pub fn op_index(name: &str) -> usize {
    OPERATIONS.iter().position(|o| *o == name).unwrap_or(OPERATIONS.len() - 1)
}

impl Metrics {
    pub fn record(&self, op: &str, status: u16, seconds: f64, bytes_in: u64, bytes_out: u64) {
        let s = &self.ops[op_index(op)];
        let class = match status {
            200..=299 => 0,
            300..=399 => 1,
            400..=499 => 2,
            _ => 3,
        };
        s.by_class[class].fetch_add(1, Ordering::Relaxed);
        let slot = LATENCY_BUCKETS.iter().position(|&b| seconds <= b).unwrap_or(LATENCY_BUCKETS.len());
        s.buckets[slot].fetch_add(1, Ordering::Relaxed);
        s.sum_us.fetch_add((seconds * 1e6) as u64, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
    }

    /// Requests of `op` answered so far, all status classes.
    pub fn requests(&self, op: &str) -> u64 {
        self.ops[op_index(op)].by_class.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// The request metrics in Prometheus text format.
    pub fn render(&self, out: &mut String) {
        out.push_str("# HELP awanfs_requests_total S3 requests answered, by operation and status class.\n# TYPE awanfs_requests_total counter\n");
        for (i, op) in OPERATIONS.iter().enumerate() {
            for (c, class) in CLASSES.iter().enumerate() {
                let n = self.ops[i].by_class[c].load(Ordering::Relaxed);
                if n > 0 {
                    let _ = writeln!(out, "awanfs_requests_total{{operation=\"{}\",status=\"{}\"}} {}", op, class, n);
                }
            }
        }
        out.push_str("# HELP awanfs_request_duration_seconds Time from request to response headers.\n# TYPE awanfs_request_duration_seconds histogram\n");
        for (i, op) in OPERATIONS.iter().enumerate() {
            let s = &self.ops[i];
            let total: u64 = s.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum();
            if total == 0 {
                continue;
            }
            let mut cum = 0;
            for (b, bound) in LATENCY_BUCKETS.iter().enumerate() {
                cum += s.buckets[b].load(Ordering::Relaxed);
                let _ = writeln!(out, "awanfs_request_duration_seconds_bucket{{operation=\"{}\",le=\"{}\"}} {}", op, bound, cum);
            }
            let _ = writeln!(out, "awanfs_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {}", op, total);
            let _ = writeln!(out, "awanfs_request_duration_seconds_sum{{operation=\"{}\"}} {}", op, s.sum_us.load(Ordering::Relaxed) as f64 / 1e6);
            let _ = writeln!(out, "awanfs_request_duration_seconds_count{{operation=\"{}\"}} {}", op, total);
        }
        let _ = writeln!(out, "# HELP awanfs_request_bytes_total Request body bytes declared by Content-Length.\n# TYPE awanfs_request_bytes_total counter\nawanfs_request_bytes_total {}", self.bytes_in.load(Ordering::Relaxed));
        let _ = writeln!(out, "# HELP awanfs_response_bytes_total Response body bytes declared by Content-Length.\n# TYPE awanfs_response_bytes_total counter\nawanfs_response_bytes_total {}", self.bytes_out.load(Ordering::Relaxed));
    }
}
