//! Per-request IDs and one JSON log line per request (#41).
//!
//! The ID is returned in `x-amz-request-id` and written into every error body's
//! `<RequestId>` (through a task-local, so the handlers need not pass it around). Log lines
//! go to a sink the embedder sets; the `s3-server` binary writes them to stderr.
//! The issue asked for the `tracing` crate; it is not in the lockfile and this change adds no
//! dependency, so the line is written directly. Same fields.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

tokio::task_local! {
    static REQUEST_ID: String;
}

/// Receives one finished JSON log line, without the newline.
pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

static COUNTER: AtomicU64 = AtomicU64::new(1);

/// 24 upper-case hex digits: the process start time in seconds and a counter, so IDs are
/// unique within a process and do not repeat across restarts.
pub fn new_request_id() -> String {
    static START: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let start = *START.get_or_init(|| SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0));
    format!("{:08X}{:016X}", start as u32, COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// The ID of the request this task is answering, if any.
pub fn current_request_id() -> Option<String> {
    REQUEST_ID.try_with(|id| id.clone()).ok()
}

/// Runs `fut` with `id` as the current request ID.
pub async fn with_request_id<F: std::future::Future>(id: String, fut: F) -> F::Output {
    REQUEST_ID.scope(id, fut).await
}

pub fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

pub struct RequestRecord<'a> {
    pub request_id: &'a str,
    pub method: &'a str,
    pub operation: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
    pub status: u16,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub duration_us: u64,
}

impl RequestRecord<'_> {
    pub fn to_json(&self) -> String {
        let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        format!(
            "{{\"ts_ms\":{},\"level\":\"info\",\"request_id\":\"{}\",\"method\":\"{}\",\"operation\":\"{}\",\"bucket\":\"{}\",\"key\":\"{}\",\"status\":{},\"bytes_in\":{},\"bytes_out\":{},\"duration_us\":{}}}",
            ts_ms,
            json_escape(self.request_id),
            json_escape(self.method),
            json_escape(self.operation),
            json_escape(self.bucket),
            json_escape(self.key),
            self.status,
            self.bytes_in,
            self.bytes_out,
            self.duration_us
        )
    }
}
