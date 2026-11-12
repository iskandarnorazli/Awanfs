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
    static ERROR_CODE: std::cell::RefCell<Option<String>>;
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

/// Runs `fut` with `id` as the current request ID. Returns its output and the S3 error code
/// of the last error body written while it ran.
pub async fn with_request_id<F: std::future::Future>(id: String, fut: F) -> (F::Output, Option<String>) {
    ERROR_CODE
        .scope(std::cell::RefCell::new(None), async move {
            let out = REQUEST_ID.scope(id, fut).await;
            (out, ERROR_CODE.with(|c| c.borrow().clone()))
        })
        .await
}

/// Called by `s3_error_xml`: remembers the error code for the access log.
pub fn note_error_code(code: &str) {
    let _ = ERROR_CODE.try_with(|c| *c.borrow_mut() = Some(code.to_string()));
}

/// The access key a request claims: `Credential=<key>/...` in the Authorization header or
/// `X-Amz-Credential=<key>%2F...` in the query. `-` when there is none.
pub fn claimed_access_key(authorization: Option<&str>, query: &str) -> String {
    if let Some(a) = authorization {
        if let Some(rest) = a.split("Credential=").nth(1) {
            if let Some(k) = rest.split('/').next().filter(|k| !k.is_empty()) {
                return k.to_string();
            }
        }
    }
    for pair in query.split('&') {
        if let Some(v) = pair.strip_prefix("X-Amz-Credential=") {
            let decoded = v.replace("%2F", "/").replace("%2f", "/");
            if let Some(k) = decoded.split('/').next().filter(|k| !k.is_empty()) {
                return k.to_string();
            }
        }
    }
    "-".to_string()
}

/// One line in the Amazon S3 server access log format (space separated, `-` for none,
/// quoted where S3 quotes). Fields AwanFS has no value for are `-`.
#[allow(clippy::too_many_arguments)]
pub fn s3_access_log_line(
    bucket: &str,
    remote_ip: &str,
    requester: &str,
    request_id: &str,
    operation: &str,
    key: &str,
    request_uri: &str,
    status: u16,
    error_code: Option<&str>,
    bytes_sent: u64,
    total_ms: u64,
    user_agent: &str,
    host: &str,
    sig_version: &str,
    auth_type: &str,
) -> String {
    let dash = |s: &str| if s.is_empty() { "-".to_string() } else { s.replace(' ', "%20") };
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "%22"));
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    format!(
        "- {} [{}] {} {} {} {} {} {} {} {} {} - {} - - {} - - {} - {} {} -",
        dash(bucket),
        clf_time(now),
        dash(remote_ip),
        dash(requester),
        dash(request_id),
        operation,
        dash(key),
        quote(request_uri),
        status,
        error_code.map(dash).unwrap_or_else(|| "-".to_string()),
        if bytes_sent == 0 { "-".to_string() } else { bytes_sent.to_string() },
        total_ms,
        quote(user_agent),
        dash(sig_version),
        dash(auth_type),
        dash(host),
    )
}

/// `06/Feb/2019:00:00:38 +0000` from Unix seconds (UTC).
pub fn clf_time(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
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
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    format!("{:02}/{}/{}:{:02}:{:02}:{:02} +0000", d, MONTHS[(m - 1) as usize], y, rem / 3600, rem / 60 % 60, rem % 60)
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
