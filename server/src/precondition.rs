//! Conditional request precondition evaluation (RFC 7232 / S3, #181).
//!
//! Pure decision function: no I/O, no state machine.

use hyper::header::HeaderMap;

/// Snapshot of the target object's metadata needed to evaluate preconditions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectFacts {
    /// Object ETag (with or without surrounding quotes).
    pub etag: String,
    /// Last-modified time in whole seconds since the Unix epoch.
    pub last_modified_secs: u64,
}

/// Outcome of evaluating conditional request headers for read / copy-source requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreconditionOutcome {
    Proceed,
    NotModified,
    PreconditionFailed,
}

/// Outcome of evaluating conditional request headers for write requests
/// (`PutObject`, `CompleteMultipartUpload`, destination `CopyObject`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WritePreconditionOutcome {
    Proceed,
    PreconditionFailed,
    NoSuchKey,
}

/// Conditions supported on `DeleteObject` headers and `DeleteObjects` XML entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeleteConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_match_last_modified_secs: Option<u64>,
    pub if_match_size: Option<i64>,
}

impl DeleteConditions {
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.if_match_last_modified_secs.is_none()
            && self.if_match_size.is_none()
    }
}

/// Target state for conditional `DeleteObject` / `DeleteObjects` evaluation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeleteTarget {
    Absent,
    Object {
        etag: String,
        last_modified_secs: u64,
        size: u64,
    },
    DeleteMarker {
        last_modified_secs: u64,
    },
}

/// Default fallback timestamp (`RFC 7231 fallback instant (1794268800)`) when a legacy
/// record has `created_at == 0`.
pub const DEFAULT_LAST_MODIFIED_SECS: u64 = 1_794_268_800;

/// Evaluate RFC 7232 / S3 conditional headers against `current`.
///
/// When `copy_source` is `true`, reads `x-amz-copy-source-if-*` headers instead
/// of `If-*` headers. Note that `CopyObject` / `UploadPartCopy` map both
/// `NotModified` and `PreconditionFailed` to HTTP 412 `PreconditionFailed`.
pub fn precondition(
    headers: &HeaderMap,
    current: Option<&ObjectFacts>,
    copy_source: bool,
) -> PreconditionOutcome {
    let (h_im, h_ius, h_inm, h_ims) = if copy_source {
        (
            "x-amz-copy-source-if-match",
            "x-amz-copy-source-if-unmodified-since",
            "x-amz-copy-source-if-none-match",
            "x-amz-copy-source-if-modified-since",
        )
    } else {
        ("if-match", "if-unmodified-since", "if-none-match", "if-modified-since")
    };

    // 1. If-Match (takes precedence over If-Unmodified-Since).
    if headers.contains_key(h_im) {
        match current {
            Some(facts) if etag_matches(headers, h_im, &facts.etag, true) => {}
            _ => return PreconditionOutcome::PreconditionFailed,
        }
    } else if let Some(since) = parse_date_header(headers, h_ius) {
        // 2. If-Unmodified-Since (only evaluated when If-Match is absent).
        if let Some(facts) = current {
            if facts.last_modified_secs > since {
                return PreconditionOutcome::PreconditionFailed;
            }
        }
    }

    // 3. If-None-Match (takes precedence over If-Modified-Since).
    if headers.contains_key(h_inm) {
        if let Some(facts) = current {
            if etag_matches(headers, h_inm, &facts.etag, false) {
                return PreconditionOutcome::NotModified;
            }
        }
        return PreconditionOutcome::Proceed;
    }

    // 4. If-Modified-Since (only evaluated when If-None-Match is absent).
    if let Some(since) = parse_date_header(headers, h_ims) {
        if let Some(facts) = current {
            if facts.last_modified_secs <= since {
                return PreconditionOutcome::NotModified;
            }
        }
    }

    PreconditionOutcome::Proceed
}

/// Evaluate S3 write preconditions (`If-Match`, `If-None-Match`, `If-Unmodified-Since`,
/// `If-Modified-Since`) against the current live object at commit time.
///
/// A delete marker counts as `None` (absent) for writes. On `If-Match` when no live
/// object exists, S3 returns `404 NoSuchKey`; on any other mismatch, `412 PreconditionFailed`.
pub fn write_precondition(
    headers: &HeaderMap,
    current: Option<&ObjectFacts>,
) -> WritePreconditionOutcome {
    if headers.contains_key("if-match") {
        match current {
            None => return WritePreconditionOutcome::NoSuchKey,
            Some(facts) if etag_matches(headers, "if-match", &facts.etag, true) => {}
            Some(_) => return WritePreconditionOutcome::PreconditionFailed,
        }
    } else if let Some(since) = parse_date_header(headers, "if-unmodified-since") {
        if let Some(facts) = current {
            if facts.last_modified_secs > since {
                return WritePreconditionOutcome::PreconditionFailed;
            }
        }
    }

    if headers.contains_key("if-none-match") {
        if let Some(facts) = current {
            if etag_matches(headers, "if-none-match", &facts.etag, false) {
                return WritePreconditionOutcome::PreconditionFailed;
            }
        }
        return WritePreconditionOutcome::Proceed;
    }

    if let Some(since) = parse_date_header(headers, "if-modified-since") {
        if let Some(facts) = current {
            if facts.last_modified_secs <= since {
                return WritePreconditionOutcome::PreconditionFailed;
            }
        }
    }

    WritePreconditionOutcome::Proceed
}

/// Parse conditional delete headers from a `DeleteObject` request.
pub fn parse_delete_conditions(headers: &HeaderMap) -> Result<DeleteConditions, (&'static str, &'static str)> {
    let join_all = |name: &str| -> Option<String> {
        let mut parts = Vec::new();
        for v in headers.get_all(name) {
            if let Ok(s) = v.to_str() {
                parts.push(s.to_string());
            }
        }
        if parts.is_empty() { None } else { Some(parts.join(", ")) }
    };
    let if_match = join_all("if-match");
    let if_none_match = join_all("if-none-match");
    let if_match_last_modified_secs = match headers.get("x-amz-if-match-last-modified-time") {
        Some(v) => {
            let s = v
                .to_str()
                .map_err(|_| ("InvalidArgument", "Invalid x-amz-if-match-last-modified-time header"))?;
            let secs = parse_http_date_or_iso(s)
                .ok_or(("InvalidArgument", "Invalid x-amz-if-match-last-modified-time header"))?;
            Some(secs)
        }
        None => None,
    };
    let if_match_size = match headers.get("x-amz-if-match-size") {
        Some(v) => {
            let s = v
                .to_str()
                .map_err(|_| ("InvalidArgument", "Invalid x-amz-if-match-size header"))?;
            let sz = s
                .trim()
                .parse::<i64>()
                .map_err(|_| ("InvalidArgument", "Invalid x-amz-if-match-size header"))?;
            Some(sz)
        }
        None => None,
    };
    Ok(DeleteConditions {
        if_match,
        if_none_match,
        if_match_last_modified_secs,
        if_match_size,
    })
}

/// Evaluate conditional delete preconditions against `target`.
///
/// Missing objects (`DeleteTarget::Absent`) proceed with no error (204 / `<Deleted>`).
/// Live objects and delete markers check ETag (`*` matches any existing entry),
/// `last_modified_secs`, and `size` (0 for delete markers).
pub fn delete_precondition(conds: &DeleteConditions, target: &DeleteTarget) -> PreconditionOutcome {
    match target {
        DeleteTarget::Absent => PreconditionOutcome::Proceed,
        DeleteTarget::Object { etag, last_modified_secs, size } => {
            if let Some(ref raw) = conds.if_match {
                if !etag_str_matches(raw, etag, true) {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
            if let Some(ref raw) = conds.if_none_match {
                if etag_str_matches(raw, etag, false) {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
            if let Some(want_ts) = conds.if_match_last_modified_secs {
                if *last_modified_secs != want_ts {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
            if let Some(want_sz) = conds.if_match_size {
                if (*size as i128) != (want_sz as i128) {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
            PreconditionOutcome::Proceed
        }
        DeleteTarget::DeleteMarker { last_modified_secs } => {
            if let Some(ref raw) = conds.if_match {
                if !etag_str_is_wildcard(raw) {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
            if let Some(ref raw) = conds.if_none_match {
                if etag_str_is_wildcard(raw) {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
            if let Some(want_ts) = conds.if_match_last_modified_secs {
                if *last_modified_secs != want_ts {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
            if let Some(want_sz) = conds.if_match_size {
                if want_sz != 0 {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
            PreconditionOutcome::Proceed
        }
    }
}

fn etag_str_is_wildcard(s: &str) -> bool {
    s.split(',').map(str::trim).any(|item| item == "*")
}

fn etag_str_matches(s: &str, target_etag: &str, strong: bool) -> bool {
    let target = target_etag.trim().trim_matches('"');
    for raw_item in s.split(',') {
        let item = raw_item.trim();
        if item.is_empty() {
            continue;
        }
        if item == "*" {
            return true;
        }
        if strong {
            if item.starts_with("W/") || item.starts_with("w/") {
                continue;
            }
            if item.trim_matches('"') == target {
                return true;
            }
        } else {
            let stripped = item.strip_prefix("W/").or_else(|| item.strip_prefix("w/")).unwrap_or(item);
            if stripped.trim_matches('"') == target {
                return true;
            }
        }
    }
    false
}

fn etag_matches(headers: &HeaderMap, name: &str, target_etag: &str, strong: bool) -> bool {
    for val in headers.get_all(name) {
        let Ok(s) = val.to_str() else {
            continue;
        };
        if etag_str_matches(s, target_etag, strong) {
            return true;
        }
    }
    false
}

fn parse_date_header(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers.get(name).and_then(|v| v.to_str().ok()).and_then(parse_http_date)
}

/// Format Unix epoch seconds as an HTTP IMF-fixdate (`RFC 7231 fallback instant (1794268800)`).
pub fn format_http_date(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    // 1970-01-01 was a Thursday (index 4 where 0 = Sun).
    let wday = ((days + 4).rem_euclid(7)) as usize;
    const WDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        WDAYS[wday],
        d,
        MONTHS[(m - 1) as usize],
        y,
        rem / 3600,
        (rem / 60) % 60,
        rem % 60
    )
}

/// Parse an HTTP-date (IMF-fixdate, RFC 850, or asctime) into Unix epoch seconds.
pub fn parse_http_date(raw: &str) -> Option<u64> {
    let s = raw.trim();
    if let Some((_, rest)) = s.split_once(',') {
        let rest = rest.trim();
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() == 5 && parts[4].eq_ignore_ascii_case("GMT") {
            // IMF-fixdate: "06 Nov 1994 08:49:37 GMT"
            let day: u32 = parts[0].parse().ok()?;
            let month = parse_month(parts[1])?;
            let year: i64 = parts[2].parse().ok()?;
            let (hh, mm, ss) = parse_hms(parts[3])?;
            return to_unix_secs(year, month, day, hh, mm, ss);
        }
        if parts.len() == 3 && parts[2].eq_ignore_ascii_case("GMT") {
            // RFC 850: "06-Nov-94 08:49:37 GMT"
            let mut dmy = parts[0].split('-');
            let day: u32 = dmy.next()?.parse().ok()?;
            let month = parse_month(dmy.next()?)?;
            let yy: i64 = dmy.next()?.parse().ok()?;
            if dmy.next().is_some() {
                return None;
            }
            let year = if yy >= 70 { 1900 + yy } else { 2000 + yy };
            let (hh, mm, ss) = parse_hms(parts[1])?;
            return to_unix_secs(year, month, day, hh, mm, ss);
        }
        return None;
    }

    // asctime: "Sun Nov  6 08:49:37 1994"
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() == 5 {
        let month = parse_month(parts[1])?;
        let day: u32 = parts[2].parse().ok()?;
        let (hh, mm, ss) = parse_hms(parts[3])?;
        let year: i64 = parts[4].parse().ok()?;
        return to_unix_secs(year, month, day, hh, mm, ss);
    }
    None
}

/// Parse an HTTP-date or an ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SS(.fff)?Z`) into Unix seconds.
pub fn parse_http_date_or_iso(raw: &str) -> Option<u64> {
    if let Some(secs) = parse_http_date(raw) {
        return Some(secs);
    }
    let s = raw.trim();
    let body = s
        .strip_suffix('Z')
        .or_else(|| s.strip_suffix('z'))
        .or_else(|| s.strip_suffix("+00:00"))?;
    let (date_part, time_part) = body.split_once('T').or_else(|| body.split_once('t'))?;
    let mut ymd = date_part.split('-');
    let year: i64 = ymd.next()?.parse().ok()?;
    let month: u32 = ymd.next()?.parse().ok()?;
    let day: u32 = ymd.next()?.parse().ok()?;
    if ymd.next().is_some() {
        return None;
    }
    let hms_part = time_part.split('.').next()?;
    let (hh, mm, ss) = parse_hms(hms_part)?;
    to_unix_secs(year, month, day, hh, mm, ss)
}

fn parse_month(s: &str) -> Option<u32> {
    match s.to_ascii_lowercase().as_str() {
        "jan" => Some(1),
        "feb" => Some(2),
        "mar" => Some(3),
        "apr" => Some(4),
        "may" => Some(5),
        "jun" => Some(6),
        "jul" => Some(7),
        "aug" => Some(8),
        "sep" => Some(9),
        "oct" => Some(10),
        "nov" => Some(11),
        "dec" => Some(12),
        _ => None,
    }
}

fn parse_hms(s: &str) -> Option<(u32, u32, u32)> {
    let mut it = s.split(':');
    let hh: u32 = it.next()?.parse().ok()?;
    let mm: u32 = it.next()?.parse().ok()?;
    let ss: u32 = it.next()?.parse().ok()?;
    if it.next().is_some() || hh > 23 || mm > 59 || ss > 59 {
        return None;
    }
    Some((hh, mm, ss))
}

/// Years past this are refused before `days_from_civil`, whose `i64` arithmetic would overflow;
/// an instant that does not fit in `u64` seconds is refused below it (#480).
const MAX_YEAR: i64 = 1_000_000_000_000;

fn to_unix_secs(year: i64, month: u32, day: u32, hh: u32, mm: u32, ss: u32) -> Option<u64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || !(1970..=MAX_YEAR).contains(&year) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    if days < 0 {
        return None;
    }
    (days as u64)
        .checked_mul(86_400)?
        .checked_add(u64::from(hh) * 3600 + u64::from(mm) * 60 + u64::from(ss))
}

/// Howard Hinnant's civil-from-days algorithm (1970-01-01 = day 0).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m as u32, d as u32)
}

/// Howard Hinnant's days-from-civil algorithm (1970-01-01 = day 0).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    #[derive(Clone, Copy, Debug)]
    enum HdrState {
        Absent,
        True,
        False,
    }

    #[derive(Clone, Copy, Debug)]
    enum ObjState {
        Exists,
        DeleteMarker,
        Missing,
    }

    const ALL_HDR: [HdrState; 3] = [HdrState::Absent, HdrState::True, HdrState::False];
    const ALL_OBJ: [ObjState; 3] = [ObjState::Exists, ObjState::DeleteMarker, ObjState::Missing];

    fn expected_outcome(
        if_match: HdrState,
        if_unmod: HdrState,
        if_none_match: HdrState,
        if_mod: HdrState,
        obj: ObjState,
    ) -> PreconditionOutcome {
        let exists = matches!(obj, ObjState::Exists);
        // Step 1 & 2: If-Match / If-Unmodified-Since
        match if_match {
            HdrState::True => {
                if !exists {
                    return PreconditionOutcome::PreconditionFailed;
                }
                // If-Match true -> If-Unmodified-Since is not evaluated.
            }
            HdrState::False => return PreconditionOutcome::PreconditionFailed,
            HdrState::Absent => {
                if exists && matches!(if_unmod, HdrState::False) {
                    return PreconditionOutcome::PreconditionFailed;
                }
            }
        }
        // Step 3 & 4: If-None-Match / If-Modified-Since
        match if_none_match {
            HdrState::True => {
                if exists {
                    PreconditionOutcome::NotModified
                } else {
                    PreconditionOutcome::Proceed
                }
            }
            HdrState::False => PreconditionOutcome::Proceed,
            HdrState::Absent => {
                if exists && matches!(if_mod, HdrState::False) {
                    PreconditionOutcome::NotModified
                } else {
                    PreconditionOutcome::Proceed
                }
            }
        }
    }

    #[test]
    fn exhaustive_precedence_table_243_cases() {
        let ts = 1_794_268_800u64; // RFC 7231 fallback instant (1794268800)
        let facts = ObjectFacts {
            etag: "\"abc123\"".to_string(),
            last_modified_secs: ts,
        };
        let mut count = 0;
        for &copy_source in &[false, true] {
            for &obj in &ALL_OBJ {
                let current = match obj {
                    ObjState::Exists => Some(&facts),
                    ObjState::DeleteMarker | ObjState::Missing => None,
                };
                for &im in &ALL_HDR {
                    for &ius in &ALL_HDR {
                        for &inm in &ALL_HDR {
                            for &ims in &ALL_HDR {
                                let mut headers = HeaderMap::new();
                                let (h_im, h_ius, h_inm, h_ims) = if copy_source {
                                    (
                                        "x-amz-copy-source-if-match",
                                        "x-amz-copy-source-if-unmodified-since",
                                        "x-amz-copy-source-if-none-match",
                                        "x-amz-copy-source-if-modified-since",
                                    )
                                } else {
                                    ("if-match", "if-unmodified-since", "if-none-match", "if-modified-since")
                                };
                                match im {
                                    HdrState::Absent => {}
                                    HdrState::True => {
                                        headers.insert(h_im, HeaderValue::from_static("\"abc123\""));
                                    }
                                    HdrState::False => {
                                        headers.insert(h_im, HeaderValue::from_static("\"other\""));
                                    }
                                }
                                match ius {
                                    HdrState::Absent => {}
                                    HdrState::True => {
                                        headers.insert(h_ius, HeaderValue::from_str(&format_http_date(ts)).unwrap());
                                    }
                                    HdrState::False => {
                                        headers.insert(h_ius, HeaderValue::from_str(&format_http_date(ts - 10)).unwrap());
                                    }
                                }
                                match inm {
                                    HdrState::Absent => {}
                                    HdrState::True => {
                                        headers.insert(h_inm, HeaderValue::from_static("\"abc123\""));
                                    }
                                    HdrState::False => {
                                        headers.insert(h_inm, HeaderValue::from_static("\"other\""));
                                    }
                                }
                                match ims {
                                    HdrState::Absent => {}
                                    HdrState::True => {
                                        headers.insert(h_ims, HeaderValue::from_str(&format_http_date(ts - 10)).unwrap());
                                    }
                                    HdrState::False => {
                                        headers.insert(h_ims, HeaderValue::from_str(&format_http_date(ts)).unwrap());
                                    }
                                }
                                let got = precondition(&headers, current, copy_source);
                                let want = expected_outcome(im, ius, inm, ims, obj);
                                assert_eq!(
                                    got, want,
                                    "copy_source={copy_source} obj={obj:?} im={im:?} ius={ius:?} inm={inm:?} ims={ims:?}"
                                );
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(count, 2 * 3 * 81);
    }

    #[test]
    fn http_date_roundtrip_and_formats() {
        assert_eq!(format_http_date(1_794_355_200), "Wed, 11 Nov 2026 00:00:00 GMT");
        assert_eq!(parse_http_date("Wed, 11 Nov 2026 00:00:00 GMT"), Some(1_794_355_200));
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"), Some(784_111_777));
        assert_eq!(parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT"), Some(784_111_777));
        assert_eq!(parse_http_date("Sun Nov  6 08:49:37 1994"), Some(784_111_777));
        assert_eq!(parse_http_date("not-a-date"), None);
    }

    /// #480: the fuzzer's input; the instant does not fit in u64 seconds.
    #[test]
    fn a_date_too_far_ahead_is_refused_not_wrapped() {
        assert_eq!(parse_http_date("ju May 23 00:00:00 20132330223302"), None);
        assert_eq!(parse_http_date("Thu May 23 00:00:00 20132330223302"), None);
        assert_eq!(parse_http_date("Thu May 23 00:00:00 9223372036854775807"), None);
        assert_eq!(parse_http_date("Thu, 23 May 20132330223302 00:00:00 GMT"), None);
        assert_eq!(parse_http_date_or_iso("20132330223302-05-23T00:00:00Z"), None);
        // The largest year accepted still parses, to a value that fits.
        assert!(parse_http_date("Thu May 23 00:00:00 100000").is_some());
    }

    #[test]
    fn etag_quotes_wildcard_and_lists() {
        let facts = ObjectFacts {
            etag: "\"deadbeef\"".to_string(),
            last_modified_secs: 1_794_268_800,
        };
        let mut h = HeaderMap::new();
        h.insert("if-match", HeaderValue::from_static("deadbeef"));
        assert_eq!(precondition(&h, Some(&facts), false), PreconditionOutcome::Proceed);

        h.insert("if-match", HeaderValue::from_static("\"other\", \"deadbeef\""));
        assert_eq!(precondition(&h, Some(&facts), false), PreconditionOutcome::Proceed);

        h.insert("if-match", HeaderValue::from_static("*"));
        assert_eq!(precondition(&h, Some(&facts), false), PreconditionOutcome::Proceed);
        assert_eq!(precondition(&h, None, false), PreconditionOutcome::PreconditionFailed);

        let mut h2 = HeaderMap::new();
        h2.insert("if-none-match", HeaderValue::from_static("*"));
        assert_eq!(precondition(&h2, Some(&facts), false), PreconditionOutcome::NotModified);
        assert_eq!(precondition(&h2, None, false), PreconditionOutcome::Proceed);
    }

    fn expected_write_outcome(
        if_match: HdrState,
        if_unmod: HdrState,
        if_none_match: HdrState,
        if_mod: HdrState,
        obj: ObjState,
    ) -> WritePreconditionOutcome {
        let exists = matches!(obj, ObjState::Exists);
        match if_match {
            HdrState::True => {
                if !exists {
                    return WritePreconditionOutcome::NoSuchKey;
                }
            }
            HdrState::False => {
                if !exists {
                    return WritePreconditionOutcome::NoSuchKey;
                }
                return WritePreconditionOutcome::PreconditionFailed;
            }
            HdrState::Absent => {
                if exists && matches!(if_unmod, HdrState::False) {
                    return WritePreconditionOutcome::PreconditionFailed;
                }
            }
        }
        match if_none_match {
            HdrState::True => {
                if exists {
                    WritePreconditionOutcome::PreconditionFailed
                } else {
                    WritePreconditionOutcome::Proceed
                }
            }
            HdrState::False => WritePreconditionOutcome::Proceed,
            HdrState::Absent => {
                if exists && matches!(if_mod, HdrState::False) {
                    WritePreconditionOutcome::PreconditionFailed
                } else {
                    WritePreconditionOutcome::Proceed
                }
            }
        }
    }

    #[test]
    fn exhaustive_write_precondition_table_243_cases() {
        let ts = 1_794_268_800u64;
        let facts = ObjectFacts {
            etag: "\"abc123\"".to_string(),
            last_modified_secs: ts,
        };
        let mut count = 0;
        for &obj in &ALL_OBJ {
            let current = match obj {
                ObjState::Exists => Some(&facts),
                ObjState::DeleteMarker | ObjState::Missing => None,
            };
            for &im in &ALL_HDR {
                for &ius in &ALL_HDR {
                    for &inm in &ALL_HDR {
                        for &ims in &ALL_HDR {
                            let mut headers = HeaderMap::new();
                            match im {
                                HdrState::Absent => {}
                                HdrState::True => {
                                    headers.insert("if-match", HeaderValue::from_static("\"abc123\""));
                                }
                                HdrState::False => {
                                    headers.insert("if-match", HeaderValue::from_static("\"other\""));
                                }
                            }
                            match ius {
                                HdrState::Absent => {}
                                HdrState::True => {
                                    headers.insert("if-unmodified-since", HeaderValue::from_str(&format_http_date(ts)).unwrap());
                                }
                                HdrState::False => {
                                    headers.insert("if-unmodified-since", HeaderValue::from_str(&format_http_date(ts - 10)).unwrap());
                                }
                            }
                            match inm {
                                HdrState::Absent => {}
                                HdrState::True => {
                                    headers.insert("if-none-match", HeaderValue::from_static("\"abc123\""));
                                }
                                HdrState::False => {
                                    headers.insert("if-none-match", HeaderValue::from_static("\"other\""));
                                }
                            }
                            match ims {
                                HdrState::Absent => {}
                                HdrState::True => {
                                    headers.insert("if-modified-since", HeaderValue::from_str(&format_http_date(ts - 10)).unwrap());
                                }
                                HdrState::False => {
                                    headers.insert("if-modified-since", HeaderValue::from_str(&format_http_date(ts)).unwrap());
                                }
                            }
                            let got = write_precondition(&headers, current);
                            let want = expected_write_outcome(im, ius, inm, ims, obj);
                            assert_eq!(got, want, "obj={obj:?} im={im:?} ius={ius:?} inm={inm:?} ims={ims:?}");
                            count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(count, 3 * 81);
    }

    #[test]
    fn exhaustive_delete_precondition_table() {
        #[derive(Clone, Copy, Debug)]
        enum MatchKind {
            Absent,
            Wildcard,
            Match,
            Mismatch,
        }
        let ts = 1_794_268_800u64;
        let targets = [
            (ObjState::Missing, DeleteTarget::Absent),
            (
                ObjState::Exists,
                DeleteTarget::Object {
                    etag: "\"abc123\"".to_string(),
                    last_modified_secs: ts,
                    size: 42,
                },
            ),
            (
                ObjState::DeleteMarker,
                DeleteTarget::DeleteMarker {
                    last_modified_secs: ts,
                },
            ),
        ];
        let mut count = 0;
        for (obj_kind, target) in &targets {
            for &im in &[MatchKind::Absent, MatchKind::Wildcard, MatchKind::Match, MatchKind::Mismatch] {
                for &lmt in &[HdrState::Absent, HdrState::True, HdrState::False] {
                    for &sz in &[HdrState::Absent, HdrState::True, HdrState::False] {
                        let conds = DeleteConditions {
                            if_match: match im {
                                MatchKind::Absent => None,
                                MatchKind::Wildcard => Some("*".to_string()),
                                MatchKind::Match => Some("\"abc123\"".to_string()),
                                MatchKind::Mismatch => Some("\"other\"".to_string()),
                            },
                            if_none_match: None,
                            if_match_last_modified_secs: match lmt {
                                HdrState::Absent => None,
                                HdrState::True => Some(ts),
                                HdrState::False => Some(ts - 10),
                            },
                            if_match_size: match sz {
                                HdrState::Absent => None,
                                HdrState::True => Some(match obj_kind {
                                    ObjState::DeleteMarker => 0,
                                    _ => 42,
                                }),
                                HdrState::False => Some(999),
                            },
                        };
                        let got = delete_precondition(&conds, target);
                        let want = match obj_kind {
                            ObjState::Missing => PreconditionOutcome::Proceed,
                            ObjState::Exists => {
                                let ok_im = !matches!(im, MatchKind::Mismatch);
                                let ok_lmt = !matches!(lmt, HdrState::False);
                                let ok_sz = !matches!(sz, HdrState::False);
                                if ok_im && ok_lmt && ok_sz {
                                    PreconditionOutcome::Proceed
                                } else {
                                    PreconditionOutcome::PreconditionFailed
                                }
                            }
                            ObjState::DeleteMarker => {
                                let ok_im = matches!(im, MatchKind::Absent | MatchKind::Wildcard);
                                let ok_lmt = !matches!(lmt, HdrState::False);
                                let ok_sz = !matches!(sz, HdrState::False);
                                if ok_im && ok_lmt && ok_sz {
                                    PreconditionOutcome::Proceed
                                } else {
                                    PreconditionOutcome::PreconditionFailed
                                }
                            }
                        };
                        assert_eq!(got, want, "obj={obj_kind:?} im={im:?} lmt={lmt:?} sz={sz:?}");
                        count += 1;
                    }
                }
            }
        }
        assert_eq!(count, 3 * 4 * 3 * 3);
    }

    #[test]
    fn parse_delete_conditions_and_iso_dates() {
        assert_eq!(parse_http_date_or_iso("2026-11-11T00:00:00Z"), Some(1_794_355_200));
        assert_eq!(parse_http_date_or_iso("2026-11-11T00:00:00.123Z"), Some(1_794_355_200));
        assert_eq!(parse_http_date_or_iso("Wed, 11 Nov 2026 00:00:00 GMT"), Some(1_794_355_200));

        let mut h = HeaderMap::new();
        h.insert("if-match", HeaderValue::from_static("\"e1\""));
        h.insert("x-amz-if-match-last-modified-time", HeaderValue::from_static("Wed, 11 Nov 2026 00:00:00 GMT"));
        h.insert("x-amz-if-match-size", HeaderValue::from_static("128"));
        let conds = parse_delete_conditions(&h).unwrap();
        assert_eq!(conds.if_match.as_deref(), Some("\"e1\""));
        assert_eq!(conds.if_match_last_modified_secs, Some(1_794_355_200));
        assert_eq!(conds.if_match_size, Some(128));

        let mut bad = HeaderMap::new();
        bad.insert("x-amz-if-match-size", HeaderValue::from_static("not-a-number"));
        assert!(parse_delete_conditions(&bad).is_err());

        let mut bad_dt = HeaderMap::new();
        bad_dt.insert("x-amz-if-match-last-modified-time", HeaderValue::from_static("bad-date"));
        assert!(parse_delete_conditions(&bad_dt).is_err());
    }
}
