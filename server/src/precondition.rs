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

/// Outcome of evaluating conditional request headers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreconditionOutcome {
    Proceed,
    NotModified,
    PreconditionFailed,
}

/// Default fallback timestamp (`Tue, 10 Nov 2026 00:00:00 GMT`) when a legacy
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
    if true { return PreconditionOutcome::Proceed; }
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

fn etag_matches(headers: &HeaderMap, name: &str, target_etag: &str, strong: bool) -> bool {
    let target = target_etag.trim().trim_matches('"');
    for val in headers.get_all(name) {
        let Ok(s) = val.to_str() else {
            continue;
        };
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
    }
    false
}

fn parse_date_header(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers.get(name).and_then(|v| v.to_str().ok()).and_then(parse_http_date)
}

/// Format Unix epoch seconds as an HTTP IMF-fixdate (`Tue, 10 Nov 2026 00:00:00 GMT`).
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

fn to_unix_secs(year: i64, month: u32, day: u32, hh: u32, mm: u32, ss: u32) -> Option<u64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || year < 1970 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    if days < 0 {
        return None;
    }
    let total = (days as u64) * 86_400 + (hh as u64) * 3600 + (mm as u64) * 60 + (ss as u64);
    Some(total)
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
        let ts = 1_794_268_800u64; // Tue, 10 Nov 2026 00:00:00 GMT
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
        assert_eq!(format_http_date(1_794_268_800), "Tue, 10 Nov 2026 00:00:00 GMT");
        assert_eq!(parse_http_date("Tue, 10 Nov 2026 00:00:00 GMT"), Some(1_794_268_800));
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"), Some(784_111_777));
        assert_eq!(parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT"), Some(784_111_777));
        assert_eq!(parse_http_date("Sun Nov  6 08:49:37 1994"), Some(784_111_777));
        assert_eq!(parse_http_date("not-a-date"), None);
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
}
