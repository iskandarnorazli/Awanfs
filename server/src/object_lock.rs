//! Object Lock XML and header parsing and serialization (#191, part of #120).

use hyper::HeaderMap;
use quick_xml::events::Event;
use quick_xml::reader::Reader;

use crate::fsm::{Refusal, RetentionMode, INVALID_ARGUMENT, MALFORMED_XML};

/// S3 `InvalidRetentionPeriod` (400) for non-positive `Days` or `Years` in `DefaultRetention`.
pub const INVALID_RETENTION_PERIOD: Refusal = Refusal {
    code: "InvalidRetentionPeriod",
    status: 400,
    why: "InvalidRetentionPeriod",
};

/// Default retention period in a bucket's object lock configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionPeriod {
    Days(u32),
    Years(u32),
}

/// Default retention rule for new objects in an object-lock-enabled bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DefaultRetention {
    pub mode: RetentionMode,
    pub period: RetentionPeriod,
}

/// Parsed `<ObjectLockConfiguration>` document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockConfiguration {
    pub enabled: bool,
    pub default_retention: Option<DefaultRetention>,
}

/// Parsed `<Retention>` document or object-lock retention headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retention {
    pub mode: RetentionMode,
    /// Seconds since the Unix epoch (matches `fsm::LockEvent::PutRetention`).
    pub retain_until_epoch: u64,
    /// Exact ISO 8601 timestamp text (preserving fractional seconds and `Z`) for round-trip.
    pub retain_until_date: String,
}

/// Object lock headers parsed from `PUT` object / `POST ?uploads` (`CreateMultipartUpload`).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RequestedLock {
    pub retention: Option<Retention>,
    pub legal_hold: Option<bool>,
}

fn parse_mode(s: &str) -> Option<RetentionMode> {
    match s {
        "GOVERNANCE" => Some(RetentionMode::Governance),
        "COMPLIANCE" => Some(RetentionMode::Compliance),
        _ => None,
    }
}

fn mode_str(mode: RetentionMode) -> &'static str {
    match mode {
        RetentionMode::Governance => "GOVERNANCE",
        RetentionMode::Compliance => "COMPLIANCE",
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => if is_leap_year(year) { 29 } else { 28 },
        _ => 0,
    }
}

/// Parses an ISO 8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SS[.fffffffff](Z|+HH:MM|-HH:MM)`)
/// into seconds since the Unix epoch, preserving the validated string for exact round-trip.
pub(crate) fn parse_iso8601_z(s: &str) -> Option<(u64, String)> {
    let b = s.as_bytes();
    if b.len() < 20 || !s.is_ascii() {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let year: i64 = s[0..4].parse().ok()?;
    let month: i64 = s[5..7].parse().ok()?;
    let day: i64 = s[8..10].parse().ok()?;
    let hour: u64 = s[11..13].parse().ok()?;
    let min: u64 = s[14..16].parse().ok()?;
    let sec: u64 = s[17..19].parse().ok()?;

    let tz_start = if b.len() > 19 && b[19] == b'.' {
        let mut idx = 20;
        while idx < b.len() && b[idx].is_ascii_digit() {
            idx += 1;
        }
        let frac_len = idx - 20;
        if frac_len == 0 || frac_len > 9 {
            return None;
        }
        idx
    } else {
        19
    };

    let tz = &s[tz_start..];
    let tz_offset_secs: i64 = if tz == "Z" {
        0
    } else if tz.len() == 6 && (tz.starts_with('+') || tz.starts_with('-')) && &tz[3..4] == ":" {
        let sign: i64 = if tz.starts_with('+') { 1 } else { -1 };
        let th: i64 = tz[1..3].parse().ok()?;
        let tm: i64 = tz[4..6].parse().ok()?;
        if th > 23 || tm > 59 {
            return None;
        }
        sign * (th * 3600 + tm * 60)
    } else {
        return None;
    };

    if !(1970..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || hour > 23
        || min > 59
        || sec > 59
    {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let local_epoch = (days as i64) * 86_400 + (hour * 3600 + min * 60 + sec) as i64;
    let utc_epoch = u64::try_from(local_epoch - tz_offset_secs).ok()?;
    Some((utc_epoch, s.to_string()))
}

/// Howard Hinnant's `days_from_civil` algorithm (days since 1970-01-01).
fn days_from_civil(year: i64, month: i64, day: i64) -> u64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as u64
}

/// Howard Hinnant's `civil_from_days` algorithm (year, month 1..=12, day 1..=31).
fn civil_from_days(z: u64) -> (i64, i64, i64) {
    let z = z as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year, m, d)
}

fn epoch_to_iso8601_z(epoch: u64) -> String {
    let days = epoch / 86_400;
    let secs = epoch % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hh = secs / 3600;
    let mm = (secs % 3600) / 60;
    let ss = secs % 60;
    format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// Computes `RetainUntilDate` for a bucket's `DefaultRetention` rule relative to `now_ns`.
pub fn compute_default_retention(default: DefaultRetention, now_ns: u64) -> Retention {
    let now_sec = now_ns / 1_000_000_000;
    let epoch = match default.period {
        RetentionPeriod::Days(d) => now_sec.saturating_add(u64::from(d).saturating_mul(86_400)),
        RetentionPeriod::Years(y) => {
            let days = now_sec / 86_400;
            let secs = now_sec % 86_400;
            let (year, month, day) = civil_from_days(days);
            let target_year = (year + y as i64).min(9999);
            let target_day = day.min(days_in_month(target_year, month));
            days_from_civil(target_year, month, target_day)
                .saturating_mul(86_400)
                .saturating_add(secs)
        }
    };
    Retention {
        mode: default.mode,
        retain_until_epoch: epoch,
        retain_until_date: epoch_to_iso8601_z(epoch),
    }
}

/// Parses `<Retention>` XML, returning `Ok(None)` when `<Retention/>` / `<Retention></Retention>`
/// is empty (indicating retention clear with `BypassGovernanceRetention`).
pub fn parse_retention_or_clear(xml: &str) -> Result<Option<Retention>, Refusal> {
    let mut mode: Option<String> = None;
    let mut date: Option<String> = None;

    read_xml_fields(
        xml,
        "Retention",
        |path, val| match path {
            ["Retention", "Mode"] => set_once(&mut mode, val),
            ["Retention", "RetainUntilDate"] => set_once(&mut date, val),
            _ => Ok(()),
        },
        |_| Ok(()),
    )?;

    match (mode.as_deref(), date.as_deref()) {
        (None, None) => Ok(None),
        (Some(m), Some(d)) => {
            let parsed_mode = parse_mode(m).ok_or(MALFORMED_XML)?;
            let (retain_until_epoch, retain_until_date) =
                parse_iso8601_z(d).ok_or(MALFORMED_XML)?;
            Ok(Some(Retention {
                mode: parsed_mode,
                retain_until_epoch,
                retain_until_date,
            }))
        }
        _ => Err(MALFORMED_XML),
    }
}

/// Parses `x-amz-bypass-governance-retention: true` (case-insensitive).
pub fn parse_bypass_governance_header(headers: &HeaderMap) -> bool {
    headers
        .get("x-amz-bypass-governance-retention")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.trim().eq_ignore_ascii_case("true"))
}

/// Parses `x-amz-bucket-object-lock-enabled` on `CreateBucket`.
pub fn parse_bucket_object_lock_header(headers: &HeaderMap) -> Result<bool, Refusal> {
    match headers.get("x-amz-bucket-object-lock-enabled") {
        None => Ok(false),
        Some(v) => {
            let s = v.to_str().map_err(|_| INVALID_ARGUMENT)?.trim();
            if s.eq_ignore_ascii_case("true") {
                Ok(true)
            } else if s.eq_ignore_ascii_case("false") {
                Ok(false)
            } else {
                Err(INVALID_ARGUMENT)
            }
        }
    }
}

fn read_xml_fields(
    xml: &str,
    root: &str,
    mut on_field: impl FnMut(&[&str], &str) -> Result<(), Refusal>,
    mut on_open: impl FnMut(&[&str]) -> Result<(), Refusal>,
) -> Result<(), Refusal> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut path: Vec<String> = Vec::new();
    let mut saw_root = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if path.is_empty() {
                    if saw_root || name != root {
                        return Err(MALFORMED_XML);
                    }
                    saw_root = true;
                }
                path.push(name);
                let refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                on_open(&refs)?;
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if path.is_empty() {
                    if saw_root || name != root {
                        return Err(MALFORMED_XML);
                    }
                    saw_root = true;
                }
                path.push(name);
                let refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                on_open(&refs)?;
                on_field(&refs, "")?;
                path.pop();
            }
            Ok(Event::Text(t)) => {
                let v = t.unescape().map_err(|_| MALFORMED_XML)?;
                let refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                on_field(&refs, &v)?;
            }
            Ok(Event::CData(c)) => {
                let v = String::from_utf8_lossy(&c.into_inner()).into_owned();
                let refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                on_field(&refs, &v)?;
            }
            Ok(Event::End(_)) => {
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(MALFORMED_XML),
            Ok(_) => {}
        }
    }

    if !saw_root {
        return Err(MALFORMED_XML);
    }
    Ok(())
}

fn set_once(slot: &mut Option<String>, val: &str) -> Result<(), Refusal> {
    if slot.is_some() {
        return Err(MALFORMED_XML);
    }
    *slot = Some(val.to_string());
    Ok(())
}

/// Parses `<ObjectLockConfiguration>` XML.
pub fn parse_lock_configuration(xml: &str) -> Result<LockConfiguration, Refusal> {
    let mut enabled: Option<String> = None;
    let mut saw_rule = false;
    let mut saw_default_retention = false;
    let mut mode: Option<String> = None;
    let mut days: Option<String> = None;
    let mut years: Option<String> = None;

    read_xml_fields(
        xml,
        "ObjectLockConfiguration",
        |path, val| match path {
            ["ObjectLockConfiguration", "ObjectLockEnabled"] => set_once(&mut enabled, val),
            ["ObjectLockConfiguration", "Rule", "DefaultRetention", "Mode"] => set_once(&mut mode, val),
            ["ObjectLockConfiguration", "Rule", "DefaultRetention", "Days"] => set_once(&mut days, val),
            ["ObjectLockConfiguration", "Rule", "DefaultRetention", "Years"] => set_once(&mut years, val),
            _ => Ok(()),
        },
        |path| match path {
            ["ObjectLockConfiguration", "Rule"] => {
                if saw_rule {
                    return Err(MALFORMED_XML);
                }
                saw_rule = true;
                Ok(())
            }
            ["ObjectLockConfiguration", "Rule", "DefaultRetention"] => {
                if saw_default_retention {
                    return Err(MALFORMED_XML);
                }
                saw_default_retention = true;
                Ok(())
            }
            _ => Ok(()),
        },
    )?;

    if enabled.as_deref() != Some("Enabled") {
        return Err(MALFORMED_XML);
    }

    let default_retention = if saw_rule {
        if !saw_default_retention {
            return Err(MALFORMED_XML);
        }
        let parsed_mode = mode.as_deref().and_then(parse_mode).ok_or(MALFORMED_XML)?;
        let period = match (days.as_deref(), years.as_deref()) {
            (Some(d), None) => {
                let n: i64 = d.parse().map_err(|_| MALFORMED_XML)?;
                if n <= 0 || n > u32::MAX as i64 {
                    return Err(INVALID_RETENTION_PERIOD);
                }
                RetentionPeriod::Days(n as u32)
            }
            (None, Some(y)) => {
                let n: i64 = y.parse().map_err(|_| MALFORMED_XML)?;
                if n <= 0 || n > u32::MAX as i64 {
                    return Err(INVALID_RETENTION_PERIOD);
                }
                RetentionPeriod::Years(n as u32)
            }
            _ => return Err(MALFORMED_XML),
        };
        Some(DefaultRetention {
            mode: parsed_mode,
            period,
        })
    } else {
        None
    };

    Ok(LockConfiguration {
        enabled: true,
        default_retention,
    })
}

/// Parses `<Retention>` XML.
pub fn parse_retention(xml: &str) -> Result<Retention, Refusal> {
    let mut mode: Option<String> = None;
    let mut date: Option<String> = None;

    read_xml_fields(
        xml,
        "Retention",
        |path, val| match path {
            ["Retention", "Mode"] => set_once(&mut mode, val),
            ["Retention", "RetainUntilDate"] => set_once(&mut date, val),
            _ => Ok(()),
        },
        |_| Ok(()),
    )?;

    let parsed_mode = mode.as_deref().and_then(parse_mode).ok_or(MALFORMED_XML)?;
    let date_str = date.ok_or(MALFORMED_XML)?;
    let (retain_until_epoch, retain_until_date) =
        parse_iso8601_z(&date_str).ok_or(MALFORMED_XML)?;

    Ok(Retention {
        mode: parsed_mode,
        retain_until_epoch,
        retain_until_date,
    })
}

/// Parses `<LegalHold>` XML; returns `true` for `ON`, `false` for `OFF`.
pub fn parse_legal_hold(xml: &str) -> Result<bool, Refusal> {
    let mut status: Option<String> = None;

    read_xml_fields(
        xml,
        "LegalHold",
        |path, val| match path {
            ["LegalHold", "Status"] => set_once(&mut status, val),
            _ => Ok(()),
        },
        |_| Ok(()),
    )?;

    match status.as_deref() {
        Some("ON") => Ok(true),
        Some("OFF") => Ok(false),
        _ => Err(MALFORMED_XML),
    }
}

/// Parses `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, and
/// `x-amz-object-lock-legal-hold` request headers.
pub fn lock_headers(headers: &HeaderMap) -> Result<RequestedLock, Refusal> {
    let mode_hdr = headers.get("x-amz-object-lock-mode");
    let date_hdr = headers.get("x-amz-object-lock-retain-until-date");
    let hold_hdr = headers.get("x-amz-object-lock-legal-hold");

    let retention = match (mode_hdr, date_hdr) {
        (None, None) => None,
        (Some(m), Some(d)) => {
            let m_str = m.to_str().map_err(|_| INVALID_ARGUMENT)?;
            let d_str = d.to_str().map_err(|_| INVALID_ARGUMENT)?;
            let mode = parse_mode(m_str).ok_or(INVALID_ARGUMENT)?;
            let (retain_until_epoch, retain_until_date) =
                parse_iso8601_z(d_str).ok_or(INVALID_ARGUMENT)?;
            Some(Retention {
                mode,
                retain_until_epoch,
                retain_until_date,
            })
        }
        _ => return Err(INVALID_ARGUMENT),
    };

    let legal_hold = match hold_hdr {
        None => None,
        Some(h) => match h.to_str().map_err(|_| INVALID_ARGUMENT)? {
            "ON" => Some(true),
            "OFF" => Some(false),
            _ => return Err(INVALID_ARGUMENT),
        },
    };

    Ok(RequestedLock {
        retention,
        legal_hold,
    })
}

/// Serializes `<ObjectLockConfiguration>` XML for `GET ?object-lock`.
pub fn lock_configuration_xml(cfg: &LockConfiguration) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
           <ObjectLockEnabled>Enabled</ObjectLockEnabled>\n",
    );
    if let Some(dr) = cfg.default_retention {
        out.push_str("  <Rule>\n    <DefaultRetention>\n");
        out.push_str(&format!("      <Mode>{}</Mode>\n", mode_str(dr.mode)));
        match dr.period {
            RetentionPeriod::Days(d) => out.push_str(&format!("      <Days>{}</Days>\n", d)),
            RetentionPeriod::Years(y) => out.push_str(&format!("      <Years>{}</Years>\n", y)),
        }
        out.push_str("    </DefaultRetention>\n  </Rule>\n");
    }
    out.push_str("</ObjectLockConfiguration>");
    out
}

/// Serializes `<Retention>` XML for `GET ?retention`.
pub fn retention_xml(retention: &Retention) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Retention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
           <Mode>{}</Mode>\n  \
           <RetainUntilDate>{}</RetainUntilDate>\n\
         </Retention>",
        mode_str(retention.mode),
        retention.retain_until_date
    )
}

/// Serializes `<LegalHold>` XML for `GET ?legal-hold`.
pub fn legal_hold_xml(on: bool) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <LegalHold xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
           <Status>{}</Status>\n\
         </LegalHold>",
        if on { "ON" } else { "OFF" }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::{HeaderName, HeaderValue};
    use proptest::prelude::*;

    #[test]
    fn suite_put_obj_lock_cases() {
        struct Case {
            name: &'static str,
            xml: &'static str,
            want: Result<LockConfiguration, Refusal>,
        }
        let cases = [
            Case {
                name: "test_object_lock_put_obj_lock_governance_days",
                xml: r#"<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Ok(LockConfiguration {
                    enabled: true,
                    default_retention: Some(DefaultRetention {
                        mode: RetentionMode::Governance,
                        period: RetentionPeriod::Days(1),
                    }),
                }),
            },
            Case {
                name: "test_object_lock_put_obj_lock_compliance_years",
                xml: r#"<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Ok(LockConfiguration {
                    enabled: true,
                    default_retention: Some(DefaultRetention {
                        mode: RetentionMode::Compliance,
                        period: RetentionPeriod::Years(1),
                    }),
                }),
            },
            Case {
                name: "test_object_lock_put_obj_lock_enabled_no_rule",
                xml: r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"#,
                want: Ok(LockConfiguration {
                    enabled: true,
                    default_retention: None,
                }),
            },
            Case {
                name: "test_object_lock_put_obj_lock_with_days_and_years",
                xml: r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Err(MALFORMED_XML),
            },
            Case {
                name: "test_object_lock_put_obj_lock_invalid_days",
                xml: r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>0</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Err(INVALID_RETENTION_PERIOD),
            },
            Case {
                name: "test_object_lock_put_obj_lock_invalid_years",
                xml: r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Years>-1</Years></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Err(INVALID_RETENTION_PERIOD),
            },
            Case {
                name: "test_object_lock_put_obj_lock_invalid_mode_abc",
                xml: r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>abc</Mode><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Err(MALFORMED_XML),
            },
            Case {
                name: "test_object_lock_put_obj_lock_invalid_mode_lowercase",
                xml: r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>governance</Mode><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Err(MALFORMED_XML),
            },
            Case {
                name: "test_object_lock_put_obj_lock_invalid_status",
                xml: r#"<ObjectLockConfiguration><ObjectLockEnabled>Disabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Err(MALFORMED_XML),
            },
            Case {
                name: "missing_days_and_years",
                xml: r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode></DefaultRetention></Rule></ObjectLockConfiguration>"#,
                want: Err(MALFORMED_XML),
            },
        ];
        for c in cases {
            let got = parse_lock_configuration(c.xml);
            assert_eq!(got, c.want, "case: {}", c.name);
        }
    }

    #[test]
    fn suite_retention_cases() {
        struct Case {
            name: &'static str,
            xml: &'static str,
            want: Result<(RetentionMode, &'static str), Refusal>,
        }
        let cases = [
            Case {
                name: "test_object_lock_put_obj_retention",
                xml: r#"<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>GOVERNANCE</Mode><RetainUntilDate>2140-01-01T00:00:00Z</RetainUntilDate></Retention>"#,
                want: Ok((RetentionMode::Governance, "2140-01-01T00:00:00Z")),
            },
            Case {
                name: "test_object_lock_get_obj_retention_iso8601_frac",
                xml: r#"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2030-01-01T00:00:00.123456Z</RetainUntilDate></Retention>"#,
                want: Ok((RetentionMode::Compliance, "2030-01-01T00:00:00.123456Z")),
            },
            Case {
                name: "test_object_lock_put_obj_retention_invalid_mode_lowercase",
                xml: r#"<Retention><Mode>governance</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"#,
                want: Err(MALFORMED_XML),
            },
            Case {
                name: "test_object_lock_put_obj_retention_invalid_mode_abc",
                xml: r#"<Retention><Mode>abc</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"#,
                want: Err(MALFORMED_XML),
            },
            Case {
                name: "invalid_date",
                xml: r#"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>not-a-date</RetainUntilDate></Retention>"#,
                want: Err(MALFORMED_XML),
            },
        ];
        for c in cases {
            let got = parse_retention(c.xml).map(|r| (r.mode, r.retain_until_date));
            let want = c.want.map(|(m, d)| (m, d.to_string()));
            assert_eq!(got, want, "case: {}", c.name);
        }
    }

    #[test]
    fn suite_legal_hold_cases() {
        struct Case {
            name: &'static str,
            xml: &'static str,
            want: Result<bool, Refusal>,
        }
        let cases = [
            Case {
                name: "test_object_lock_put_legal_hold_on",
                xml: r#"<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></LegalHold>"#,
                want: Ok(true),
            },
            Case {
                name: "test_object_lock_put_legal_hold_off",
                xml: r#"<LegalHold><Status>OFF</Status></LegalHold>"#,
                want: Ok(false),
            },
            Case {
                name: "test_object_lock_put_legal_hold_invalid_status",
                xml: r#"<LegalHold><Status>abc</Status></LegalHold>"#,
                want: Err(MALFORMED_XML),
            },
            Case {
                name: "missing_status",
                xml: r#"<LegalHold></LegalHold>"#,
                want: Err(MALFORMED_XML),
            },
        ];
        for c in cases {
            let got = parse_legal_hold(c.xml);
            assert_eq!(got, c.want, "case: {}", c.name);
        }
    }

    #[test]
    fn suite_lock_headers_cases() {
        fn make_headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
            let mut h = HeaderMap::new();
            for &(k, v) in pairs {
                h.insert(
                    HeaderName::from_static(k),
                    HeaderValue::from_static(v),
                );
            }
            h
        }

        // Empty headers -> None/None
        assert_eq!(
            lock_headers(&HeaderMap::new()),
            Ok(RequestedLock {
                retention: None,
                legal_hold: None,
            })
        );

        // test_object_lock_uploading_obj: all three headers
        let h = make_headers(&[
            ("x-amz-object-lock-mode", "GOVERNANCE"),
            ("x-amz-object-lock-retain-until-date", "2030-01-01T00:00:00.000Z"),
            ("x-amz-object-lock-legal-hold", "ON"),
        ]);
        let parsed = lock_headers(&h).unwrap();
        assert_eq!(parsed.legal_hold, Some(true));
        let ret = parsed.retention.unwrap();
        assert_eq!(ret.mode, RetentionMode::Governance);
        assert_eq!(ret.retain_until_date, "2030-01-01T00:00:00.000Z");

        // Mode without date -> 400 InvalidArgument
        let h = make_headers(&[("x-amz-object-lock-mode", "GOVERNANCE")]);
        assert_eq!(lock_headers(&h), Err(INVALID_ARGUMENT));

        // Date without mode -> 400 InvalidArgument
        let h = make_headers(&[(
            "x-amz-object-lock-retain-until-date",
            "2030-01-01T00:00:00Z",
        )]);
        assert_eq!(lock_headers(&h), Err(INVALID_ARGUMENT));

        // Invalid legal hold header -> 400 InvalidArgument
        let h = make_headers(&[("x-amz-object-lock-legal-hold", "INVALID")]);
        assert_eq!(lock_headers(&h), Err(INVALID_ARGUMENT));
    }

    fn arb_mode() -> impl Strategy<Value = RetentionMode> {
        prop_oneof![
            Just(RetentionMode::Governance),
            Just(RetentionMode::Compliance),
        ]
    }

    fn arb_lock_config() -> impl Strategy<Value = LockConfiguration> {
        let period = prop_oneof![
            (1u32..=36500).prop_map(RetentionPeriod::Days),
            (1u32..=100).prop_map(RetentionPeriod::Years),
        ];
        let default_ret = prop::option::of((arb_mode(), period).prop_map(|(mode, period)| DefaultRetention { mode, period }));
        default_ret.prop_map(|default_retention| LockConfiguration {
            enabled: true,
            default_retention,
        })
    }

    fn arb_iso8601() -> impl Strategy<Value = String> {
        (
            1970u32..=2200,
            1u32..=12,
            1u32..=28,
            0u32..=23,
            0u32..=59,
            0u32..=59,
            prop::option::of(proptest::string::string_regex("[0-9]{1,9}").unwrap()),
        )
            .prop_map(|(y, m, d, hh, mm, ss, frac)| match frac {
                Some(f) => format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{f}Z"),
                None => format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z"),
            })
    }

    proptest! {
        #[test]
        fn prop_roundtrip_lock_configuration(cfg in arb_lock_config()) {
            let xml = lock_configuration_xml(&cfg);
            let parsed = parse_lock_configuration(&xml).expect("valid lock config xml");
            prop_assert_eq!(parsed, cfg);
        }

        #[test]
        fn prop_roundtrip_retention_preserves_text(mode in arb_mode(), dt in arb_iso8601()) {
            let input_xml = format!(
                "<Retention><Mode>{}</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>",
                match mode {
                    RetentionMode::Governance => "GOVERNANCE",
                    RetentionMode::Compliance => "COMPLIANCE",
                },
                dt
            );
            let parsed = parse_retention(&input_xml).expect("valid retention xml");
            prop_assert_eq!(&parsed.retain_until_date, &dt);
            let xml = retention_xml(&parsed);
            let reparsed = parse_retention(&xml).expect("serialized retention xml parses");
            prop_assert_eq!(reparsed, parsed);
        }

        #[test]
        fn prop_roundtrip_legal_hold(on in any::<bool>()) {
            let xml = legal_hold_xml(on);
            let parsed = parse_legal_hold(&xml).expect("valid legal hold xml");
            prop_assert_eq!(parsed, on);
        }
    }

    #[test]
    fn compute_default_retention_days_and_leap_years() {
        // 2024-02-29T12:00:00Z
        let (leap_epoch, _) = parse_iso8601_z("2024-02-29T12:00:00Z").unwrap();
        let now_ns = leap_epoch * 1_000_000_000;

        let d1 = compute_default_retention(
            DefaultRetention {
                mode: RetentionMode::Governance,
                period: RetentionPeriod::Days(2),
            },
            now_ns,
        );
        assert_eq!(d1.retain_until_date, "2024-03-02T12:00:00Z");

        let y1 = compute_default_retention(
            DefaultRetention {
                mode: RetentionMode::Compliance,
                period: RetentionPeriod::Years(1),
            },
            now_ns,
        );
        assert_eq!(y1.retain_until_date, "2025-02-28T12:00:00Z");
    }

    #[test]
    fn parse_retention_clear_and_bucket_lock_header() {
        assert_eq!(parse_retention_or_clear("<Retention/>"), Ok(None));
        assert_eq!(parse_retention_or_clear("<Retention></Retention>"), Ok(None));
        assert_eq!(parse_retention_or_clear("<Retention><Mode>GOVERNANCE</Mode></Retention>"), Err(MALFORMED_XML));

        let mut h = HeaderMap::new();
        assert_eq!(parse_bucket_object_lock_header(&h), Ok(false));
        h.insert("x-amz-bucket-object-lock-enabled", HeaderValue::from_static("TrUe"));
        assert_eq!(parse_bucket_object_lock_header(&h), Ok(true));
        h.insert("x-amz-bucket-object-lock-enabled", HeaderValue::from_static("maybe"));
        assert_eq!(parse_bucket_object_lock_header(&h), Err(INVALID_ARGUMENT));
    }
}
