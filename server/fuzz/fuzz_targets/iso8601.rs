#![no_main]
//! ISO 8601 basic date (`YYYYMMDDTHHMMSSZ`): never panics, and agrees with a formatter on
//! every value it accepts. The SigV4 time read from an HTTP `Date` header (#457) parses back
//! to the same instant.
use libfuzzer_sys::fuzz_target;
use s3_server::{format_iso8601_basic, parse_iso8601_epoch, sigv4_time_from_http_date};

fn format_iso8601_epoch(epoch: u64) -> Option<String> {
    let days = (epoch / 86_400) as i64;
    let rem = epoch % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    if !(1970..=9999).contains(&y) {
        return None;
    }
    Some(format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        y,
        m,
        d,
        rem / 3600,
        (rem / 60) % 60,
        rem % 60
    ))
}

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    if let Some(epoch) = parse_iso8601_epoch(&s) {
        let formatted = format_iso8601_epoch(epoch).expect("accepted epoch must be in 1970..=9999");
        assert_eq!(formatted, s, "canonical round-trip mismatch");
        assert_eq!(parse_iso8601_epoch(&formatted), Some(epoch));
    }

    if let Some((iso, epoch)) = sigv4_time_from_http_date(&s) {
        assert_eq!(parse_iso8601_epoch(&iso), Some(epoch), "Date header {:?} gave {}", s, iso);
        assert_eq!(format_iso8601_basic(epoch).as_deref(), Some(iso.as_str()));
    }

    if data.len() >= 8 {
        // 9999-12-31T23:59:59Z = 253402300799
        let epoch = u64::from_le_bytes(data[..8].try_into().unwrap()) % 253_402_300_800;
        let formatted = format_iso8601_epoch(epoch).unwrap();
        assert_eq!(parse_iso8601_epoch(&formatted), Some(epoch));
        assert_eq!(format_iso8601_basic(epoch), Some(formatted));
    }
});
