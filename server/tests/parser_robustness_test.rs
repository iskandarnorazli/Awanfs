//! Generated-input tests for the request parsers (#48, the part that runs on every PR without
//! cargo-fuzz): no input panics, and where a reference exists the parser agrees with it.
//! Deterministic: a fixed-seed xorshift generator, no clock.

use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use s3_server::{authenticate, decode_aws_chunked, parse_iso8601_epoch, parse_lifecycle_xml, parse_range_header, parse_s3_target, percent_decode, AuthConfig, ByteRange, SigV4KeyCache};
use std::panic::catch_unwind;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[self.below(xs.len() as u64) as usize]
    }
    fn bytes(&mut self, alphabet: &[u8], max: usize) -> Vec<u8> {
        (0..self.below(max as u64 + 1)).map(|_| alphabet[self.below(alphabet.len() as u64) as usize]).collect()
    }
}

/// RFC 9110 single byte range, independent of the implementation. u128 so huge numbers are
/// compared as numbers.
fn reference_range(header: &str, size: u64) -> Result<Option<(u64, u64)>, ()> {
    let h = header.trim();
    let Some(spec) = h.strip_prefix("bytes=") else { return Ok(None) };
    let spec = spec.trim();
    if spec.contains(',') || size == 0 {
        return Err(());
    }
    let num = |s: &str| -> Option<u128> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Some(s.bytes().fold(0u128, |n, d| (n * 10 + (d - b'0') as u128).min(u64::MAX as u128)))
    };
    let size = size as u128;
    let (a, b) = spec.split_once('-').ok_or(())?;
    if a.is_empty() {
        let n = num(b).ok_or(())?;
        if n == 0 {
            return Err(());
        }
        return Ok(Some(((size - n.min(size)) as u64, (size - 1) as u64)));
    }
    let start = num(a).ok_or(())?;
    if start >= size {
        return Err(());
    }
    if b.is_empty() {
        return Ok(Some((start as u64, (size - 1) as u64)));
    }
    let end = num(b).ok_or(())?;
    if start > end {
        return Err(());
    }
    Ok(Some((start as u64, end.min(size - 1) as u64)))
}

#[test]
fn range_header_agrees_with_the_rfc_reference() {
    let mut r = Rng(0x9e3779b97f4a7c15);
    let parts = ["", "0", "1", "7", "4095", "4096", "99999999999999999999999", "18446744073709551615", "-", " ", ",", "+5", "x"];
    for _ in 0..50_000 {
        let size = [0u64, 1, 10, 4096, 1 << 20][r.below(5) as usize];
        let mut h = String::from(r.pick(&["bytes=", "bytes=", "bytes= ", "items=", " bytes="]));
        for _ in 0..r.below(4) {
            h.push_str(r.pick(&parts));
        }
        let got = parse_range_header(&h, size).map(|o| o.map(|ByteRange { start, end }| (start, end)));
        assert_eq!(got, reference_range(&h, size), "Range {:?} on a {}-byte object", h, size);
    }
}

fn frame(payload: &[u8], r: &mut Rng) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = payload;
    while !rest.is_empty() {
        let n = (1 + r.below(rest.len() as u64)) as usize;
        out.extend_from_slice(format!("{:x};chunk-signature={:064x}\r\n", n, r.next()).as_bytes());
        out.extend_from_slice(&rest[..n]);
        out.extend_from_slice(b"\r\n");
        rest = &rest[n..];
    }
    out.extend_from_slice(b"0;chunk-signature=00\r\n\r\n");
    out
}

#[test]
fn aws_chunked_round_trips_and_rejects_broken_framing() {
    let mut r = Rng(42);
    for _ in 0..2_000 {
        let payload = r.bytes(b"ab\r\n0123;=", 300);
        let framed = frame(&payload, &mut r);
        assert_eq!(decode_aws_chunked(&framed).unwrap().as_ref(), &payload[..]);

        // Cut it anywhere before the final chunk: an error, never a shorter object.
        if framed.len() > 25 {
            let cut = r.below(framed.len() as u64 - 25) as usize;
            assert!(decode_aws_chunked(&framed[..cut]).is_err(), "a body cut at {} of {} decoded", cut, framed.len());
        }
    }
    assert!(decode_aws_chunked(b"5\r\nhello\r\nzz\r\nrest\r\n0\r\n\r\n").is_err(), "a bad header mid-body");
    assert!(decode_aws_chunked(b"5\r\nhelloX0\r\n\r\n").is_err(), "data not followed by CRLF");
    assert!(decode_aws_chunked(b"just raw bytes").is_err(), "no framing at all");
}

#[test]
fn no_parser_panics_on_generated_input() {
    let mut r = Rng(7);
    let alphabet: Vec<u8> = (0u8..=255).collect();
    let text: &[u8] = b"<>/RuleIDStatusPrefixExpirationDays0123456789%+-=&;,: \r\n\xc3\xa9AbortIncompleteMultipartUpload";
    for i in 0..20_000 {
        let raw = r.bytes(if i % 2 == 0 { &alphabet } else { text }, 200);
        let s = String::from_utf8_lossy(&raw).into_owned();
        let res = catch_unwind(|| {
            let _ = decode_aws_chunked(&raw);
            let _ = parse_lifecycle_xml(&s);
            let _ = percent_decode(&s);
            let _ = parse_iso8601_epoch(&s);
            let _ = parse_s3_target(&s, Some(&s));
            let _ = parse_range_header(&s, 100);
        });
        assert!(res.is_ok(), "panic on {:?}", s);
    }
}

#[test]
fn authenticate_never_panics_on_generated_headers() {
    let mut r = Rng(99);
    let cfg = AuthConfig { credentials: Some(("AK".into(), "SK".into())), allow_anonymous: false };
    let cache = SigV4KeyCache::new();
    let pieces = ["AWS4-HMAC-SHA256", " Credential=", "AK/", "20261112/", "us-east-1/s3/aws4_request", ",", "SignedHeaders=", "host;x-amz-date", "Signature=", "00ff", ";", "/", "=", "%", "é"];
    let dates = ["20261112T000000Z", "2026", "", "99999999T999999Z", "20261112T0000é0Z"];
    let queries = ["", "X-Amz-Algorithm=AWS4-HMAC-SHA256", "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AK%2F20261112%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261112T000000Z&X-Amz-Expires=999999999999999999999&X-Amz-SignedHeaders=host&X-Amz-Signature=00", "a=%", "%%%", "X-Amz-Expires=-1"];
    for _ in 0..20_000 {
        let mut auth = String::new();
        for _ in 0..r.below(8) {
            auth.push_str(r.pick(&pieces));
        }
        let mut h = HeaderMap::new();
        if let Ok(v) = HeaderValue::from_str(&auth) {
            h.insert("authorization", v);
        }
        if let Ok(v) = HeaderValue::from_str(r.pick(&dates)) {
            h.insert("x-amz-date", v);
        }
        h.insert(HeaderName::from_static("host"), HeaderValue::from_static("localhost"));
        let q = r.pick(&queries).to_string();
        // The key cache has a lock inside; a panic would fail the test anyway.
        let res = catch_unwind(std::panic::AssertUnwindSafe(|| authenticate(&cfg, &cache, "GET", "/b/k", &q, &h, 1_794_441_600)));
        assert!(res.is_ok(), "panic on Authorization {:?}, query {:?}", auth, q);
    }
}

/// The input the `lifecycle_xml` fuzz target crashed on in CI run 43575 (job 132378686962),
/// reduced: a rule id whose last character is a newline written as a character reference.
#[test]
fn a_rule_id_and_prefix_keep_their_whitespace_through_the_serialiser() {
    let xml = "<LifecycleConfiguration><Rule><ID>IL&gt;S&#10;</ID><Status>Enabled</Status><Filter><Prefix>&#32;logs/&#9;x&#32;</Prefix></Filter></Rule></LifecycleConfiguration>";
    let rules = parse_lifecycle_xml(xml).unwrap();
    assert_eq!(rules[0].id, "IL>S\n");
    assert_eq!(rules[0].prefix, " logs/\tx ");
    let again = parse_lifecycle_xml(&s3_server::lifecycle_config_xml(&rules)).unwrap();
    assert_eq!(again, rules);
    for exact in ["", " ", "  ", " a", "a ", " a b ", "\n", "a\r\nb", "\t&<>\"'"] {
        let one = vec![s3_server::LifecycleRule { id: "r".to_string(), status: true, prefix: exact.to_string(), expiration_days: Some(1), abort_multipart_days: None }];
        let back = parse_lifecycle_xml(&s3_server::lifecycle_config_xml(&one)).unwrap();
        assert_eq!(back, one, "prefix {:?}", exact);
    }
}

#[test]
fn lifecycle_xml_round_trips_through_its_serialiser() {
    let mut r = Rng(1234);
    // Whitespace at the ends and line breaks inside are part of the value: the generator used
    // to trim them away, which hid that the serialiser lost them (fuzz finding, 2026-11-14).
    let bits = ["a", "logs/", "&", "&amp;", "<Status>Disabled</Status>", "&lt;", "\"", "'", " ", "é", "]]>", "x", "\n", "\t", "\r", "  "];
    for _ in 0..5_000 {
        let mut rules = Vec::new();
        for i in 0..(1 + r.below(3)) {
            let mut id = String::new();
            let mut prefix = String::new();
            for _ in 0..r.below(4) {
                id.push_str(r.pick(&bits));
                prefix.push_str(r.pick(&bits));
            }
            rules.push(s3_server::LifecycleRule {
                id: if id.trim().is_empty() { format!("rule-{}", i + 1) } else { id },
                status: r.below(2) == 0,
                prefix,
                expiration_days: if r.below(2) == 0 { Some(r.below(1000) as u32) } else { None },
                abort_multipart_days: if r.below(2) == 0 { Some(r.below(30) as u32) } else { None },
            });
        }
        let xml = s3_server::lifecycle_config_xml(&rules);
        let back = parse_lifecycle_xml(&xml).unwrap_or_else(|e| panic!("{} for {}", e, xml));
        assert_eq!(back, rules, "round trip of {}", xml);
    }
}
