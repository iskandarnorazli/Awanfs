//! Property-based tests for S3 listing pagination and URL encoding (#275).
//!
//! Covers ListBuckets pagination (`max-buckets`, `continuation-token`, `prefix`) and
//! ListObjects v1/v2 pagination (`max-keys`, `marker`/`continuation-token`, `start-after`,
//! `prefix`, `delimiter`, `encoding-type=url`).

use proptest::prelude::*;
use proptest::test_runner::{Config, RngAlgorithm, TestError, TestRng, TestRunner};
use reqwest::StatusCode;
use s3_server::names::validate_bucket_name;
use s3_server::s3_api::validate_s3_key;
use s3_server::{start_s3_reactor, S3ServiceState, ServerHandle};
use std::collections::{BTreeSet, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

const CASE_COUNT: u32 = 48;
const BOUNDARY_CASE_COUNT: u32 = 4;

fn seed_for(tag: &[u8]) -> [u8; 32] {
    let mut seed = [0u8; 32];
    for (i, &b) in tag.iter().enumerate() {
        seed[i % 32] ^= b.wrapping_add((i as u8).wrapping_mul(31));
    }
    if let Ok(raw) = std::env::var("LISTING_PROPS_SEED") {
        for (i, b) in raw.bytes().enumerate() {
            seed[i % 32] ^= b;
        }
    }
    seed
}

fn run_prop<S, F>(name: &str, cases: u32, strategy: S, check: F)
where
    S: Strategy,
    S::Value: std::fmt::Debug,
    F: Fn(S::Value) -> Result<(), TestCaseError>,
{
    let seed = seed_for(name.as_bytes());
    let config = Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    };
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(config, rng);
    match runner.run(&strategy, |val| check(val)) {
        Ok(()) => {}
        Err(TestError::Fail(reason, minimal)) => {
            panic!(
                "property `{}` failed (seed={:02x?}): {}\nminimal failing input: {:#?}",
                name, seed, reason, minimal
            );
        }
        Err(TestError::Abort(reason)) => {
            panic!("property `{}` aborted (seed={:02x?}): {}", name, seed, reason);
        }
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn encode_path_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn encode_query_value(val: &str) -> String {
    let mut out = String::with_capacity(val.len());
    for b in val.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn url_decode_bytes(s: &str, plus_as_space: bool) -> Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            if i + 2 >= b.len() {
                return Err(format!("truncated percent escape in {:?}", s));
            }
            let hi = hex_val(b[i + 1]).ok_or_else(|| format!("invalid hex in {:?}", s))?;
            let lo = hex_val(b[i + 2]).ok_or_else(|| format!("invalid hex in {:?}", s))?;
            out.push((hi << 4) | lo);
            i += 3;
        } else if plus_as_space && b[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| format!("invalid UTF-8 after URL decode of {:?}: {}", s, e))
}

fn xml_unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let after = &rest[amp..];
        if let Some(semi) = after.find(';') {
            let entity = &after[1..semi];
            let decoded = match entity {
                "amp" => Some('&'),
                "lt" => Some('<'),
                "gt" => Some('>'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                    u32::from_str_radix(&entity[2..], 16).ok().and_then(char::from_u32)
                }
                _ if entity.starts_with('#') => {
                    entity[1..].parse::<u32>().ok().and_then(char::from_u32)
                }
                _ => None,
            };
            if let Some(ch) = decoded {
                out.push(ch);
                rest = &after[semi + 1..];
                continue;
            }
        }
        out.push('&');
        rest = &rest[amp + 1..];
    }
    out.push_str(rest);
    out
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)?;
    Some(xml_unescape(&xml[start..start + end]))
}

fn extract_blocks<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    xml.split(&open)
        .skip(1)
        .filter_map(|part| part.split(&close).next())
        .collect()
}

struct ParsedBucketsPage {
    names: Vec<String>,
    continuation_token: Option<String>,
    prefix: Option<String>,
}

fn parse_list_buckets_xml(xml: &str) -> Result<ParsedBucketsPage, String> {
    let b_open = xml.find("<Buckets>").ok_or_else(|| format!("missing <Buckets> in:\n{}", xml))?;
    let b_close = xml[b_open..].find("</Buckets>").ok_or_else(|| format!("missing </Buckets> in:\n{}", xml))? + b_open;
    let buckets_inner = &xml[b_open + "<Buckets>".len()..b_close];
    let outer = format!("{}{}", &xml[..b_open], &xml[b_close + "</Buckets>".len()..]);

    let mut names = Vec::new();
    for blk in extract_blocks(buckets_inner, "Bucket") {
        let name = extract_tag(blk, "Name").ok_or_else(|| format!("missing <Name> in <Bucket>: {}", blk))?;
        names.push(name);
    }
    Ok(ParsedBucketsPage {
        names,
        continuation_token: extract_tag(&outer, "ContinuationToken"),
        prefix: extract_tag(&outer, "Prefix"),
    })
}

struct ParsedObjectsPage {
    prefix: String,
    delimiter: Option<String>,
    marker: Option<String>,
    start_after: Option<String>,
    encoding_type: Option<String>,
    key_count: Option<usize>,
    is_truncated: bool,
    next_marker: Option<String>,
    next_continuation_token: Option<String>,
    keys: Vec<String>,
    common_prefixes: Vec<String>,
}

fn parse_list_objects_xml(xml: &str) -> Result<ParsedObjectsPage, String> {
    let first_item = [xml.find("<Contents>"), xml.find("<CommonPrefixes>")]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(xml.len());
    let header = &xml[..first_item];

    let prefix = extract_tag(header, "Prefix").ok_or_else(|| format!("missing <Prefix> in:\n{}", xml))?;
    let is_truncated = match extract_tag(header, "IsTruncated").as_deref() {
        Some("true") => true,
        Some("false") => false,
        other => return Err(format!("invalid <IsTruncated> {:?} in:\n{}", other, xml)),
    };
    let key_count = match extract_tag(header, "KeyCount") {
        Some(s) => Some(s.parse::<usize>().map_err(|e| format!("bad <KeyCount> {:?}: {}", s, e))?),
        None => None,
    };
    let mut keys = Vec::new();
    for blk in extract_blocks(xml, "Contents") {
        let k = extract_tag(blk, "Key").ok_or_else(|| format!("missing <Key> in <Contents>: {}", blk))?;
        keys.push(k);
    }
    let mut common_prefixes = Vec::new();
    for blk in extract_blocks(xml, "CommonPrefixes") {
        let cp = extract_tag(blk, "Prefix").ok_or_else(|| format!("missing <Prefix> in <CommonPrefixes>: {}", blk))?;
        common_prefixes.push(cp);
    }

    Ok(ParsedObjectsPage {
        prefix,
        delimiter: extract_tag(header, "Delimiter"),
        marker: extract_tag(header, "Marker"),
        start_after: extract_tag(header, "StartAfter"),
        encoding_type: extract_tag(header, "EncodingType"),
        key_count,
        is_truncated,
        next_marker: extract_tag(header, "NextMarker"),
        next_continuation_token: extract_tag(header, "NextContinuationToken"),
        keys,
        common_prefixes,
    })
}

fn decode_listed_item(raw: &str, url_encode: bool) -> Result<String, String> {
    if !url_encode {
        return Ok(raw.to_string());
    }
    let strict = url_decode_bytes(raw, false)?;
    let plus_aware = url_decode_bytes(raw, true)?;
    if strict != plus_aware {
        return Err(format!(
            "URL-encoded item {:?} differs between unquote ({:?}) and unquote_plus ({:?})",
            raw, strict, plus_aware
        ));
    }
    Ok(strict)
}

fn page_size_strategy() -> impl Strategy<Value = usize> {
    prop_oneof![
        6 => 1usize..=5,
        2 => 6usize..=32,
        1 => 33usize..=999,
        1 => Just(1000usize),
    ]
}

fn valid_bucket_name_strategy() -> impl Strategy<Value = String> {
    let alnum = prop::sample::select(b"abcdefghijklmnopqrstuvwxyz0123456789".to_vec());
    let mid_full = prop::sample::select(b"abcdefghijklmnopqrstuvwxyz0123456789.-".to_vec());
    let mid_tight = prop::sample::select(b"ab01.-".to_vec());
    let mid_len = prop_oneof![
        1 => Just(1usize),   // length 3
        5 => 1usize..=8,     // short names to trigger prefix overlap
        2 => 9usize..=61,
        1 => Just(61usize),  // length 63
    ];
    let use_tight = any::<bool>();
    (alnum.clone(), use_tight, mid_len, alnum)
        .prop_flat_map(move |(first, tight, len, last)| {
            let mid_char = if tight { mid_tight.clone() } else { mid_full.clone() };
            (
                Just(first),
                prop::collection::vec(mid_char, len),
                Just(last),
            )
        })
        .prop_map(|(first, middle, last)| {
            let mut bytes = Vec::with_capacity(2 + middle.len());
            bytes.push(first);
            bytes.extend(middle);
            bytes.push(last);
            String::from_utf8(bytes).unwrap()
        })
        .prop_filter("must satisfy S3 bucket naming rules", |s| {
            !s.contains("..")
                && !s.contains(".-")
                && !s.contains("-.")
                && s.parse::<Ipv4Addr>().is_err()
                && !s.starts_with("xn--")
                && !s.starts_with("sthree-")
                && !s.ends_with("-s3alias")
                && !s.ends_with("--ol-s3")
                && validate_bucket_name(s).is_ok()
        })
}

fn key_piece_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => prop::sample::select(vec![
            "a", "b", "0", "1", " ", "+", "%", "/", "&", "<", ">", "\"", "'", "=", "?", "#",
            "-", "_", ".", "~", "é", "ñ", "中", "🦀", "Ω",
        ]).prop_map(str::to_string),
        1 => prop::sample::select(vec![
            "a b+c", "%20", "%25", "%FF", "%0A", "a%2Bb", "sp ace/item", "café/🦀", "d&e<f>g", "x=1?y#z", "++", "  ",
        ]).prop_map(str::to_string),
    ]
}

fn valid_object_key_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(key_piece_strategy(), 1..=10)
        .prop_map(|parts| parts.concat())
        .prop_filter("must be a valid S3 object key without dot path segments", |k| {
            !k.is_empty()
                && k.len() <= 1024
                && !k.split('/').any(|seg| seg == "." || seg == "..")
                && validate_s3_key(k).is_ok()
        })
}

fn special_prefix_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => Just(String::new()),
        2 => prop::collection::vec(key_piece_strategy(), 1..=3).prop_map(|p| p.concat()),
        1 => prop::sample::select(vec!["\n", " ", "+", "%", "a b+c%20é/", "a&b<c>", "/"]).prop_map(str::to_string),
    ]
}

struct TestServer {
    rt: Runtime,
    url: String,
    client: reqwest::Client,
    _handle: ServerHandle,
    next_bucket: AtomicUsize,
}

impl TestServer {
    fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (url, handle) = rt.block_on(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::new())).await.unwrap();
            (format!("http://{}", handle.local_addr), handle)
        });
        let client = reqwest::Client::new();
        Self {
            rt,
            url,
            client,
            _handle: handle,
            next_bucket: AtomicUsize::new(1),
        }
    }

    fn fresh_bucket_name(&self, tag: &str) -> String {
        let id = self.next_bucket.fetch_add(1, Ordering::Relaxed);
        format!("prop-{}-{}", tag, id)
    }
}

async fn put_bucket_and_keys(
    client: &reqwest::Client,
    url: &str,
    bucket: &str,
    keys: &BTreeSet<String>,
) -> Result<(), String> {
    let resp = client
        .put(format!("{}/{}", url, bucket))
        .send()
        .await
        .map_err(|e| format!("PUT bucket {} failed: {}", bucket, e))?;
    if resp.status() != StatusCode::OK {
        return Err(format!("PUT bucket {} returned {}", bucket, resp.status()));
    }

    let mut set = JoinSet::new();
    let max_in_flight = 16usize;
    let mut iter = keys.iter().cloned();

    loop {
        while set.len() < max_in_flight {
            let Some(k) = iter.next() else { break };
            let c = client.clone();
            let target = format!("{}/{}/{}", url, bucket, encode_path_key(&k));
            set.spawn(async move {
                let resp = c
                    .put(&target)
                    .body("x")
                    .send()
                    .await
                    .map_err(|e| format!("PUT {:?} ({}) failed: {}", k, target, e))?;
                if resp.status() != StatusCode::OK {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(format!("PUT {:?} ({}) failed: {}", k, target, body));
                }
                Ok(())
            });
        }
        match set.join_next().await {
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(e))) => return Err(e),
            Some(Err(e)) => return Err(format!("join error: {}", e)),
            None => break,
        }
    }
    Ok(())
}

fn assert_strict_byte_order(items: &[String], context: &str) -> Result<(), TestCaseError> {
    for pair in items.windows(2) {
        if pair[0].as_bytes() >= pair[1].as_bytes() {
            return Err(TestCaseError::fail(format!(
                "{}: out of strict byte order: {:?} >= {:?}",
                context, pair[0], pair[1]
            )));
        }
    }
    Ok(())
}

fn derived_prefix(keys: &BTreeSet<String>, idx: usize, random_pfx: String) -> String {
    if keys.is_empty() || idx % 3 == 0 {
        return String::new();
    }
    if idx % 3 == 1 {
        let sample = keys.iter().nth(idx % keys.len()).unwrap();
        let char_count = sample.chars().count();
        let take = (idx % char_count.max(1)).max(1);
        return sample.chars().take(take).collect();
    }
    random_pfx
}

// covers: ListBuckets
#[test]
fn prop_list_buckets_pagination_and_prefix() {
    let srv = TestServer::new();
    let strat = (
        prop::collection::btree_set(valid_bucket_name_strategy(), 0..=24),
        page_size_strategy(),
        prop_oneof![
            3 => Just(None),
            2 => Just(Some(String::new())),
            3 => valid_bucket_name_strategy().prop_map(|b| Some(b[..1 + (b.len() % 4).min(b.len() - 1)].to_string())),
            2 => any::<usize>().prop_map(|n| Some(format!("PICK:{}", n))),
        ],
    );

    run_prop("list_buckets_pagination_and_prefix", CASE_COUNT, strat, |(buckets, max_buckets, raw_prefix)| {
        let prefix = match raw_prefix {
            Some(ref s) if s.starts_with("PICK:") && !buckets.is_empty() => {
                let idx = s["PICK:".len()..].parse::<usize>().unwrap_or(0) % buckets.len();
                let b = buckets.iter().nth(idx).unwrap();
                let cut = 1 + (idx % b.len());
                Some(b[..cut].to_string())
            }
            Some(ref s) if s.starts_with("PICK:") => Some("nonexistent".to_string()),
            other => other,
        };

        let result: Result<(), TestCaseError> = srv.rt.block_on(async {
            for b in &buckets {
                let resp = srv
                    .client
                    .put(format!("{}/{}", srv.url, b))
                    .send()
                    .await
                    .map_err(|e| TestCaseError::fail(format!("PUT bucket {}: {}", b, e)))?;
                prop_assert_eq!(resp.status(), StatusCode::OK);
            }

            let expected: BTreeSet<String> = buckets
                .iter()
                .filter(|b| prefix.as_deref().is_none_or(|p| b.starts_with(p)))
                .cloned()
                .collect();

            let mut all_returned = Vec::new();
            let mut seen = HashSet::new();
            let mut token: Option<String> = None;
            let max_pages = buckets.len() + 5;

            for page_idx in 0..max_pages {
                let mut qs = format!("max-buckets={}", max_buckets);
                if let Some(ref p) = prefix {
                    qs.push_str(&format!("&prefix={}", encode_query_value(p)));
                }
                if let Some(ref tok) = token {
                    qs.push_str(&format!("&continuation-token={}", encode_query_value(tok)));
                }
                let resp = srv
                    .client
                    .get(format!("{}/?{}", srv.url, qs))
                    .send()
                    .await
                    .map_err(|e| TestCaseError::fail(format!("GET /?{} failed: {}", qs, e)))?;
                prop_assert_eq!(resp.status(), StatusCode::OK, "GET /?{}", qs);
                let xml = resp
                    .text()
                    .await
                    .map_err(|e| TestCaseError::fail(format!("read body: {}", e)))?;
                let parsed = parse_list_buckets_xml(&xml).map_err(TestCaseError::fail)?;

                prop_assert_eq!(parsed.prefix.as_deref(), prefix.as_deref(), "echoed <Prefix>");
                prop_assert!(
                    parsed.names.len() <= max_buckets,
                    "page {} returned {} > max-buckets {}",
                    page_idx,
                    parsed.names.len(),
                    max_buckets
                );

                for name in &parsed.names {
                    prop_assert!(seen.insert(name.clone()), "duplicate bucket {:?} on page {}", name, page_idx);
                    all_returned.push(name.clone());
                }

                if let Some(next_tok) = parsed.continuation_token {
                    prop_assert!(!next_tok.is_empty(), "empty <ContinuationToken> on page {}", page_idx);
                    prop_assert_eq!(
                        parsed.names.len(),
                        max_buckets,
                        "non-final page {} returned {} != max-buckets {}",
                        page_idx,
                        parsed.names.len(),
                        max_buckets
                    );
                    token = Some(next_tok);
                } else {
                    break;
                }
                if page_idx + 1 == max_pages {
                    return Err(TestCaseError::fail(format!(
                        "ListBuckets exceeded {} pages (infinite pagination loop?)",
                        max_pages
                    )));
                }
            }

            assert_strict_byte_order(&all_returned, "ListBuckets")?;
            let returned_set: BTreeSet<String> = all_returned.into_iter().collect();
            prop_assert_eq!(returned_set, expected, "ListBuckets union mismatch");
            Ok(())
        });

        // Clean up buckets before returning so the next case/shrink starts from empty service.
        srv.rt.block_on(async {
            for b in &buckets {
                let resp = srv.client.delete(format!("{}/{}", srv.url, b)).send().await.unwrap();
                assert_eq!(resp.status(), StatusCode::NO_CONTENT, "cleanup DELETE {}", b);
            }
        });

        result
    });
}

// covers: ListObjectsV2
#[test]
fn prop_list_objects_v2_pagination_and_encoding() {
    let srv = TestServer::new();
    let strat = (
        prop::collection::btree_set(valid_object_key_strategy(), 0..=35),
        page_size_strategy(),
        prop_oneof![3 => Just(true), 1 => Just(false)],
        any::<usize>(),
        special_prefix_strategy(),
        proptest::option::of(valid_object_key_strategy()),
    );

    run_prop(
        "list_objects_v2_pagination_and_encoding",
        CASE_COUNT,
        strat,
        |(keys, max_keys, url_encode, pfx_pick, rand_pfx, mut start_after)| {
            let bucket = srv.fresh_bucket_name("v2");
            let prefix = derived_prefix(&keys, pfx_pick, rand_pfx);
            if !keys.is_empty() && pfx_pick % 2 == 0 && start_after.is_some() {
                // Sample start_after from actual keys half the time to hit boundary exclusions.
                start_after = Some(keys.iter().nth(pfx_pick % keys.len()).unwrap().clone());
            }

            srv.rt.block_on(async {
                put_bucket_and_keys(&srv.client, &srv.url, &bucket, &keys)
                    .await
                    .map_err(TestCaseError::fail)?;

                let expected: BTreeSet<String> = keys
                    .iter()
                    .filter(|k| k.starts_with(&prefix))
                    .filter(|k| start_after.as_ref().is_none_or(|sa| k.as_bytes() > sa.as_bytes()))
                    .cloned()
                    .collect();

                let mut all_decoded = Vec::new();
                let mut seen = HashSet::new();
                let mut token: Option<String> = None;
                let max_pages = keys.len() + 5;

                for page_idx in 0..max_pages {
                    let mut qs = format!("list-type=2&max-keys={}", max_keys);
                    if !prefix.is_empty() {
                        qs.push_str(&format!("&prefix={}", encode_query_value(&prefix)));
                    }
                    if let Some(ref sa) = start_after {
                        qs.push_str(&format!("&start-after={}", encode_query_value(sa)));
                    }
                    if url_encode {
                        qs.push_str("&encoding-type=url");
                    }
                    if let Some(ref tok) = token {
                        qs.push_str(&format!("&continuation-token={}", encode_query_value(tok)));
                    }

                    let resp = srv
                        .client
                        .get(format!("{}/{}?{}", srv.url, bucket, qs))
                        .send()
                        .await
                        .map_err(|e| TestCaseError::fail(format!("GET v2 ?{} failed: {}", qs, e)))?;
                    prop_assert_eq!(resp.status(), StatusCode::OK);
                    let xml = resp
                        .text()
                        .await
                        .map_err(|e| TestCaseError::fail(format!("read body: {}", e)))?;
                    let page = parse_list_objects_xml(&xml).map_err(TestCaseError::fail)?;

                    if url_encode {
                        prop_assert_eq!(page.encoding_type.as_deref(), Some("url"));
                    } else {
                        prop_assert_eq!(page.encoding_type, None);
                    }
                    let decoded_pfx = decode_listed_item(&page.prefix, url_encode).map_err(TestCaseError::fail)?;
                    prop_assert_eq!(&decoded_pfx, &prefix, "V2 <Prefix> round-trip");
                    if let Some(ref sa) = start_after {
                        let raw_sa = page.start_after.as_deref().unwrap_or("");
                        let decoded_sa = decode_listed_item(raw_sa, url_encode).map_err(TestCaseError::fail)?;
                        prop_assert_eq!(&decoded_sa, sa, "V2 <StartAfter> round-trip");
                    }
                    prop_assert_eq!(page.key_count, Some(page.keys.len()));
                    prop_assert!(page.common_prefixes.is_empty());
                    prop_assert!(page.keys.len() <= max_keys);

                    for raw_key in &page.keys {
                        let decoded = decode_listed_item(raw_key, url_encode).map_err(TestCaseError::fail)?;
                        prop_assert!(seen.insert(decoded.clone()), "duplicate key {:?} on page {}", decoded, page_idx);
                        all_decoded.push(decoded);
                    }

                    if page.is_truncated {
                        prop_assert_eq!(
                            page.keys.len(),
                            max_keys,
                            "V2 truncated page {} returned {} != max-keys {}",
                            page_idx,
                            page.keys.len(),
                            max_keys
                        );
                        let next_tok = page
                            .next_continuation_token
                            .ok_or_else(|| TestCaseError::fail("IsTruncated=true without <NextContinuationToken>"))?;
                        prop_assert!(!next_tok.is_empty());
                        token = Some(next_tok);
                    } else {
                        prop_assert!(
                            page.next_continuation_token.is_none(),
                            "IsTruncated=false must not emit <NextContinuationToken>"
                        );
                        break;
                    }
                    if page_idx + 1 == max_pages {
                        return Err(TestCaseError::fail(format!(
                            "ListObjectsV2 exceeded {} pages (infinite loop?)",
                            max_pages
                        )));
                    }
                }

                assert_strict_byte_order(&all_decoded, "ListObjectsV2")?;
                let returned_set: BTreeSet<String> = all_decoded.into_iter().collect();
                prop_assert_eq!(returned_set, expected, "ListObjectsV2 union mismatch");
                Ok(())
            })
        },
    );
}

// covers: ListObjects
#[test]
fn prop_list_objects_v1_pagination_and_prefix_encoding() {
    let srv = TestServer::new();
    let strat = (
        prop::collection::btree_set(valid_object_key_strategy(), 0..=35),
        page_size_strategy(),
        prop_oneof![3 => Just(true), 1 => Just(false)],
        any::<usize>(),
        special_prefix_strategy(),
    );

    run_prop(
        "list_objects_v1_pagination_and_prefix_encoding",
        CASE_COUNT,
        strat,
        |(keys, max_keys, url_encode, pfx_pick, rand_pfx)| {
            let bucket = srv.fresh_bucket_name("v1");
            let prefix = derived_prefix(&keys, pfx_pick, rand_pfx);

            srv.rt.block_on(async {
                put_bucket_and_keys(&srv.client, &srv.url, &bucket, &keys)
                    .await
                    .map_err(TestCaseError::fail)?;

                let expected: BTreeSet<String> = keys
                    .iter()
                    .filter(|k| k.starts_with(&prefix))
                    .cloned()
                    .collect();

                let mut all_decoded = Vec::new();
                let mut seen = HashSet::new();
                let mut marker: Option<String> = None;
                let max_pages = keys.len() + 5;

                for page_idx in 0..max_pages {
                    let mut qs = format!("max-keys={}", max_keys);
                    if !prefix.is_empty() {
                        qs.push_str(&format!("&prefix={}", encode_query_value(&prefix)));
                    }
                    if url_encode {
                        qs.push_str("&encoding-type=url");
                    }
                    if let Some(ref m) = marker {
                        qs.push_str(&format!("&marker={}", encode_query_value(m)));
                    }

                    let resp = srv
                        .client
                        .get(format!("{}/{}?{}", srv.url, bucket, qs))
                        .send()
                        .await
                        .map_err(|e| TestCaseError::fail(format!("GET v1 ?{} failed: {}", qs, e)))?;
                    prop_assert_eq!(resp.status(), StatusCode::OK);
                    let xml = resp
                        .text()
                        .await
                        .map_err(|e| TestCaseError::fail(format!("read body: {}", e)))?;
                    let page = parse_list_objects_xml(&xml).map_err(TestCaseError::fail)?;

                    if url_encode {
                        prop_assert_eq!(page.encoding_type.as_deref(), Some("url"));
                    } else {
                        prop_assert_eq!(page.encoding_type, None);
                    }
                    // PR #241 (#114): ListObjects v1 <Prefix> is XML-escaped raw prefix, NOT URL-encoded.
                    prop_assert_eq!(&page.prefix, &prefix, "V1 <Prefix> must remain raw (not URL-encoded)");

                    if let Some(ref m) = marker {
                        let raw_m = page.marker.as_deref().unwrap_or("");
                        let decoded_m = decode_listed_item(raw_m, url_encode).map_err(TestCaseError::fail)?;
                        prop_assert_eq!(&decoded_m, m, "V1 <Marker> round-trip");
                    }
                    prop_assert!(page.common_prefixes.is_empty());
                    prop_assert!(page.keys.len() <= max_keys);

                    let mut page_decoded_keys = Vec::with_capacity(page.keys.len());
                    for raw_key in &page.keys {
                        let decoded = decode_listed_item(raw_key, url_encode).map_err(TestCaseError::fail)?;
                        prop_assert!(seen.insert(decoded.clone()), "duplicate key {:?} on page {}", decoded, page_idx);
                        page_decoded_keys.push(decoded.clone());
                        all_decoded.push(decoded);
                    }

                    if page.is_truncated {
                        prop_assert_eq!(
                            page.keys.len(),
                            max_keys,
                            "V1 truncated page {} returned {} != max-keys {}",
                            page_idx,
                            page.keys.len(),
                            max_keys
                        );
                        let raw_nm = page
                            .next_marker
                            .ok_or_else(|| TestCaseError::fail("V1 IsTruncated=true without <NextMarker>"))?;
                        let next_m = decode_listed_item(&raw_nm, url_encode).map_err(TestCaseError::fail)?;
                        prop_assert_eq!(
                            page_decoded_keys.last(),
                            Some(&next_m),
                            "V1 <NextMarker> must match last key on page"
                        );
                        marker = Some(next_m);
                    } else {
                        prop_assert!(page.next_marker.is_none(), "IsTruncated=false must not emit <NextMarker>");
                        break;
                    }
                    if page_idx + 1 == max_pages {
                        return Err(TestCaseError::fail(format!(
                            "ListObjectsV1 exceeded {} pages (infinite loop?)",
                            max_pages
                        )));
                    }
                }

                assert_strict_byte_order(&all_decoded, "ListObjectsV1")?;
                let returned_set: BTreeSet<String> = all_decoded.into_iter().collect();
                prop_assert_eq!(returned_set, expected, "ListObjectsV1 union mismatch");
                Ok(())
            })
        },
    );
}

#[test]
fn prop_list_objects_v1_and_v2_delimiter_pagination() {
    let srv = TestServer::new();
    let strat = (
        prop::collection::btree_set(valid_object_key_strategy(), 0..=35),
        page_size_strategy(),
        any::<bool>(), // v2 vs v1
        prop_oneof![3 => Just(true), 1 => Just(false)], // url_encode
        prop_oneof![5 => Just("/"), 1 => Just("-")],
        any::<usize>(),
    );

    run_prop(
        "list_objects_v1_and_v2_delimiter_pagination",
        CASE_COUNT,
        strat,
        |(keys, max_keys, is_v2, url_encode, delimiter, pfx_pick)| {
            let bucket = srv.fresh_bucket_name("delim");
            let prefix = if keys.is_empty() || pfx_pick % 2 == 0 {
                String::new()
            } else {
                let sample = keys.iter().nth(pfx_pick % keys.len()).unwrap();
                match sample.find(delimiter) {
                    Some(pos) => sample[..pos + delimiter.len()].to_string(),
                    None => sample.chars().take(1).collect(),
                }
            };

            let mut expected_keys = BTreeSet::new();
            let mut expected_cps = BTreeSet::new();
            for k in &keys {
                if !k.starts_with(&prefix) {
                    continue;
                }
                let tail = &k[prefix.len()..];
                if let Some(pos) = tail.find(delimiter) {
                    expected_cps.insert(k[..prefix.len() + pos + delimiter.len()].to_string());
                } else {
                    expected_keys.insert(k.clone());
                }
            }

            srv.rt.block_on(async {
                put_bucket_and_keys(&srv.client, &srv.url, &bucket, &keys)
                    .await
                    .map_err(TestCaseError::fail)?;

                let mut all_keys = Vec::new();
                let mut all_cps = Vec::new();
                let mut seen_keys = HashSet::new();
                let mut seen_cps = HashSet::new();
                let mut prev_page_max: Option<String> = None;
                let mut cursor: Option<String> = None;
                let max_pages = expected_keys.len() + expected_cps.len() + 5;

                for page_idx in 0..max_pages {
                    let mut qs = if is_v2 {
                        format!("list-type=2&max-keys={}", max_keys)
                    } else {
                        format!("max-keys={}", max_keys)
                    };
                    qs.push_str(&format!("&delimiter={}", encode_query_value(delimiter)));
                    if !prefix.is_empty() {
                        qs.push_str(&format!("&prefix={}", encode_query_value(&prefix)));
                    }
                    if url_encode {
                        qs.push_str("&encoding-type=url");
                    }
                    if let Some(ref cur) = cursor {
                        if is_v2 {
                            qs.push_str(&format!("&continuation-token={}", encode_query_value(cur)));
                        } else {
                            qs.push_str(&format!("&marker={}", encode_query_value(cur)));
                        }
                    }

                    let resp = srv
                        .client
                        .get(format!("{}/{}?{}", srv.url, bucket, qs))
                        .send()
                        .await
                        .map_err(|e| TestCaseError::fail(format!("GET delim ?{} failed: {}", qs, e)))?;
                    prop_assert_eq!(resp.status(), StatusCode::OK);
                    let xml = resp
                        .text()
                        .await
                        .map_err(|e| TestCaseError::fail(format!("read body: {}", e)))?;
                    let page = parse_list_objects_xml(&xml).map_err(TestCaseError::fail)?;

                    if let Some(ref raw_delim) = page.delimiter {
                        let decoded_delim = decode_listed_item(raw_delim, url_encode).map_err(TestCaseError::fail)?;
                        prop_assert_eq!(decoded_delim, delimiter);
                    }
                    let item_count = page.keys.len() + page.common_prefixes.len();
                    prop_assert!(item_count <= max_keys);
                    if is_v2 {
                        prop_assert_eq!(page.key_count, Some(item_count));
                    }

                    let mut page_items = Vec::with_capacity(item_count);
                    for raw_k in &page.keys {
                        let dk = decode_listed_item(raw_k, url_encode).map_err(TestCaseError::fail)?;
                        prop_assert!(seen_keys.insert(dk.clone()), "duplicate key {:?}", dk);
                        all_keys.push(dk.clone());
                        page_items.push(dk);
                    }
                    for raw_cp in &page.common_prefixes {
                        let dcp = decode_listed_item(raw_cp, url_encode).map_err(TestCaseError::fail)?;
                        prop_assert!(seen_cps.insert(dcp.clone()), "duplicate CommonPrefix {:?}", dcp);
                        all_cps.push(dcp.clone());
                        page_items.push(dcp);
                    }

                    page_items.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
                    if let (Some(prev_max), Some(curr_min)) = (&prev_page_max, page_items.first()) {
                        prop_assert!(
                            prev_max.as_bytes() < curr_min.as_bytes(),
                            "page {} item {:?} not strictly after prior page max {:?}",
                            page_idx,
                            curr_min,
                            prev_max
                        );
                    }
                    if let Some(curr_max) = page_items.last() {
                        prev_page_max = Some(curr_max.clone());
                    }

                    if page.is_truncated {
                        prop_assert_eq!(
                            item_count,
                            max_keys,
                            "delim truncated page {} returned {} != max-keys {}",
                            page_idx,
                            item_count,
                            max_keys
                        );
                        if is_v2 {
                            cursor = Some(
                                page.next_continuation_token
                                    .ok_or_else(|| TestCaseError::fail("V2 delim truncated without token"))?,
                            );
                        } else {
                            let raw_nm = page
                                .next_marker
                                .ok_or_else(|| TestCaseError::fail("V1 delim truncated without NextMarker"))?;
                            let nm = decode_listed_item(&raw_nm, url_encode).map_err(TestCaseError::fail)?;
                            cursor = Some(nm);
                        }
                    } else {
                        prop_assert!(page.next_continuation_token.is_none());
                        prop_assert!(page.next_marker.is_none());
                        break;
                    }
                    if page_idx + 1 == max_pages {
                        return Err(TestCaseError::fail(format!(
                            "delim listing exceeded {} pages",
                            max_pages
                        )));
                    }
                }

                assert_strict_byte_order(&all_keys, "delimiter keys")?;
                assert_strict_byte_order(&all_cps, "delimiter common prefixes")?;
                let got_keys: BTreeSet<String> = all_keys.into_iter().collect();
                let got_cps: BTreeSet<String> = all_cps.into_iter().collect();
                prop_assert_eq!(got_keys, expected_keys, "delimiter keys union mismatch");
                prop_assert_eq!(got_cps, expected_cps, "delimiter common prefixes union mismatch");
                Ok(())
            })
        },
    );
}

#[test]
fn prop_list_objects_thousand_batch_boundary() {
    let srv = TestServer::new();
    let strat = (
        1000usize..=1015,
        prop::sample::select(vec![333usize, 999, 1000]),
        any::<bool>(), // v2 vs v1
    );

    run_prop(
        "list_objects_thousand_batch_boundary",
        BOUNDARY_CASE_COUNT,
        strat,
        |(count, max_keys, is_v2)| {
            let bucket = srv.fresh_bucket_name("k1000");
            let mut keys = BTreeSet::new();
            for i in 0..count {
                let k = match i % 5 {
                    0 => format!("k/{:04} sp+%", i),
                    1 => format!("k/{:04}/café", i),
                    2 => format!("k/{:04}+🦀", i),
                    3 => format!("k/{:04}%20", i),
                    _ => format!("k/{:04}", i),
                };
                keys.insert(k);
            }

            srv.rt.block_on(async {
                put_bucket_and_keys(&srv.client, &srv.url, &bucket, &keys)
                    .await
                    .map_err(TestCaseError::fail)?;

                let mut all_decoded = Vec::new();
                let mut seen = HashSet::new();
                let mut cursor: Option<String> = None;
                let max_pages = (count / max_keys) + 4;

                for page_idx in 0..max_pages {
                    let mut qs = if is_v2 {
                        format!("list-type=2&max-keys={}&encoding-type=url", max_keys)
                    } else {
                        format!("max-keys={}&encoding-type=url", max_keys)
                    };
                    if let Some(ref cur) = cursor {
                        if is_v2 {
                            qs.push_str(&format!("&continuation-token={}", encode_query_value(cur)));
                        } else {
                            qs.push_str(&format!("&marker={}", encode_query_value(cur)));
                        }
                    }
                    let resp = srv
                        .client
                        .get(format!("{}/{}?{}", srv.url, bucket, qs))
                        .send()
                        .await
                        .map_err(|e| TestCaseError::fail(format!("GET ?{} failed: {}", qs, e)))?;
                    prop_assert_eq!(resp.status(), StatusCode::OK);
                    let xml = resp.text().await.map_err(|e| TestCaseError::fail(format!("body: {}", e)))?;
                    let page = parse_list_objects_xml(&xml).map_err(TestCaseError::fail)?;

                    for raw_k in &page.keys {
                        let dk = decode_listed_item(raw_k, true).map_err(TestCaseError::fail)?;
                        prop_assert!(seen.insert(dk.clone()), "duplicate key {:?}", dk);
                        all_decoded.push(dk);
                    }

                    if page.is_truncated {
                        prop_assert_eq!(page.keys.len(), max_keys);
                        cursor = if is_v2 {
                            page.next_continuation_token
                        } else {
                            let raw_nm = page
                                .next_marker
                                .ok_or_else(|| TestCaseError::fail("truncated V1 without NextMarker"))?;
                            Some(decode_listed_item(&raw_nm, true).map_err(TestCaseError::fail)?)
                        };
                    } else {
                        prop_assert!(page.next_continuation_token.is_none());
                        prop_assert!(page.next_marker.is_none());
                        break;
                    }
                    if page_idx + 1 == max_pages {
                        return Err(TestCaseError::fail(format!(
                            "1000-batch listing exceeded {} pages",
                            max_pages
                        )));
                    }
                }

                assert_strict_byte_order(&all_decoded, "1000-batch boundary")?;
                let returned_set: BTreeSet<String> = all_decoded.into_iter().collect();
                prop_assert_eq!(returned_set, keys, "1000-batch union mismatch");
                Ok(())
            })
        },
    );
}
