//! #26: the metadata segments answer exactly as the in-memory index does, across PUT, DELETE,
//! GET, listing, restart and the segments' flushes and compactions.
//!
//! Two servers get the same seeded operations over HTTP (3 rounds of 2,500 over 3,000 keys:
//! the smallest spill memtable, 64 KiB, holds about 530 entries, so A flushes in every round).
//! Server A keeps 40 objects in its
//! in-memory table and gives the spill index its smallest budget, so almost every key lives in
//! segments and a few thousand PUTs flush and compact them. Server B keeps every object in
//! memory. Every status, body, ETag and listing page must be the same; after each round both are
//! stopped and opened again from their data directories (the spill index is rebuilt from the
//! volume replay, as at every boot).

use reqwest::StatusCode;
use s3_server::manifest::read_manifest;
use s3_server::sim::SimRng;
use s3_server::vfile::DiskFs;
use s3_server::clock::{Clock, ManualClock, SystemClock};
use s3_server::{start_s3_reactor, OpenLimits, S3ServiceState, ServerHandle};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const BUCKET: &str = "tiered-bucket";
const KEYS: u64 = 3_000;
const ROUNDS: usize = 3;
const OPS_PER_ROUND: usize = 2_500;

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!("awanfs_tiered_{}_{}_{}", name, std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

struct Server {
    dir: PathBuf,
    limits: OpenLimits,
    clock: Arc<dyn Clock>,
    url: String,
    state: Arc<S3ServiceState>,
    handle: ServerHandle,
}

impl Server {
    async fn start(dir: &Path, limits: OpenLimits) -> Server {
        Self::start_with_clock(dir, limits, Arc::new(SystemClock)).await
    }

    async fn start_with_clock(dir: &Path, limits: OpenLimits, clock: Arc<dyn Clock>) -> Server {
        let state = Arc::new(S3ServiceState::try_with_data_dir_limits(dir, Arc::clone(&clock), limits).expect("open data dir"));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
        Server { dir: dir.to_path_buf(), limits, clock, url: format!("http://{}", handle.local_addr), state, handle }
    }

    /// Stops the server and waits until nothing else holds its state, so every volume and
    /// segment is closed before the directory is opened again (Windows refuses to delete an open
    /// file, and boot clears the segments).
    async fn stop(self) -> (PathBuf, OpenLimits) {
        let Server { dir, limits, state, handle, .. } = self;
        assert!(handle.shutdown_gracefully(Duration::from_secs(10)).await, "connections did not close");
        for _ in 0..500 {
            if Arc::strong_count(&state) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(Arc::strong_count(&state), 1, "the stopped server still holds its state");
        drop(state);
        (dir, limits)
    }

    async fn restart(self) -> Server {
        let clock = Arc::clone(&self.clock);
        let (dir, limits) = self.stop().await;
        Server::start_with_clock(&dir, limits, clock).await
    }
}

fn key(i: u64) -> String {
    format!("{}/{:05}", ["a", "b", "c"][(i % 3) as usize], i)
}

fn tag(xml: &str, name: &str) -> Vec<String> {
    let (open, close) = (format!("<{}>", name), format!("</{}>", name));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let j = after.find(&close).expect("closing tag");
        out.push(after[..j].to_string());
        rest = &after[j + close.len()..];
    }
    out
}

type Page = (StatusCode, Vec<String>, Vec<String>, Vec<String>, Vec<String>, Option<String>);

/// One page: (status, keys, sizes, etags, prefixes (the request's and the common ones), next
/// token).
async fn list_page(c: &reqwest::Client, url: &str, prefix: &str, delimiter: Option<&str>, max_keys: u64, token: Option<&str>) -> Page {
    let mut q = vec![("list-type", "2".to_string()), ("prefix", prefix.to_string()), ("max-keys", max_keys.to_string())];
    if let Some(d) = delimiter {
        q.push(("delimiter", d.to_string()));
    }
    if let Some(t) = token {
        q.push(("continuation-token", t.to_string()));
    }
    let res = c.get(format!("{}/{}", url, BUCKET)).query(&q).send().await.unwrap();
    let status = res.status();
    let body = res.text().await.unwrap();
    let truncated = tag(&body, "IsTruncated").first().map(|s| s == "true").unwrap_or(false);
    let next = if truncated { tag(&body, "NextContinuationToken").into_iter().next() } else { None };
    (status, tag(&body, "Key"), tag(&body, "Size"), tag(&body, "ETag"), tag(&body, "Prefix"), next)
}

/// Every key under the prefix, through every page.
async fn list_all(c: &reqwest::Client, url: &str, prefix: &str, max_keys: u64) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let (status, keys, sizes, etags, _, next) = list_page(c, url, prefix, None, max_keys, token.as_deref()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!((keys.len(), keys.len()), (sizes.len(), etags.len()));
        out.extend(keys.into_iter().zip(sizes).zip(etags).map(|((k, s), e)| (k, s, e)));
        match next {
            Some(t) => token = Some(t),
            None => return out,
        }
    }
}

/// (status, ETag, body). An error's body is its S3 error code: the rest of an error document
/// (the RequestId) differs between servers by design.
async fn get(c: &reqwest::Client, url: &str, k: &str) -> (StatusCode, Option<String>, Vec<u8>) {
    let res = c.get(format!("{}/{}/{}", url, BUCKET, k)).send().await.unwrap();
    let status = res.status();
    let etag = res.headers().get("etag").map(|v| v.to_str().unwrap().to_string());
    let body = res.bytes().await.unwrap().to_vec();
    if status.is_success() {
        return (status, etag, body);
    }
    let code = tag(&String::from_utf8_lossy(&body), "Code").into_iter().next().unwrap_or_default();
    (status, etag, code.into_bytes())
}

/// Segments at each level in a stopped server's manifest.
fn levels(dir: &Path) -> Vec<usize> {
    let mut by_level = vec![0usize; 8];
    if let Some((_, m)) = read_manifest(&DiskFs, dir).expect("read manifest") {
        for s in &m.segments {
            by_level[s.level as usize] += 1;
        }
    }
    by_level
}

#[tokio::test]
async fn the_segments_answer_as_the_in_memory_index_across_restarts_and_compactions() {
    let c = reqwest::Client::new();
    let tiered = OpenLimits { max_objects: 40, spill_cache_bytes: Some(256 << 10) };
    let mut a = Server::start(&temp_dir("a"), tiered).await;
    let mut b = Server::start(&temp_dir("b"), OpenLimits::default()).await;
    for s in [&a, &b] {
        assert_eq!(c.put(format!("{}/{}", s.url, BUCKET)).send().await.unwrap().status(), StatusCode::OK);
    }
    let mut rng = SimRng::new(26);
    let mut deeper_levels_seen = false;
    for round in 0..ROUNDS {
        for op in 0..OPS_PER_ROUND {
            let k = key(rng.below(KEYS));
            let ctx = format!("round {} op {} key {}", round, op, k);
            match rng.below(20) {
                0..=10 => {
                    let n = rng.below(300) as usize;
                    let fill = rng.next_u64();
                    let body: Vec<u8> = (0..n).map(|i| (fill.rotate_left(i as u32 % 64) as u8) ^ i as u8).collect();
                    let mut res = Vec::new();
                    for s in [&a, &b] {
                        // Content-Length always: reqwest sends none for an empty body, and the
                        // server answers 411 MissingContentLength to that (#160).
                        let r = c.put(format!("{}/{}/{}", s.url, BUCKET, k)).header("content-length", body.len()).body(body.clone()).send().await.unwrap();
                        res.push((r.status(), r.headers().get("etag").map(|v| v.to_str().unwrap().to_string())));
                    }
                    assert_eq!(res[0], res[1], "PUT {}", ctx);
                    assert_eq!(res[0].0, StatusCode::OK, "PUT {}", ctx);
                }
                11..=14 => {
                    let mut st = Vec::new();
                    for s in [&a, &b] {
                        st.push(c.delete(format!("{}/{}/{}", s.url, BUCKET, k)).send().await.unwrap().status());
                    }
                    assert_eq!(st[0], st[1], "DELETE {}", ctx);
                }
                15..=17 => {
                    assert_eq!(get(&c, &a.url, &k).await, get(&c, &b.url, &k).await, "GET {}", ctx);
                }
                _ => {
                    let prefix = ["", "a/", "b/0", "c/00", "a/01", "zz"][rng.below(6) as usize];
                    let max_keys = [1, 7, 100, 1000][rng.below(4) as usize];
                    let delimiter = [None, Some("/"), Some("0")][rng.below(3) as usize];
                    // A page that starts after a key, as a continuation token would.
                    let token = (rng.below(2) == 0).then(|| key(rng.below(KEYS)).bytes().map(|b| format!("{:02x}", b)).collect::<String>());
                    let pa = list_page(&c, &a.url, prefix, delimiter, max_keys, token.as_deref()).await;
                    let pb = list_page(&c, &b.url, prefix, delimiter, max_keys, token.as_deref()).await;
                    assert_eq!(pa, pb, "LIST prefix {:?} delimiter {:?} max-keys {} token {:?} {}", prefix, delimiter, max_keys, token, ctx);
                }
            }
        }
        // Whole listings, page by page, and every key's GET.
        let la = list_all(&c, &a.url, "", 113).await;
        assert_eq!(la, list_all(&c, &b.url, "", 113).await, "full listing after round {}", round);
        assert!(la.len() > 400, "round {}: only {} keys live; the test should hold hundreds", round, la.len());
        for i in 0..KEYS {
            let k = key(i);
            assert_eq!(get(&c, &a.url, &k).await, get(&c, &b.url, &k).await, "GET after round {} key {}", round, k);
        }
        // A's segments, before the restart clears them: the index must have flushed and
        // compacted, or this test checks nothing about them.
        let (dir, limits) = a.stop().await;
        let lv = levels(&dir);
        assert!(lv.iter().sum::<usize>() > 0, "round {}: server A wrote no metadata segments", round);
        deeper_levels_seen |= lv[1..].iter().any(|&n| n > 0);
        a = Server::start(&dir, limits).await;
        b = b.restart().await;
        // After the restart, both rebuilt from their volumes, the answers are still the same.
        assert_eq!(list_all(&c, &a.url, "", 1000).await, la, "full listing after restart {}", round);
        assert_eq!(list_all(&c, &b.url, "", 1000).await, la, "full listing of B after restart {}", round);
    }
    assert!(deeper_levels_seen, "no compaction moved segments below level 0 in any round");
    let (da, _) = a.stop().await;
    let (db, _) = b.stop().await;
    let _ = std::fs::remove_dir_all(da);
    let _ = std::fs::remove_dir_all(db);
}

const VER_BUCKET: &str = "tiered-ver";
const MPU_BUCKET: &str = "tiered-mpu";
const VER_KEYS: u64 = 2_200;
const MPU_KEYS: u64 = 450;
const VM_OPS_PER_ROUND: usize = 2_000;

fn ver_key(i: u64) -> String {
    format!("{}/{:05}", ["a", "b", "c"][(i % 3) as usize], i)
}

fn mpu_key(i: u64) -> String {
    format!("m/{}/{:05}", ["x", "y", "z"][(i % 3) as usize], i)
}

fn first_tag(xml: &str, name: &str) -> Option<String> {
    tag(xml, name).into_iter().next()
}

fn header_str(res: &reqwest::Response, name: &str) -> Option<String> {
    res.headers().get(name).map(|v| v.to_str().unwrap().to_string())
}

fn seeded_bytes(rng: &mut SimRng, max_len: u64) -> Vec<u8> {
    let n = rng.below(max_len) as usize;
    let fill = rng.next_u64();
    (0..n).map(|i| (fill.rotate_left(i as u32 % 64) as u8) ^ i as u8).collect()
}

fn common_prefixes_xml(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(i) = rest.find("<CommonPrefixes>") {
        let after = &rest[i + "<CommonPrefixes>".len()..];
        let j = after.find("</CommonPrefixes>").expect("closing CommonPrefixes");
        out.extend(tag(&after[..j], "Prefix"));
        rest = &after[j + "</CommonPrefixes>".len()..];
    }
    out
}

#[derive(Debug, PartialEq, Eq)]
struct ObjectReply {
    status: StatusCode,
    etag: Option<String>,
    version_id: Option<String>,
    delete_marker: Option<String>,
    last_modified: Option<String>,
    parts_count: Option<String>,
    body_or_code: Vec<u8>,
}

async fn get_object_reply(c: &reqwest::Client, url: &str, bucket: &str, key: &str, query: &[(&str, &str)]) -> ObjectReply {
    let res = c.get(format!("{}/{}/{}", url, bucket, key)).query(query).send().await.unwrap();
    let status = res.status();
    let etag = header_str(&res, "etag");
    let version_id = header_str(&res, "x-amz-version-id");
    let delete_marker = header_str(&res, "x-amz-delete-marker");
    let last_modified = header_str(&res, "last-modified");
    let parts_count = header_str(&res, "x-amz-mp-parts-count");
    let body = res.bytes().await.unwrap().to_vec();
    let body_or_code = if status.is_success() {
        body
    } else {
        first_tag(&String::from_utf8_lossy(&body), "Code").unwrap_or_default().into_bytes()
    };
    ObjectReply { status, etag, version_id, delete_marker, last_modified, parts_count, body_or_code }
}

#[derive(Debug, PartialEq, Eq)]
struct PutReply {
    status: StatusCode,
    etag: Option<String>,
    version_id: Option<String>,
    code: Option<String>,
}

async fn put_object_reply(c: &reqwest::Client, url: &str, bucket: &str, key: &str, body: &[u8]) -> PutReply {
    let res = c
        .put(format!("{}/{}/{}", url, bucket, key))
        .header("content-length", body.len())
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    let status = res.status();
    let etag = header_str(&res, "etag");
    let version_id = header_str(&res, "x-amz-version-id");
    let text = res.text().await.unwrap();
    let code = (!status.is_success()).then(|| first_tag(&text, "Code").unwrap_or_default());
    PutReply { status, etag, version_id, code }
}

#[derive(Debug, PartialEq, Eq)]
struct DeleteReply {
    status: StatusCode,
    version_id: Option<String>,
    delete_marker: Option<String>,
    code: Option<String>,
}

async fn delete_object_reply(c: &reqwest::Client, url: &str, bucket: &str, key: &str, version_id: Option<&str>) -> DeleteReply {
    let mut req = c.delete(format!("{}/{}/{}", url, bucket, key));
    if let Some(vid) = version_id {
        req = req.query(&[("versionId", vid)]);
    }
    let res = req.send().await.unwrap();
    let status = res.status();
    let resp_vid = header_str(&res, "x-amz-version-id");
    let delete_marker = header_str(&res, "x-amz-delete-marker");
    let text = res.text().await.unwrap();
    let code = (!status.is_success()).then(|| first_tag(&text, "Code").unwrap_or_default());
    DeleteReply { status, version_id: resp_vid, delete_marker, code }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionEntry {
    kind: &'static str,
    key: String,
    version_id: String,
    is_latest: bool,
    last_modified: String,
    etag: Option<String>,
    size: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct VersionPage {
    status: StatusCode,
    error_code: Option<String>,
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_version_id_marker: Option<String>,
    entries: Vec<VersionEntry>,
    common_prefixes: Vec<String>,
}

fn parse_version_entries(xml: &str) -> Vec<VersionEntry> {
    let mut out = Vec::new();
    let mut rest = xml;
    loop {
        let v = rest.find("<Version>");
        let d = rest.find("<DeleteMarker>");
        let (kind, at, open_len, close) = match (v, d) {
            (Some(a), Some(b)) if a < b => ("Version", a, "<Version>".len(), "</Version>"),
            (Some(a), None) => ("Version", a, "<Version>".len(), "</Version>"),
            (_, Some(b)) => ("DeleteMarker", b, "<DeleteMarker>".len(), "</DeleteMarker>"),
            (None, None) => break,
        };
        let after = &rest[at + open_len..];
        let end = after.find(close).expect("closing version entry");
        let item = &after[..end];
        out.push(VersionEntry {
            kind,
            key: first_tag(item, "Key").unwrap_or_default(),
            version_id: first_tag(item, "VersionId").unwrap_or_default(),
            is_latest: first_tag(item, "IsLatest").as_deref() == Some("true"),
            last_modified: first_tag(item, "LastModified").unwrap_or_default(),
            etag: first_tag(item, "ETag"),
            size: first_tag(item, "Size"),
        });
        rest = &after[end + close.len()..];
    }
    out
}

async fn list_versions_page(
    c: &reqwest::Client,
    url: &str,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: u64,
    key_marker: Option<&str>,
    version_id_marker: Option<&str>,
    url_encode: bool,
) -> VersionPage {
    let mut q = vec![("prefix", prefix.to_string()), ("max-keys", max_keys.to_string())];
    if let Some(d) = delimiter {
        q.push(("delimiter", d.to_string()));
    }
    if let Some(km) = key_marker {
        q.push(("key-marker", km.to_string()));
    }
    if let Some(vm) = version_id_marker {
        q.push(("version-id-marker", vm.to_string()));
    }
    if url_encode {
        q.push(("encoding-type", "url".to_string()));
    }
    let res = c.get(format!("{}/{}?versions", url, bucket)).query(&q).send().await.unwrap();
    let status = res.status();
    let body = res.text().await.unwrap();
    if !status.is_success() {
        return VersionPage {
            status,
            error_code: first_tag(&body, "Code"),
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            entries: Vec::new(),
            common_prefixes: Vec::new(),
        };
    }
    let is_truncated = first_tag(&body, "IsTruncated").as_deref() == Some("true");
    VersionPage {
        status,
        error_code: None,
        is_truncated,
        next_key_marker: is_truncated.then(|| first_tag(&body, "NextKeyMarker")).flatten(),
        next_version_id_marker: is_truncated.then(|| first_tag(&body, "NextVersionIdMarker")).flatten(),
        entries: parse_version_entries(&body),
        common_prefixes: common_prefixes_xml(&body),
    }
}

async fn list_all_versions(c: &reqwest::Client, url: &str, bucket: &str, max_keys: u64) -> Vec<VersionPage> {
    let mut pages = Vec::new();
    let mut km: Option<String> = None;
    let mut vm: Option<String> = None;
    loop {
        let page = list_versions_page(c, url, bucket, "", None, max_keys, km.as_deref(), vm.as_deref(), false).await;
        assert_eq!(page.status, StatusCode::OK);
        let next = if page.is_truncated {
            let nkm = page.next_key_marker.clone().expect("truncated page has NextKeyMarker");
            let nvm = page.next_version_id_marker.clone();
            Some((nkm, nvm))
        } else {
            None
        };
        pages.push(page);
        match next {
            Some((nkm, nvm)) => {
                km = Some(nkm);
                vm = nvm;
            }
            None => return pages,
        }
    }
}

async fn list_v2_page_in(
    c: &reqwest::Client,
    url: &str,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: u64,
    token: Option<&str>,
) -> Page {
    let mut q = vec![("list-type", "2".to_string()), ("prefix", prefix.to_string()), ("max-keys", max_keys.to_string())];
    if let Some(d) = delimiter {
        q.push(("delimiter", d.to_string()));
    }
    if let Some(t) = token {
        q.push(("continuation-token", t.to_string()));
    }
    let res = c.get(format!("{}/{}", url, bucket)).query(&q).send().await.unwrap();
    let status = res.status();
    let body = res.text().await.unwrap();
    let truncated = first_tag(&body, "IsTruncated").as_deref() == Some("true");
    let next = if truncated { first_tag(&body, "NextContinuationToken") } else { None };
    (status, tag(&body, "Key"), tag(&body, "Size"), tag(&body, "ETag"), common_prefixes_xml(&body), next)
}

async fn list_v2_all_in(c: &reqwest::Client, url: &str, bucket: &str, max_keys: u64) -> Vec<Page> {
    let mut pages = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = list_v2_page_in(c, url, bucket, "", None, max_keys, token.as_deref()).await;
        assert_eq!(page.0, StatusCode::OK);
        let next = page.5.clone();
        pages.push(page);
        match next {
            Some(t) => token = Some(t),
            None => return pages,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct PartsPage {
    status: StatusCode,
    error_code: Option<String>,
    is_truncated: bool,
    next_part_number_marker: Option<String>,
    parts: Vec<(String, String, String, String)>,
}

#[derive(Debug, PartialEq, Eq)]
struct UploadsPage {
    status: StatusCode,
    error_code: Option<String>,
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
    uploads: Vec<(String, String, String)>,
    common_prefixes: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct CompleteReply {
    status: StatusCode,
    etag: Option<String>,
    version_id: Option<String>,
    xml_etag: Option<String>,
    error_code: Option<String>,
}

async fn create_multipart(c: &reqwest::Client, url: &str, bucket: &str, key: &str) -> (StatusCode, Option<String>) {
    let res = c.post(format!("{}/{}/{}?uploads", url, bucket, key)).send().await.unwrap();
    let status = res.status();
    let body = res.text().await.unwrap();
    let val = if status.is_success() { first_tag(&body, "UploadId") } else { first_tag(&body, "Code") };
    (status, val)
}

async fn upload_part_reply(
    c: &reqwest::Client,
    url: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_num: u32,
    body: &[u8],
) -> (StatusCode, Option<String>) {
    let res = c
        .put(format!("{}/{}/{}", url, bucket, key))
        .query(&[("partNumber", part_num.to_string()), ("uploadId", upload_id.to_string())])
        .header("content-length", body.len())
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    let status = res.status();
    let etag = header_str(&res, "etag");
    let text = res.text().await.unwrap();
    let val = if status.is_success() { etag } else { first_tag(&text, "Code") };
    (status, val)
}

async fn list_parts_page(
    c: &reqwest::Client,
    url: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    max_parts: Option<u64>,
    marker: Option<u32>,
) -> PartsPage {
    let mut q = vec![("uploadId", upload_id.to_string())];
    if let Some(m) = max_parts {
        q.push(("max-parts", m.to_string()));
    }
    if let Some(mk) = marker {
        q.push(("part-number-marker", mk.to_string()));
    }
    let res = c.get(format!("{}/{}/{}", url, bucket, key)).query(&q).send().await.unwrap();
    let status = res.status();
    let body = res.text().await.unwrap();
    if !status.is_success() {
        return PartsPage {
            status,
            error_code: first_tag(&body, "Code"),
            is_truncated: false,
            next_part_number_marker: None,
            parts: Vec::new(),
        };
    }
    let mut parts = Vec::new();
    let mut rest = body.as_str();
    while let Some(i) = rest.find("<Part>") {
        let after = &rest[i + "<Part>".len()..];
        let j = after.find("</Part>").expect("closing Part");
        let block = &after[..j];
        parts.push((
            first_tag(block, "PartNumber").unwrap_or_default(),
            first_tag(block, "LastModified").unwrap_or_default(),
            first_tag(block, "ETag").unwrap_or_default(),
            first_tag(block, "Size").unwrap_or_default(),
        ));
        rest = &after[j + "</Part>".len()..];
    }
    PartsPage {
        status,
        error_code: None,
        is_truncated: first_tag(&body, "IsTruncated").as_deref() == Some("true"),
        next_part_number_marker: first_tag(&body, "NextPartNumberMarker"),
        parts,
    }
}

async fn list_uploads_page(
    c: &reqwest::Client,
    url: &str,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_uploads: Option<u64>,
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
) -> UploadsPage {
    let mut q = vec![("prefix", prefix.to_string())];
    if let Some(d) = delimiter {
        q.push(("delimiter", d.to_string()));
    }
    if let Some(m) = max_uploads {
        q.push(("max-uploads", m.to_string()));
    }
    if let Some(km) = key_marker {
        q.push(("key-marker", km.to_string()));
    }
    if let Some(um) = upload_id_marker {
        q.push(("upload-id-marker", um.to_string()));
    }
    let res = c.get(format!("{}/{}?uploads", url, bucket)).query(&q).send().await.unwrap();
    let status = res.status();
    let body = res.text().await.unwrap();
    if !status.is_success() {
        return UploadsPage {
            status,
            error_code: first_tag(&body, "Code"),
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
            uploads: Vec::new(),
            common_prefixes: Vec::new(),
        };
    }
    let mut uploads = Vec::new();
    let mut rest = body.as_str();
    while let Some(i) = rest.find("<Upload>") {
        let after = &rest[i + "<Upload>".len()..];
        let j = after.find("</Upload>").expect("closing Upload");
        let block = &after[..j];
        uploads.push((
            first_tag(block, "Key").unwrap_or_default(),
            first_tag(block, "UploadId").unwrap_or_default(),
            first_tag(block, "Initiated").unwrap_or_default(),
        ));
        rest = &after[j + "</Upload>".len()..];
    }
    UploadsPage {
        status,
        error_code: None,
        is_truncated: first_tag(&body, "IsTruncated").as_deref() == Some("true"),
        next_key_marker: first_tag(&body, "NextKeyMarker"),
        next_upload_id_marker: first_tag(&body, "NextUploadIdMarker"),
        uploads,
        common_prefixes: common_prefixes_xml(&body),
    }
}

async fn complete_multipart(
    c: &reqwest::Client,
    url: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[(u32, String)],
) -> CompleteReply {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (n, etag) in parts {
        xml.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>", n, etag));
    }
    xml.push_str("</CompleteMultipartUpload>");
    let res = c
        .post(format!("{}/{}/{}", url, bucket, key))
        .query(&[("uploadId", upload_id)])
        .body(xml)
        .send()
        .await
        .unwrap();
    let status = res.status();
    let etag = header_str(&res, "etag");
    let version_id = header_str(&res, "x-amz-version-id");
    let text = res.text().await.unwrap();
    CompleteReply {
        status,
        etag,
        version_id,
        xml_etag: status.is_success().then(|| first_tag(&text, "ETag")).flatten(),
        error_code: (!status.is_success()).then(|| first_tag(&text, "Code")).flatten(),
    }
}

async fn abort_multipart(c: &reqwest::Client, url: &str, bucket: &str, key: &str, upload_id: &str) -> (StatusCode, Option<String>) {
    let res = c
        .delete(format!("{}/{}/{}", url, bucket, key))
        .query(&[("uploadId", upload_id)])
        .send()
        .await
        .unwrap();
    let status = res.status();
    let text = res.text().await.unwrap();
    let code = (!status.is_success()).then(|| first_tag(&text, "Code")).flatten();
    (status, code)
}

async fn set_bucket_versioning(c: &reqwest::Client, a: &Server, b: &Server, bucket: &str, status_str: &str) {
    let xml = format!("<VersioningConfiguration><Status>{}</Status></VersioningConfiguration>", status_str);
    for s in [a, b] {
        let res = c.put(format!("{}/{}?versioning", s.url, bucket)).body(xml.clone()).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "PUT ?versioning {}", status_str);
    }
    let ga = c.get(format!("{}/{}?versioning", a.url, bucket)).send().await.unwrap().text().await.unwrap();
    let gb = c.get(format!("{}/{}?versioning", b.url, bucket)).send().await.unwrap().text().await.unwrap();
    assert_eq!(ga, gb, "GET ?versioning mismatch");
}

async fn run_multipart_upload(
    c: &reqwest::Client,
    a: &Server,
    b: &Server,
    clock: &ManualClock,
    rng: &mut SimRng,
    bucket: &str,
    key: &str,
    mode: u64,
    ctx: &str,
) -> (Option<String>, Option<String>) {
    let ca = create_multipart(c, &a.url, bucket, key).await;
    let cb = create_multipart(c, &b.url, bucket, key).await;
    assert_eq!(ca, cb, "CreateMultipartUpload {}", ctx);
    assert_eq!(ca.0, StatusCode::OK, "CreateMultipartUpload {}", ctx);
    let upload_id = ca.1.unwrap();
    clock.advance(1_000_000);

    let num_parts = 2 + (rng.below(2) as u32);
    let mut parts = Vec::new();
    for pn in 1..=num_parts {
        let data = seeded_bytes(rng, 2_048);
        let ua = upload_part_reply(c, &a.url, bucket, key, &upload_id, pn, &data).await;
        let ub = upload_part_reply(c, &b.url, bucket, key, &upload_id, pn, &data).await;
        assert_eq!(ua, ub, "UploadPart {} part {}", ctx, pn);
        assert_eq!(ua.0, StatusCode::OK, "UploadPart {} part {}", ctx, pn);
        parts.push((pn, ua.1.unwrap()));
        clock.advance(1_000_000);
    }

    if rng.below(3) == 0 {
        let data = seeded_bytes(rng, 1_536);
        let ua = upload_part_reply(c, &a.url, bucket, key, &upload_id, 1, &data).await;
        let ub = upload_part_reply(c, &b.url, bucket, key, &upload_id, 1, &data).await;
        assert_eq!(ua, ub, "UploadPart overwrite {} part 1", ctx);
        parts[0].1 = ua.1.unwrap();
        clock.advance(1_000_000);
    }

    assert_eq!(
        list_parts_page(c, &a.url, bucket, key, &upload_id, Some(1), None).await,
        list_parts_page(c, &b.url, bucket, key, &upload_id, Some(1), None).await,
        "ListParts page 1 {}",
        ctx
    );
    assert_eq!(
        list_parts_page(c, &a.url, bucket, key, &upload_id, Some(2), Some(1)).await,
        list_parts_page(c, &b.url, bucket, key, &upload_id, Some(2), Some(1)).await,
        "ListParts page 2 {}",
        ctx
    );
    assert_eq!(
        list_uploads_page(c, &a.url, bucket, "", Some("/"), Some(5), None, None).await,
        list_uploads_page(c, &b.url, bucket, "", Some("/"), Some(5), None, None).await,
        "ListMultipartUploads {}",
        ctx
    );

    if mode == 0 {
        let aa = abort_multipart(c, &a.url, bucket, key, &upload_id).await;
        let ab = abort_multipart(c, &b.url, bucket, key, &upload_id).await;
        assert_eq!(aa, ab, "AbortMultipartUpload {}", ctx);
        assert_eq!(aa.0, StatusCode::NO_CONTENT, "AbortMultipartUpload {}", ctx);
        assert_eq!(
            list_parts_page(c, &a.url, bucket, key, &upload_id, None, None).await,
            list_parts_page(c, &b.url, bucket, key, &upload_id, None, None).await,
            "ListParts after abort {}",
            ctx
        );
        clock.advance(1_000_000);
        return (None, None);
    }

    if mode == 1 {
        return (None, Some(upload_id));
    }

    let comp_a = complete_multipart(c, &a.url, bucket, key, &upload_id, &parts).await;
    let comp_b = complete_multipart(c, &b.url, bucket, key, &upload_id, &parts).await;
    assert_eq!(comp_a, comp_b, "CompleteMultipartUpload {}", ctx);
    assert_eq!(comp_a.status, StatusCode::OK, "CompleteMultipartUpload {}", ctx);
    if rng.below(4) == 0 {
        assert_eq!(
            complete_multipart(c, &a.url, bucket, key, &upload_id, &parts).await,
            complete_multipart(c, &b.url, bucket, key, &upload_id, &parts).await,
            "CompleteMultipartUpload retry {}",
            ctx
        );
    }
    clock.advance(1_000_000);
    (comp_a.version_id, None)
}

#[tokio::test]
async fn versioned_buckets_and_multipart_match_across_restarts_and_compactions() {
    let c = reqwest::Client::new();
    let tiered = OpenLimits { max_objects: 40, spill_cache_bytes: Some(256 << 10) };
    let clock = Arc::new(ManualClock::new(1_795_000_000_000_000_000));
    let mut a = Server::start_with_clock(&temp_dir("ver_a"), tiered, Arc::clone(&clock) as Arc<dyn Clock>).await;
    let mut b = Server::start_with_clock(&temp_dir("ver_b"), OpenLimits::default(), Arc::clone(&clock) as Arc<dyn Clock>).await;

    for s in [&a, &b] {
        for bkt in [VER_BUCKET, MPU_BUCKET] {
            assert_eq!(c.put(format!("{}/{}", s.url, bkt)).send().await.unwrap().status(), StatusCode::OK);
        }
    }

    let mut rng = SimRng::new(332);
    let mut ver_history: Vec<(String, String)> = Vec::new();
    let mut delete_markers: Vec<(String, String)> = Vec::new();
    let mut deleted_vids: Vec<(String, String)> = Vec::new();
    let mut mp_ver_history: Vec<(String, String)> = Vec::new();
    let mut mpu_completed: BTreeSet<String> = BTreeSet::new();

    // Seed pre-versioning keys past the 40-row limit before enabling versioning so null versions
    // live in both the in-memory table and the spill segments.
    for i in 0..45u64 {
        let vk = ver_key(i);
        if i < 40 {
            let body = seeded_bytes(&mut rng, 250);
            let pa = put_object_reply(&c, &a.url, VER_BUCKET, &vk, &body).await;
            let pb = put_object_reply(&c, &b.url, VER_BUCKET, &vk, &body).await;
            assert_eq!(pa, pb, "pre-versioning PUT {}", vk);
            ver_history.push((vk, "null".to_string()));
            clock.advance(1_000_000);
        } else {
            run_multipart_upload(&c, &a, &b, &clock, &mut rng, VER_BUCKET, &vk, 2, &format!("seed ver mpu {}", vk)).await;
            ver_history.push((vk.clone(), "null".to_string()));
            mp_ver_history.push((vk, "null".to_string()));
        }

        let mk = mpu_key(i);
        if i < 35 {
            let body = seeded_bytes(&mut rng, 250);
            let pa = put_object_reply(&c, &a.url, MPU_BUCKET, &mk, &body).await;
            let pb = put_object_reply(&c, &b.url, MPU_BUCKET, &mk, &body).await;
            assert_eq!(pa, pb, "seed mpu PUT {}", mk);
            clock.advance(1_000_000);
        } else {
            run_multipart_upload(&c, &a, &b, &clock, &mut rng, MPU_BUCKET, &mk, 2, &format!("seed mpu {}", mk)).await;
            mpu_completed.insert(mk);
        }
    }

    set_bucket_versioning(&c, &a, &b, VER_BUCKET, "Enabled").await;

    let mut deeper_levels_seen = false;
    for round in 0..ROUNDS {
        if round == 2 {
            set_bucket_versioning(&c, &a, &b, VER_BUCKET, "Enabled").await;
        }
        let mut suspended = false;
        let mut pending_uploads: Vec<(String, String, String)> = Vec::new();

        for op in 0..VM_OPS_PER_ROUND {
            if round == 1 && op == VM_OPS_PER_ROUND / 2 {
                set_bucket_versioning(&c, &a, &b, VER_BUCKET, "Suspended").await;
                suspended = true;
            }
            let ctx = format!("round {} op {}", round, op);
            match rng.below(100) {
                0..=39 => {
                    let k = ver_key(rng.below(VER_KEYS));
                    let body = seeded_bytes(&mut rng, 350);
                    let pa = put_object_reply(&c, &a.url, VER_BUCKET, &k, &body).await;
                    let pb = put_object_reply(&c, &b.url, VER_BUCKET, &k, &body).await;
                    assert_eq!(pa, pb, "PUT {} {}", ctx, k);
                    assert_eq!(pa.status, StatusCode::OK, "PUT {} {}", ctx, k);
                    let vid = pa.version_id.unwrap_or_else(|| "null".to_string());
                    if suspended {
                        assert!(pa.etag.is_some());
                    }
                    ver_history.push((k, vid));
                    clock.advance(1_000_000);
                }
                40..=54 => {
                    let k = ver_key(rng.below(VER_KEYS));
                    let da = delete_object_reply(&c, &a.url, VER_BUCKET, &k, None).await;
                    let db = delete_object_reply(&c, &b.url, VER_BUCKET, &k, None).await;
                    assert_eq!(da, db, "DELETE marker {} {}", ctx, k);
                    assert_eq!(da.status, StatusCode::NO_CONTENT, "DELETE marker {} {}", ctx, k);
                    if let Some(vid) = da.version_id {
                        ver_history.push((k.clone(), vid.clone()));
                        delete_markers.push((k, vid));
                    }
                    clock.advance(1_000_000);
                }
                55..=63 => {
                    if !ver_history.is_empty() && rng.below(10) < 7 {
                        let idx = rng.below(ver_history.len() as u64) as usize;
                        let (k, vid) = ver_history.swap_remove(idx);
                        let da = delete_object_reply(&c, &a.url, VER_BUCKET, &k, Some(&vid)).await;
                        let db = delete_object_reply(&c, &b.url, VER_BUCKET, &k, Some(&vid)).await;
                        assert_eq!(da, db, "DELETE ?versionId {} {} {}", ctx, k, vid);
                        deleted_vids.push((k, vid));
                    } else {
                        let k = ver_key(rng.below(VER_KEYS));
                        let vid = if !deleted_vids.is_empty() && rng.below(2) == 0 {
                            deleted_vids[rng.below(deleted_vids.len() as u64) as usize].1.clone()
                        } else if rng.below(2) == 0 {
                            "00000000000000ff".to_string()
                        } else {
                            "bad-version-id".to_string()
                        };
                        let da = delete_object_reply(&c, &a.url, VER_BUCKET, &k, Some(&vid)).await;
                        let db = delete_object_reply(&c, &b.url, VER_BUCKET, &k, Some(&vid)).await;
                        assert_eq!(da, db, "DELETE stale/bad ?versionId {} {} {}", ctx, k, vid);
                    }
                    clock.advance(1_000_000);
                }
                64..=77 => {
                    let k = mpu_key(rng.below(MPU_KEYS));
                    if rng.below(3) < 2 {
                        let body = seeded_bytes(&mut rng, 350);
                        let pa = put_object_reply(&c, &a.url, MPU_BUCKET, &k, &body).await;
                        let pb = put_object_reply(&c, &b.url, MPU_BUCKET, &k, &body).await;
                        assert_eq!(pa, pb, "mpu-bucket PUT {} {}", ctx, k);
                    } else {
                        let da = delete_object_reply(&c, &a.url, MPU_BUCKET, &k, None).await;
                        let db = delete_object_reply(&c, &b.url, MPU_BUCKET, &k, None).await;
                        assert_eq!(da, db, "mpu-bucket DELETE {} {}", ctx, k);
                    }
                    clock.advance(1_000_000);
                }
                78..=85 => {
                    match rng.below(4) {
                        0 if !ver_history.is_empty() => {
                            let (k, vid) = &ver_history[rng.below(ver_history.len() as u64) as usize];
                            let q = [("versionId", vid.as_str())];
                            assert_eq!(
                                get_object_reply(&c, &a.url, VER_BUCKET, k, &q).await,
                                get_object_reply(&c, &b.url, VER_BUCKET, k, &q).await,
                                "GET ?versionId {} {} {}",
                                ctx,
                                k,
                                vid
                            );
                        }
                        1 if !delete_markers.is_empty() => {
                            let (k, vid) = &delete_markers[rng.below(delete_markers.len() as u64) as usize];
                            let q = [("versionId", vid.as_str())];
                            assert_eq!(
                                get_object_reply(&c, &a.url, VER_BUCKET, k, &q).await,
                                get_object_reply(&c, &b.url, VER_BUCKET, k, &q).await,
                                "GET delete marker {} {} {}",
                                ctx,
                                k,
                                vid
                            );
                        }
                        2 if !mpu_completed.is_empty() => {
                            let idx = rng.below(mpu_completed.len() as u64) as usize;
                            let k = mpu_completed.iter().nth(idx).unwrap();
                            let pn = if rng.below(4) == 0 { "99" } else { "1" };
                            let q = [("partNumber", pn)];
                            assert_eq!(
                                get_object_reply(&c, &a.url, MPU_BUCKET, k, &q).await,
                                get_object_reply(&c, &b.url, MPU_BUCKET, k, &q).await,
                                "GET ?partNumber={} {} {}",
                                pn,
                                ctx,
                                k
                            );
                        }
                        _ => {
                            let k = ver_key(rng.below(VER_KEYS));
                            assert_eq!(
                                get_object_reply(&c, &a.url, VER_BUCKET, &k, &[]).await,
                                get_object_reply(&c, &b.url, VER_BUCKET, &k, &[]).await,
                                "GET {} {}",
                                ctx,
                                k
                            );
                        }
                    }
                }
                86..=92 => {
                    let bkt = if rng.below(4) == 0 { MPU_BUCKET } else { VER_BUCKET };
                    let prefix = if bkt == VER_BUCKET {
                        ["", "a/", "b/0", "c/00", "zz"][rng.below(5) as usize]
                    } else {
                        ["", "m/", "m/x/", "m/y/0", "zz"][rng.below(5) as usize]
                    };
                    let max_keys = [1, 3, 7, 100, 1000][rng.below(5) as usize];
                    let delimiter = [None, Some("/"), Some("0")][rng.below(3) as usize];
                    let url_enc = rng.below(4) == 0;
                    let (km, vm) = if rng.below(5) == 0 {
                        (None, Some("0000000000000001".to_string()))
                    } else if rng.below(2) == 0 && !ver_history.is_empty() && bkt == VER_BUCKET {
                        let (k, v) = &ver_history[rng.below(ver_history.len() as u64) as usize];
                        (Some(k.clone()), (rng.below(2) == 0).then(|| v.clone()))
                    } else if rng.below(2) == 0 {
                        let k = if bkt == VER_BUCKET { ver_key(rng.below(VER_KEYS)) } else { mpu_key(rng.below(MPU_KEYS)) };
                        (Some(k), None)
                    } else {
                        (None, None)
                    };
                    let pa = list_versions_page(&c, &a.url, bkt, prefix, delimiter, max_keys, km.as_deref(), vm.as_deref(), url_enc).await;
                    let pb = list_versions_page(&c, &b.url, bkt, prefix, delimiter, max_keys, km.as_deref(), vm.as_deref(), url_enc).await;
                    assert_eq!(pa, pb, "ListObjectVersions {} bkt={} prefix={:?} km={:?} vm={:?}", ctx, bkt, prefix, km, vm);
                }
                93..=97 => {
                    let bkt = if rng.below(3) == 0 { MPU_BUCKET } else { VER_BUCKET };
                    let prefix = if bkt == VER_BUCKET {
                        ["", "a/", "b/0", "zz"][rng.below(4) as usize]
                    } else {
                        ["", "m/", "m/x/", "zz"][rng.below(4) as usize]
                    };
                    let max_keys = [1, 7, 100][rng.below(3) as usize];
                    let delimiter = [None, Some("/")][rng.below(2) as usize];
                    let pa = list_v2_page_in(&c, &a.url, bkt, prefix, delimiter, max_keys, None).await;
                    let pb = list_v2_page_in(&c, &b.url, bkt, prefix, delimiter, max_keys, None).await;
                    assert_eq!(pa, pb, "ListObjectsV2 {} bkt={}", ctx, bkt);
                }
                _ => {
                    let is_ver = rng.below(2) == 0;
                    let (bkt, k) = if is_ver {
                        (VER_BUCKET, ver_key(rng.below(VER_KEYS)))
                    } else {
                        (MPU_BUCKET, mpu_key(rng.below(MPU_KEYS)))
                    };
                    let mode = match rng.below(20) {
                        0..=2 => 0,
                        3 if pending_uploads.len() < 2 => 1,
                        _ => 2,
                    };
                    let (vid_opt, pending_opt) = run_multipart_upload(&c, &a, &b, &clock, &mut rng, bkt, &k, mode, &ctx).await;
                    if let Some(uid) = pending_opt {
                        pending_uploads.push((bkt.to_string(), k, uid));
                    } else if mode == 2 {
                        if is_ver {
                            let vid = vid_opt.unwrap_or_else(|| "null".to_string());
                            ver_history.push((k.clone(), vid.clone()));
                            mp_ver_history.push((k, vid));
                        } else {
                            mpu_completed.insert(k);
                        }
                    }
                }
            }
        }

        let ver_pages = list_all_versions(&c, &a.url, VER_BUCKET, 97).await;
        assert_eq!(ver_pages, list_all_versions(&c, &b.url, VER_BUCKET, 97).await, "ListObjectVersions VER_BUCKET after round {}", round);
        let ver_entry_count: usize = ver_pages.iter().map(|p| p.entries.len()).sum();
        assert!(ver_entry_count > 400, "round {}: only {} version entries", round, ver_entry_count);

        let mpu_ver_pages = list_all_versions(&c, &a.url, MPU_BUCKET, 61).await;
        assert_eq!(mpu_ver_pages, list_all_versions(&c, &b.url, MPU_BUCKET, 61).await, "ListObjectVersions MPU_BUCKET after round {}", round);

        let ver_v2_pages = list_v2_all_in(&c, &a.url, VER_BUCKET, 113).await;
        assert_eq!(ver_v2_pages, list_v2_all_in(&c, &b.url, VER_BUCKET, 113).await, "ListObjectsV2 VER_BUCKET after round {}", round);
        let mpu_v2_pages = list_v2_all_in(&c, &a.url, MPU_BUCKET, 89).await;
        assert_eq!(mpu_v2_pages, list_v2_all_in(&c, &b.url, MPU_BUCKET, 89).await, "ListObjectsV2 MPU_BUCKET after round {}", round);

        for i in 0..VER_KEYS {
            let k = ver_key(i);
            assert_eq!(
                get_object_reply(&c, &a.url, VER_BUCKET, &k, &[]).await,
                get_object_reply(&c, &b.url, VER_BUCKET, &k, &[]).await,
                "GET VER_BUCKET after round {} key {}",
                round,
                k
            );
        }
        for i in 0..MPU_KEYS {
            let k = mpu_key(i);
            assert_eq!(
                get_object_reply(&c, &a.url, MPU_BUCKET, &k, &[]).await,
                get_object_reply(&c, &b.url, MPU_BUCKET, &k, &[]).await,
                "GET MPU_BUCKET after round {} key {}",
                round,
                k
            );
        }
        for (k, vid) in ver_history.iter().rev().take(1_200) {
            let q = [("versionId", vid.as_str())];
            assert_eq!(
                get_object_reply(&c, &a.url, VER_BUCKET, k, &q).await,
                get_object_reply(&c, &b.url, VER_BUCKET, k, &q).await,
                "GET ?versionId after round {} key {} vid {}",
                round,
                k,
                vid
            );
        }
        for (k, vid) in delete_markers.iter().rev().take(200) {
            let q = [("versionId", vid.as_str())];
            assert_eq!(
                get_object_reply(&c, &a.url, VER_BUCKET, k, &q).await,
                get_object_reply(&c, &b.url, VER_BUCKET, k, &q).await,
                "GET marker ?versionId after round {} key {} vid {}",
                round,
                k,
                vid
            );
        }

        let (dir, limits) = a.stop().await;
        let lv = levels(&dir);
        assert!(lv.iter().sum::<usize>() > 0, "round {}: server A wrote no metadata segments", round);
        deeper_levels_seen |= lv[1..].iter().any(|&n| n > 0);
        a = Server::start_with_clock(&dir, limits, Arc::clone(&clock) as Arc<dyn Clock>).await;
        b = b.restart().await;

        assert_eq!(list_all_versions(&c, &a.url, VER_BUCKET, 97).await, ver_pages, "ListObjectVersions VER_BUCKET A after restart {}", round);
        assert_eq!(list_all_versions(&c, &b.url, VER_BUCKET, 97).await, ver_pages, "ListObjectVersions VER_BUCKET B after restart {}", round);
        assert_eq!(list_all_versions(&c, &a.url, MPU_BUCKET, 61).await, mpu_ver_pages, "ListObjectVersions MPU_BUCKET A after restart {}", round);
        assert_eq!(list_all_versions(&c, &b.url, MPU_BUCKET, 61).await, mpu_ver_pages, "ListObjectVersions MPU_BUCKET B after restart {}", round);
        assert_eq!(list_v2_all_in(&c, &a.url, VER_BUCKET, 113).await, ver_v2_pages, "ListObjectsV2 VER_BUCKET A after restart {}", round);
        assert_eq!(list_v2_all_in(&c, &b.url, VER_BUCKET, 113).await, ver_v2_pages, "ListObjectsV2 VER_BUCKET B after restart {}", round);
        assert_eq!(list_v2_all_in(&c, &a.url, MPU_BUCKET, 89).await, mpu_v2_pages, "ListObjectsV2 MPU_BUCKET A after restart {}", round);
        assert_eq!(list_v2_all_in(&c, &b.url, MPU_BUCKET, 89).await, mpu_v2_pages, "ListObjectsV2 MPU_BUCKET B after restart {}", round);

        for bkt in [VER_BUCKET, MPU_BUCKET] {
            let ua = list_uploads_page(&c, &a.url, bkt, "", None, None, None, None).await;
            let ub = list_uploads_page(&c, &b.url, bkt, "", None, None, None, None).await;
            assert_eq!(ua, ub, "ListMultipartUploads after restart {} bkt={}", round, bkt);
            assert!(ua.uploads.is_empty(), "in-flight uploads cleared on restart {} bkt={}", round, bkt);
        }
        for (bkt, k, uid) in &pending_uploads {
            let pa = list_parts_page(&c, &a.url, bkt, k, uid, None, None).await;
            let pb = list_parts_page(&c, &b.url, bkt, k, uid, None, None).await;
            assert_eq!(pa, pb, "stale uploadId {} after restart {}", uid, round);
            assert_eq!(pa.status, StatusCode::NOT_FOUND);
            assert_eq!(pa.error_code.as_deref(), Some("NoSuchUpload"));
        }

        for (k, vid) in &mp_ver_history {
            let q = [("versionId", vid.as_str())];
            assert_eq!(
                get_object_reply(&c, &a.url, VER_BUCKET, k, &q).await,
                get_object_reply(&c, &b.url, VER_BUCKET, k, &q).await,
                "multipart version {} {} after restart {}",
                k,
                vid,
                round
            );
        }
        for k in &mpu_completed {
            assert_eq!(
                get_object_reply(&c, &a.url, MPU_BUCKET, k, &[]).await,
                get_object_reply(&c, &b.url, MPU_BUCKET, k, &[]).await,
                "multipart object {} after restart {}",
                k,
                round
            );
            assert_eq!(
                get_object_reply(&c, &a.url, MPU_BUCKET, k, &[("partNumber", "1")]).await,
                get_object_reply(&c, &b.url, MPU_BUCKET, k, &[("partNumber", "1")]).await,
                "multipart object {} part 1 after restart {}",
                k,
                round
            );
        }

        run_multipart_upload(&c, &a, &b, &clock, &mut rng, MPU_BUCKET, "post-restart-probe", 0, &format!("post-restart {}", round)).await;
    }

    assert!(deeper_levels_seen, "no compaction moved segments below level 0 in any round");
    let (da, _) = a.stop().await;
    let (db, _) = b.stop().await;
    let _ = std::fs::remove_dir_all(da);
    let _ = std::fs::remove_dir_all(db);
}
