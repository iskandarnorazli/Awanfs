//! #26: the metadata segments answer exactly as the in-memory index does, across PUT, DELETE,
//! GET, listing, restart and the segments' flushes and compactions.
//!
//! Two servers get the same seeded operations over HTTP. Server A keeps 40 objects in its
//! in-memory table and gives the spill index its smallest budget, so almost every key lives in
//! segments and a few thousand PUTs flush and compact them. Server B keeps every object in
//! memory. Every status, body, ETag and listing page must be the same; after each round both are
//! stopped and opened again from their data directories (the spill index is rebuilt from the
//! volume replay, as at every boot).

use reqwest::StatusCode;
use s3_server::manifest::read_manifest;
use s3_server::sim::SimRng;
use s3_server::vfile::DiskFs;
use s3_server::clock::SystemClock;
use s3_server::{start_s3_reactor, OpenLimits, S3ServiceState, ServerHandle};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const BUCKET: &str = "tiered-bucket";
const KEYS: u64 = 1_200;
const ROUNDS: usize = 3;
const OPS_PER_ROUND: usize = 1_200;

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
    url: String,
    state: Arc<S3ServiceState>,
    handle: ServerHandle,
}

impl Server {
    async fn start(dir: &Path, limits: OpenLimits) -> Server {
        let state = Arc::new(S3ServiceState::try_with_data_dir_limits(dir, Arc::new(SystemClock), limits).expect("open data dir"));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
        Server { dir: dir.to_path_buf(), limits, url: format!("http://{}", handle.local_addr), state, handle }
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
        let (dir, limits) = self.stop().await;
        Server::start(&dir, limits).await
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
