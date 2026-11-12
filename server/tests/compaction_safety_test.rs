//! #14: online compaction is safe with several volumes and concurrent writes, and what it
//! leaves behind survives a restart.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_csafe_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path, span: u64) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    state.set_volume_span(span);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn body(key: usize, version: usize) -> Vec<u8> {
    (0..3000 + key * 13).map(|j| ((j * 31 + key * 7 + version * 11) % 251) as u8).collect()
}

async fn check(c: &reqwest::Client, url: &str, expect: &HashMap<usize, Option<usize>>, when: &str) {
    for (&k, &v) in expect {
        let res = c.get(format!("{}/cs-bkt/k{}", url, k)).send().await.unwrap();
        match v {
            None => assert_eq!(res.status(), StatusCode::NOT_FOUND, "k{} is deleted ({})", k, when),
            Some(ver) => {
                assert_eq!(res.status(), StatusCode::OK, "k{} ({})", k, when);
                assert!(&res.bytes().await.unwrap()[..] == &body(k, ver)[..], "k{} version {} ({})", k, ver, when);
            }
        }
    }
}

#[tokio::test]
async fn compacting_the_oldest_of_several_volumes_keeps_every_object_and_every_delete() {
    let dir = temp_dir("multi");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir, 64 * 1024).await;
    assert_eq!(c.put(format!("{}/cs-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let mut expect: HashMap<usize, Option<usize>> = HashMap::new();
    for k in 0..30 {
        assert_eq!(c.put(format!("{}/cs-bkt/k{}", url, k)).body(body(k, 0)).send().await.unwrap().status(), StatusCode::OK);
        expect.insert(k, Some(0));
    }
    // Overwrite some, delete some: their old needles and tombstones sit in older volumes.
    for k in 0..10 {
        assert_eq!(c.put(format!("{}/cs-bkt/k{}", url, k)).body(body(k, 1)).send().await.unwrap().status(), StatusCode::OK);
        expect.insert(k, Some(1));
    }
    for k in 10..20 {
        assert_eq!(c.delete(format!("{}/cs-bkt/k{}", url, k)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
        expect.insert(k, None);
    }
    let volumes_before = state.volumes.read().len();
    assert!(volumes_before >= 4, "the test needs several volumes, got {}", volumes_before);

    // Compact the oldest volume, one after another, until one sealed volume is left.
    let mut copied = 0;
    while state.volumes.read().len() > 2 {
        let r = state.compact_oldest_volume().await.expect("compaction");
        copied += r.live_needles_copied;
        state.payload_cache.clear();
        check(&c, &url, &expect, "after a compaction").await;
    }
    assert!(copied > 0, "some live needles were moved");
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir, 64 * 1024).await;
    check(&c, &url, &expect, "after the restart").await;
}

#[tokio::test]
async fn writes_racing_with_compaction_keep_the_last_acknowledged_version() {
    let dir = temp_dir("race");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir, 64 * 1024).await;
    assert_eq!(c.put(format!("{}/cs-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for k in 0..24 {
        assert_eq!(c.put(format!("{}/cs-bkt/k{}", url, k)).body(body(k, 0)).send().await.unwrap().status(), StatusCode::OK);
    }

    // Overwrite and delete every key while the oldest volumes are being compacted.
    let writer = {
        let c = c.clone();
        let url = url.clone();
        tokio::spawn(async move {
            let mut expect = HashMap::new();
            for round in 1..=3 {
                for k in 0..24 {
                    if (k + round) % 5 == 0 {
                        assert_eq!(c.delete(format!("{}/cs-bkt/k{}", url, k)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
                        expect.insert(k, None);
                    } else {
                        assert_eq!(c.put(format!("{}/cs-bkt/k{}", url, k)).body(body(k, round)).send().await.unwrap().status(), StatusCode::OK);
                        expect.insert(k, Some(round));
                    }
                }
            }
            expect
        })
    };
    for _ in 0..6 {
        state.compact_oldest_volume().await.expect("compaction");
        tokio::task::yield_now().await;
    }
    let expect = writer.await.unwrap();
    state.payload_cache.clear();
    check(&c, &url, &expect, "live").await;
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir, 64 * 1024).await;
    check(&c, &url, &expect, "after the restart").await;
}

#[tokio::test]
async fn a_multipart_object_moved_by_compaction_survives_a_restart() {
    let dir = temp_dir("mp");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir, 64 * 1024).await;
    assert_eq!(c.put(format!("{}/cs-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let xml = c.post(format!("{}/cs-bkt/mp?uploads", url)).send().await.unwrap().text().await.unwrap();
    let id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let part = body(5, 5);
    let res = c.put(format!("{}/cs-bkt/mp?partNumber=1&uploadId={}", url, id)).body(part.clone()).send().await.unwrap();
    let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let done = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etag);
    assert_eq!(c.post(format!("{}/cs-bkt/mp?uploadId={}", url, id)).body(done).send().await.unwrap().status(), StatusCode::OK);

    let r = state.perform_online_compaction().await.expect("compaction");
    assert_eq!(r.live_needles_copied, 1, "the manifest needle moved");
    assert_eq!(&c.get(format!("{}/cs-bkt/mp", url)).send().await.unwrap().bytes().await.unwrap()[..], &part[..]);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir, 64 * 1024).await;
    let res = c.get(format!("{}/cs-bkt/mp", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(&res.bytes().await.unwrap()[..], &part[..]);
}
