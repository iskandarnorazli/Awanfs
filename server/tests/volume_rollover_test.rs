//! #37: when the active volume is full, the server seals it and writes to the next one;
//! a restart opens every volume in order. Before, the server only ever wrote volume 1 and
//! every write after it filled was a 500.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const SPAN: u64 = 64 * 1024;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_roll_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    state.set_volume_span(SPAN);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn body(i: usize, round: u8) -> Vec<u8> {
    (0..5000 + i * 37).map(|j| ((j * 31 + i * 7 + round as usize) % 251) as u8).collect()
}

fn volume_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name().to_string_lossy().to_string();
            n.starts_with("volume_") && n.ends_with(".dat")
        })
        .count()
}

#[tokio::test]
async fn writes_continue_past_a_full_volume_and_survive_a_restart() {
    let dir = temp_dir("api");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/roll-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // 40 objects of 2 sectors each, then 10 overwrites and 5 deletes: about 110 sectors in
    // volumes of 15 usable sectors, so the writes cross several volumes.
    for i in 0..40 {
        let res = c.put(format!("{}/roll-bkt/o{}", url, i)).body(body(i, 0)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "PUT o{}", i);
    }
    for i in 0..10 {
        let res = c.put(format!("{}/roll-bkt/o{}", url, i)).body(body(i, 1)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "overwrite o{}", i);
    }
    for i in 10..15 {
        assert_eq!(c.delete(format!("{}/roll-bkt/o{}", url, i)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    }
    // Read back before the restart from cold storage, not the cache.
    state.payload_cache.clear();
    for i in 0..40 {
        let res = c.get(format!("{}/roll-bkt/o{}", url, i)).send().await.unwrap();
        if (10..15).contains(&i) {
            assert_eq!(res.status(), StatusCode::NOT_FOUND, "o{} was deleted", i);
        } else {
            let want = body(i, if i < 10 { 1 } else { 0 });
            assert_eq!(&res.bytes().await.unwrap()[..], &want[..], "o{} before the restart", i);
        }
    }
    let files = volume_files(&dir);
    assert!(files >= 5, "expected writes across several volumes, found {} volume files", files);
    handle.shutdown();
    drop(state);

    let (_state, url, _handle) = start(&dir).await;
    for i in 0..40 {
        let res = c.get(format!("{}/roll-bkt/o{}", url, i)).send().await.unwrap();
        if (10..15).contains(&i) {
            assert_eq!(res.status(), StatusCode::NOT_FOUND, "o{} stays deleted after the restart", i);
        } else {
            assert_eq!(res.status(), StatusCode::OK, "o{} after the restart", i);
            let want = body(i, if i < 10 { 1 } else { 0 });
            assert_eq!(&res.bytes().await.unwrap()[..], &want[..], "o{} after the restart", i);
        }
    }
    // New writes after the restart go to the newest volume, not over an old one.
    let res = c.put(format!("{}/roll-bkt/after", url)).body(body(99, 0)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let res = c.get(format!("{}/roll-bkt/o20", url)).send().await.unwrap();
    assert_eq!(&res.bytes().await.unwrap()[..], &body(20, 0)[..]);
}

#[tokio::test]
async fn a_multipart_object_recorded_in_a_later_volume_survives_a_restart() {
    let dir = temp_dir("mp");
    let c = reqwest::Client::new();
    let (_state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/roll-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for i in 0..20 {
        assert_eq!(c.put(format!("{}/roll-bkt/f{}", url, i)).body(body(i, 2)).send().await.unwrap().status(), StatusCode::OK);
    }
    let xml = c.post(format!("{}/roll-bkt/mp?uploads", url)).send().await.unwrap().text().await.unwrap();
    let id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let part = body(7, 9);
    let res = c.put(format!("{}/roll-bkt/mp?partNumber=1&uploadId={}", url, id)).body(part.clone()).send().await.unwrap();
    let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let done = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etag);
    assert_eq!(c.post(format!("{}/roll-bkt/mp?uploadId={}", url, id)).body(done).send().await.unwrap().status(), StatusCode::OK);
    assert!(volume_files(&dir) >= 2);
    handle.shutdown();

    let (_state, url, _handle) = start(&dir).await;
    let res = c.get(format!("{}/roll-bkt/mp", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(&res.bytes().await.unwrap()[..], &part[..]);
}

#[tokio::test]
async fn a_needle_larger_than_a_whole_volume_is_refused_without_rolling() {
    let dir = temp_dir("huge");
    let c = reqwest::Client::new();
    let (_state, url, _handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/roll-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let res = c.put(format!("{}/roll-bkt/huge", url)).body(vec![7u8; SPAN as usize]).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(volume_files(&dir), 1, "no volume opened for a needle that can never fit");
}
