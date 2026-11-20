//! #155 requirement 5, decision 53: a multipart object above 4 MiB is read with positioned
//! reads, part by part, never through a mapping of its part files. The nightly full matrix
//! (run 44674110492) showed why: a 1 GiB multipart range GET peaked at 1,190.8 MB RSS on Linux,
//! 1,033.0 MB of it the part files' mapped pages. Every way of reading such an object (whole,
//! a range inside a part, a range across parts, one part by number) returns the stored bytes
//! and leaves no part mapped.

use reqwest::StatusCode;
use s3_server::sim::SimRng;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const MIB: usize = 1024 * 1024;

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!("awanfs_mpu_reads_{}_{}_{}", name, std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn start(name: &str) -> (PathBuf, Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let dir = temp_dir(name);
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    assert_eq!(reqwest::Client::new().put(format!("{}/mpr-bucket", url)).send().await.unwrap().status(), StatusCode::OK);
    (dir, state, url, handle)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

/// Uploads `parts` as one multipart object and returns the whole object's bytes.
async fn upload(c: &reqwest::Client, url: &str, key: &str, parts: &[Vec<u8>]) -> Vec<u8> {
    let init = c.post(format!("{}/mpr-bucket/{}?uploads", url, key)).send().await.unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let xml = init.text().await.unwrap();
    let id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let mut done = String::from("<CompleteMultipartUpload>");
    for (i, data) in parts.iter().enumerate() {
        let res = c
            .put(format!("{}/mpr-bucket/{}?partNumber={}&uploadId={}", url, key, i + 1, id))
            .header("content-length", data.len())
            .body(data.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "part {}", i + 1);
        let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
        done.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>", i + 1, etag));
    }
    done.push_str("</CompleteMultipartUpload>");
    let res = c.post(format!("{}/mpr-bucket/{}?uploadId={}", url, key, id)).body(done).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    parts.concat()
}

fn mapped_parts(state: &S3ServiceState) -> usize {
    state.multipart.staged_mmaps.read().len()
}

async fn get_range(c: &reqwest::Client, url: &str, first: usize, last: usize) -> (StatusCode, Option<String>, Vec<u8>) {
    let res = c.get(url).header("range", format!("bytes={}-{}", first, last)).send().await.unwrap();
    let status = res.status();
    let cr = res.headers().get("content-range").map(|v| v.to_str().unwrap().to_string());
    (status, cr, res.bytes().await.unwrap().to_vec())
}

#[tokio::test]
async fn a_large_multipart_object_is_read_without_mapping_its_parts() {
    let (_dir, state, url, _h) = start("large").await;
    let c = reqwest::Client::new();
    // Three 5 MiB parts and a 2 MiB tail: 17 MiB, above the 4 MiB line.
    let parts: Vec<Vec<u8>> = [5 * MIB, 5 * MIB, 5 * MIB, 2 * MIB].iter().enumerate().map(|(i, &n)| pattern(n, i + 1)).collect();
    let whole = upload(&c, &url, "big", &parts).await;
    let obj = format!("{}/mpr-bucket/big", url);
    assert_eq!(mapped_parts(&state), 0, "uploading maps no part");

    let res = c.get(&obj).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("content-length").unwrap().to_str().unwrap(), whole.len().to_string());
    assert!(res.bytes().await.unwrap() == whole, "a full GET returns the stored bytes");
    assert_eq!(mapped_parts(&state), 0, "a full GET maps no part");

    let head = c.head(&obj).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(mapped_parts(&state), 0, "HEAD maps no part");

    // Ranges inside one part, across one and across several part boundaries, at the edges.
    let p = 5 * MIB;
    let fixed = [(0, 0), (0, 1023), (p - 10, p - 1), (p - 10, p + 9), (p + 1, 2 * p - 2), (p - 1, 3 * p), (0, whole.len() - 1), (whole.len() - 1, whole.len() - 1)];
    let mut rng = SimRng::new(155);
    let random: Vec<(usize, usize)> = (0..40)
        .map(|_| {
            let a = rng.below(whole.len() as u64) as usize;
            let b = a + rng.below((whole.len() - a) as u64).min(3 * MIB as u64) as usize;
            (a, b)
        })
        .collect();
    for (first, last) in fixed.iter().copied().chain(random) {
        let (status, cr, body) = get_range(&c, &obj, first, last).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT, "range {}-{}", first, last);
        assert_eq!(cr.as_deref(), Some(format!("bytes {}-{}/{}", first, last, whole.len()).as_str()), "range {}-{}", first, last);
        assert!(body == whole[first..=last], "range {}-{}: wrong bytes", first, last);
        assert_eq!(mapped_parts(&state), 0, "range {}-{} mapped a part", first, last);
    }

    // One part by number.
    for (i, part) in parts.iter().enumerate() {
        let res = c.get(format!("{}?partNumber={}", obj, i + 1)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "part {}", i + 1);
        assert_eq!(res.headers().get("x-amz-mp-parts-count").unwrap().to_str().unwrap(), parts.len().to_string());
        assert!(res.bytes().await.unwrap() == *part, "part {}: wrong bytes", i + 1);
        assert_eq!(mapped_parts(&state), 0, "GET of part {} mapped it", i + 1);
    }
}

#[tokio::test]
async fn a_small_multipart_object_keeps_its_path_and_its_bytes() {
    let (_dir, state, url, _h) = start("small").await;
    let c = reqwest::Client::new();
    // 4 MiB and under keeps today's path (#155 requirement 7); only its answers are checked.
    let parts = vec![pattern(300 * 1024, 1), pattern(200 * 1024, 2)];
    let whole = upload(&c, &url, "small", &parts).await;
    let obj = format!("{}/mpr-bucket/small", url);
    assert!(c.get(&obj).send().await.unwrap().bytes().await.unwrap() == whole);
    let (status, _, body) = get_range(&c, &obj, 300 * 1024 - 5, 300 * 1024 + 4).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(body == whole[300 * 1024 - 5..300 * 1024 + 5]);
    let res = c.get(format!("{}?partNumber=2", obj)).send().await.unwrap();
    assert!(res.bytes().await.unwrap() == parts[1]);
    let _ = state;
}
