//! #155 Step 4 TDD tests:
//! - Versioned multipart GET (full, range, partNumber) leaves `staged_mmaps` empty
//! - CopyObject and UploadPartCopy on large multipart objects leave `staged_mmaps` empty
//! - CompleteMultipartUpload with FULL_OBJECT checksum leaves `staged_mmaps` empty
//! - Small object CRC32C mismatch returns 500, increments `read_checksum_failures`, and reports on `/_awanfs/scrub`

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const MIB: usize = 1024 * 1024;

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "awanfs_step4_unmapped_{}_{}_{}",
        name,
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
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
    (dir, state, url, handle)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

fn crc32_b64(data: &[u8]) -> String {
    use base64_like::*;
    let sum = crc32fast::hash(data);
    encode_b64(&sum.to_be_bytes())
}

mod base64_like {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    pub fn encode_b64(bytes: &[u8]) -> String {
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let triple = (b0 << 16) | (b1 << 8) | b2;
            out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
            out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
            if chunk.len() > 1 {
                out.push(TABLE[((triple >> 6) & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
            if chunk.len() > 2 {
                out.push(TABLE[(triple & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
        }
        out
    }
}

async fn upload_mpu(
    c: &reqwest::Client,
    url: &str,
    bucket: &str,
    key: &str,
    parts: &[Vec<u8>],
    full_object_crc32: bool,
) -> (Vec<u8>, Option<String>) {
    let mut init_req = c.post(format!("{}/{}/{}?uploads", url, bucket, key));
    if full_object_crc32 {
        init_req = init_req
            .header("x-amz-checksum-algorithm", "CRC32")
            .header("x-amz-checksum-type", "FULL_OBJECT");
    }
    let init = init_req.send().await.unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let xml = init.text().await.unwrap();
    let id = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();

    let mut done = String::from("<CompleteMultipartUpload>");
    for (i, data) in parts.iter().enumerate() {
        let mut req = c
            .put(format!(
                "{}/{}/{}?partNumber={}&uploadId={}",
                url,
                bucket,
                key,
                i + 1,
                id
            ))
            .header("content-length", data.len());
        let part_crc = full_object_crc32.then(|| crc32_b64(data));
        if let Some(ref ck) = part_crc {
            req = req.header("x-amz-checksum-crc32", ck);
        }
        let res = req.body(data.clone()).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "part {}", i + 1);
        let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
        if let Some(ck) = part_crc {
            done.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag><ChecksumCRC32>{}</ChecksumCRC32></Part>",
                i + 1,
                etag,
                ck
            ));
        } else {
            done.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                i + 1,
                etag
            ));
        }
    }
    done.push_str("</CompleteMultipartUpload>");
    let whole = parts.concat();
    let mut cmp_req = c.post(format!("{}/{}/{}?uploadId={}", url, bucket, key, id));
    if full_object_crc32 {
        cmp_req = cmp_req.header("x-amz-checksum-crc32", crc32_b64(&whole));
    }
    let res = cmp_req.body(done).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "complete failed");
    let vid = res
        .headers()
        .get("x-amz-version-id")
        .map(|v| v.to_str().unwrap().to_string());
    (whole, vid)
}

#[tokio::test]
async fn versioned_multipart_and_copy_and_full_object_leave_staged_mmaps_empty() {
    let (dir, state, url, handle) = start("vmpu").await;
    let c = reqwest::Client::new();
    assert_eq!(
        c.put(format!("{}/vbkt", url)).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        c.put(format!("{}/vbkt?versioning", url))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // 3 x 2 MiB = 6 MiB (> 4 MiB threshold) with FULL_OBJECT CRC32 checksum.
    let parts1 = vec![
        pattern(2 * MIB, 1),
        pattern(2 * MIB, 2),
        pattern(2 * MIB + 123, 3),
    ];
    let (whole1, vid1) = upload_mpu(&c, &url, "vbkt", "obj", &parts1, true).await;
    let vid1 = vid1.expect("versioned upload must return version id");
    assert_eq!(
        state.multipart.staged_mmaps.read().len(),
        0,
        "FULL_OBJECT completion must not mmap parts"
    );

    // Upload a second version so vid1 becomes non-current.
    let parts2 = vec![pattern(3 * MIB, 4), pattern(2 * MIB + 55, 5)];
    let (whole2, vid2) = upload_mpu(&c, &url, "vbkt", "obj", &parts2, false).await;
    let vid2 = vid2.expect("second version id");
    assert_eq!(state.multipart.staged_mmaps.read().len(), 0);

    // Current versioned GET, Range GET, and partNumber GET.
    let res = c.get(format!("{}/vbkt/obj", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.bytes().await.unwrap()[..] == whole2[..]);
    assert_eq!(
        state.multipart.staged_mmaps.read().len(),
        0,
        "versioned full GET must not mmap parts"
    );

    let res = c
        .get(format!("{}/vbkt/obj?versionId={}", url, vid1))
        .header("range", format!("bytes={}-{}", 2 * MIB - 10, 2 * MIB + 9))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert!(res.bytes().await.unwrap()[..] == whole1[2 * MIB - 10..=2 * MIB + 9]);
    assert_eq!(
        state.multipart.staged_mmaps.read().len(),
        0,
        "versioned range GET must not mmap parts"
    );

    let res = c
        .get(format!("{}/vbkt/obj?partNumber=2&versionId={}", url, vid1))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.bytes().await.unwrap()[..] == parts1[1][..]);
    assert_eq!(
        state.multipart.staged_mmaps.read().len(),
        0,
        "versioned partNumber GET must not mmap parts"
    );

    // CopyObject from large multipart object (current and explicit version).
    let cp = c
        .put(format!("{}/vbkt/copied", url))
        .header("x-amz-copy-source", format!("/vbkt/obj?versionId={}", vid2))
        .send()
        .await
        .unwrap();
    assert_eq!(cp.status(), StatusCode::OK);
    assert_eq!(
        state.multipart.staged_mmaps.read().len(),
        0,
        "CopyObject must not mmap parts"
    );
    let got_cp = c.get(format!("{}/vbkt/copied", url)).send().await.unwrap().bytes().await.unwrap();
    assert!(got_cp[..] == whole2[..]);

    // UploadPartCopy from large multipart object.
    let init = c
        .post(format!("{}/vbkt/upc?uploads", url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let upc_id = init
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();
    let upc_res = c
        .put(format!("{}/vbkt/upc?partNumber=1&uploadId={}", url, upc_id))
        .header("x-amz-copy-source", "/vbkt/obj")
        .header("x-amz-copy-source-range", format!("bytes=0-{}", 5 * MIB - 1))
        .send()
        .await
        .unwrap();
    assert_eq!(upc_res.status(), StatusCode::OK);
    assert_eq!(
        state.multipart.staged_mmaps.read().len(),
        0,
        "UploadPartCopy must not mmap parts"
    );

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn small_object_crc_mismatch_returns_500_counts_failure_and_marks_scrub_bad() {
    let (dir, state, url, handle) = start("small_crc").await;
    let c = reqwest::Client::new();
    assert_eq!(
        c.put(format!("{}/sbkt", url)).send().await.unwrap().status(),
        StatusCode::OK
    );
    let data = pattern(64 * 1024, 9);
    assert_eq!(
        c.put(format!("{}/sbkt/item", url))
            .body(data.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Corrupt a payload byte on disk and clear payload cache.
    let dat = dir.join("volume_0001.dat");
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&dat)
        .unwrap();
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();
    let pos = 4096
        + buf[4096..]
            .windows(64)
            .position(|w| w == &data[1000..1064])
            .unwrap();
    f.seek(SeekFrom::Start(pos as u64)).unwrap();
    f.write_all(&[buf[pos] ^ 0xFF]).unwrap();
    f.sync_all().unwrap();
    drop(f);
    state.payload_cache.clear();

    // Both full GET and range GET must return 500 InternalError and increment read_checksum_failures.
    let res = c.get(format!("{}/sbkt/item", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = res.text().await.unwrap();
    assert!(body.contains("InternalError"), "expected InternalError, got: {}", body);
    assert_eq!(state.read_checksum_failures.load(Ordering::SeqCst), 1);

    let res_rng = c
        .get(format!("{}/sbkt/item", url))
        .header("range", "bytes=0-100")
        .send()
        .await
        .unwrap();
    assert_eq!(res_rng.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(state.read_checksum_failures.load(Ordering::SeqCst), 2);

    // Scrub endpoint must report the bad needle.
    let scrub_res = c.get(format!("{}/_awanfs/scrub", url)).send().await.unwrap();
    assert_eq!(scrub_res.status(), StatusCode::OK);
    let scrub_json = scrub_res.text().await.unwrap();
    assert!(
        scrub_json.contains("sbkt/item") || scrub_json.contains("offset"),
        "expected scrub report to list bad needle, got: {}",
        scrub_json
    );

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
