//! Data-integrity contracts: a failed read or write is an error to the client, never a
//! 200 with wrong or missing data, and a completed multipart object survives a restart.

use reqwest::header::ETAG;
use reqwest::StatusCode;
use s3_server::{start_s3_reactor, AlignedBuffer, S3ServiceState, VolumeManager};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_integrity_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn(state: Arc<S3ServiceState>) -> (String, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, state).await.expect("start reactor");
    (format!("http://{}", handle.local_addr), handle)
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed as usize * 7 + 13) % 251) as u8).collect()
}

#[test]
fn full_volume_refuses_writes_instead_of_overwriting_earlier_needles() {
    let dir = temp_dir("wrap");
    let vm = VolumeManager::open(&dir, 1).unwrap();
    vm.set_max_span(64 * 1024);

    let first = pattern(16 * 1024, 3);
    let loc = vm.write_needle(1, "first", &first, 1, 0, 0).expect("first needle fits");

    let mut refused = 0;
    for i in 0..8u8 {
        if vm.write_needle(1, &format!("n{}", i), &pattern(16 * 1024, 4 + i), 1, 0, 0).is_err() {
            refused += 1;
        }
    }

    let back = vm
        .read_needle_bytes(loc.volume_id, loc.offset, loc.payload_crc32c)
        .expect("the first needle must still be intact after the volume fills up");
    assert_eq!(&back[..], &first[..]);
    assert!(refused > 0, "writes past the volume span must be refused");
}

#[tokio::test]
async fn put_that_cannot_be_written_is_an_error_not_a_200() {
    let dir = temp_dir("full");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let small_dir = dir.join("small");
    std::fs::create_dir_all(&small_dir).unwrap();
    let vm = VolumeManager::open(&small_dir, 1).unwrap();
    vm.set_max_span(64 * 1024);
    *state.volume_manager.write() = Some(Arc::new(vm));

    let (url, _handle) = spawn(Arc::clone(&state)).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let res = c.put(format!("{}/bkt/big", url)).body(pattern(128 * 1024, 2)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR, "a PUT the volume could not store was acknowledged");

    let get = c.get(format!("{}/bkt/big", url)).send().await.unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND, "a failed PUT must not create the object");
}

#[tokio::test]
async fn get_of_a_corrupted_payload_is_an_error_not_zeros() {
    let dir = temp_dir("corrupt");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let (url, _handle) = spawn(Arc::clone(&state)).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let payload = pattern(64 * 1024, 1);
    let put = c.put(format!("{}/bkt/obj", url)).body(payload.clone()).send().await.unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Flip one payload byte on disk, then drop the cached copy so the GET reads the volume.
    let dat = dir.join("volume_0001.dat");
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&dat).unwrap();
    let mut head = vec![0u8; 1 << 20];
    f.read_exact(&mut head).unwrap();
    // Search after the 4 KiB superblock: needles start at offset 4096.
    let pos = 4096
        + head[4096..]
            .windows(64)
            .position(|w| w == &payload[1000..1064])
            .expect("payload bytes are in the first MiB of the volume");
    f.seek(SeekFrom::Start(pos as u64)).unwrap();
    f.write_all(&[head[pos] ^ 0xFF]).unwrap();
    f.sync_all().unwrap();
    drop(f);
    state.payload_cache.clear();

    // Diagnostics for the assertion message: what the file and the volume manager see now.
    let mut check = std::fs::File::open(&dat).unwrap();
    let mut now = vec![0u8; pos + 1];
    check.read_exact(&mut now).unwrap();
    let entries: Vec<((u32, u64), u32)> = state.needle_crc_map.read().iter().map(|(k, v)| (*k, *v)).collect();
    let direct = entries.first().map(|&((_, off), crc)| {
        let vm = state.volume_manager.read().clone().unwrap();
        (off, vm.read_needle_bytes(1, off, crc).map(|b| b.len()).map_err(|e| format!("{:?}", e)))
    });

    let res = c.get(format!("{}/bkt/obj", url)).send().await.unwrap();
    let status = res.status();
    let body = res.bytes().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "a payload that fails its CRC was served ({} bytes, all zero: {}, equal to the original: {}); \
         flipped file offset {} (byte now {:#x}, was {:#x}); crc map {:?}; direct volume read {:?}",
        body.len(),
        body.iter().all(|b| *b == 0),
        &body[..] == &payload[..],
        pos,
        now[pos],
        head[pos],
        entries,
        direct
    );
}

async fn upload_multipart(c: &reqwest::Client, url: &str, parts: &[&[u8]]) {
    let init = c.post(format!("{}/bkt/big?uploads", url)).send().await.unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let xml = init.text().await.unwrap();
    let upload_id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();

    let mut complete = String::from("<CompleteMultipartUpload>");
    for (i, data) in parts.iter().enumerate() {
        let n = i + 1;
        let res = c
            .put(format!("{}/bkt/big?partNumber={}&uploadId={}", url, n, upload_id))
            .body(data.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let etag = res.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
        complete.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>", n, etag));
    }
    complete.push_str("</CompleteMultipartUpload>");
    let res = c
        .post(format!("{}/bkt/big?uploadId={}", url, upload_id))
        .body(complete)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn completed_multipart_object_survives_restart() {
    let dir = temp_dir("mpu_restart");
    let p1 = pattern(300 * 1024, 5);
    let p2 = pattern(200 * 1024, 6);
    let mut whole = p1.clone();
    whole.extend_from_slice(&p2);
    let c = reqwest::Client::new();

    {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir));
        let (url, handle) = spawn(state).await;
        assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
        upload_multipart(&c, &url, &[&p1, &p2]).await;
        let res = c.get(format!("{}/bkt/big", url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(&res.bytes().await.unwrap()[..], &whole[..]);
        handle.shutdown();
    }

    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let (url, _handle) = spawn(state).await;
    let res = c.get(format!("{}/bkt/big", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "the multipart object is gone after a restart");
    let body = res.bytes().await.unwrap();
    assert_eq!(body.len(), whole.len());
    assert!(&body[..] == &whole[..], "the multipart object's bytes changed across a restart");
}

#[test]
fn new_volume_superblock_holds_no_stale_heap_bytes() {
    // Leave a freed, filled sector-aligned block on the heap, as an earlier request would.
    for _ in 0..8 {
        let mut stale = AlignedBuffer::new(4096);
        stale.iter_mut().for_each(|b| *b = 0xAB);
        drop(stale);
    }
    let dir = temp_dir("superblock");
    let vm = VolumeManager::open(&dir, 1).unwrap();
    drop(vm);
    let mut sb = vec![0u8; 4096];
    std::fs::File::open(dir.join("volume_0001.dat")).unwrap().read_exact(&mut sb).unwrap();
    // Bytes 20..24 hold the format version since #50 (owner decision 20); the rest is unused.
    assert_eq!(u32::from_le_bytes(sb[20..24].try_into().unwrap()), s3_server::FORMAT_VERSION);
    let stale = sb[24..].iter().filter(|b| **b != 0).count();
    assert_eq!(stale, 0, "the superblock's unused bytes must be zero, found {} non-zero bytes", stale);
}
