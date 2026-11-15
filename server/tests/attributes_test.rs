//! #150, stage one: a needle can carry an attributes block (volume format 3, decision 43), and
//! Content-Type is its first attribute. These tests hold the invariants that do not need
//! attribute updates: what a block survives (restart, compaction, a streamed PUT's copy,
//! CopyObject), that a needle without one is byte for byte a format 2 needle, and that a
//! format 2 volume keeps serving while new attributes go to a format 3 volume.

use reqwest::StatusCode;
use s3_server::attrs::{Attrs, TAG_CONTENT_TYPE};
use s3_server::{start_s3_reactor, S3ServiceState, VolumeManager, AWAN_FLAG_ACTIVE, FLAG_ATTRS, FORMAT_VERSION, NEEDLE_MAGIC, STREAM_PUT_MIN, SUPERBLOCK_MAGIC};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_attrs_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

/// Status, Content-Type and body of a GET (or HEAD).
async fn fetch(c: &reqwest::Client, url: &str, head: bool) -> (StatusCode, String, Vec<u8>) {
    let res = if head { c.head(url).send().await.unwrap() } else { c.get(url).send().await.unwrap() };
    let status = res.status();
    let ct = res.headers().get("content-type").map(|v| v.to_str().unwrap().to_string()).unwrap_or_default();
    (status, ct, res.bytes().await.unwrap().to_vec())
}

async fn assert_object(c: &reqwest::Client, url: &str, ct: &str, body: &[u8], when: &str) {
    let (status, got_ct, got) = fetch(c, url, false).await;
    assert_eq!(status, StatusCode::OK, "{}", when);
    assert_eq!(got_ct, ct, "GET Content-Type {}", when);
    assert!(got == body, "the body is intact {}", when);
    let (status, got_ct, _) = fetch(c, url, true).await;
    assert_eq!(status, StatusCode::OK, "{}", when);
    assert_eq!(got_ct, ct, "HEAD Content-Type {}", when);
}

#[tokio::test]
async fn content_type_survives_a_restart_and_compaction() {
    let dir = temp_dir("ct");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let small = pattern(3000, 1);
    let big = pattern(STREAM_PUT_MIN as usize + 300_000, 2);
    let put = |k: &str, ct: &str, b: Vec<u8>| c.put(format!("{}/abkt/{}", url, k)).header("content-type", ct.to_string()).body(b).send();
    assert_eq!(put("small.txt", "text/plain; charset=utf-8", small.clone()).await.unwrap().status(), StatusCode::OK);
    assert_eq!(put("big.png", "image/png", big.clone()).await.unwrap().status(), StatusCode::OK);
    assert_object(&c, &format!("{}/abkt/small.txt", url), "text/plain; charset=utf-8", &small, "after the PUT").await;
    assert_object(&c, &format!("{}/abkt/big.png", url), "image/png", &big, "after the streamed PUT").await;

    let report = state.perform_online_compaction().await.unwrap();
    assert!(report.live_needles_copied >= 2, "{:?}", report);
    state.payload_cache.clear();
    assert_object(&c, &format!("{}/abkt/small.txt", url), "text/plain; charset=utf-8", &small, "after compaction").await;
    assert_object(&c, &format!("{}/abkt/big.png", url), "image/png", &big, "after compaction").await;
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    assert_object(&c, &format!("{}/abkt/small.txt", url), "text/plain; charset=utf-8", &small, "after a restart").await;
    assert_object(&c, &format!("{}/abkt/big.png", url), "image/png", &big, "after a restart").await;
}

#[tokio::test]
async fn an_object_sent_without_content_type_has_no_block_and_the_default_type() {
    let dir = temp_dir("none");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);
    // reqwest adds no Content-Type to a bytes body.
    assert_eq!(c.put(format!("{}/abkt/plain", url)).body(pattern(100, 3)).send().await.unwrap().status(), StatusCode::OK);
    let b_id = state.engine.read().lookup_bucket("abkt").unwrap();
    let meta = state.engine.read().get_object(b_id, b"plain").unwrap();
    assert_eq!(s3_server::unpack_flags(meta.bucket_flags) & FLAG_ATTRS, 0, "no block, so no flag: HEAD reads nothing more");
    assert_object(&c, &format!("{}/abkt/plain", url), "application/octet-stream", &pattern(100, 3), "without a Content-Type").await;
}

#[tokio::test]
async fn copy_keeps_the_source_content_type_and_replace_takes_the_requests() {
    let dir = temp_dir("copy");
    let c = reqwest::Client::new();
    let (_state, url, _h) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let body = pattern(5000, 4);
    let res = c.put(format!("{}/abkt/src", url)).header("content-type", "text/csv").body(body.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = c.put(format!("{}/abkt/copied", url)).header("x-amz-copy-source", "/abkt/src").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_object(&c, &format!("{}/abkt/copied", url), "text/csv", &body, "after a COPY-directive copy").await;

    let res = c
        .put(format!("{}/abkt/replaced", url))
        .header("x-amz-copy-source", "/abkt/src")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_object(&c, &format!("{}/abkt/replaced", url), "application/json", &body, "after a REPLACE copy").await;
}

/// A needle written without attributes is the format 2 layout byte for byte (FORMAT.md),
/// built here from the document, not from the code.
#[test]
fn a_needle_without_attributes_is_byte_identical_to_format_2() {
    let dir = temp_dir("golden");
    let vm = VolumeManager::open(&dir, 1).unwrap();
    assert_eq!(vm.format(), FORMAT_VERSION);
    let (key, payload, created) = ("k", b"golden payload".to_vec(), 1_700_000_000_000_000_001u64);
    let loc = vm.write_needle_with_attrs(1, key, &[], &payload, AWAN_FLAG_ACTIVE, 7, 9, Some(created)).unwrap();
    vm.sync_now().unwrap();

    let mut h = [0u8; 64];
    h[0..4].copy_from_slice(&NEEDLE_MAGIC.to_le_bytes());
    h[8..12].copy_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    h[12..14].copy_from_slice(&AWAN_FLAG_ACTIVE.to_le_bytes());
    h[14..16].copy_from_slice(&1u16.to_le_bytes());
    h[16..18].copy_from_slice(&(key.len() as u16).to_le_bytes());
    h[20..28].copy_from_slice(&xxhash_rust::xxh3::xxh3_64(key.as_bytes()).to_le_bytes());
    h[28..36].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    h[36..44].copy_from_slice(&created.to_le_bytes());
    h[44..52].copy_from_slice(&7u64.to_le_bytes());
    h[52..60].copy_from_slice(&9u64.to_le_bytes());
    let header_crc = crc32c::crc32c(&h[8..64]);
    h[4..8].copy_from_slice(&header_crc.to_le_bytes());
    let mut want = h.to_vec();
    want.extend_from_slice(key.as_bytes());
    want.extend_from_slice(&payload);
    want.resize(4096, 0);

    let file = std::fs::read(dir.join("volume_0001.dat")).unwrap();
    let at = loc.offset as usize;
    assert_eq!(&file[at..at + 4096], &want[..], "format 3 without attributes writes the format 2 needle");
}

/// A block bigger than a sector, through the streaming writer: it spans the header sector and
/// the chunk buffer, and the payload after it must land where the header says.
#[test]
fn a_streamed_needle_with_a_block_larger_than_a_sector_reads_back() {
    let dir = temp_dir("bigblock");
    let vm = Arc::new(VolumeManager::open(&dir, 1).unwrap());
    let mut a = Attrs::new();
    a.set(TAG_CONTENT_TYPE, b"application/x-test");
    a.set(0x8001, &pattern(10_000, 5));
    let block = a.encode().unwrap();
    let payload = pattern(3 * 1024 * 1024 + 17, 6);
    let mut w = vm.begin_needle_with_attrs(1, "big-block", &block, payload.len() as u64, AWAN_FLAG_ACTIVE).unwrap();
    for piece in payload.chunks(700_001) {
        w.write(piece).unwrap();
    }
    let loc = w.finish(0, 0).unwrap();
    assert_eq!(vm.read_attrs(loc.offset).unwrap(), a);
    let got = vm.read_needle_bytes(1, loc.offset, loc.payload_crc32c).unwrap();
    assert!(got[..] == payload[..]);
    let mut seen = Vec::new();
    vm.scan_needles(|n| seen.push((n.key, n.flags))).unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, "big-block");
    assert_ne!(seen[0].1 & FLAG_ATTRS, 0, "the header says the needle has a block");
}

/// Superblock of format `version`, with no needles.
fn write_empty_volume(dir: &Path, version: u32) {
    let mut vol = vec![0u8; 4096];
    vol[0..4].copy_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
    vol[4..8].copy_from_slice(&1u32.to_le_bytes());
    vol[8..16].copy_from_slice(&1_700_000_000_000_000_000u64.to_le_bytes());
    let crc = crc32c::crc32c(&vol[0..16]);
    vol[16..20].copy_from_slice(&crc.to_le_bytes());
    vol[20..24].copy_from_slice(&version.to_le_bytes());
    std::fs::write(dir.join("volume_0001.dat"), vol).unwrap();
}

#[tokio::test]
async fn a_format_2_volume_keeps_serving_and_attributes_go_to_a_format_3_volume() {
    let dir = temp_dir("fmt2");
    write_empty_volume(&dir, 2);
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(state.volume_manager.read().clone().unwrap().format(), 2, "an existing volume keeps its format");
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let plain = pattern(1000, 7);
    assert_eq!(c.put(format!("{}/abkt/plain", url)).body(plain.clone()).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(state.volume_ids(), vec![1], "a needle without attributes stays in the format 2 volume");
    let typed = pattern(1000, 8);
    let res = c.put(format!("{}/abkt/typed", url)).header("content-type", "text/html").body(typed.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(state.volume_manager.read().clone().unwrap().format(), FORMAT_VERSION, "the needle with a block went to a new volume");
    assert!(state.volume_ids().len() >= 2);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    assert_object(&c, &format!("{}/abkt/plain", url), "application/octet-stream", &plain, "from the format 2 volume").await;
    assert_object(&c, &format!("{}/abkt/typed", url), "text/html", &typed, "from the format 3 volume").await;
}

/// #199 and #200 together: a streamed PUT with a Content-Type whose key was written meanwhile
/// is copied to the end of the log (decision 51). The copy reads the payload in chunks
/// (`copy_payload`) and must start after the attributes block, not after the key.
#[tokio::test]
async fn a_relocated_streamed_put_keeps_its_block_and_its_bytes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = temp_dir("reloc");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let body = pattern(STREAM_PUT_MIN as usize + 2_000_000, 9);
    let host = url.trim_start_matches("http://").to_string();
    let mut s = tokio::net::TcpStream::connect(&host).await.unwrap();
    let head = format!("PUT /abkt/k HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nContent-Type: image/webp\r\nConnection: close\r\n\r\n", host, body.len());
    s.write_all(head.as_bytes()).await.unwrap();
    let half = body.len() / 2;
    s.write_all(&body[..half]).await.unwrap();
    s.flush().await.unwrap();
    for _ in 0..400 {
        if state.volume_writers_in_flight(1) == Some(1) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(1), "the PUT reserved its needle");
    // Written after the reservation: the streamed PUT must relocate when it commits.
    assert_eq!(c.put(format!("{}/abkt/k", url)).body(pattern(500, 10)).send().await.unwrap().status(), StatusCode::OK);
    s.write_all(&body[half..]).await.unwrap();
    s.flush().await.unwrap();
    let mut answer = Vec::new();
    s.read_to_end(&mut answer).await.unwrap();
    assert!(String::from_utf8_lossy(&answer).starts_with("HTTP/1.1 200"), "{}", String::from_utf8_lossy(&answer));
    assert_eq!(state.stream_relocations.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_object(&c, &format!("{}/abkt/k", url), "image/webp", &body, "after the relocation").await;
    handle.shutdown();
    drop(state);
    let (_state, url, _h) = start(&dir).await;
    assert_object(&c, &format!("{}/abkt/k", url), "image/webp", &body, "after a restart").await;
}
