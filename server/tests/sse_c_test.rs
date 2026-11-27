//! #121 PR 1/3: Server-Side Encryption with Customer-Provided Keys (SSE-C).

use md5::{Digest, Md5};
use reqwest::StatusCode;
use ring::hmac;
use s3_server::bucket_config::base64_encode;
use s3_server::iam::ReloadingIam;
use s3_server::request_log::clf_time;
use s3_server::sse::{
    parse_read_sse_headers, parse_write_sse_headers, SseAttr, HDR_SSE, HDR_SSE_C_ALGO, HDR_SSE_C_KEY,
    HDR_SSE_C_KEY_MD5, HDR_SSE_KMS_KEY_ID,
};
use s3_server::{compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState, ServerHandle, SigV4KeyCache};
use sha2::Sha256;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const KEY_A_B64: &str = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
const MD5_A_B64: &str = "DWygnHRtgiJ77HCm+1rvHw==";
const KEY_B_B64: &str = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=";
const MD5_B_B64: &str = "arxBvwY2V4SiOne6yppVPQ==";

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_ssec_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn spawn(dir: &Path) -> (String, ServerHandle, Arc<S3ServiceState>) {
    let st = Arc::new(S3ServiceState::with_data_dir(dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let h = start_s3_reactor(addr, Arc::clone(&st)).await.unwrap();
    (format!("http://{}", h.local_addr), h, st)
}

async fn spawn_compressed(dir: &Path) -> (String, ServerHandle, Arc<S3ServiceState>) {
    let cfg = dir.join("awanfs.toml");
    std::fs::write(&cfg, "[[buckets]]\nname = \"comp-sse\"\ncompression = \"zstd\"\n").unwrap();
    let iam = ReloadingIam::open(&cfg).unwrap();
    let st = Arc::new(S3ServiceState::with_data_dir(dir));
    *st.iam.write() = Some(iam);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let h = start_s3_reactor(addr, Arc::clone(&st)).await.unwrap();
    (format!("http://{}", h.local_addr), h, st)
}

fn with_ssec(req: reqwest::RequestBuilder, key_b64: &str, md5_b64: &str) -> reqwest::RequestBuilder {
    req.header(HDR_SSE_C_ALGO, "AES256")
        .header(HDR_SSE_C_KEY, key_b64)
        .header(HDR_SSE_C_KEY_MD5, md5_b64)
}

fn with_copy_ssec(req: reqwest::RequestBuilder, key_b64: &str, md5_b64: &str) -> reqwest::RequestBuilder {
    req.header("x-amz-copy-source-server-side-encryption-customer-algorithm", "AES256")
        .header("x-amz-copy-source-server-side-encryption-customer-key", key_b64)
        .header("x-amz-copy-source-server-side-encryption-customer-key-md5", md5_b64)
}

fn md5_hex(data: &[u8]) -> String {
    let digest: [u8; 16] = Md5::digest(data).into();
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

fn pseudo_bytes(len: usize, mut seed: u64) -> Vec<u8> {
    (0..len)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect()
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            collect_files(&p, out);
        } else {
            out.push(p);
        }
    }
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let s = xml.find(&open)? + open.len();
    let e = xml[s..].find(&close)?;
    Some(xml[s..s + e].trim().to_string())
}

#[test]
fn header_conflicts_and_malformed_ssec_are_refused() {
    let mut h = hyper::HeaderMap::new();
    h.insert(HDR_SSE_C_ALGO, "AES256".parse().unwrap());
    h.insert(HDR_SSE_C_KEY, KEY_A_B64.parse().unwrap());
    h.insert(HDR_SSE_C_KEY_MD5, MD5_A_B64.parse().unwrap());
    h.insert(HDR_SSE, "AES256".parse().unwrap());
    assert_eq!(parse_write_sse_headers(&h).unwrap_err().code, "InvalidArgument");

    let mut h = hyper::HeaderMap::new();
    h.insert(HDR_SSE_C_ALGO, "AES256".parse().unwrap());
    h.insert(HDR_SSE_C_KEY, KEY_A_B64.parse().unwrap());
    h.insert(HDR_SSE_C_KEY_MD5, MD5_A_B64.parse().unwrap());
    h.insert(HDR_SSE, "aws:kms".parse().unwrap());
    h.insert(HDR_SSE_KMS_KEY_ID, "key-1".parse().unwrap());
    assert_eq!(parse_write_sse_headers(&h).unwrap_err().code, "InvalidArgument");

    let mut h = hyper::HeaderMap::new();
    h.insert(HDR_SSE, "AES256".parse().unwrap());
    h.insert(HDR_SSE_KMS_KEY_ID, "key-1".parse().unwrap());
    assert_eq!(parse_write_sse_headers(&h).unwrap_err().code, "InvalidArgument");

    let mut h = hyper::HeaderMap::new();
    h.insert(HDR_SSE, "aes:kms".parse().unwrap());
    assert_eq!(parse_write_sse_headers(&h).unwrap_err().code, "InvalidArgument");

    let mut h = hyper::HeaderMap::new();
    h.insert(HDR_SSE, "aws:kms".parse().unwrap());
    assert_eq!(parse_read_sse_headers(&h).unwrap_err().code, "InvalidArgument");

    let key = parse_write_sse_headers(&{
        let mut ok = hyper::HeaderMap::new();
        ok.insert(HDR_SSE_C_ALGO, "AES256".parse().unwrap());
        ok.insert(HDR_SSE_C_KEY, KEY_A_B64.parse().unwrap());
        ok.insert(HDR_SSE_C_KEY_MD5, MD5_A_B64.parse().unwrap());
        ok
    })
    .unwrap()
    .unwrap();
    let dbg = format!("{:?}", key);
    assert!(!dbg.contains(KEY_A_B64));
    assert!(!dbg.contains("a4edeea4"));
}

#[tokio::test]
async fn single_part_ssec_roundtrip_all_boundaries_and_errors() {
    let dir = temp_dir("single");
    let _g = DirGuard(dir.clone());
    let (base, h, _st) = spawn(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/ssec-bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    // Malformed / conflicting HTTP PUT headers -> 400
    let bad = c
        .put(format!("{}/ssec-bkt/bad1", base))
        .header(HDR_SSE_C_ALGO, "AES256")
        .header(HDR_SSE_C_KEY, KEY_A_B64)
        .header(HDR_SSE_C_KEY_MD5, "AAAAAAAAAAAAAAAAAAAAAA==")
        .body("abc")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);

    let conflict = with_ssec(c.put(format!("{}/ssec-bkt/bad2", base)), KEY_A_B64, MD5_A_B64)
        .header(HDR_SSE, "AES256")
        .body("abc")
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::BAD_REQUEST);

    for (idx, size) in [0usize, 1, 13, 64 * 1024, 64 * 1024 + 1, 1024 * 1024, 5 * 1024 * 1024 + 7]
        .into_iter()
        .enumerate()
    {
        let key = format!("obj-{}", idx);
        let url = format!("{}/ssec-bkt/{}", base, key);
        let payload = pseudo_bytes(size, 0x9E37_79B9 + idx as u64);
        let plain_etag = md5_hex(&payload);

        let put_res = with_ssec(c.put(&url), KEY_A_B64, MD5_A_B64)
            .header("content-length", size.to_string())
            .body(payload.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(put_res.status(), StatusCode::OK, "PUT size={}", size);
        assert_eq!(put_res.headers()[HDR_SSE_C_ALGO], "AES256");
        assert_eq!(put_res.headers()[HDR_SSE_C_KEY_MD5], MD5_A_B64);
        let put_etag = put_res.headers()["etag"].to_str().unwrap().trim_matches('"').to_string();
        assert_ne!(put_etag, plain_etag, "SSE-C ETag must not equal plaintext MD5 (size={})", size);

        // GET & HEAD without key -> 400
        assert_eq!(c.get(&url).send().await.unwrap().status(), StatusCode::BAD_REQUEST);
        assert_eq!(c.head(&url).send().await.unwrap().status(), StatusCode::BAD_REQUEST);

        // GET & HEAD with wrong key -> 400 and no bytes leaked
        let wrong = with_ssec(c.get(&url), KEY_B_B64, MD5_B_B64).send().await.unwrap();
        assert_eq!(wrong.status(), StatusCode::BAD_REQUEST);
        let wrong_body = wrong.bytes().await.unwrap();
        assert!(payload.is_empty() || !wrong_body.windows(payload.len().min(32)).any(|w| payload.starts_with(w)));

        // GET with right key -> exact plaintext + headers
        let get_res = with_ssec(c.get(&url), KEY_A_B64, MD5_A_B64).send().await.unwrap();
        assert_eq!(get_res.status(), StatusCode::OK);
        assert_eq!(get_res.headers()[HDR_SSE_C_ALGO], "AES256");
        assert_eq!(get_res.headers()[HDR_SSE_C_KEY_MD5], MD5_A_B64);
        assert_eq!(get_res.headers()["content-length"], size.to_string());
        assert_eq!(get_res.bytes().await.unwrap().as_ref(), payload.as_slice());

        // HEAD with right key
        let head_res = with_ssec(c.head(&url), KEY_A_B64, MD5_A_B64).send().await.unwrap();
        assert_eq!(head_res.status(), StatusCode::OK);
        assert_eq!(head_res.headers()[HDR_SSE_C_ALGO], "AES256");
        assert_eq!(head_res.headers()[HDR_SSE_C_KEY_MD5], MD5_A_B64);
        assert_eq!(head_res.headers()["content-length"], size.to_string());

        // GetObjectAttributes without / with key
        let goa_no = c
            .get(format!("{}?attributes", url))
            .header("x-amz-object-attributes", "ETag,ObjectSize,StorageClass")
            .send()
            .await
            .unwrap();
        assert_eq!(goa_no.status(), StatusCode::BAD_REQUEST);
        let goa_ok = with_ssec(c.get(format!("{}?attributes", url)), KEY_A_B64, MD5_A_B64)
            .header("x-amz-object-attributes", "ETag,ObjectSize,StorageClass")
            .send()
            .await
            .unwrap();
        assert_eq!(goa_ok.status(), StatusCode::OK);
        assert_eq!(goa_ok.headers()[HDR_SSE_C_ALGO], "AES256");
        let xml = goa_ok.text().await.unwrap();
        assert_eq!(extract_tag(&xml, "ObjectSize").as_deref(), Some(size.to_string().as_str()));
        assert_eq!(extract_tag(&xml, "ETag").as_deref(), Some(put_etag.as_str()));
    }

    // Bare aws:kms / AES256 pass-through remains preserved until PR 2 / PR 3.
    let kms_res = c
        .put(format!("{}/ssec-bkt/kms-pass", base))
        .header(HDR_SSE, "aws:kms")
        .body("ok")
        .send()
        .await
        .unwrap();
    assert_eq!(kms_res.status(), StatusCode::OK);
    let kms_get = c.get(format!("{}/ssec-bkt/kms-pass", base)).send().await.unwrap();
    assert_eq!(kms_get.status(), StatusCode::OK);
    assert_eq!(kms_get.text().await.unwrap(), "ok");

    h.shutdown();
}

struct TraceRecorder(Arc<Mutex<Vec<u8>>>);
struct TraceVisitor<'a>(&'a Mutex<Vec<u8>>);
impl<'a> tracing::field::Visit for TraceVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let mut g = self.0.lock().unwrap();
        g.extend_from_slice(format!("{}={:?};", field.name(), value).as_bytes());
    }
}
impl tracing::Subscriber for TraceRecorder {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        span.record(&mut TraceVisitor(&self.0));
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        values.record(&mut TraceVisitor(&self.0));
    }
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut TraceVisitor(&self.0));
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

#[tokio::test]
async fn plaintext_and_customer_key_never_appear_on_disk_or_in_logs() {
    let dir = temp_dir("noleak");
    let _g = DirGuard(dir.clone());
    let logs = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = TraceRecorder(Arc::clone(&logs));
    let _guard = tracing::subscriber::set_default(subscriber);

    let (base, h, _st) = spawn(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/leak-bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let marker = b"AWANFS_UNIQUE_PLAINTEXT_SECRET_MARKER_64BYTES_0123456789ABCDEF!!";
    let mut single_plain = Vec::with_capacity(64 * 1024 + 512);
    while single_plain.len() < 64 * 1024 + 512 {
        single_plain.extend_from_slice(marker);
    }

    assert_eq!(
        with_ssec(c.put(format!("{}/leak-bkt/single", base)), KEY_A_B64, MD5_A_B64)
            .body(single_plain.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Also do a multipart upload with the marker
    let init = with_ssec(c.post(format!("{}/leak-bkt/mp?uploads", base)), KEY_A_B64, MD5_A_B64)
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let uid = extract_tag(&init.text().await.unwrap(), "UploadId").unwrap();
    let up = with_ssec(
        c.put(format!("{}/leak-bkt/mp?partNumber=1&uploadId={}", base, uid)),
        KEY_A_B64,
        MD5_A_B64,
    )
    .body(single_plain.clone())
    .send()
    .await
    .unwrap();
    assert_eq!(up.status(), StatusCode::OK);
    let etag = up.headers()["etag"].to_str().unwrap().to_string();
    let cmp = with_ssec(c.post(format!("{}/leak-bkt/mp?uploadId={}", base, uid)), KEY_A_B64, MD5_A_B64)
        .body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etag))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);

    // Exercise reads too
    assert_eq!(
        with_ssec(c.get(format!("{}/leak-bkt/mp", base)), KEY_A_B64, MD5_A_B64)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    h.shutdown();

    let raw_key = s3_server::bucket_config::base64_decode(KEY_A_B64).unwrap();
    let mut files = Vec::new();
    collect_files(&dir, &mut files);
    assert!(!files.is_empty());
    for f in files {
        let bytes = std::fs::read(&f).unwrap();
        assert!(
            !bytes.windows(marker.len()).any(|w| w == marker),
            "plaintext marker leaked into {}",
            f.display()
        );
        assert!(
            !bytes.windows(raw_key.len()).any(|w| w == raw_key.as_slice()),
            "raw SSE-C key leaked into {}",
            f.display()
        );
        assert!(
            !bytes.windows(KEY_A_B64.len()).any(|w| w == KEY_A_B64.as_bytes()),
            "base64 SSE-C key leaked into {}",
            f.display()
        );
    }

    let log_bytes = logs.lock().unwrap();
    assert!(!log_bytes.windows(marker.len()).any(|w| w == marker));
    assert!(!log_bytes.windows(raw_key.len()).any(|w| w == raw_key.as_slice()));
    assert!(!log_bytes.windows(KEY_A_B64.len()).any(|w| w == KEY_A_B64.as_bytes()));
}

#[tokio::test]
async fn range_get_touches_only_overlapped_chunks() {
    let dir = temp_dir("range");
    let _g = DirGuard(dir.clone());
    let (base, h, st) = spawn(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/rng-bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let data = pseudo_bytes(512 * 1024, 0x1122_3344);
    let url = format!("{}/rng-bkt/obj", base);
    assert_eq!(
        with_ssec(c.put(&url), KEY_A_B64, MD5_A_B64)
            .body(data.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Range spanning end of chunk 0 (65536) and start of chunk 1.
    let res = with_ssec(c.get(&url), KEY_A_B64, MD5_A_B64)
        .header("Range", "bytes=65530-65545")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers()["content-range"], "bytes 65530-65545/524288");
    assert_eq!(res.bytes().await.unwrap().as_ref(), &data[65530..=65545]);

    // Corrupt chunk 5 inside volume_0001.dat (leaving chunks 0..2 untouched): range in chunks 0..2
    // MUST still succeed, proving untouched chunks are neither read nor authenticated for range GETs.
    {
        use std::io::{Read, Seek, SeekFrom, Write};
        let b_id = st.engine.read().lookup_bucket("rng-bkt").unwrap();
        let meta = st.engine.read().get_object(b_id, b"obj").unwrap();
        let (vid, local) = s3_server::split_offset(meta.vol_offset);
        let vol_path = dir.join(format!("volume_{:04}.dat", vid));
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&vol_path).unwrap();
        // Header (64B) + key ("obj" = 3B) + TAG_SSE attrs block + AWE1 header (16B) + 5 chunks
        let mut hbuf = [0u8; 64];
        f.seek(SeekFrom::Start(local)).unwrap();
        f.read_exact(&mut hbuf).unwrap();
        let key_len = u16::from_le_bytes([hbuf[16], hbuf[17]]) as u64;
        let attrs_len = u16::from_le_bytes([hbuf[18], hbuf[19]]) as u64;
        let chunk5_off = local + 64 + key_len + attrs_len + 16 + 5 * (s3_server::sse::AWE1_CHUNK_STORED as u64) + 10;
        let mut byte = [0u8; 1];
        f.seek(SeekFrom::Start(chunk5_off)).unwrap();
        f.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        f.seek(SeekFrom::Start(chunk5_off)).unwrap();
        f.write_all(&byte).unwrap();
        f.sync_all().unwrap();
    }
    let untouched_range = with_ssec(c.get(&url), KEY_A_B64, MD5_A_B64)
        .header("Range", "bytes=65530-65545")
        .send()
        .await
        .unwrap();
    assert_eq!(untouched_range.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(untouched_range.bytes().await.unwrap().as_ref(), &data[65530..=65545]);
    let corrupted_chunk5_range = with_ssec(c.get(&url), KEY_A_B64, MD5_A_B64)
        .header("Range", "bytes=327680-327700")
        .send()
        .await
        .unwrap();
    assert_eq!(corrupted_chunk5_range.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // Streamed large object (> 4 MiB) range check
    let big = pseudo_bytes(5 * 1024 * 1024 + 123, 0x5566_7788);
    let big_url = format!("{}/rng-bkt/big", base);
    assert_eq!(
        with_ssec(c.put(&big_url), KEY_A_B64, MD5_A_B64)
            .body(big.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let b_snap = st.work.snapshot();
    let r_big = with_ssec(c.get(&big_url), KEY_A_B64, MD5_A_B64)
        .header("Range", "bytes=4194290-4194320")
        .send()
        .await
        .unwrap();
    assert_eq!(r_big.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(r_big.bytes().await.unwrap().as_ref(), &big[4194290..=4194320]);
    let b_delta = st.work.snapshot().since(&b_snap);
    assert!(b_delta.disk_reads <= 4, "streamed range GET issued too many disk reads: {}", b_delta.disk_reads);

    // Invalid range -> 416
    assert_eq!(
        with_ssec(c.get(&url), KEY_A_B64, MD5_A_B64)
            .header("Range", "bytes=999999-1000000")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::RANGE_NOT_SATISFIABLE
    );

    h.shutdown();
}

#[tokio::test]
async fn multipart_ssec_resend_distinct_nonce_sparse_parts_and_fsm_refusals() {
    let dir = temp_dir("mp");
    let _g = DirGuard(dir.clone());
    let (base, h, st) = spawn(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/mp-bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let init = with_ssec(c.post(format!("{}/mp-bkt/obj?uploads", base)), KEY_A_B64, MD5_A_B64)
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    assert_eq!(init.headers()[HDR_SSE_C_ALGO], "AES256");
    let uid = extract_tag(&init.text().await.unwrap(), "UploadId").unwrap();

    // Missing / wrong key on UploadPart -> 400
    assert_eq!(
        c.put(format!("{}/mp-bkt/obj?partNumber=1&uploadId={}", base, uid))
            .body("abc")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        with_ssec(
            c.put(format!("{}/mp-bkt/obj?partNumber=1&uploadId={}", base, uid)),
            KEY_B_B64,
            MD5_B_B64,
        )
        .body("abc")
        .send()
        .await
        .unwrap()
        .status(),
        StatusCode::BAD_REQUEST
    );

    // Sparse part numbers 1 (> 4 MiB streamed) & 3 (unaligned buffered 64 KiB + 7B), resending part 3
    let p1 = pseudo_bytes(4 * 1024 * 1024 + 321, 1);
    let p3 = pseudo_bytes(64 * 1024 + 7, 3);
    let r1 = with_ssec(
        c.put(format!("{}/mp-bkt/obj?partNumber=1&uploadId={}", base, uid)),
        KEY_A_B64,
        MD5_A_B64,
    )
    .body(p1.clone())
    .send()
    .await
    .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let e1 = r1.headers()["etag"].to_str().unwrap().to_string();

    let r3a = with_ssec(
        c.put(format!("{}/mp-bkt/obj?partNumber=3&uploadId={}", base, uid)),
        KEY_A_B64,
        MD5_A_B64,
    )
    .body(p3.clone())
    .send()
    .await
    .unwrap();
    assert_eq!(r3a.status(), StatusCode::OK);
    let e3a = r3a.headers()["etag"].to_str().unwrap().to_string();

    let part3_file = st.multipart.staging_dir.join(&uid).join("part_00003.dat");
    let hdr_before = std::fs::read(&part3_file).unwrap()[..16].to_vec();

    // Resend part 3: same ETag returned, distinct nonce_prefix stored on disk
    let r3b = with_ssec(
        c.put(format!("{}/mp-bkt/obj?partNumber=3&uploadId={}", base, uid)),
        KEY_A_B64,
        MD5_A_B64,
    )
    .body(p3.clone())
    .send()
    .await
    .unwrap();
    assert_eq!(r3b.status(), StatusCode::OK);
    let e3b = r3b.headers()["etag"].to_str().unwrap().to_string();
    assert_eq!(e3a, e3b);
    let hdr_after = std::fs::read(&part3_file).unwrap()[..16].to_vec();
    assert_ne!(&hdr_before[12..16], &hdr_after[12..16], "resent part must get fresh nonce_prefix");

    let complete_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>3</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        e1, e3a
    );
    // Complete without / wrong SSE-C headers -> 400
    assert_eq!(
        c.post(format!("{}/mp-bkt/obj?uploadId={}", base, uid))
            .body(complete_xml.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        with_ssec(
            c.post(format!("{}/mp-bkt/obj?uploadId={}", base, uid)),
            KEY_B_B64,
            MD5_B_B64,
        )
        .body(complete_xml.clone())
        .send()
        .await
        .unwrap()
        .status(),
        StatusCode::BAD_REQUEST
    );

    let cmp = with_ssec(
        c.post(format!("{}/mp-bkt/obj?uploadId={}", base, uid)),
        KEY_A_B64,
        MD5_A_B64,
    )
    .body(complete_xml)
    .send()
    .await
    .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);
    assert_eq!(cmp.headers()[HDR_SSE_C_ALGO], "AES256");

    let mut want = p1.clone();
    want.extend_from_slice(&p3);

    // Full GET
    let g = with_ssec(c.get(format!("{}/mp-bkt/obj", base)), KEY_A_B64, MD5_A_B64)
        .send()
        .await
        .unwrap();
    assert_eq!(g.status(), StatusCode::OK);
    assert_eq!(g.bytes().await.unwrap().as_ref(), want.as_slice());

    // Range spanning boundary between part 1 and sparse part 3
    let mid = p1.len() as u64;
    let rng = with_ssec(c.get(format!("{}/mp-bkt/obj", base)), KEY_A_B64, MD5_A_B64)
        .header("Range", format!("bytes={}-{}", mid - 50, mid + 50))
        .send()
        .await
        .unwrap();
    assert_eq!(rng.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        rng.bytes().await.unwrap().as_ref(),
        &want[(mid - 50) as usize..=(mid + 50) as usize]
    );

    // ?partNumber=1 and ?partNumber=2 with right key, and ?partNumber=5 without headers -> InvalidPart
    let gp2 = with_ssec(c.get(format!("{}/mp-bkt/obj?partNumber=2", base)), KEY_A_B64, MD5_A_B64)
        .send()
        .await
        .unwrap();
    assert_eq!(gp2.status(), StatusCode::OK);
    assert_eq!(gp2.headers()["x-amz-mp-parts-count"], "2");
    assert_eq!(gp2.bytes().await.unwrap().as_ref(), p3.as_slice());

    let gp5 = c.get(format!("{}/mp-bkt/obj?partNumber=5", base)).send().await.unwrap();
    assert_eq!(gp5.status(), StatusCode::BAD_REQUEST);
    let err_xml = gp5.text().await.unwrap();
    assert_eq!(extract_tag(&err_xml, "Code").as_deref(), Some("InvalidPart"));

    // Unexpected SSE-C header on unencrypted multipart upload -> 400
    let plain_init = c.post(format!("{}/mp-bkt/plain?uploads", base)).send().await.unwrap();
    let puid = extract_tag(&plain_init.text().await.unwrap(), "UploadId").unwrap();
    assert_eq!(
        with_ssec(
            c.put(format!("{}/mp-bkt/plain?partNumber=1&uploadId={}", base, puid)),
            KEY_A_B64,
            MD5_A_B64,
        )
        .body("abc")
        .send()
        .await
        .unwrap()
        .status(),
        StatusCode::BAD_REQUEST
    );

    h.shutdown();
}

#[tokio::test]
async fn copy_object_and_upload_part_copy_matrix_including_streamed_and_self_copy() {
    let dir = temp_dir("copy");
    let _g = DirGuard(dir.clone());
    let (base, h, _st) = spawn(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/cp-bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let big = pseudo_bytes(4 * 1024 * 1024 + 500, 0xABCD);
    assert_eq!(
        c.put(format!("{}/cp-bkt/plain-big", base))
            .body(big.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // 1. unencrypted -> sse-c(A) (> 4 MiB streamed)
    let c1 = with_ssec(c.put(format!("{}/cp-bkt/enc-a", base)), KEY_A_B64, MD5_A_B64)
        .header("x-amz-copy-source", "/cp-bkt/plain-big")
        .send()
        .await
        .unwrap();
    assert_eq!(c1.status(), StatusCode::OK);
    assert_eq!(c1.headers()[HDR_SSE_C_ALGO], "AES256");

    // 2. sse-c(A) -> sse-c(B) key rotation without source key must fail 400
    let fail = with_ssec(c.put(format!("{}/cp-bkt/enc-b", base)), KEY_B_B64, MD5_B_B64)
        .header("x-amz-copy-source", "/cp-bkt/enc-a")
        .send()
        .await
        .unwrap();
    assert_eq!(fail.status(), StatusCode::BAD_REQUEST);

    // 3. sse-c(A) -> sse-c(B) self-copy then cross-key copy
    let ok_b = with_copy_ssec(
        with_ssec(c.put(format!("{}/cp-bkt/enc-b", base)), KEY_B_B64, MD5_B_B64),
        KEY_A_B64,
        MD5_A_B64,
    )
    .header("x-amz-copy-source", "/cp-bkt/enc-a")
    .send()
    .await
    .unwrap();
    assert_eq!(ok_b.status(), StatusCode::OK);

    // Self-copy enc-a -> enc-a with rotated key B
    let self_rot = with_copy_ssec(
        with_ssec(c.put(format!("{}/cp-bkt/enc-a", base)), KEY_B_B64, MD5_B_B64),
        KEY_A_B64,
        MD5_A_B64,
    )
    .header("x-amz-copy-source", "/cp-bkt/enc-a")
    .send()
    .await
    .unwrap();
    assert_eq!(self_rot.status(), StatusCode::OK);
    assert_eq!(
        with_ssec(c.get(format!("{}/cp-bkt/enc-a", base)), KEY_B_B64, MD5_B_B64)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        big.as_slice()
    );

    // 4. sse-c(B) -> unencrypted
    let to_plain = with_copy_ssec(c.put(format!("{}/cp-bkt/dec", base)), KEY_B_B64, MD5_B_B64)
        .header("x-amz-copy-source", "/cp-bkt/enc-b")
        .send()
        .await
        .unwrap();
    assert_eq!(to_plain.status(), StatusCode::OK);
    assert_eq!(
        c.get(format!("{}/cp-bkt/dec", base)).send().await.unwrap().bytes().await.unwrap().as_ref(),
        big.as_slice()
    );

    // 5. UploadPartCopy from sse-c(B) slice into sse-c(A) multipart upload
    let init = with_ssec(c.post(format!("{}/cp-bkt/upc?uploads", base)), KEY_A_B64, MD5_A_B64)
        .send()
        .await
        .unwrap();
    let uid = extract_tag(&init.text().await.unwrap(), "UploadId").unwrap();
    let upc = with_copy_ssec(
        with_ssec(
            c.put(format!("{}/cp-bkt/upc?partNumber=1&uploadId={}", base, uid)),
            KEY_A_B64,
            MD5_A_B64,
        ),
        KEY_B_B64,
        MD5_B_B64,
    )
    .header("x-amz-copy-source", "/cp-bkt/enc-b")
    .header("x-amz-copy-source-range", "bytes=1000-70999")
    .send()
    .await
    .unwrap();
    assert_eq!(upc.status(), StatusCode::OK);
    let et = upc.headers()["etag"].to_str().unwrap().to_string();
    let cmp = with_ssec(c.post(format!("{}/cp-bkt/upc?uploadId={}", base, uid)), KEY_A_B64, MD5_A_B64)
        .body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", et))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);
    let got = with_ssec(c.get(format!("{}/cp-bkt/upc", base)), KEY_A_B64, MD5_A_B64)
        .send()
        .await
        .unwrap();
    assert_eq!(got.bytes().await.unwrap().as_ref(), &big[1000..=70999]);

    h.shutdown();
}

#[tokio::test]
async fn restart_recovery_compaction_and_zstd_skip_preserve_ssec() {
    let dir = temp_dir("restart");
    let _g = DirGuard(dir.clone());
    let payload = b"compressible-sse-text-".repeat(4000);

    {
        let (base, h, st) = spawn_compressed(&dir).await;
        st.set_volume_span(512 * 1024);
        let c = reqwest::Client::new();
        assert_eq!(c.put(format!("{}/comp-sse", base)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(
            with_ssec(c.put(format!("{}/comp-sse/enc", base)), KEY_A_B64, MD5_A_B64)
                .body(payload.clone())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        // SSE-C object never cached in payload_cache and never zstd-compressed
        assert_eq!(st.payload_cache.len(), 0);
        let meta = st.engine.read().get_object(st.engine.read().lookup_bucket("comp-sse").unwrap(), b"enc").unwrap();
        let flags = s3_server::meta_index::unpack_flags(meta.bucket_flags);
        assert_eq!(flags & s3_server::meta_index::FLAG_COMPRESSED, 0);

        // Overwrite another large blob to create dead space across rolled volumes and compact
        let filler = pseudo_bytes(350 * 1024, 99);
        assert_eq!(c.put(format!("{}/comp-sse/fill", base)).body(filler.clone()).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.put(format!("{}/comp-sse/fill", base)).body(filler).send().await.unwrap().status(), StatusCode::OK);
        let report = st.perform_online_compaction().await.unwrap();
        assert!(report.live_needles_copied >= 1);
        h.shutdown();
    }

    // Reopen server on same data_dir
    let (base2, h2, st2) = spawn_compressed(&dir).await;
    let c = reqwest::Client::new();
    let list_xml = c.get(format!("{}/comp-sse?list-type=2&prefix=enc", base2)).send().await.unwrap().text().await.unwrap();
    assert_eq!(extract_tag(&list_xml, "Size").as_deref(), Some(payload.len().to_string().as_str()));
    let got = with_ssec(c.get(format!("{}/comp-sse/enc", base2)), KEY_A_B64, MD5_A_B64)
        .send()
        .await
        .unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert_eq!(got.bytes().await.unwrap().as_ref(), payload.as_slice());
    assert_eq!(c.get(format!("{}/comp-sse/enc", base2)).send().await.unwrap().status(), StatusCode::BAD_REQUEST);
    let _ = st2;
    h2.shutdown();
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, msg).as_ref().to_vec()
}

fn ymd_hms(secs: u64) -> (String, String) {
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    (format!("{}{:02}{}", &t[7..11], m, &t[0..2]), format!("{}{}{}", &t[12..14], &t[15..17], &t[18..20]))
}

#[tokio::test]
async fn test_encryption_sse_c_post_object_authenticated_request() {
    let ak = "AKPOSTSSEC00";
    let sk = "post-ssec-secret";
    let mut state = S3ServiceState::new();
    state.auth = AuthConfig {
        credentials: Some((ak.into(), sk.into())),
        allow_anonymous: false,
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(state)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let (d, t) = ymd_hms(now_secs);
    let date = format!("{}T{}Z", d, t);
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);

    // Create bucket with SigV4
    let empty_hash = sha256_hex(b"");
    let canon = format!(
        "PUT\n/post-ssec\n\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\n{}",
        host, empty_hash, date, empty_hash
    );
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canon.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), sk, &date[..8], "us-east-1", "s3", &sts);
    assert_eq!(
        c.put(format!("http://{}/post-ssec", host))
            .header("x-amz-date", &date)
            .header("x-amz-content-sha256", &empty_hash)
            .header(
                "authorization",
                format!("AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}", ak, scope, sig),
            )
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let (ed, et) = ymd_hms(now_secs + 6000);
    let exp = format!("{}-{}-{}T{}:{}:{}Z", &ed[0..4], &ed[4..6], &ed[6..8], &et[0..2], &et[2..4], &et[4..6]);
    let cred = format!("{}/{}", ak, scope);
    let policy_json = format!(
        r#"{{"expiration":"{}","conditions":[{{"bucket":"post-ssec"}},["starts-with","$key","foo"],{{"acl":"private"}},["starts-with","$Content-Type","text/plain"],["starts-with","$x-amz-server-side-encryption-customer-algorithm",""],["starts-with","$x-amz-server-side-encryption-customer-key",""],["starts-with","$x-amz-server-side-encryption-customer-key-md5",""],{{"x-amz-algorithm":"AWS4-HMAC-SHA256"}},{{"x-amz-credential":"{}"}},{{"x-amz-date":"{}"}},["content-length-range",0,1024]]}}"#,
        exp, cred, date
    );
    let policy_b64 = base64_encode(policy_json.as_bytes());
    let k1 = hmac_sha256(format!("AWS4{}", sk).as_bytes(), &date.as_bytes()[..8]);
    let k2 = hmac_sha256(&k1, b"us-east-1");
    let k3 = hmac_sha256(&k2, b"s3");
    let skey = hmac_sha256(&k3, b"aws4_request");
    let form_sig: String = hmac_sha256(&skey, policy_b64.as_bytes())
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();

    let boundary = "ssecpostboundary";
    let mut body = Vec::new();
    for (k, v) in [
        ("key", "foo.txt"),
        ("acl", "private"),
        ("Content-Type", "text/plain"),
        (HDR_SSE_C_ALGO, "AES256"),
        (HDR_SSE_C_KEY, KEY_A_B64),
        (HDR_SSE_C_KEY_MD5, MD5_A_B64),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-credential", cred.as_str()),
        ("x-amz-date", date.as_str()),
        ("policy", policy_b64.as_str()),
        ("x-amz-signature", form_sig.as_str()),
    ] {
        body.extend_from_slice(format!("--{}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n{}\r\n", boundary, k, v).as_bytes());
    }
    body.extend_from_slice(format!("--{}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"bar\"\r\n\r\nbar\r\n--{}--\r\n", boundary, boundary).as_bytes());

    let post_res = c
        .post(format!("http://{}/post-ssec", host))
        .header("content-type", format!("multipart/form-data; boundary={}", boundary))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(post_res.status(), StatusCode::NO_CONTENT);

    // Signed GET without SSE-C -> 400; with SSE-C -> 200 "bar"
    let c_no = format!(
        "GET\n/post-ssec/foo.txt\n\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\n{}",
        host, empty_hash, date, empty_hash
    );
    let sts_no = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(c_no.as_bytes()));
    let sig_no = compute_sigv4_signature(&SigV4KeyCache::new(), sk, &date[..8], "us-east-1", "s3", &sts_no);
    assert_eq!(
        c.get(format!("http://{}/post-ssec/foo.txt", host))
            .header("x-amz-date", &date)
            .header("x-amz-content-sha256", &empty_hash)
            .header("authorization", format!("AWS4-HMAC-SHA256 Credential={},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}", cred, sig_no))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );

    let c_ok = format!(
        "GET\n/post-ssec/foo.txt\n\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n{}:{}\n{}:{}\n{}:{}\n\nhost;x-amz-content-sha256;x-amz-date;{};{};{}\n{}",
        host, empty_hash, date, HDR_SSE_C_ALGO, "AES256", HDR_SSE_C_KEY, KEY_A_B64, HDR_SSE_C_KEY_MD5, MD5_A_B64,
        HDR_SSE_C_ALGO, HDR_SSE_C_KEY, HDR_SSE_C_KEY_MD5, empty_hash
    );
    let sts_ok = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(c_ok.as_bytes()));
    let sig_ok = compute_sigv4_signature(&SigV4KeyCache::new(), sk, &date[..8], "us-east-1", "s3", &sts_ok);
    let get_ok = with_ssec(c.get(format!("http://{}/post-ssec/foo.txt", host)), KEY_A_B64, MD5_A_B64)
        .header("x-amz-date", &date)
        .header("x-amz-content-sha256", &empty_hash)
        .header(
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={},SignedHeaders=host;x-amz-content-sha256;x-amz-date;{};{};{},Signature={}",
                cred, HDR_SSE_C_ALGO, HDR_SSE_C_KEY, HDR_SSE_C_KEY_MD5, sig_ok
            ),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(get_ok.status(), StatusCode::OK);
    assert_eq!(get_ok.text().await.unwrap(), "bar");

    let _ = (SseAttr::plaintext_size_from_raw, Sha256::new());
    handle.shutdown();
}
