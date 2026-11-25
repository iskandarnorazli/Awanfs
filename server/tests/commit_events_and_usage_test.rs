//! Tests for #159: commit event stream, atomic bucket usage counters, and bucket namespace hooks.

use s3_server::events::{CommitEvent, CommitKind, SUBSCRIBER_QUEUE_CAP};
use s3_server::meta_index::BucketUsage;
use s3_server::s3_api::{
    compute_md5, render_metrics, BucketUsageReport, OpenLimits, S3ServiceState,
    PER_BUCKET_METRICS_CARDINALITY_CAP,
};
use s3_server::{start_s3_reactor_with, ServerHandle};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "awanfs_159_{}_{}_{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start_server(state: Arc<S3ServiceState>) -> (ServerHandle, SocketAddr) {
    let handle = start_s3_reactor_with("127.0.0.1:0".parse().unwrap(), state, None)
        .await
        .expect("start reactor");
    let addr = handle.local_addr;
    (handle, addr)
}

async fn raw_http(
    addr: SocketAddr,
    method: &str,
    path_and_query: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, HashMap<String, String>, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut head = format!(
        "{} {} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\nContent-Length: {}\r\n",
        method,
        path_and_query,
        addr.port(),
        body.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{}: {}\r\n", k, v));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    if !body.is_empty() {
        stream.write_all(body).await.unwrap();
    }
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("HTTP headers end");
    let hdr_str = std::str::from_utf8(&buf[..split]).unwrap();
    let mut lines = hdr_str.split("\r\n");
    let status_line = lines.next().unwrap();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut hdrs = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            hdrs.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let body_slice = &buf[split + 4..];
    let decoded_body = if hdrs
        .get("transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        let mut out = Vec::new();
        let mut pos = 0;
        loop {
            let nl = body_slice[pos..]
                .windows(2)
                .position(|w| w == b"\r\n")
                .unwrap();
            let sz_str = std::str::from_utf8(&body_slice[pos..pos + nl]).unwrap();
            let sz = usize::from_str_radix(sz_str.trim(), 16).unwrap();
            pos += nl + 2;
            if sz == 0 {
                break;
            }
            out.extend_from_slice(&body_slice[pos..pos + sz]);
            pos += sz + 2;
        }
        out
    } else {
        body_slice.to_vec()
    };
    (status, hdrs, decoded_body)
}

fn scan_usage_oracle(state: &S3ServiceState, b_id: u32) -> BucketUsageReport {
    let items = state.engine.read().list_keys(b_id, b"", b"", usize::MAX);
    let current = BucketUsage {
        objects: items.len() as u64,
        bytes: items.iter().map(|(_, m)| m.size).sum(),
    };
    let mut all_objects = 0u64;
    let mut all_bytes = 0u64;
    let mut seen_vs = std::collections::HashSet::new();
    let vs = state.versions.read();
    for key in vs.keys_of(b_id, "") {
        seen_vs.insert(key.clone());
        if let Some(list) = vs.versions(b_id, &key) {
            for v in list {
                if !v.marker {
                    all_objects += 1;
                    all_bytes += v.size;
                }
            }
        }
    }
    for (k, m) in items {
        let ks = String::from_utf8_lossy(&k).into_owned();
        if !seen_vs.contains(&ks) {
            all_objects += 1;
            all_bytes += m.size;
        }
    }
    BucketUsageReport {
        current,
        all_versions: BucketUsage {
            objects: all_objects,
            bytes: all_bytes,
        },
    }
}

struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn gen_range(&mut self, upper: usize) -> usize {
        (self.next_u64() as usize) % upper
    }
}

#[tokio::test]
async fn commit_events_concurrent_order_and_final_state_match_index() {
    let dir = temp_dir("concurrent_events");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let sub = state.events.subscribe("recorder");
    let (_server, addr) = start_server(Arc::clone(&state)).await;

    let (st, _, _) = raw_http(addr, "PUT", "/ev-bkt-a", &[], &[]).await;
    assert_eq!(st, 200);
    let (st, _, _) = raw_http(addr, "PUT", "/ev-bkt-b", &[], &[]).await;
    assert_eq!(st, 200);

    let mut handles = Vec::new();
    for worker in 0..4usize {
        let addr_c = addr;
        handles.push(tokio::spawn(async move {
            let mut rng = Rng(0x9e37_79b9_7f4a_7c15u64 ^ ((worker as u64 + 1) * 17));
            for step in 0..30usize {
                let bucket = if step % 3 == 0 { "ev-bkt-b" } else { "ev-bkt-a" };
                let key = format!("k{}", rng.gen_range(8));
                let sz = (rng.gen_range(256) + 1) as usize;
                let payload = vec![b'a' + (worker as u8); sz];
                let path = format!("/{}/{}", bucket, key);
                if rng.gen_range(4) == 0 {
                    let (s, hdrs, _) = raw_http(addr_c, "DELETE", &path, &[], &[]).await;
                    assert!(s == 204 || s == 200);
                    assert!(hdrs.contains_key("x-amz-request-id"));
                } else {
                    let (s, hdrs, _) = raw_http(addr_c, "PUT", &path, &[], &payload).await;
                    assert_eq!(s, 200);
                    assert!(hdrs.contains_key("x-amz-request-id"));
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Exercise CopyObject, MultipartComplete, Attribute update, and Bucket config events too.
    let copy_hdrs = [("x-amz-copy-source", "/ev-bkt-a/k0")];
    let _ = raw_http(addr, "PUT", "/ev-bkt-a/k0", &[], b"seed-copy-body").await;
    let (st, _, _) = raw_http(addr, "PUT", "/ev-bkt-a/copied", &copy_hdrs, &[]).await;
    assert_eq!(st, 200);
    let tagging_xml = "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    let (st, _, _) = raw_http(addr, "PUT", "/ev-bkt-a/copied?tagging", &[], tagging_xml.as_bytes()).await;
    assert_eq!(st, 200);
    let (st, _, _) = raw_http(addr, "PUT", "/ev-bkt-a?tagging", &[], tagging_xml.as_bytes()).await;
    assert_eq!(st, 204);
    let (_, _, mp_init) = raw_http(addr, "POST", "/ev-bkt-a/mp-key?uploads", &[], &[]).await;
    let uid = std::str::from_utf8(&mp_init)
        .unwrap()
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();
    let part = vec![b'z'; 64];
    let (st, phdrs, _) = raw_http(
        addr,
        "PUT",
        &format!("/ev-bkt-a/mp-key?partNumber=1&uploadId={}", uid),
        &[],
        &part,
    )
    .await;
    assert_eq!(st, 200);
    let etag1 = phdrs.get("etag").unwrap().clone();
    let cmp_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etag1
    );
    let (st, _, _) = raw_http(
        addr,
        "POST",
        &format!("/ev-bkt-a/mp-key?uploadId={}", uid),
        &[],
        cmp_xml.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);

    let events = sub.drain();
    assert!(!events.is_empty());

    // Check strict per-bucket seq order and replay object state against index.
    let mut last_seq: HashMap<String, u64> = HashMap::new();
    let mut replayed: HashMap<String, BTreeMap<String, (u64, String)>> = HashMap::new();
    let mut saw_kinds = std::collections::HashSet::new();

    for ev in &events {
        saw_kinds.insert(ev.kind);
        assert!(!ev.request_id.is_empty(), "event missing request_id: {:?}", ev);
        assert!(!ev.principal.is_empty(), "event missing principal: {:?}", ev);
        let prev = last_seq.get(&ev.bucket).copied().unwrap_or(0);
        assert_eq!(ev.seq, prev + 1, "per-bucket seq gap/inversion for {:?}", ev);
        last_seq.insert(ev.bucket.clone(), ev.seq);

        let bmap = replayed.entry(ev.bucket.clone()).or_default();
        match ev.kind {
            CommitKind::Put | CommitKind::Copy | CommitKind::MultipartComplete => {
                bmap.insert(ev.key.clone(), (ev.size, ev.etag.clone()));
            }
            CommitKind::Delete => {
                bmap.remove(&ev.key);
            }
            CommitKind::AttrUpdate
            | CommitKind::BucketCreate
            | CommitKind::BucketConfig
            | CommitKind::BucketVersioning
            | CommitKind::BucketDelete
            | CommitKind::DeleteMarker
            | CommitKind::VersionDelete => {}
        }
    }

    assert!(saw_kinds.contains(&CommitKind::BucketCreate));
    assert!(saw_kinds.contains(&CommitKind::BucketConfig));
    assert!(saw_kinds.contains(&CommitKind::Put));
    assert!(saw_kinds.contains(&CommitKind::Copy));
    assert!(saw_kinds.contains(&CommitKind::MultipartComplete));
    assert!(saw_kinds.contains(&CommitKind::AttrUpdate));

    for bname in ["ev-bkt-a", "ev-bkt-b"] {
        let b_id = state.engine.read().lookup_bucket(bname).unwrap();
        let actual: BTreeMap<String, u64> = state
            .engine
            .read()
            .list_keys(b_id, b"", b"", usize::MAX)
            .into_iter()
            .map(|(k, m)| (String::from_utf8(k).unwrap(), m.size))
            .collect();
        let expected: BTreeMap<String, u64> = replayed
            .get(bname)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, (sz, _))| (k, sz))
            .collect();
        assert_eq!(actual, expected, "replayed commit events disagree with index for {}", bname);
    }

    drop(sub);
    assert!(!state.events.has_subscribers(), "has_subscribers must drop to false with 0 subscribers");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn failed_requests_emit_no_commit_events() {
    let state = Arc::new(S3ServiceState::new());
    state.engine.write().max_objects = 2;
    let (_server, addr) = start_server(Arc::clone(&state)).await;
    assert_eq!(raw_http(addr, "PUT", "/fail-bkt", &[], &[]).await.0, 200);
    assert_eq!(raw_http(addr, "PUT", "/fail-bkt/k1", &[], b"hello").await.0, 200);

    // Subscribe AFTER seeding so only test failures run under subscription.
    let sub = state.events.subscribe("fail-watch");

    // 1. Precondition failure (If-None-Match: * on existing key)
    let (st, _, _) = raw_http(addr, "PUT", "/fail-bkt/k1", &[("if-none-match", "*")], b"nope").await;
    assert_eq!(st, 412);

    // 2. Bad Content-MD5
    let (st, _, _) = raw_http(addr, "PUT", "/fail-bkt/k2", &[("content-md5", "AAAAAAAAAAAAAAAAAAAAAA==")], b"bad").await;
    assert_eq!(st, 400);

    // 3. CopyObject from missing source
    let (st, _, _) = raw_http(addr, "PUT", "/fail-bkt/k2", &[("x-amz-copy-source", "/fail-bkt/missing")], &[]).await;
    assert_eq!(st, 404);

    // 4. Delete missing unversioned key
    let (st, _, _) = raw_http(addr, "DELETE", "/fail-bkt/missing", &[], &[]).await;
    assert!(st == 204 || st == 404);

    // 5. PutObjectTagging on missing key
    let tag_xml = "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>";
    let (st, _, _) = raw_http(addr, "PUT", "/fail-bkt/missing?tagging", &[], tag_xml.as_bytes()).await;
    assert_eq!(st, 404);

    // 6. Malformed bucket tagging XML
    let (st, _, _) = raw_http(addr, "PUT", "/fail-bkt?tagging", &[], b"<broken").await;
    assert_eq!(st, 400);

    // 7. CreateBucket invalid name + idempotent re-create emit 0 events
    let (st, _, _) = raw_http(addr, "PUT", "/Invalid_Bucket", &[], &[]).await;
    assert_eq!(st, 400);
    let (st, _, _) = raw_http(addr, "PUT", "/fail-bkt", &[], &[]).await;
    assert_eq!(st, 200);

    assert!(sub.drain().is_empty(), "failed or no-op requests must emit zero events");

    // Verify 1 valid PUT emits 1 event, then capacity limit error emits 0 events.
    assert_eq!(raw_http(addr, "PUT", "/fail-bkt/k2", &[], b"ok2").await.0, 200);
    let evs = sub.drain();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].kind, CommitKind::Put);
    assert_eq!(evs[0].key, "k2");

    // Engine capacity overflow (max_objects = 2 without spill)
    let (st, _, _) = raw_http(addr, "PUT", "/fail-bkt/k3", &[], b"overflow").await;
    assert_ne!(st, 200);
    assert!(sub.drain().is_empty(), "capacity failure must emit zero events");
}

#[tokio::test]
async fn usage_counters_match_oracle_through_ops_spill_compaction_versioning_and_restart() {
    let dir = temp_dir("usage_oracle");
    let limits = OpenLimits {
        max_objects: 6,
        spill_cache_bytes: Some(1 << 20),
    };
    let state = Arc::new(
        S3ServiceState::try_with_data_dir_limits(&dir, Arc::new(s3_server::clock::SystemClock), limits).unwrap(),
    );
    let b_unver = state.engine.write().create_bucket("u-unver").unwrap();
    let _ = state.bucket_configs.lock().apply_bucket_create(b_unver, "u-unver", 10, "root", "");
    let b_ver = state.engine.write().create_bucket("u-ver").unwrap();
    let _ = state.bucket_configs.lock().apply_bucket_create(b_ver, "u-ver", 20, "root", "");
    state.set_versioning(b_ver, s3_server::versions::Versioning::Enabled).unwrap();

    let (_server, addr) = start_server(Arc::clone(&state)).await;
    let mut rng = Rng(0x517c_c1b7_2722_0a95u64);

    for step in 0..80usize {
        let bucket = if step % 2 == 0 { "u-unver" } else { "u-ver" };
        let b_id = if step % 2 == 0 { b_unver } else { b_ver };
        let key = format!("obj-{}", rng.gen_range(12)); // pushes past max_objects=6 into spill
        let path = format!("/{}/{}", bucket, key);
        match rng.gen_range(5) {
            0..=2 => {
                let len = if step % 11 == 0 { 0 } else { (rng.gen_range(300) + 1) as usize };
                let body = vec![b'x'; len];
                let (st, _, _) = raw_http(addr, "PUT", &path, &[], &body).await;
                assert_eq!(st, 200);
            }
            3 => {
                let (st, _, _) = raw_http(addr, "DELETE", &path, &[], &[]).await;
                assert!(st == 204 || st == 200);
            }
            _ => {
                // In versioned bucket, delete oldest noncurrent or current version by id if any
                let vid_opt = state
                    .versions
                    .read()
                    .versions(b_id, &key)
                    .and_then(|l| l.last().map(|v| v.id_string()));
                if let Some(vid) = vid_opt {
                    let (st, _, _) = raw_http(addr, "DELETE", &format!("{}?versionId={}", path, vid), &[], &[]).await;
                    assert!(st == 204 || st == 200);
                } else {
                    let body = vec![b'y'; 42];
                    let (st, _, _) = raw_http(addr, "PUT", &path, &[], &body).await;
                    assert_eq!(st, 200);
                }
            }
        }
        assert_eq!(state.bucket_usage(b_id).unwrap(), scan_usage_oracle(&state, b_id), "mismatch at step {}", step);
    }

    // Trigger compaction and check again.
    let _ = state.perform_online_compaction().await.unwrap();
    assert_eq!(state.bucket_usage(b_unver).unwrap(), scan_usage_oracle(&state, b_unver));
    assert_eq!(state.bucket_usage(b_ver).unwrap(), scan_usage_oracle(&state, b_ver));

    // Check distinct current vs all_versions counts explicitly on a fresh versioned key.
    assert_eq!(raw_http(addr, "PUT", "/u-ver/pair", &[], b"12345").await.0, 200);
    assert_eq!(raw_http(addr, "PUT", "/u-ver/pair", &[], b"1234567890").await.0, 200);
    let before_del = state.bucket_usage(b_ver).unwrap();
    assert_eq!(raw_http(addr, "DELETE", "/u-ver/pair", &[], &[]).await.0, 204);
    let after_dm = state.bucket_usage(b_ver).unwrap();
    assert_eq!(after_dm.current.objects, before_del.current.objects - 1);
    assert_eq!(after_dm.current.bytes, before_del.current.bytes - 10);
    assert_eq!(after_dm.all_versions, before_del.all_versions);

    let unver_before = state.bucket_usage(b_unver).unwrap();
    let ver_before = state.bucket_usage(b_ver).unwrap();

    // Restart from disk with the same OpenLimits and verify recovery rebuilds exact counters!
    drop(_server);
    drop(state);
    let recovered = Arc::new(
        S3ServiceState::try_with_data_dir_limits(&dir, Arc::new(s3_server::clock::SystemClock), limits).unwrap(),
    );
    let r_unver = recovered.engine.read().lookup_bucket("u-unver").unwrap();
    let r_ver = recovered.engine.read().lookup_bucket("u-ver").unwrap();
    assert_eq!(recovered.bucket_usage(r_unver).unwrap(), unver_before);
    assert_eq!(recovered.bucket_usage(r_ver).unwrap(), ver_before);
    assert_eq!(recovered.bucket_usage(r_unver).unwrap(), scan_usage_oracle(&recovered, r_unver));
    assert_eq!(recovered.bucket_usage(r_ver).unwrap(), scan_usage_oracle(&recovered, r_ver));

    // Unused variable cleanup
    let _ = compute_md5;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn stalled_subscriber_does_not_block_commits_and_counts_drops() {
    let dir = temp_dir("stalled_sub");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let b_id = state.engine.write().create_bucket("fast-bkt").unwrap();

    let stalled = state.events.subscribe("stalled");
    let healthy = state.events.subscribe("healthy");

    let total = SUBSCRIBER_QUEUE_CAP + 250;
    for i in 0..total {
        state.events.emit_with(b_id, |seq| CommitEvent {
            seq,
            time: i as u64,
            bucket: "fast-bkt".to_string(),
            key: format!("k{}", i),
            version: None,
            kind: CommitKind::Put,
            size: 16,
            etag: "00".to_string(),
            principal: "root".to_string(),
            request_id: "req".to_string(),
        });
        if i % 512 == 0 {
            let _ = healthy.drain();
        }
    }

    let mut received_healthy = 0usize;
    // Actually we already drained periodically above; check total drops vs stalled directly:
    received_healthy += healthy.drain().len();
    assert!(received_healthy > 0);
    assert_eq!(healthy.dropped(), 0);
    assert_eq!(stalled.dropped(), 250);
    assert_eq!(state.events.dropped("stalled"), 250);

    let metrics_text = render_metrics(&state);
    assert!(
        metrics_text.contains("awanfs_commit_events_dropped_total{subscriber=\"stalled\"} 250"),
        "expected drop metric in /_awanfs/metrics, got:\n{}",
        metrics_text
    );
    drop(stalled);
    drop(healthy);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn namespace_round_trips_through_restart_and_reads_legacy_manifest() {
    let dir = temp_dir("namespace");
    let manifest_path = dir.join("buckets.manifest");
    std::fs::write(
        &manifest_path,
        concat!(
            "+1:legacy-bkt\n",
            "+2:shared-name:1000:root:tenant-a\n",
            "+3:shared-name:2000:root:tenant-b\n",
            "+4:no-ts-bkt::owner-4:tenant-c\n",
            "+91:tmp1\n-91:tmp1\n",
            "+92:tmp2\n-92:tmp2\n",
            "+93:tmp3\n-93:tmp3\n",
        ),
    )
    .unwrap();

    let state = Arc::new(S3ServiceState::try_with_data_dir(&dir).unwrap());
    let engine = state.engine.read();
    assert_eq!(engine.lookup_bucket("legacy-bkt"), Ok(1));
    assert_eq!(engine.buckets[&1].namespace, "");
    assert_eq!(engine.lookup_bucket_in("tenant-a", "shared-name"), Ok(2));
    assert_eq!(engine.lookup_bucket_in("tenant-b", "shared-name"), Ok(3));
    assert_eq!(engine.lookup_bucket_in("tenant-c", "no-ts-bkt"), Ok(4));
    assert_eq!(engine.buckets[&2].namespace, "tenant-a");
    assert_eq!(engine.buckets[&3].namespace, "tenant-b");
    assert_eq!(engine.buckets[&4].namespace, "tenant-c");
    drop(engine);

    state.engine.write().put_object(2, b"k", 4096, 11, 0, 0, 0, 0).unwrap();
    state.engine.write().put_object(3, b"k", 8192, 22, 0, 0, 0, 0).unwrap();
    assert_eq!(state.engine.read().bucket_usage(2).unwrap().bytes, 11);
    assert_eq!(state.engine.read().bucket_usage(3).unwrap().bytes, 22);
    drop(state);

    // Reload after initial manifest compaction (6 dead lines > 4 live lines) and verify
    // namespace + owner survived compaction, including when created_ns is None.
    let reopened = Arc::new(S3ServiceState::try_with_data_dir(&dir).unwrap());
    let engine = reopened.engine.read();
    assert_eq!(engine.lookup_bucket("legacy-bkt"), Ok(1));
    assert_eq!(engine.buckets[&1].namespace, "");
    assert_eq!(engine.lookup_bucket_in("tenant-a", "shared-name"), Ok(2));
    assert_eq!(engine.lookup_bucket_in("tenant-b", "shared-name"), Ok(3));
    assert_eq!(engine.lookup_bucket_in("tenant-c", "no-ts-bkt"), Ok(4));
    assert_eq!(reopened.bucket_config_reader.load_bucket(1).unwrap().namespace, "");
    assert_eq!(reopened.bucket_config_reader.load_bucket(2).unwrap().namespace, "tenant-a");
    assert_eq!(reopened.bucket_config_reader.load_bucket(3).unwrap().namespace, "tenant-b");
    let cfg4 = reopened.bucket_config_reader.load_bucket(4).unwrap();
    assert_eq!(cfg4.namespace, "tenant-c");
    assert_eq!(cfg4.owner, "owner-4");
    drop(engine);
    drop(reopened);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn prometheus_bucket_metrics_respect_cardinality_cap_and_flag() {
    let state = S3ServiceState::new();
    let b1 = state.engine.write().create_bucket("alpha-bkt").unwrap();
    state.engine.write().put_object(b1, b"k1", 4096, 123, 0, 0, 0, 0).unwrap();

    let txt = render_metrics(&state);
    assert!(txt.contains("awanfs_objects_total 1"));
    assert!(txt.contains("awanfs_bytes_total 123"));
    assert!(txt.contains("awanfs_bucket_objects{bucket=\"alpha-bkt\"} 1"));
    assert!(txt.contains("awanfs_bucket_bytes{bucket=\"alpha-bkt\"} 123"));
    assert!(txt.contains("awanfs_bucket_all_versions_objects{bucket=\"alpha-bkt\"} 1"));
    assert!(txt.contains("awanfs_bucket_all_versions_bytes{bucket=\"alpha-bkt\"} 123"));

    {
        let mut engine = state.engine.write();
        for i in 0..(PER_BUCKET_METRICS_CARDINALITY_CAP + 5) {
            let _ = engine.create_bucket(&format!("many-{:04}", i));
        }
    }
    let capped = render_metrics(&state);
    assert!(capped.contains("awanfs_objects_total 1"));
    assert!(capped.contains("awanfs_bytes_total 123"));
    assert!(
        !capped.contains("awanfs_bucket_objects{"),
        "default cap should omit per-bucket series past {}",
        PER_BUCKET_METRICS_CARDINALITY_CAP
    );

    state.set_per_bucket_metrics(true);
    let enabled = render_metrics(&state);
    assert!(enabled.contains("awanfs_objects_total 1"));
    assert!(enabled.contains("awanfs_bytes_total 123"));
    assert!(enabled.contains("awanfs_bucket_objects{bucket=\"alpha-bkt\"} 1"));
    assert!(enabled.contains("awanfs_bucket_objects{bucket=\"many-0000\"} 0"));
}

#[tokio::test]
async fn late_failure_after_record_emits_no_commit_event() {
    let dir = temp_dir("late_failure_events");
    let state = Arc::new(S3ServiceState::try_with_data_dir(&dir).unwrap());
    let (_server, addr) = start_server(Arc::clone(&state)).await;
    assert_eq!(raw_http(addr, "PUT", "/lf-bkt", &[], &[]).await.0, 200);
    assert_eq!(raw_http(addr, "PUT", "/lf-bkt/k1", &[], b"seed").await.0, 200);

    let sub = state.events.subscribe("lf-watch");
    let vm = state.volume_manager.read().clone().unwrap();

    // 1. AfterRecord failure on PUT sync emits 0 events even though live index updates.
    vm.fail_next_sync();
    let (st, _, _) = raw_http(addr, "PUT", "/lf-bkt/k1", &[], b"updated-val").await;
    assert_eq!(st, 500);
    assert!(sub.drain().is_empty(), "500 AfterRecord PUT must emit zero commit events");

    // 2. AfterRecord failure on PutObjectTagging journal write emits 0 events.
    vm.fail_next_index();
    let tag_xml = "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>";
    let (st, _, _) = raw_http(addr, "PUT", "/lf-bkt/k1?tagging", &[], tag_xml.as_bytes()).await;
    assert_eq!(st, 500);
    assert!(sub.drain().is_empty(), "500 AfterRecord AttrUpdate must emit zero commit events");

    // 3. AfterRecord failure on DELETE sync emits 0 events.
    vm.fail_next_sync();
    let (st, _, _) = raw_http(addr, "DELETE", "/lf-bkt/k1", &[], &[]).await;
    assert_eq!(st, 500);
    assert!(sub.drain().is_empty(), "500 AfterRecord DELETE must emit zero commit events");

    let b_id = state.engine.read().lookup_bucket("lf-bkt").unwrap();
    assert_eq!(state.bucket_usage(b_id).unwrap(), scan_usage_oracle(&state, b_id));

    drop(sub);
    drop(_server);
    drop(state);
    let reopened = Arc::new(S3ServiceState::try_with_data_dir(&dir).unwrap());
    let rb = reopened.engine.read().lookup_bucket("lf-bkt").unwrap();
    assert_eq!(reopened.bucket_usage(rb).unwrap(), scan_usage_oracle(&reopened, rb));
    let _ = std::fs::remove_dir_all(&dir);
}
