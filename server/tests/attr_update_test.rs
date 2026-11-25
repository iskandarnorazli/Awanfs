//! #203 (#150 stage two): 0x200 attribute-update needles, side map, ObjectView, compaction,
//! fsm::key_step replay/refusal, and 10,000-sequence differential verification.

use bytes::Bytes;
use reqwest::StatusCode;
use s3_server::attrs::{encode_tags, Attrs, TAG_TAGGING};
use s3_server::clock::{Clock, ManualClock};
use s3_server::meta_index::{unpack_flags, FLAG_ACTIVE, FLAG_ATTRS, FLAG_ATTR_UPDATE};
use s3_server::s3_api::{render_metrics, S3ServiceState, SetAttrError};
use s3_server::start_s3_reactor;
use s3_server::tagging::{Tag, TagSet};
use s3_server::versions::Versioning;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_attr_upd_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn start(dir: &Path) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::try_with_data_dir(dir).unwrap());
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn tag_xml(pairs: &[(&str, &str)]) -> String {
    let mut s = String::from("<Tagging><TagSet>");
    for (k, v) in pairs {
        s.push_str(&format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", k, v));
    }
    s.push_str("</TagSet></Tagging>");
    s
}

#[tokio::test]
async fn tagging_update_writes_0x200_delta_without_rewriting_payload_and_survives_restart_compaction() {
    let td = TempDir(temp_dir("delta"));
    let dir = &td.0;
    let c = reqwest::Client::new();
    let (state, url, handle) = start(dir).await;

    assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let body = vec![0x42u8; 8192];
    assert_eq!(
        c.put(format!("{}/bkt/obj", url))
            .header("content-type", "text/plain")
            .header("x-amz-tagging", "k0=v0")
            .body(body.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let b_id = state.engine.read().lookup_bucket("bkt").unwrap();
    let orig_meta = state.engine.read().get_object(b_id, b"obj").unwrap();
    let orig_offset = orig_meta.vol_offset;

    // PutObjectTagging must append a 0x200 delta needle without moving the payload needle.
    assert_eq!(
        c.put(format!("{}/bkt/obj?tagging", url))
            .body(tag_xml(&[("k1", "v1"), ("k2", "v2")]))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let after_meta = state.engine.read().get_object(b_id, b"obj").unwrap();
    assert_eq!(after_meta.vol_offset, orig_offset, "payload needle must not be rewritten");
    assert_eq!(state.attr_updates.entry_count(), 1);

    // Roll into volume 2 and compact volume 1 so the base needle moves past the 0x200 update.
    let vm1 = state.volume_manager.read().clone().unwrap();
    assert_eq!(c.put(format!("{}/bkt/roll", url)).body(vec![1u8; 64]).send().await.unwrap().status(), StatusCode::OK);
    let upd_vm = state.volume_manager.read().clone().unwrap();
    assert_eq!(upd_vm.id(), vm1.id());
    state.perform_online_compaction().await.ok(); // ok if lone volume

    // Check GET, HEAD, GetObjectTagging live.
    let head = c.head(format!("{}/bkt/obj", url)).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers().get("content-type").unwrap(), "text/plain");
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap(), "2");
    let got_tags = c.get(format!("{}/bkt/obj?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(got_tags.contains("<Key>k1</Key>") && got_tags.contains("<Value>v1</Value>"), "{got_tags}");
    assert!(got_tags.contains("<Key>k2</Key>") && got_tags.contains("<Value>v2</Value>"), "{got_tags}");
    assert!(!got_tags.contains("k0"), "{got_tags}");
    let got_body = c.get(format!("{}/bkt/obj", url)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(&got_body[..], &body[..]);

    // CopyObject copies the merged ObjectView (including 0x200 tag update).
    assert_eq!(
        c.put(format!("{}/bkt/copied", url))
            .header("x-amz-copy-source", "/bkt/obj")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let copied_tags = c.get(format!("{}/bkt/copied?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(copied_tags.contains("<Key>k1</Key>") && copied_tags.contains("<Value>v1</Value>"), "{copied_tags}");
    let copied_head = c.head(format!("{}/bkt/copied", url)).send().await.unwrap();
    assert_eq!(copied_head.headers().get("content-type").unwrap(), "text/plain");

    // DeleteObjectTagging writes a 0x200 clearing update overriding base x-amz-tagging.
    assert_eq!(
        c.delete(format!("{}/bkt/obj?tagging", url)).send().await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
    let cleared_head = c.head(format!("{}/bkt/obj", url)).send().await.unwrap();
    assert_eq!(cleared_head.headers().get("x-amz-tagging-count"), None);
    let cleared_tags = c.get(format!("{}/bkt/obj?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(!cleared_tags.contains("<Tag>"), "{cleared_tags}");

    handle.shutdown();
    drop(state);

    // Restart: everything must agree with live state.
    let (_state, url, h2) = start(dir).await;
    let cleared_tags2 = c.get(format!("{}/bkt/obj?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(!cleared_tags2.contains("<Tag>"), "after restart: {cleared_tags2}");
    let copied_tags2 = c.get(format!("{}/bkt/copied?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(copied_tags2.contains("<Key>k1</Key>") && copied_tags2.contains("<Value>v1</Value>"), "after restart: {copied_tags2}");
    h2.shutdown();
}

#[tokio::test]
async fn absent_version_and_delete_marker_are_refused_without_writing_needles() {
    let td = TempDir(temp_dir("refuse"));
    let dir = &td.0;
    let c = reqwest::Client::new();
    let (state, url, handle) = start(dir).await;

    assert_eq!(c.put(format!("{}/vbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{}/vbkt?versioning", url))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let b_id = state.engine.read().lookup_bucket("vbkt").unwrap();
    let bytes_before_absent = state.total_volume_bytes.load(Ordering::SeqCst);

    // Absent key via HTTP and direct set_attribute_group: refused without disk write.
    assert_eq!(
        c.put(format!("{}/vbkt/missing?tagging", url))
            .body(tag_xml(&[("a", "b")]))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let mut upd = Attrs::new();
    upd.set_storage_class("GLACIER");
    let err = state.set_attribute_group(b_id, "missing", None, upd.clone()).await.unwrap_err();
    assert!(matches!(err, SetAttrError::Refused { refusal, .. } if refusal.status == 404));
    assert_eq!(state.total_volume_bytes.load(Ordering::SeqCst), bytes_before_absent);

    // Create version v1 then delete it (creating a delete marker dm).
    let put_r = c.put(format!("{}/vbkt/k", url)).body("v1-data").send().await.unwrap();
    assert_eq!(put_r.status(), StatusCode::OK);
    let v1_id = put_r.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    let del_r = c.delete(format!("{}/vbkt/k", url)).send().await.unwrap();
    assert_eq!(del_r.status(), StatusCode::NO_CONTENT);
    let dm_id = del_r.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    let bytes_before_dm = state.total_volume_bytes.load(Ordering::SeqCst);

    // Updating delete marker with explicit versionId -> 405 MethodNotAllowed, zero writes.
    assert_eq!(
        c.put(format!("{}/vbkt/k?tagging&versionId={}", url, dm_id))
            .body(tag_xml(&[("a", "b")]))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    // Absent versionId -> 404 NoSuchVersion, zero writes.
    assert_eq!(
        c.put(format!("{}/vbkt/k?tagging&versionId=999999999999999999", url))
            .body(tag_xml(&[("a", "b")]))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let err_absent_ver = state
        .set_attribute_group(b_id, "k", Some("999999999999999999"), upd.clone())
        .await
        .unwrap_err();
    assert!(matches!(err_absent_ver, SetAttrError::Refused { refusal, .. } if refusal.code == "NoSuchVersion"));
    assert_eq!(state.total_volume_bytes.load(Ordering::SeqCst), bytes_before_dm);

    // Updating older non-marker version v1 AFTER delete marker is allowed and targets ONLY v1.
    state.set_attribute_group(b_id, "k", Some(&v1_id), upd.clone()).await.unwrap();
    let v1_parsed = s3_server::versions::parse_id(&v1_id).unwrap();
    let v1 = state.versions.read().find(b_id, "k", v1_parsed).unwrap();
    let view = state.object_view_for(b_id, "k", v1_parsed, v1.vol_offset, v1.flags).unwrap();
    assert_eq!(view.attrs.storage_class(), Some("GLACIER"));

    handle.shutdown();
    drop(state);

    // Restart preserves v1's storage class update and delete marker remains current.
    let (state2, url2, h2) = start(dir).await;
    assert_eq!(c.head(format!("{}/vbkt/k", url2)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
    let view2 = state2.object_view_for(b_id, "k", v1_parsed, v1.vol_offset, v1.flags).unwrap();
    assert_eq!(view2.attrs.storage_class(), Some("GLACIER"));
    h2.shutdown();
}

#[tokio::test]
async fn compaction_moves_base_past_update_keeps_latest_and_drops_dead_updates() {
    let td = TempDir(temp_dir("compact"));
    let dir = &td.0;
    let clock = Arc::new(ManualClock::new(10_000_000_000));
    let state = Arc::new(
        S3ServiceState::try_with_data_dir_and_clock(dir, Arc::clone(&clock) as Arc<dyn Clock>)
            .unwrap(),
    );

    // Limit volume span so we easily span multiple volumes for compaction.
    state.volume_manager.read().clone().unwrap().set_max_span(32 * 1024);
    let b_id = state.engine.write().create_bucket("cbkt").unwrap();
    state.bucket_configs.lock().apply_bucket_create(b_id, "cbkt", clock.now_ns(), "root", "").unwrap();

    // Put two base objects in Volume 1: "keep" and "drop".
    let data = Bytes::from(vec![7u8; 4096]);
    for k in ["keep", "drop", "pad1", "pad2", "pad3"] {
        let ts = state.unversioned_write_ts(b_id, k);
        let (_vm, loc, g) = state
            .write_to_active_with(b_id, k, &[], &data, FLAG_ACTIVE, 1, 2, Some(ts))
            .unwrap();
        state.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
        state.engine.write().put_object_at(b_id, k.as_bytes(), g, data.len() as u64, 1, 2, 0, FLAG_ACTIVE, ts / 1_000_000).unwrap();
        clock.advance(1_000_000);
    }
    let vol1_id = 1u32;
    assert!(state.volumes.read().len() >= 2, "must have rolled past volume 1");

    // In newer volume(s): add update u1 (ts T1) then u2 (ts T2 > T1) on "keep",
    // AND an update on "drop" which is then deleted.
    let mut a1 = Attrs::new();
    a1.set_storage_class("STANDARD_IA");
    state.set_attribute_group(b_id, "keep", None, a1).await.unwrap();

    clock.advance(2_000_000);
    let mut a2 = Attrs::new();
    a2.set_storage_class("GLACIER");
    let out2 = state.set_attribute_group(b_id, "keep", None, a2).await.unwrap();

    let mut ad = Attrs::new();
    ad.set_storage_class("ONEZONE_IA");
    state.set_attribute_group(b_id, "drop", None, ad).await.unwrap();
    state.delete_key(b_id, "drop").await.unwrap();

    // Compact Volume 1: base "keep" needle moves from Vol 1 to active volume (AFTER u1 and u2!).
    let report = state.compact_oldest_volume().await.unwrap();
    assert!(report.volume_removed, "volume 1 should be removed");
    assert!(!state.volumes.read().contains_key(&vol1_id));

    // Verify "keep" moved AFTER u2 on disk, yet keeps GLACIER live and after restart!
    let meta = state.engine.read().get_object(b_id, b"keep").unwrap();
    assert!(meta.vol_offset > out2.vol_offset, "compacted base must sit at a later offset than u2");
    let view = state.object_view_for(b_id, "keep", 0, meta.vol_offset, unpack_flags(meta.bucket_flags)).unwrap();
    assert_eq!(view.attrs.storage_class(), Some("GLACIER"));

    drop(state);

    // Reopen: recovery folds base first then updates in created_at_ns order, so GLACIER wins!
    let state2 = Arc::new(S3ServiceState::try_with_data_dir_and_clock(dir, clock).unwrap());
    let meta2 = state2.engine.read().get_object(b_id, b"keep").unwrap();
    let view2 = state2.object_view_for(b_id, "keep", 0, meta2.vol_offset, unpack_flags(meta2.bucket_flags)).unwrap();
    assert_eq!(view2.attrs.storage_class(), Some("GLACIER"));
    assert!(state2.engine.read().get_object(b_id, b"drop").is_err(), "deleted key never resurrected");
    // Dead updates for "drop" were ignored/counted or dropped without leaking into side map.
    assert_eq!(state2.attr_updates.entry_count(), 1);
}

#[tokio::test]
async fn compaction_of_updates_keeps_only_latest_live_group_and_drops_superseded() {
    let td = TempDir(temp_dir("compact_updates"));
    let dir = &td.0;
    let clock = Arc::new(ManualClock::new(20_000_000_000));
    let state = Arc::new(
        S3ServiceState::try_with_data_dir_and_clock(dir, Arc::clone(&clock) as Arc<dyn Clock>)
            .unwrap(),
    );

    state.volume_manager.read().clone().unwrap().set_max_span(32 * 1024);
    let b_id = state.engine.write().create_bucket("ubkt").unwrap();
    state.bucket_configs.lock().apply_bucket_create(b_id, "ubkt", clock.now_ns(), "root", "").unwrap();

    // Write base key, then write multiple updates that stay in Volume 1, then roll past Volume 1.
    let ts0 = state.unversioned_write_ts(b_id, "k");
    let (_vm, loc0, g0) = state.write_to_active_with(b_id, "k", &[], b"hello", FLAG_ACTIVE, 1, 1, Some(ts0)).unwrap();
    state.total_volume_bytes.fetch_add(loc0.total_len, Ordering::Relaxed);
    state.engine.write().put_object_at(b_id, b"k", g0, 5, 1, 1, 0, FLAG_ACTIVE, ts0 / 1_000_000).unwrap();

    let mut a1 = Attrs::new();
    a1.set_storage_class("STANDARD_IA");
    state.set_attribute_group(b_id, "k", None, a1).await.unwrap();
    clock.advance(1_000_000);
    let mut a2 = Attrs::new();
    a2.set_storage_class("GLACIER");
    let u2_out = state.set_attribute_group(b_id, "k", None, a2).await.unwrap();

    // Also add an update on "gone" and delete "gone" while all are in Vol 1, then pad to roll Vol 1.
    let tsg = state.unversioned_write_ts(b_id, "gone");
    let (_vm, locg, gg) = state.write_to_active_with(b_id, "gone", &[], b"bye", FLAG_ACTIVE, 2, 2, Some(tsg)).unwrap();
    state.total_volume_bytes.fetch_add(locg.total_len, Ordering::Relaxed);
    state.engine.write().put_object_at(b_id, b"gone", gg, 3, 2, 2, 0, FLAG_ACTIVE, tsg / 1_000_000).unwrap();
    let mut ag = Attrs::new();
    ag.set_storage_class("DEEP_ARCHIVE");
    state.set_attribute_group(b_id, "gone", None, ag).await.unwrap();
    state.delete_key(b_id, "gone").await.unwrap();

    let pad = Bytes::from(vec![9u8; 4096]);
    for i in 0..5 {
        let pkey = format!("pad{}", i);
        let tsp = state.unversioned_write_ts(b_id, &pkey);
        let (_vm, locp, gp) = state.write_to_active_with(b_id, &pkey, &[], &pad, FLAG_ACTIVE, 3, i as u64, Some(tsp)).unwrap();
        state.total_volume_bytes.fetch_add(locp.total_len, Ordering::Relaxed);
        state.engine.write().put_object_at(b_id, pkey.as_bytes(), gp, pad.len() as u64, 3, i as u64, 0, FLAG_ACTIVE, tsp / 1_000_000).unwrap();
    }

    // Check volume_usage recognizes superseded u1 + dead gone update before compaction.
    let (dead_vol1, _) = state.volume_usage(1).unwrap();
    assert!(dead_vol1 >= 3 * 4096, "dead update + base + tomb must count in volume_usage: {}", dead_vol1);

    let report = state.compact_oldest_volume().await.unwrap();
    assert!(report.volume_removed);
    // Verify u1 was reclaimed (not copied) and u2 moved with preserved created_at_ns.
    let updates = state.attr_updates.get(b_id, "k", 0);
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].created_at_ns, u2_out.created_at_ns);
    assert!(updates[0].vol_offset > u2_out.vol_offset);

    // Verify active volume on disk contains ONLY 1 0x200 update needle for "k", none for "gone".
    let mut attr_needles_for_k = 0usize;
    let mut attr_needles_for_gone = 0usize;
    for vm in state.volumes.read().values() {
        vm.scan_needles(|n| {
            if n.flags & FLAG_ATTR_UPDATE != 0 {
                if n.key == "k" {
                    attr_needles_for_k += 1;
                } else if n.key == "gone" {
                    attr_needles_for_gone += 1;
                }
            }
        }).unwrap();
    }
    assert_eq!(attr_needles_for_k, 1, "superseded update must be dropped on compaction");
    assert_eq!(attr_needles_for_gone, 0, "deleted key update must be dropped on compaction");

    drop(state);
    let state2 = S3ServiceState::try_with_data_dir_and_clock(dir, clock).unwrap();
    assert_eq!(state2.attr_updates_ignored.load(Ordering::Relaxed), 0);
    let m2 = state2.engine.read().get_object(b_id, b"k").unwrap();
    let v2 = state2.object_view_for(b_id, "k", 0, m2.vol_offset, unpack_flags(m2.bucket_flags)).unwrap();
    assert_eq!(v2.attrs.storage_class(), Some("GLACIER"));
}

#[tokio::test]
async fn head_without_attrs_or_updates_does_zero_needle_reads_and_accounting_includes_attrs() {
    let td = TempDir(temp_dir("reads_bytes"));
    let dir = &td.0;
    let c = reqwest::Client::new();
    let (state, url, handle) = start(dir).await;
    assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // Object with no attributes and no updates: zero reads on HEAD even when other objects have updates.
    assert_eq!(c.put(format!("{}/bkt/plain", url)).body("plain-body").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{}/bkt/tagged", url))
            .header("content-type", "text/plain")
            .body("tag-body")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        c.put(format!("{}/bkt/tagged?tagging", url))
            .body(tag_xml(&[("a", "1")]))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let b_id = state.engine.read().lookup_bucket("bkt").unwrap();
    let plain_meta = state.engine.read().get_object(b_id, b"plain").unwrap();
    let before_copies = state.work.snapshot().payload_copies;
    let before_reads = state.work.snapshot().disk_reads;
    assert_eq!(c.head(format!("{}/bkt/plain", url)).send().await.unwrap().status(), StatusCode::OK);
    let _view = state
        .object_view_for(b_id, "plain", 0, plain_meta.vol_offset, unpack_flags(plain_meta.bucket_flags))
        .unwrap();
    assert_eq!(state.work.snapshot().disk_reads, before_reads, "HEAD/object_view_for on un-updated plain object reads nothing");
    assert_eq!(state.work.snapshot().payload_copies, before_copies);

    // Supersede update on "tagged", then overwrite with a large attrs block: check tombstone_bytes & total_volume_bytes.
    assert_eq!(
        c.put(format!("{}/bkt/tagged?tagging", url))
            .body(tag_xml(&[("a", "2")]))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let dead_after_supersede = state.tombstone_bytes.load(Ordering::SeqCst);
    assert!(dead_after_supersede >= 4096, "superseded 0x200 needle counted in tombstone_bytes");

    let metrics_text = render_metrics(&state);
    assert!(metrics_text.contains("awanfs_attr_updates_bytes"));
    assert!(metrics_text.contains("awanfs_attr_updates_ignored_total"));

    // Overwrite clears side map entry and retires dead base + live update.
    assert_eq!(
        c.put(format!("{}/bkt/tagged", url)).body("new-body").send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(state.attr_updates.entry_count(), 0);
    assert_eq!(state.attr_updates.estimated_bytes(), 0);
    assert!(state.tombstone_bytes.load(Ordering::SeqCst) > dead_after_supersede);

    handle.shutdown();
}

/// Deterministic XorShift64 PRNG for the 10,000-sequence differential test.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn gen_range(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OracleVersion {
    id: u64,
    ts: u64,
    marker: bool,
    payload: Vec<u8>,
    base_ct: Option<String>,
    tags: Vec<(String, String)>,
    storage_class: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct OracleKey {
    unver: Option<OracleVersion>,
    hist: Option<Vec<OracleVersion>>,
}

impl OracleKey {
    fn current(&self) -> Option<&OracleVersion> {
        match &self.hist {
            None => self.unver.as_ref(),
            Some(vs) => vs.iter().max_by_key(|v| v.ts),
        }
    }
}

#[tokio::test]
async fn differential_10000_sequences_live_vs_restart() {
    let td = TempDir(temp_dir("diff10k"));
    let dir = &td.0;
    let clock = Arc::new(ManualClock::new(1_000_000_000_000));
    let mut state = Arc::new(
        S3ServiceState::try_with_data_dir_and_clock(dir, Arc::clone(&clock) as Arc<dyn Clock>)
            .unwrap(),
    );
    let b_id = state.engine.write().create_bucket("diffbkt").unwrap();
    state
        .bucket_configs
        .lock()
        .apply_bucket_create(b_id, "diffbkt", clock.now_ns(), "root", "")
        .unwrap();
    state.volume_manager.read().clone().unwrap().set_max_span(128 * 1024);

    let keys = ["k0", "k1", "k2", "k3", "k4", "k5"];
    let mut oracle: BTreeMap<String, OracleKey> = BTreeMap::new();
    let mut versioning = Versioning::Unset;
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);

    const TOTAL_STEPS: usize = 10_000;
    const CHECKPOINT_EVERY: usize = 500;

    for step in 0..TOTAL_STEPS {
        // Advance clock cleanly by 0, 1ms, or 2ms (including 0ns ticks to stress next_ts monotonicity!).
        let tick = match rng.gen_range(4) {
            0 => 0,
            1 => 1,
            _ => 1_000_000,
        };
        clock.advance(tick);

        let key = keys[rng.gen_range(keys.len())].to_string();
        let op = rng.gen_range(12);

        match op {
            // Enable or suspend versioning occasionally without losing history.
            0 if step % 200 == 0 => {
                let target = if versioning == Versioning::Enabled {
                    Versioning::Suspended
                } else {
                    Versioning::Enabled
                };
                if state
                    .bucket_configs
                    .lock()
                    .apply_versioning(b_id, target)
                    .is_ok()
                {
                    state.versioning.write().insert(b_id, target);
                    versioning = target;
                }
            }
            // PUT object (with or without base Attrs block).
            1..=4 => {
                let with_ct = rng.gen_range(2) == 0;
                let with_tag = rng.gen_range(2) == 0;
                let mut attrs = Attrs::new();
                let base_ct = if with_ct {
                    let ct = if rng.gen_range(2) == 0 { "text/plain" } else { "application/json" };
                    attrs.set(s3_server::attrs::TAG_CONTENT_TYPE, ct.as_bytes());
                    Some(ct.to_string())
                } else {
                    None
                };
                let init_tags = if with_tag {
                    let t = vec![("init".to_string(), format!("s{}", step))];
                    let ts = TagSet::new(t.iter().map(|(k, v)| Tag::new(k, v)).collect());
                    attrs.set(TAG_TAGGING, &encode_tags(&ts));
                    t
                } else {
                    Vec::new()
                };
                let attrs_bytes = attrs.encode().unwrap();
                let payload = format!("p_{}_{}", key, step).into_bytes();
                let (hi, lo, _, _) = s3_server::compute_md5(&payload);

                let _g = state.lock_key(b_id, &key).await;
                let stamp = state.version_stamp(b_id, &key);
                let mut flags = FLAG_ACTIVE;
                if !attrs_bytes.is_empty() {
                    flags |= FLAG_ATTRS;
                }
                flags |= stamp.map_or(0, |s| s.0);
                let created_ns = stamp.map(|s| s.1).unwrap_or_else(|| state.unversioned_write_ts(b_id, &key));
                let old_info = state
                    .engine
                    .read()
                    .get_object(b_id, key.as_bytes())
                    .ok()
                    .map(|m| (m.vol_offset, m.size, unpack_flags(m.bucket_flags)));

                let (_vm, loc, g) = state
                    .write_to_active_with(b_id, &key, &attrs_bytes, &payload, flags, hi, lo, Some(created_ns))
                    .unwrap();
                state.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);

                if let Some((extra, ts)) = stamp {
                    let is_null = extra & s3_server::meta_index::FLAG_NULL_ID != 0;
                    let v = s3_server::versions::Version {
                        ts,
                        modified: ts,
                        null: is_null,
                        marker: false,
                        vol_offset: g,
                        size: payload.len() as u64,
                        etag_hi: hi,
                        etag_lo: lo,
                        part_count: 0,
                        flags,
                    };
                    state.record_version(b_id, &key, s3_server::versions::Event::Put(v)).unwrap();

                    let entry = oracle.entry(key.clone()).or_default();
                    let hist = entry.hist.get_or_insert_with(|| {
                        entry.unver.take().map(|mut u| {
                            u.id = 0;
                            u.ts = 0;
                            vec![u]
                        }).unwrap_or_default()
                    });
                    let vid = if is_null { 0 } else { ts };
                    hist.retain(|x| !(is_null && x.id == 0));
                    hist.push(OracleVersion {
                        id: vid,
                        ts,
                        marker: false,
                        payload,
                        base_ct,
                        tags: init_tags,
                        storage_class: None,
                    });
                } else {
                    state
                        .engine
                        .write()
                        .put_object_at(b_id, key.as_bytes(), g, payload.len() as u64, hi, lo, 0, flags, created_ns / 1_000_000)
                        .unwrap();
                    state.retire_unversioned_overwrite(b_id, &key, old_info, g, created_ns, flags);
                    let entry = oracle.entry(key.clone()).or_default();
                    entry.unver = Some(OracleVersion {
                        id: 0,
                        ts: 0,
                        marker: false,
                        payload,
                        base_ct,
                        tags: init_tags,
                        storage_class: None,
                    });
                }
            }
            // DELETE key (unversioned delete or versioned delete marker).
            5 => {
                let res = state.delete_key(b_id, &key).await.unwrap();
                let entry = oracle.entry(key.clone()).or_default();
                match res {
                    None => {
                        entry.unver = None;
                    }
                    Some(dm_vid_str) => {
                        let dm_id = s3_server::versions::parse_id(&dm_vid_str).unwrap();
                        let dm_ts = state.versions.read().find(b_id, &key, dm_id).unwrap().ts;
                        let hist = entry.hist.get_or_insert_with(|| {
                            entry.unver.take().map(|mut u| {
                                u.id = 0;
                                u.ts = 0;
                                vec![u]
                            }).unwrap_or_default()
                        });
                        hist.retain(|x| !(dm_id == 0 && x.id == 0));
                        hist.push(OracleVersion {
                            id: dm_id,
                            ts: dm_ts,
                            marker: true,
                            payload: Vec::new(),
                            base_ct: None,
                            tags: Vec::new(),
                            storage_class: None,
                        });
                    }
                }
            }
            // Delete specific version (latest, oldest, or absent).
            6 => {
                let target_id = oracle
                    .get(&key)
                    .and_then(|o| o.hist.as_ref().and_then(|h| h.get(rng.gen_range(h.len().max(1))).map(|v| v.id)))
                    .unwrap_or(987654321);
                let deleted = state.delete_version(b_id, &key, target_id).await.unwrap();
                if let Some(o) = oracle.get_mut(&key) {
                    if let Some(hist) = o.hist.as_mut() {
                        let had = hist.iter().any(|v| v.id == target_id);
                        assert_eq!(deleted.is_some(), had);
                        hist.retain(|v| v.id != target_id);
                    } else if target_id == 0 {
                        if let Some(_u) = o.unver.take() {
                            assert!(deleted.is_some());
                        } else {
                            assert!(deleted.is_none());
                        }
                    } else {
                        assert!(deleted.is_none());
                    }
                }
            }
            // Set attribute group (Tagging or StorageClass or Clear Tagging) on current or specific version.
            7..=10 => {
                let pick_explicit_ver = rng.gen_range(3) == 0;
                let (vid_str, target_vid_opt): (Option<String>, Option<u64>) = if pick_explicit_ver {
                    if let Some(hist) = oracle.get(&key).and_then(|o| o.hist.as_ref()) {
                        if let Some(v) = hist.get(rng.gen_range(hist.len().max(1))) {
                            let s = s3_server::versions::id_string(v.id);
                            (Some(s), Some(v.id))
                        } else {
                            (Some("999999999".to_string()), Some(999_999_999))
                        }
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                let mut upd = Attrs::new();
                let is_tagging = op == 7 || op == 8;
                let clearing = op == 8;
                let new_tags = if is_tagging {
                    if clearing {
                        upd.set(TAG_TAGGING, &encode_tags(&TagSet::default()));
                        Vec::new()
                    } else {
                        let t = vec![(format!("tg{}", step % 5), format!("val{}", step))];
                        let ts = TagSet::new(t.iter().map(|(k, v)| Tag::new(k, v)).collect());
                        upd.set(TAG_TAGGING, &encode_tags(&ts));
                        t
                    }
                } else {
                    let sc = if op == 9 { "GLACIER" } else { "STANDARD_IA" };
                    upd.set_storage_class(sc);
                    Vec::new()
                };

                let res = state.set_attribute_group(b_id, &key, vid_str.as_deref(), upd).await;
                let entry = oracle.get_mut(&key);
                let oracle_target = match (entry, target_vid_opt) {
                    (None, _) => None,
                    (Some(o), Some(id)) => match o.hist.as_mut() {
                        Some(hist) => hist.iter_mut().find(|v| v.id == id),
                        None if id == 0 => o.unver.as_mut(),
                        None => None,
                    },
                    (Some(o), None) => match o.hist.as_mut() {
                        Some(hist) => hist.iter_mut().max_by_key(|v| v.ts),
                        None => o.unver.as_mut(),
                    },
                };

                match oracle_target {
                    None => assert!(matches!(res, Err(SetAttrError::Refused { .. })), "missing target must refuse at step {step}"),
                    Some(v) if v.marker => assert!(matches!(res, Err(SetAttrError::Refused { delete_marker: Some(_), .. })), "delete marker must refuse at step {step}"),
                    Some(v) => {
                        res.unwrap_or_else(|e| panic!("unexpected attr error at step {step}: {:?}", e));
                        if is_tagging {
                            v.tags = new_tags;
                        } else {
                            v.storage_class = Some(if op == 9 { "GLACIER" } else { "STANDARD_IA" }.to_string());
                        }
                    }
                }
            }
            // Compact oldest volume occasionally.
            _ => {
                let _ = state.compact_oldest_volume().await.unwrap();
            }
        }

        // Every CHECKPOINT_EVERY steps and at the very end, restart from disk and assert live == restarted == oracle.
        if (step + 1) % CHECKPOINT_EVERY == 0 || step + 1 == TOTAL_STEPS {
            state.sync_all_volumes().unwrap();
            verify_matches_oracle(&state, b_id, &keys, &oracle, step, "live");
            drop(state);

            state = Arc::new(
                S3ServiceState::try_with_data_dir_and_clock(dir, Arc::clone(&clock) as Arc<dyn Clock>)
                    .unwrap(),
            );
            state.volume_manager.read().clone().unwrap().set_max_span(128 * 1024);
            verify_matches_oracle(&state, b_id, &keys, &oracle, step, "restarted");
        }
    }
}

fn verify_matches_oracle(
    state: &S3ServiceState,
    b_id: u32,
    keys: &[&str],
    oracle: &BTreeMap<String, OracleKey>,
    step: usize,
    phase: &str,
) {
    for &k in keys {
        let o = oracle.get(k).cloned().unwrap_or_default();
        match o.current() {
            None => {
                assert!(
                    state.engine.read().get_object(b_id, k.as_bytes()).is_err(),
                    "step {step} ({phase}): {k} should be absent in engine"
                );
            }
            Some(v) if v.marker => {
                assert!(
                    state.engine.read().get_object(b_id, k.as_bytes()).is_err(),
                    "step {step} ({phase}): {k} current is delete marker so must be absent in engine"
                );
            }
            Some(v) => {
                let meta = state
                    .engine
                    .read()
                    .get_object(b_id, k.as_bytes())
                    .unwrap_or_else(|_| panic!("step {step} ({phase}): {k} missing in engine"));
                let flags = unpack_flags(meta.bucket_flags);
                let view = state
                    .object_view_for(b_id, k, v.id, meta.vol_offset, flags)
                    .unwrap();
                assert_eq!(view.attrs.content_type().map(str::to_string), v.base_ct, "step {step} ({phase}) {k} ct");
                assert_eq!(view.attrs.storage_class().map(str::to_string), v.storage_class, "step {step} ({phase}) {k} sc");
                let mut got_tags: Vec<(String, String)> = view
                    .attrs
                    .tag_set()
                    .unwrap()
                    .tags
                    .into_iter()
                    .map(|t| (t.key, t.value))
                    .collect();
                got_tags.sort();
                let mut exp_tags = v.tags.clone();
                exp_tags.sort();
                assert_eq!(got_tags, exp_tags, "step {step} ({phase}) {k} tags");
                let body = state.read_object_bytes(b_id, k, meta.vol_offset, meta.size, flags).unwrap();
                assert_eq!(&body[..], &v.payload[..], "step {step} ({phase}) {k} body");
            }
        }
        // Also verify every version in history (including non-current and updated old versions).
        if let Some(hist) = &o.hist {
            for v in hist {
                let live_v = state.versions.read().find(b_id, k, v.id);
                let lv = live_v.unwrap_or_else(|| panic!("step {step} ({phase}): {k} ver {} missing", v.id));
                assert_eq!(lv.marker, v.marker, "step {step} ({phase}) {k} ver {} marker", v.id);
                if !v.marker {
                    let view = state.object_view_for(b_id, k, v.id, lv.vol_offset, lv.flags).unwrap();
                    assert_eq!(view.attrs.storage_class().map(str::to_string), v.storage_class);
                    let mut got_tags: Vec<(String, String)> = view
                        .attrs
                        .tag_set()
                        .unwrap()
                        .tags
                        .into_iter()
                        .map(|t| (t.key, t.value))
                        .collect();
                    got_tags.sort();
                    let mut exp_tags = v.tags.clone();
                    exp_tags.sort();
                    assert_eq!(got_tags, exp_tags, "step {step} ({phase}) {k} ver {} tags", v.id);
                }
            }
        }
    }
}


#[tokio::test]
async fn copy_object_without_version_id_on_versioned_object_carries_0x200_tag_updates_and_deletes() {
    let td = TempDir(temp_dir("copy_ver_no_vid"));
    let dir = &td.0;
    let c = reqwest::Client::new();
    let (state, url, handle) = start(dir).await;

    assert_eq!(c.put(format!("{}/vcopy", url)).send().await.unwrap().status(), StatusCode::OK);
    let vcfg = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(
        c.put(format!("{}/vcopy?versioning", url))
            .body(vcfg)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let payload = b"versioned-body-for-copy".to_vec();
    let put = c
        .put(format!("{}/vcopy/src", url))
        .header("x-amz-tagging", "k0=v0")
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let vid = put.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    assert_ne!(vid, "null");

    // Update tags via 0x200 delta needle on the current non-null version.
    assert_eq!(
        c.put(format!("{}/vcopy/src?tagging", url))
            .body(tag_xml(&[("a", "1"), ("b", "2")]))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // CopyObject WITHOUT ?versionId= must resolve the current version and carry 0x200 tags.
    assert_eq!(
        c.put(format!("{}/vcopy/dst1", url))
            .header("x-amz-copy-source", "/vcopy/src")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let dst1_tags = c.get(format!("{}/vcopy/dst1?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(dst1_tags.contains("<Key>a</Key>") && dst1_tags.contains("<Value>1</Value>"), "{dst1_tags}");
    assert!(dst1_tags.contains("<Key>b</Key>") && dst1_tags.contains("<Value>2</Value>"), "{dst1_tags}");
    assert!(!dst1_tags.contains("k0"), "{dst1_tags}");

    // Delete tags via 0x200 delta needle, then CopyObject without ?versionId= must copy zero tags.
    assert_eq!(
        c.delete(format!("{}/vcopy/src?tagging", url)).send().await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        c.put(format!("{}/vcopy/dst2", url))
            .header("x-amz-copy-source", "/vcopy/src")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let dst2_tags = c.get(format!("{}/vcopy/dst2?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(!dst2_tags.contains("<Tag>"), "{dst2_tags}");

    // Verify copies survive restart identically.
    state.sync_all_volumes().unwrap();
    handle.shutdown();
    drop(state);
    let (_state, url, handle2) = start(dir).await;
    let dst1_after = c.get(format!("{}/vcopy/dst1?tagging", url)).send().await.unwrap().text().await.unwrap();
    let dst2_after = c.get(format!("{}/vcopy/dst2?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(dst1_after.contains("<Key>a</Key>") && dst1_after.contains("<Key>b</Key>"), "{dst1_after}");
    assert!(!dst2_after.contains("<Tag>"), "{dst2_after}");
    handle2.shutdown();
}

#[tokio::test]
async fn streamed_put_relocation_preserves_monotonic_created_at_over_concurrent_tag_update() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let td = TempDir(temp_dir("relocate_ts"));
    let dir = &td.0;
    let clock = Arc::new(ManualClock::new(1_000_200_000));
    let state = Arc::new(S3ServiceState::try_with_data_dir_and_clock(dir, Arc::clone(&clock) as Arc<dyn Clock>).unwrap());
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/rbkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // Start a stalled streamed PUT (> STREAM_PUT_MIN) on /rbkt/k.
    let streamed_len = s3_server::STREAM_PUT_MIN as usize + 4096;
    let streamed_body = vec![0x5Au8; streamed_len];
    let mut stream = TcpStream::connect(&host).await.unwrap();
    let req_head = format!(
        "PUT /rbkt/k HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        host, streamed_len
    );
    stream.write_all(req_head.as_bytes()).await.unwrap();
    let half = streamed_len / 2;
    stream.write_all(&streamed_body[..half]).await.unwrap();
    stream.flush().await.unwrap();
    for _ in 0..400 {
        if state.volume_writers_in_flight(1) == Some(1) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(1));

    // Concurrent PUT + 0x200 tag update while ManualClock is at 1_000_500_000 ns (1000 ms).
    // Tag update sets created_at_ns = (1000 + 1) * 1_000_000 + 1 = 1_001_000_001 > clock.now_ns().
    assert_eq!(
        c.put(format!("{}/rbkt/k", url)).body(b"concurrent-base".to_vec()).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        c.put(format!("{}/rbkt/k?tagging", url))
            .body(tag_xml(&[("stale", "tag")]))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Streamed PUT commits while clock is STILL 1_000_200_000 (< u1.created_at_ns = 1_001_000_001).
    // Relocation must preserve created_ns (1_001_000_002), not fall back to clock.now_ns().
    stream.write_all(&streamed_body[half..]).await.unwrap();
    stream.flush().await.unwrap();
    let mut resp_buf = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(20), stream.read_to_end(&mut resp_buf))
        .await
        .unwrap()
        .unwrap();
    let resp_text = String::from_utf8_lossy(&resp_buf);
    assert!(resp_text.starts_with("HTTP/1.1 200 OK"), "{resp_text}");
    assert_eq!(state.stream_relocations.load(Ordering::SeqCst), 1);

    let live_tags = c.get(format!("{}/rbkt/k?tagging", url)).send().await.unwrap().text().await.unwrap();
    assert!(!live_tags.contains("stale"), "live must not keep retired tags: {live_tags}");

    state.sync_all_volumes().unwrap();
    handle.shutdown();
    drop(state);

    let (_state2, url2, handle2) = start(dir).await;
    let after_tags = c.get(format!("{}/rbkt/k?tagging", url2)).send().await.unwrap().text().await.unwrap();
    assert!(
        !after_tags.contains("stale"),
        "restarted replay must not resurrect retired 0x200 tag over relocated needle: {after_tags}"
    );
    let after_body = c.get(format!("{}/rbkt/k", url2)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(&after_body[..], &streamed_body[..]);
    handle2.shutdown();
}

#[tokio::test]
async fn put_tagging_with_nonexistent_version_id_on_unversioned_key_returns_nosuchversion() {
    let td = TempDir(temp_dir("unver_nosuchver"));
    let dir = &td.0;
    let c = reqwest::Client::new();
    let (_state, url, handle) = start(dir).await;

    assert_eq!(c.put(format!("{}/ubkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/ubkt/k", url)).body(b"hello".to_vec()).send().await.unwrap().status(), StatusCode::OK);

    let res = c
        .put(format!("{}/ubkt/k?tagging&versionId=999999999999999", url))
        .body(tag_xml(&[("k", "v")]))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body = res.text().await.unwrap();
    assert!(body.contains("<Code>NoSuchVersion</Code>"), "expected NoSuchVersion, got: {body}");
    handle.shutdown();
}
