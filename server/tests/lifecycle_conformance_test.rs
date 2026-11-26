//! #118: S3 Lifecycle Conformance tests (validation, roundtrip, filters, versioning/noncurrent/
//! delete-markers, x-amz-expiration, Object Lock safety, restart durability, and metrics).

use md5::{Digest, Md5};
use reqwest::StatusCode;
use s3_server::clock::{Clock, ManualClock};
use s3_server::fsm::{Lock, RetentionMode};
use s3_server::object_lock::{lock_configuration_xml, DefaultRetention, LockConfiguration, RetentionPeriod};
use s3_server::{run_lifecycle_sweep, start_s3_reactor, AuthConfig, S3ServiceState, ServerHandle};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_lc_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn_clocked(
    dir: Option<&Path>,
    initial_epoch_sec: u64,
    day_seconds: u64,
) -> (String, Arc<S3ServiceState>, Arc<ManualClock>, ServerHandle) {
    let clock = Arc::new(ManualClock::new(initial_epoch_sec * 1_000_000_000));
    let mut state = match dir {
        Some(d) => S3ServiceState::with_data_dir_and_clock(d, Arc::clone(&clock) as _),
        None => {
            let mut s = S3ServiceState::new();
            s.clock = Arc::clone(&clock) as _;
            s
        }
    };
    state.auth = AuthConfig::open();
    state.lifecycle_day_seconds.store(day_seconds, Ordering::Relaxed);
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    (format!("http://{}", handle.local_addr), state, clock, handle)
}

#[tokio::test]
async fn lifecycle_validation_errors_roundtrip_and_restart_persistence() {
    let dir = temp_dir("persist");
    let (base, _state, _clock, handle) = spawn_clocked(Some(&dir), 1_750_000_000, 86_400).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{base}/lc-val")).send().await.unwrap().status(), StatusCode::OK);

    // 1. Validation error codes required by ceph/s3-tests (#118).
    let long_id = "a".repeat(256);
    let bad_cases = [
        (
            format!("<LifecycleConfiguration><Rule><ID>{long_id}</ID><Prefix>p/</Prefix><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"),
            "InvalidArgument",
        ),
        (
            "<LifecycleConfiguration><Rule><ID>dup</ID><Prefix>a/</Prefix><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule><Rule><ID>dup</ID><Prefix>b/</Prefix><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule></LifecycleConfiguration>".to_string(),
            "InvalidArgument",
        ),
        (
            "<LifecycleConfiguration><Rule><ID>r1</ID><Prefix>p/</Prefix><Status>enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>".to_string(),
            "MalformedXML",
        ),
        (
            "<LifecycleConfiguration><Rule><ID>r1</ID><Prefix>p/</Prefix><Status>Enabled</Status><Expiration><Date>20200101</Date></Expiration></Rule></LifecycleConfiguration>".to_string(),
            "InvalidArgument",
        ),
        (
            "<LifecycleConfiguration><Rule><ID>r1</ID><Prefix>p/</Prefix><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule></LifecycleConfiguration>".to_string(),
            "InvalidArgument",
        ),
    ];
    for (xml, code) in &bad_cases {
        let res = c.put(format!("{base}/lc-val?lifecycle")).body(xml.clone()).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "xml={xml}");
        let body = res.text().await.unwrap();
        assert!(body.contains(&format!("<Code>{code}</Code>")), "expected {code}, got: {body}");
    }

    // >1000 rules refused with 400 InvalidArgument.
    let mut too_many = String::from("<LifecycleConfiguration>");
    for i in 0..1001 {
        too_many.push_str(&format!("<Rule><ID>r{i}</ID><Prefix>{i}/</Prefix><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>"));
    }
    too_many.push_str("</LifecycleConfiguration>");
    let res = c.put(format!("{base}/lc-val?lifecycle")).body(too_many).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("<Code>InvalidArgument</Code>"));

    // 2. Legacy <Prefix> stays <Prefix> (not <Filter><Prefix>), and Transition round-trips cleanly.
    let cfg_xml = "<LifecycleConfiguration>\
        <Rule><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Days>31</Days></Expiration></Rule>\
        <Rule><ID>glacier</ID><Filter><Prefix>arc/</Prefix></Filter><Status>Disabled</Status><Transition><Days>10</Days><StorageClass>GLACIER</StorageClass></Transition></Rule>\
        </LifecycleConfiguration>";
    assert_eq!(
        c.put(format!("{base}/lc-val?lifecycle")).body(cfg_xml).send().await.unwrap().status(),
        StatusCode::OK
    );

    // 3. Config survives restart (backed by buckets.manifest, not ephemeral memory).
    drop(handle);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let (base2, _state2, _clock2, handle2) = spawn_clocked(Some(&dir), 1_750_000_000, 86_400).await;
    let got = c.get(format!("{base2}/lc-val?lifecycle")).send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    let text = got.text().await.unwrap();
    assert!(text.contains("<Prefix>test1/</Prefix>"), "legacy prefix must stay top-level: {text}");
    assert!(!text.contains("<Filter><Prefix>test1/</Prefix></Filter>"), "unexpected Filter wrapper: {text}");
    assert!(text.contains("<StorageClass>GLACIER</StorageClass>"), "transition lost across restart: {text}");
    drop(handle2);
}

#[tokio::test]
async fn lifecycle_filters_date_disabled_header_and_tags() {
    // 2025-06-15T12:00:00Z = 1_749_988_800
    let start_sec = 1_749_988_800u64;
    let (base, state, clock, _handle) = spawn_clocked(None, start_sec, 10).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{base}/lc-filt")).send().await.unwrap().status(), StatusCode::OK);

    // Rule 1: dual Prefix (<Prefix>days1/</Prefix> + <And><Prefix>days1</Prefix><Tags>...>) matching Ceph's setup_lifecycle_tags2
    // Rule 2: size > 2000 & size < 4000
    // Rule 3: Date in past (2020-01-01) -> expires on first sweep regardless of day_seconds
    // Rule 4: Disabled rule -> never expires
    let xml = "<LifecycleConfiguration>\
        <Rule><ID>tag-dual</ID><Status>Enabled</Status>\
          <Filter><Prefix>days1/</Prefix><Tag><Key>tom</Key><Value>sawyer</Value></Tag>\
            <And><Prefix>days1</Prefix><Tag><Key>huck</Key><Value>finn</Value></Tag></And></Filter>\
          <Expiration><Days>1</Days></Expiration></Rule>\
        <Rule><ID>size-band</ID><Status>Enabled</Status>\
          <Filter><Prefix>sz/</Prefix><ObjectSizeGreaterThan>2000</ObjectSizeGreaterThan><ObjectSizeLessThan>4000</ObjectSizeLessThan></Filter>\
          <Expiration><Days>1</Days></Expiration></Rule>\
        <Rule><ID>past-date</ID><Status>Enabled</Status>\
          <Filter><Prefix>past/</Prefix></Filter>\
          <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
        <Rule><ID>off</ID><Status>Disabled</Status>\
          <Filter><Prefix>keep/</Prefix></Filter>\
          <Expiration><Days>1</Days></Expiration></Rule>\
        <Rule><ID>hdr</ID><Status>Enabled</Status>\
          <Filter><Prefix>hdr/</Prefix></Filter>\
          <Expiration><Days>3</Days></Expiration></Rule>\
        <Rule><ID>tag-hdr</ID><Status>Enabled</Status>\
          <Filter><Tag><Key>env</Key><Value>tmp</Value></Tag></Filter>\
          <Expiration><Days>2</Days></Expiration></Rule>\
    </LifecycleConfiguration>";
    assert_eq!(c.put(format!("{base}/lc-filt?lifecycle")).body(xml).send().await.unwrap().status(), StatusCode::OK);

    // Put objects
    assert_eq!(
        c.put(format!("{base}/lc-filt/days1/tom"))
            .header("x-amz-tagging", "tom=sawyer")
            .body("tom")
            .send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        c.put(format!("{base}/lc-filt/days1/huck"))
            .header("x-amz-tagging", "tom=sawyer&huck=finn")
            .body("huck")
            .send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(c.put(format!("{base}/lc-filt/sz/small")).body(vec![b'a'; 1000]).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{base}/lc-filt/sz/mid")).body(vec![b'b'; 2500]).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{base}/lc-filt/sz/big")).body(vec![b'c'; 5000]).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{base}/lc-filt/past/x")).body("past").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{base}/lc-filt/keep/x")).body("keep").send().await.unwrap().status(), StatusCode::OK);

    // x-amz-expiration on PUT, GET, HEAD
    let put_hdr = c.put(format!("{base}/lc-filt/hdr/a")).body("abc").send().await.unwrap();
    assert_eq!(put_hdr.status(), StatusCode::OK);
    let exp_put = put_hdr.headers().get("x-amz-expiration").and_then(|v| v.to_str().ok()).unwrap_or("");
    assert!(exp_put.contains("rule-id=\"hdr\"") && exp_put.contains("2025 00:00:00 GMT"), "PUT header: {exp_put}");

    let head_hdr = c.head(format!("{base}/lc-filt/hdr/a")).send().await.unwrap();
    assert_eq!(head_hdr.headers().get("x-amz-expiration").and_then(|v| v.to_str().ok()), Some(exp_put));

    // Tagging after PUT exposes x-amz-expiration on HEAD/GET via 0x200 attrs.
    assert_eq!(c.put(format!("{base}/lc-filt/plain")).body("123").send().await.unwrap().status(), StatusCode::OK);
    assert!(c.head(format!("{base}/lc-filt/plain")).send().await.unwrap().headers().get("x-amz-expiration").is_none());
    assert_eq!(
        c.put(format!("{base}/lc-filt/plain?tagging"))
            .body("<Tagging><TagSet><Tag><Key>env</Key><Value>tmp</Value></Tag></TagSet></Tagging>")
            .send().await.unwrap().status(),
        StatusCode::OK
    );
    let tagged_head = c.head(format!("{base}/lc-filt/plain")).send().await.unwrap();
    let tag_exp = tagged_head.headers().get("x-amz-expiration").and_then(|v| v.to_str().ok()).unwrap_or("");
    assert!(tag_exp.contains("rule-id=\"tag-hdr\""), "tagged HEAD header: {tag_exp}");

    // Sweep at +12s (day_seconds=10 -> 1 day passed)
    clock.advance(12 * 1_000_000_000);
    run_lifecycle_sweep(&state, &state.lifecycle, clock.now_ns() / 1_000_000);

    // Expired: days1/huck, sz/mid, past/x
    assert_eq!(c.head(format!("{base}/lc-filt/days1/huck")).send().await.unwrap().status(), StatusCode::NOT_FOUND);
    assert_eq!(c.head(format!("{base}/lc-filt/sz/mid")).send().await.unwrap().status(), StatusCode::NOT_FOUND);
    assert_eq!(c.head(format!("{base}/lc-filt/past/x")).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Kept: days1/tom, sz/small, sz/big, keep/x (disabled), hdr/a (needs 3 days -> 30s)
    assert_eq!(c.head(format!("{base}/lc-filt/days1/tom")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.head(format!("{base}/lc-filt/sz/small")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.head(format!("{base}/lc-filt/sz/big")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.head(format!("{base}/lc-filt/keep/x")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.head(format!("{base}/lc-filt/hdr/a")).send().await.unwrap().status(), StatusCode::OK);

    // Check Prometheus metrics exposed
    let metrics = c.get(format!("{base}/_awanfs/metrics")).send().await.unwrap().text().await.unwrap();
    assert!(metrics.contains("awanfs_lifecycle_objects_expired_total 3"), "metrics:\n{metrics}");
}

#[tokio::test]
async fn lifecycle_versioned_noncurrent_delete_marker_and_restart() {
    let dir = temp_dir("ver_dm");
    let (base, state, clock, handle) = spawn_clocked(Some(&dir), 1_750_000_000, 2).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{base}/lc-ver")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{base}/lc-ver?versioning"))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send().await.unwrap().status(),
        StatusCode::OK
    );

    // Upload 5 versions of newer-keep/k; rule keeps NewerNoncurrentVersions=2, expires older noncurrent after 1 day (=2s).
    for i in 0..5 {
        clock.advance(500_000_000);
        assert_eq!(c.put(format!("{base}/lc-ver/newer-keep/k")).body(format!("v{i}")).send().await.unwrap().status(), StatusCode::OK);
    }

    // Upload 1 version of dm-sole/a + delete it -> delete marker + 1 noncurrent version.
    assert_eq!(c.put(format!("{base}/lc-ver/dm-sole/a")).body("only").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.delete(format!("{base}/lc-ver/dm-sole/a")).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    // Upload 1 version of dm-days/a + delete it -> Expiration Days=5 + NoncurrentDays=1.
    assert_eq!(c.put(format!("{base}/lc-ver/dm-days/a")).body("d").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.delete(format!("{base}/lc-ver/dm-days/a")).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    let xml = "<LifecycleConfiguration>\
        <Rule><ID>nc</ID><Filter><Prefix>newer-keep/</Prefix></Filter><Status>Enabled</Status>\
          <NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule>\
        <Rule><ID>dm1</ID><Filter><Prefix>dm-sole/</Prefix></Filter><Status>Enabled</Status>\
          <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>\
          <NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule>\
        <Rule><ID>dm2</ID><Filter><Prefix>dm-days/</Prefix></Filter><Status>Enabled</Status>\
          <Expiration><Days>5</Days></Expiration>\
          <NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule>\
    </LifecycleConfiguration>";
    assert_eq!(c.put(format!("{base}/lc-ver?lifecycle")).body(xml).send().await.unwrap().status(), StatusCode::OK);

    // +3s (1.5 days): noncurrent > 1 day expire; newer-keep retains 1 current + 2 newer = 3 versions.
    // dm-sole/a: noncurrent expires -> marker becomes sole -> removed immediately by ExpiredObjectDeleteMarker.
    // dm-days/a: noncurrent expires, delete marker kept until 5 days (=10s).
    clock.advance(3 * 1_000_000_000);
    run_lifecycle_sweep(&state, &state.lifecycle, clock.now_ns() / 1_000_000);

    let list1 = c.get(format!("{base}/lc-ver?versions")).send().await.unwrap().text().await.unwrap();
    let keep_count = list1.matches("<Key>newer-keep/k</Key>").count();
    assert_eq!(keep_count, 3, "expected 1 current + 2 newer noncurrent, got:\n{list1}");
    assert!(!list1.contains("dm-sole/a"), "sole delete marker should be removed:\n{list1}");
    assert_eq!(list1.matches("<Key>dm-days/a</Key>").count(), 1, "dm-days marker kept until 5d:\n{list1}");

    // +8s (total 11s > 5 days = 10s): dm-days sole delete marker expires too.
    clock.advance(8 * 1_000_000_000);
    run_lifecycle_sweep(&state, &state.lifecycle, clock.now_ns() / 1_000_000);
    let list2 = c.get(format!("{base}/lc-ver?versions")).send().await.unwrap().text().await.unwrap();
    assert!(!list2.contains("dm-days/a"), "dm-days delete marker should expire after 5d:\n{list2}");

    // Restart: nothing resurrected.
    drop(handle);
    drop(state);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let (base2, _s2, _c2, handle2) = spawn_clocked(Some(&dir), 1_750_000_015, 2).await;
    let list3 = c.get(format!("{base2}/lc-ver?versions")).send().await.unwrap().text().await.unwrap();
    assert_eq!(list3.matches("<Key>newer-keep/k</Key>").count(), 3);
    assert!(!list3.contains("dm-sole/a"));
    assert!(!list3.contains("dm-days/a"));
    drop(handle2);
}

#[tokio::test]
async fn lifecycle_property_never_deletes_locked_version_or_premature_object() {
    let dir = temp_dir("lock_prop");
    let start_sec = 1_750_000_000u64;
    let (base, state, clock, _handle) = spawn_clocked(Some(&dir), start_sec, 10).await;
    let c = reqwest::Client::new();

    // Bucket with Object Lock enabled (also turns on versioning).
    assert_eq!(
        c.put(format!("{base}/lc-lock"))
            .header("x-amz-bucket-object-lock-enabled", "true")
            .send().await.unwrap().status(),
        StatusCode::OK
    );
    let cfg_xml = lock_configuration_xml(&LockConfiguration {
        enabled: true,
        default_retention: Some(DefaultRetention {
            mode: RetentionMode::Compliance,
            period: RetentionPeriod::Days(10),
        }),
    });
    assert_eq!(c.put(format!("{base}/lc-lock?object-lock")).body(cfg_xml).send().await.unwrap().status(), StatusCode::OK);

    // Expire current after 1 day AND noncurrent after 1 day.
    let lc_xml = "<LifecycleConfiguration><Rule><ID>all</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status>\
        <Expiration><Days>1</Days></Expiration>\
        <NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>";
    assert_eq!(c.put(format!("{base}/lc-lock?lifecycle")).body(lc_xml).send().await.unwrap().status(), StatusCode::OK);

    // Put v1 (locked by COMPLIANCE default retention for 10 real days), then v2 (also locked), then v3 with legal hold ON.
    let mut locks: BTreeMap<String, (u64, Lock)> = BTreeMap::new();
    for i in 0..3 {
        let now_sec = clock.now_ns() / 1_000_000_000;
        let data = format!("payload-{i}");
        let raw_md5: [u8; 16] = Md5::digest(data.as_bytes()).into();
        let md5_b64 = s3_server::digest::base64_encode(&raw_md5);
        let mut req = c.put(format!("{base}/lc-lock/k")).header("content-md5", md5_b64);
        if i == 2 {
            req = req.header("x-amz-object-lock-legal-hold", "ON");
        }
        let res = req.body(data).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let vid = res.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
        locks.insert(
            vid,
            (
                now_sec,
                Lock {
                    retention: Some((RetentionMode::Compliance, now_sec + 10 * 86_400)),
                    legal_hold: i == 2,
                },
            ),
        );
        clock.advance(2 * 1_000_000_000);
    }

    // Sweep at +30s (3 "lifecycle days" -> current v3 gets a delete marker if not yet deleted, all noncurrent fail age vs lock).
    // Crucially: NO version whose Lock::may_delete(now_sec, false) is Err may be removed!
    for step in 0..4 {
        clock.advance(20 * 1_000_000_000);
        let now_sec = clock.now_ns() / 1_000_000_000;
        run_lifecycle_sweep(&state, &state.lifecycle, now_sec * 1_000);
        for (vid, (_created, lock)) in &locks {
            if lock.may_delete(now_sec, false).is_err() {
                let r = c.head(format!("{base}/lc-lock/k?versionId={vid}")).send().await.unwrap();
                assert_eq!(r.status(), StatusCode::OK, "step {step}: locked version {vid} was deleted by lifecycle!");
            }
        }
    }
}

#[tokio::test]
async fn lifecycle_aborts_incomplete_multipart_uploads() {
    let dir = temp_dir("mpu");
    let (base, state, clock, _handle) = spawn_clocked(Some(&dir), 1_750_000_000, 10).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{base}/lc-mpu")).send().await.unwrap().status(), StatusCode::OK);

    let init = c.post(format!("{base}/lc-mpu/big/obj?uploads")).send().await.unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let init_xml = init.text().await.unwrap();
    let uid = init_xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .unwrap()
        .to_string();
    assert_eq!(
        c.put(format!("{base}/lc-mpu/big/obj?partNumber=1&uploadId={uid}"))
            .body("part-bytes")
            .send().await.unwrap().status(),
        StatusCode::OK
    );

    let xml = "<LifecycleConfiguration><Rule><ID>mpu</ID><Filter><Prefix>big/</Prefix></Filter><Status>Enabled</Status>\
        <AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>";
    assert_eq!(c.put(format!("{base}/lc-mpu?lifecycle")).body(xml).send().await.unwrap().status(), StatusCode::OK);

    // At +15s (1.5 days < 2 days), upload stays.
    clock.advance(15 * 1_000_000_000);
    run_lifecycle_sweep(&state, &state.lifecycle, clock.now_ns() / 1_000_000);
    let list1 = c.get(format!("{base}/lc-mpu?uploads")).send().await.unwrap().text().await.unwrap();
    assert!(list1.contains(&uid), "upload should still exist before 2d:\n{list1}");

    // At +25s (2.5 days >= 2 days), upload aborts.
    clock.advance(10 * 1_000_000_000);
    run_lifecycle_sweep(&state, &state.lifecycle, clock.now_ns() / 1_000_000);
    let list2 = c.get(format!("{base}/lc-mpu?uploads")).send().await.unwrap().text().await.unwrap();
    assert!(!list2.contains(&uid), "upload should be aborted after 2d:\n{list2}");

    let metrics = c.get(format!("{base}/_awanfs/metrics")).send().await.unwrap().text().await.unwrap();
    assert!(metrics.contains("awanfs_lifecycle_uploads_aborted_total 1"), "metrics:\n{metrics}");
}

#[tokio::test]
async fn lifecycle_rereads_state_tags_and_rules_under_key_lock() {
    let dir = temp_dir("toctou");
    let (base, state, clock, _handle) = spawn_clocked(Some(&dir), 1_750_000_000, 10).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{base}/lc-race")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{base}/lc-race?versioning"))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send().await.unwrap().status(),
        StatusCode::OK
    );

    // Put v1 then v2; delete v2 so v1 is promoted back to current.
    // Noncurrent-only rule must NOT delete v1 once it is current again.
    let v1_res = c.put(format!("{base}/lc-race/k")).body("v1").send().await.unwrap();
    assert_eq!(v1_res.status(), StatusCode::OK);
    let _v1 = v1_res.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    clock.advance(1_000_000_000);
    let v2_res = c.put(format!("{base}/lc-race/k")).body("v2").send().await.unwrap();
    assert_eq!(v2_res.status(), StatusCode::OK);
    let v2 = v2_res.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    assert_eq!(c.delete(format!("{base}/lc-race/k?versionId={v2}")).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    let nc_xml = "<LifecycleConfiguration><Rule><ID>nc-only</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status>\
        <NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>";
    assert_eq!(c.put(format!("{base}/lc-race?lifecycle")).body(nc_xml).send().await.unwrap().status(), StatusCode::OK);

    // Put an object with expire=yes tag, then remove its tag before sweep -> must survive.
    assert_eq!(
        c.put(format!("{base}/lc-race/tagged"))
            .header("x-amz-tagging", "expire=yes")
            .body("stay")
            .send().await.unwrap().status(),
        StatusCode::OK
    );
    let tag_xml = "<LifecycleConfiguration><Rule><ID>tag-exp</ID><Filter><Tag><Key>expire</Key><Value>yes</Value></Tag></Filter><Status>Enabled</Status>\
        <Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
    assert_eq!(c.put(format!("{base}/lc-race?lifecycle")).body(tag_xml).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.delete(format!("{base}/lc-race/tagged?tagging")).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    // Put k and tagged back on nc-only + tag-exp rule won't touch promoted v1 or untagged object.
    assert_eq!(c.put(format!("{base}/lc-race?lifecycle")).body(nc_xml).send().await.unwrap().status(), StatusCode::OK);
    clock.advance(20 * 1_000_000_000);
    run_lifecycle_sweep(&state, &state.lifecycle, clock.now_ns() / 1_000_000);
    assert_eq!(c.get(format!("{base}/lc-race/k")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.get(format!("{base}/lc-race/tagged")).send().await.unwrap().status(), StatusCode::OK);
}
