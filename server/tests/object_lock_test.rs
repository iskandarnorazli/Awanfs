//! #120: Object Lock, Retention (GOVERNANCE/COMPLIANCE), Legal Hold, Bypass Permission,
//! Compaction/Restart Survival, Lifecycle Protection, Reference-Model Property Tests,
//! and Grep Guard enforcing all retention rules live exclusively in `server/src/fsm.rs`.

use md5::{Digest, Md5};
use reqwest::{Method, StatusCode};
use s3_server::clock::{Clock, ManualClock};
use s3_server::digest::base64_encode;
use s3_server::fsm::{lock_step, Lock, LockEvent, RetentionMode};
use s3_server::iam::ReloadingIam;
use s3_server::lifecycle::{run_lifecycle_sweep, LifecycleRule};
use s3_server::{
    compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState,
    ServerHandle, SigV4KeyCache,
};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn md5_b64(data: &[u8]) -> String {
    let raw: [u8; 16] = Md5::digest(data).into();
    base64_encode(&raw)
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_olock_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_users_toml(dir: &Path, text: &str) -> PathBuf {
    let p = dir.join("users.toml");
    std::fs::write(&p, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    p
}

async fn spawn_clocked(
    dir: Option<&Path>,
    initial_epoch_sec: u64,
    iam: Option<Arc<ReloadingIam>>,
    auth: AuthConfig,
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
    state.auth = auth;
    if let Some(i) = iam {
        *state.iam.write() = Some(i);
    }
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, state, clock, handle)
}

fn amz_date(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let secs = unix_secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year,
        m,
        d,
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

fn signed_req(
    c: &reqwest::Client,
    ak: &str,
    sk: &str,
    method: Method,
    host: &str,
    path_and_query: &str,
    body: &[u8],
    now_sec: u64,
) -> reqwest::RequestBuilder {
    let (path, query) = path_and_query.split_once('?').unwrap_or((path_and_query, ""));
    let mut q: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (p.to_string(), String::new()),
        })
        .collect();
    q.sort();
    let canonical_query = q.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&");
    let date = amz_date(now_sec);
    let hash = sha256_hex(body);
    let canonical = format!(
        "{}\n{}\n{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\n{}",
        method.as_str(),
        path,
        canonical_query,
        host,
        hash,
        date,
        hash
    );
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canonical.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), sk, &date[..8], "us-east-1", "s3", &sts);
    c.request(method, format!("http://{}{}", host, path_and_query))
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", hash)
        .header(
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}",
                ak, scope, sig
            ),
        )
        .body(body.to_vec())
}

#[tokio::test]
async fn bucket_lock_config_enable_after_create_and_suspend_refused() {
    let (base, _state, _clock, _h) = spawn_clocked(None, 1_750_000_000, None, AuthConfig::open()).await;
    let c = reqwest::Client::new();

    // Non-lock bucket returns 404 ObjectLockConfigurationNotFoundError on GET, 409 InvalidBucketState on PUT.
    assert_eq!(c.put(format!("{base}/plain-bkt")).send().await.unwrap().status(), StatusCode::OK);
    let res = c.get(format!("{base}/plain-bkt?object-lock")).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("ObjectLockConfigurationNotFoundError"));

    let lock_cfg = r#"<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>2</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#;
    let res = c.put(format!("{base}/plain-bkt?object-lock")).body(lock_cfg).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);
    assert!(res.text().await.unwrap().contains("InvalidBucketState"));

    // Enabling versioning to Suspended still refuses enabling Object Lock; Enabled allows it!
    let sus_xml = "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{base}/plain-bkt?versioning")).body(sus_xml).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{base}/plain-bkt?object-lock")).body(lock_cfg).send().await.unwrap().status(), StatusCode::CONFLICT);

    let en_xml = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{base}/plain-bkt?versioning")).body(en_xml).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{base}/plain-bkt?object-lock")).body(lock_cfg).send().await.unwrap().status(), StatusCode::OK);
    let got = c.get(format!("{base}/plain-bkt?object-lock")).send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert!(got.text().await.unwrap().contains("<Mode>GOVERNANCE</Mode>"));

    // Suspending versioning on an object-lock bucket refuses with 409 InvalidBucketState.
    let res = c.put(format!("{base}/plain-bkt?versioning")).body(sus_xml).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);
    assert!(res.text().await.unwrap().contains("InvalidBucketState"));

    // CreateBucket with x-amz-bucket-object-lock-enabled: true turns versioning Enabled automatically.
    let res = c
        .put(format!("{base}/created-lock"))
        .header("x-amz-bucket-object-lock-enabled", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let ver = c.get(format!("{base}/created-lock?versioning")).send().await.unwrap().text().await.unwrap();
    assert!(ver.contains("<Status>Enabled</Status>"), "got: {ver}");
    assert_eq!(
        c.put(format!("{base}/created-lock?versioning")).body(sus_xml).send().await.unwrap().status(),
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn retention_legal_hold_headers_iso8601_md5_copy_and_multipart() {
    // 2025-06-15T12:00:00Z = 1749988800
    let (base, _state, _clock, _h) = spawn_clocked(None, 1_749_988_800, None, AuthConfig::open()).await;
    let c = reqwest::Client::new();

    // Non-lock bucket refuses retention/legal-hold with 400 InvalidRequest.
    assert_eq!(c.put(format!("{base}/no-lock")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{base}/no-lock/k")).body("abc").send().await.unwrap().status(), StatusCode::OK);
    let r = c.get(format!("{base}/no-lock/k?retention")).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert!(r.text().await.unwrap().contains("InvalidRequest"));
    let r = c.get(format!("{base}/no-lock/k?legal-hold")).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert!(r.text().await.unwrap().contains("InvalidRequest"));

    // Lock bucket with DefaultRetention = GOVERNANCE 3 days.
    assert_eq!(
        c.put(format!("{base}/olock"))
            .header("x-amz-bucket-object-lock-enabled", "true")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let cfg_xml = r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>3</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#;
    assert_eq!(c.put(format!("{base}/olock?object-lock")).body(cfg_xml).send().await.unwrap().status(), StatusCode::OK);

    // PutObject without Content-MD5 or checksum on a bucket with DefaultRetention fails with 400 InvalidRequest.
    let res = c.put(format!("{base}/olock/missing-md5")).body("abc").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("InvalidRequest"));

    // PutObject with Content-MD5 gets DefaultRetention = 2025-06-18T12:00:00Z from ManualClock.
    let res = c
        .put(format!("{base}/olock/def"))
        .header("content-md5", md5_b64(b"abc"))
        .body("abc")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let head = c.head(format!("{base}/olock/def")).send().await.unwrap();
    assert_eq!(head.headers().get("x-amz-object-lock-mode").unwrap(), "GOVERNANCE");
    assert_eq!(head.headers().get("x-amz-object-lock-retain-until-date").unwrap(), "2025-06-18T12:00:00Z");

    // PutObject with explicit fractional ISO8601 + legal hold ON round-trips exact date string in GET and HEAD.
    let res = c
        .put(format!("{base}/olock/exp"))
        .header("content-md5", md5_b64(b"xyz"))
        .header("x-amz-object-lock-mode", "COMPLIANCE")
        .header("x-amz-object-lock-retain-until-date", "2030-01-01T00:00:00.123456Z")
        .header("x-amz-object-lock-legal-hold", "ON")
        .body("xyz")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let vid_exp = res.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    let head = c.head(format!("{base}/olock/exp?versionId={vid_exp}")).send().await.unwrap();
    assert_eq!(head.headers().get("x-amz-object-lock-mode").unwrap(), "COMPLIANCE");
    assert_eq!(head.headers().get("x-amz-object-lock-retain-until-date").unwrap(), "2030-01-01T00:00:00.123456Z");
    assert_eq!(head.headers().get("x-amz-object-lock-legal-hold").unwrap(), "ON");
    let ret_xml = c.get(format!("{base}/olock/exp?retention")).send().await.unwrap().text().await.unwrap();
    assert!(ret_xml.contains("<RetainUntilDate>2030-01-01T00:00:00.123456Z</RetainUntilDate>"), "{ret_xml}");

    // CopyObject does NOT copy source COMPLIANCE lock or legal hold; it applies destination explicit/default lock.
    let cp = c
        .put(format!("{base}/olock/copied"))
        .header("x-amz-copy-source", format!("/olock/exp?versionId={vid_exp}"))
        .send()
        .await
        .unwrap();
    assert_eq!(cp.status(), StatusCode::OK);
    let cp_head = c.head(format!("{base}/olock/copied")).send().await.unwrap();
    assert_eq!(cp_head.headers().get("x-amz-object-lock-mode").unwrap(), "GOVERNANCE");
    assert!(cp_head.headers().get("x-amz-object-lock-legal-hold").is_none());

    // Multipart CreateMultipartUpload + CompleteMultipartUpload with lock headers.
    let init = c
        .post(format!("{base}/olock/mp?uploads"))
        .header("x-amz-object-lock-mode", "GOVERNANCE")
        .header("x-amz-object-lock-retain-until-date", "2031-02-03T04:05:06Z")
        .header("x-amz-object-lock-legal-hold", "ON")
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let init_xml = init.text().await.unwrap();
    let uid = init_xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap();
    let part = c.put(format!("{base}/olock/mp?partNumber=1&uploadId={uid}")).body("hello").send().await.unwrap();
    let etag = part.headers().get("etag").unwrap().to_str().unwrap().trim_matches('"').to_string();
    let comp_xml = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>");
    let comp = c.post(format!("{base}/olock/mp?uploadId={uid}")).body(comp_xml).send().await.unwrap();
    assert_eq!(comp.status(), StatusCode::OK);
    let mp_head = c.head(format!("{base}/olock/mp")).send().await.unwrap();
    assert_eq!(mp_head.headers().get("x-amz-object-lock-mode").unwrap(), "GOVERNANCE");
    assert_eq!(mp_head.headers().get("x-amz-object-lock-retain-until-date").unwrap(), "2031-02-03T04:05:06Z");
    assert_eq!(mp_head.headers().get("x-amz-object-lock-legal-hold").unwrap(), "ON");
}

#[tokio::test]
async fn bypass_governance_permission_and_compliance_immutability() {
    let cfg2 = r#"
[[users]]
name = "admin"
account = "acct1"
access_key = "AKADMIN"
secret_key = "secret-admin"
policy = '{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}'

[[users]]
name = "helper-with-bypass"
account = "acct1"
access_key = "AKBYP"
secret_key = "secret-byp"
policy = '{"Statement": {"Effect": "Allow", "Action": ["s3:PutObjectRetention", "s3:DeleteObject", "s3:BypassGovernanceRetention"], "Resource": "*"}}'

[[users]]
name = "helper-no-bypass"
account = "acct1"
access_key = "AKNOBYP"
secret_key = "secret-nobyp"
policy = '{"Statement": {"Effect": "Allow", "Action": ["s3:PutObjectRetention", "s3:DeleteObject"], "Resource": "*"}}'
"#;
    let dir2 = temp_dir("iam_bypass_2");
    let iam2 = ReloadingIam::open(&write_users_toml(&dir2, cfg2)).unwrap();
    let auth2 = AuthConfig {
        credentials: Some(("AKROOT".into(), "secret-root".into())),
        allow_anonymous: false,
    };
    let (base, _state, clock, _h2) = spawn_clocked(Some(&dir2), 1_750_000_000, Some(iam2), auth2).await;
    let host = base.strip_prefix("http://").unwrap();
    let c = reqwest::Client::new();

    let now_sec = clock.now_ns() / 1_000_000_000;
    // Admin creates lock bucket owned by acct1.
    let res = signed_req(&c, "AKADMIN", "secret-admin", Method::PUT, host, "/bkt-iam", b"", now_sec)
        .header("x-amz-bucket-object-lock-enabled", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Put GOVERNANCE object until 2030.
    let body = b"gov-data";
    let put = signed_req(&c, "AKADMIN", "secret-admin", Method::PUT, host, "/bkt-iam/gov", body, now_sec)
        .header("content-md5", md5_b64(body))
        .header("x-amz-object-lock-mode", "GOVERNANCE")
        .header("x-amz-object-lock-retain-until-date", "2030-01-01T00:00:00Z")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let vid_gov = put.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    // User WITHOUT s3:BypassGovernanceRetention tries to shorten governance WITH bypass header -> 403!
    let shorten_xml = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2028-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let res = signed_req(&c, "AKNOBYP", "secret-nobyp", Method::PUT, host, "/bkt-iam/gov?retention", shorten_xml, now_sec)
        .header("x-amz-bypass-governance-retention", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // Same user tries to delete version WITH bypass header -> 403!
    let res = signed_req(&c, "AKNOBYP", "secret-nobyp", Method::DELETE, host, &format!("/bkt-iam/gov?versionId={vid_gov}"), b"", now_sec)
        .header("x-amz-bypass-governance-retention", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // User WITH s3:BypassGovernanceRetention shortens governance and deletes version -> 200 & 204!
    let res = signed_req(&c, "AKBYP", "secret-byp", Method::PUT, host, "/bkt-iam/gov?retention", shorten_xml, now_sec)
        .header("x-amz-bypass-governance-retention", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let res = signed_req(&c, "AKBYP", "secret-byp", Method::DELETE, host, &format!("/bkt-iam/gov?versionId={vid_gov}"), b"", now_sec)
        .header("x-amz-bypass-governance-retention", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // Put COMPLIANCE object until 2025-07-01T00:00:00Z (1751328000) in root-owned bucket.
    let res = signed_req(&c, "AKROOT", "secret-root", Method::PUT, host, "/bkt-comp", b"", now_sec)
        .header("x-amz-bucket-object-lock-enabled", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let put_c = signed_req(&c, "AKROOT", "secret-root", Method::PUT, host, "/bkt-comp/obj", body, now_sec)
        .header("content-md5", md5_b64(body))
        .header("x-amz-object-lock-mode", "COMPLIANCE")
        .header("x-amz-object-lock-retain-until-date", "2025-07-01T00:00:00Z")
        .send()
        .await
        .unwrap();
    assert_eq!(put_c.status(), StatusCode::OK);
    let vid_c = put_c.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    // Neither shorten, switch to GOVERNANCE, clear retention, nor delete works even for ROOT with bypass=true!
    for req_body in [
        &b"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2025-06-20T00:00:00Z</RetainUntilDate></Retention>"[..],
        &b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"[..],
        &b"<Retention/>"[..],
    ] {
        let r = signed_req(&c, "AKROOT", "secret-root", Method::PUT, host, &format!("/bkt-comp/obj?retention&versionId={vid_c}"), req_body, now_sec)
            .header("x-amz-bypass-governance-retention", "true")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN);
    }
    let del = signed_req(&c, "AKROOT", "secret-root", Method::DELETE, host, &format!("/bkt-comp/obj?versionId={vid_c}"), b"", now_sec)
        .header("x-amz-bypass-governance-retention", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), StatusCode::FORBIDDEN);

    // Advance ManualClock past 2025-07-01T00:00:00Z -> COMPLIANCE expired -> delete succeeds!
    clock.set(1_751_328_001 * 1_000_000_000);
    let del_ok = signed_req(&c, "AKROOT", "secret-root", Method::DELETE, host, &format!("/bkt-comp/obj?versionId={vid_c}"), b"", 1_751_328_001)
        .send()
        .await
        .unwrap();
    assert_eq!(del_ok.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn lock_survives_compaction_and_restart_and_blocks_lifecycle() {
    let dir = temp_dir("compact_restart");
    // 2025-06-01T00:00:00Z = 1748736000
    let (base, state, _clock, handle) = spawn_clocked(Some(&dir), 1_748_736_000, None, AuthConfig::open()).await;
    state.set_volume_span(64 * 1024);
    let c = reqwest::Client::new();

    assert_eq!(
        c.put(format!("{base}/bkt-survive"))
            .header("x-amz-bucket-object-lock-enabled", "true")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Put object with COMPLIANCE retention until 2026-01-01T00:00:00Z (1767225600).
    let payload = vec![b'A'; 16 * 1024];
    let put = c
        .put(format!("{base}/bkt-survive/keep"))
        .header("content-md5", md5_b64(&payload))
        .header("x-amz-object-lock-mode", "COMPLIANCE")
        .header("x-amz-object-lock-retain-until-date", "2026-01-01T00:00:00Z")
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let vid = put.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    // Put some throwaway unlocked versions to force volume roll, then delete those unlocked versions.
    let garbage = vec![b'G'; 24 * 1024];
    for i in 0..3 {
        let r = c
            .put(format!("{base}/bkt-survive/trash-{i}"))
            .body(garbage.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let tv = r.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
        assert_eq!(
            c.delete(format!("{base}/bkt-survive/trash-{i}?versionId={tv}")).send().await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
    }

    // Configure lifecycle 1-day expiration and sweep far in the future for lifecycle age (say 2 days after 2025-06-01), while COMPLIANCE holds until 2026.
    state.lifecycle.set_rules(
        "bkt-survive",
        vec![LifecycleRule {
            id: "exp".to_string(),
            status: true,
            prefix: String::new(),
            expiration_days: Some(1),
            abort_multipart_days: None,
        }],
    );
    run_lifecycle_sweep(&state, &state.lifecycle, (1_748_736_000 + 2 * 86_400) * 1000);
    assert_eq!(
        c.delete(format!("{base}/bkt-survive/keep?versionId={vid}")).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    // Compact oldest volume -> locked base needle moves to active volume.
    let rep = state.compact_oldest_volume().await.unwrap();
    assert!(rep.live_needles_copied >= 1, "expected kept needle to be copied: {rep:?}");

    // Update retention (extend to 2027) AND legal hold ON via 0x200 delta needles AFTER compaction.
    let ext_xml = "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>";
    assert_eq!(c.put(format!("{base}/bkt-survive/keep?retention&versionId={vid}")).body(ext_xml).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{base}/bkt-survive/keep?legal-hold&versionId={vid}"))
            .body("<LegalHold><Status>ON</Status></LegalHold>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Shutdown and restart from same data_dir with clock still in 2025.
    drop(handle);
    drop(state);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (base2, _state2, clock2, _h2) = spawn_clocked(Some(&dir), 1_748_736_500, None, AuthConfig::open()).await;
    let head = c.head(format!("{base2}/bkt-survive/keep?versionId={vid}")).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers().get("x-amz-object-lock-mode").unwrap(), "COMPLIANCE");
    assert_eq!(head.headers().get("x-amz-object-lock-retain-until-date").unwrap(), "2027-01-01T00:00:00Z");
    assert_eq!(head.headers().get("x-amz-object-lock-legal-hold").unwrap(), "ON");

    // Cannot suspend versioning after restart; cannot delete version.
    assert_eq!(
        c.put(format!("{base2}/bkt-survive?versioning"))
            .body("<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        c.delete(format!("{base2}/bkt-survive/keep?versionId={vid}")).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    // Advance clock past 2027-01-01 (1798761600): legal hold STILL ON -> still 403!
    clock2.set(1_800_000_000 * 1_000_000_000);
    assert_eq!(
        c.delete(format!("{base2}/bkt-survive/keep?versionId={vid}")).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    // Turn legal hold OFF -> now expired retention + hold OFF -> delete succeeds!
    assert_eq!(
        c.put(format!("{base2}/bkt-survive/keep?legal-hold&versionId={vid}"))
            .body("<LegalHold><Status>OFF</Status></LegalHold>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        c.delete(format!("{base2}/bkt-survive/keep?versionId={vid}")).send().await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn batch_delete_objects_reports_version_id_in_errors_and_delete_marker_keeps_version() {
    let (base, _state, _clock, _h) = spawn_clocked(None, 1_750_000_000, None, AuthConfig::open()).await;
    let c = reqwest::Client::new();
    assert_eq!(
        c.put(format!("{base}/bkt-multi"))
            .header("x-amz-bucket-object-lock-enabled", "true")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let p1 = c
        .put(format!("{base}/bkt-multi/k1"))
        .header("content-md5", md5_b64(b"one"))
        .header("x-amz-object-lock-mode", "GOVERNANCE")
        .header("x-amz-object-lock-retain-until-date", "2030-01-01T00:00:00Z")
        .body("one")
        .send()
        .await
        .unwrap();
    let v1 = p1.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    let p2 = c.put(format!("{base}/bkt-multi/k2")).body("two").send().await.unwrap();
    let v2 = p2.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    // Unversioned DELETE creates a delete marker while preserving k1@v1!
    let dm = c.delete(format!("{base}/bkt-multi/k1")).send().await.unwrap();
    assert_eq!(dm.status(), StatusCode::NO_CONTENT);
    assert_eq!(dm.headers().get("x-amz-delete-marker").unwrap(), "true");
    assert_eq!(c.get(format!("{base}/bkt-multi/k1?versionId={v1}")).send().await.unwrap().status(), StatusCode::OK);

    // Batch DeleteObjects without bypass: k2@v2 deleted, k1@v1 errors with <VersionId>.
    let del_xml = format!("<Delete><Object><Key>k1</Key><VersionId>{v1}</VersionId></Object><Object><Key>k2</Key><VersionId>{v2}</VersionId></Object></Delete>");
    let res = c.post(format!("{base}/bkt-multi?delete")).body(del_xml).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<Deleted>"), "{body}");
    assert!(body.contains(&format!("<VersionId>{v1}</VersionId>")), "{body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");

    // Batch DeleteObjects WITH bypass=true deletes k1@v1.
    let del_k1 = format!("<Delete><Object><Key>k1</Key><VersionId>{v1}</VersionId></Object></Delete>");
    let res = c
        .post(format!("{base}/bkt-multi?delete"))
        .header("x-amz-bypass-governance-retention", "true")
        .body(del_k1)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(!body.contains("<Error>"), "{body}");
    assert!(body.contains(&format!("<VersionId>{v1}</VersionId>")), "{body}");
}

#[derive(Clone, Copy)]
struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

#[tokio::test]
async fn randomized_sequence_never_deletes_version_while_may_delete_refuses() {
    let dir = temp_dir("prop_seq");
    let t0 = 1_750_000_000u64;
    let (mut base, mut state, mut clock, mut handle) = spawn_clocked(Some(&dir), t0, None, AuthConfig::open()).await;
    state.set_volume_span(48 * 1024);
    let c = reqwest::Client::new();

    assert_eq!(
        c.put(format!("{base}/prop-bkt"))
            .header("x-amz-bucket-object-lock-enabled", "true")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Reference model: vid -> Lock
    let mut model: BTreeMap<String, Lock> = BTreeMap::new();
    let mut rng = SplitMix64(0x120_0B1EC_7_0001);
    let key = "item";

    for step_idx in 0..80 {
        let now_sec = clock.now_ns() / 1_000_000_000;
        match rng.next() % 7 {
            0 => {
                // Put new version with random optional lock headers.
                let payload = vec![b'P'; 4096];
                let mut req = c.put(format!("{base}/prop-bkt/{key}")).header("content-md5", md5_b64(&payload));
                let mut lock = Lock::default();
                if rng.next() & 1 == 0 {
                    let mode = if rng.next() & 1 == 0 { RetentionMode::Governance } else { RetentionMode::Compliance };
                    let delta = ((rng.next() % 5) + 1) * 100;
                    let until = now_sec + delta;
                    let iso = {
                        let days = (until / 86_400) as i64;
                        let secs = until % 86_400;
                        let z = days + 719_468;
                        let era = z.div_euclid(146_097);
                        let doe = z - era * 146_097;
                        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
                        let y = yoe + era * 400;
                        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
                        let mp = (5 * doy + 2) / 153;
                        let d = doy - (153 * mp + 2) / 5 + 1;
                        let m = if mp < 10 { mp + 3 } else { mp - 9 };
                        let year = y + if m <= 2 { 1 } else { 0 };
                        format!("{year:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", secs / 3600, (secs % 3600) / 60, secs % 60)
                    };
                    req = req
                        .header("x-amz-object-lock-mode", if mode == RetentionMode::Governance { "GOVERNANCE" } else { "COMPLIANCE" })
                        .header("x-amz-object-lock-retain-until-date", iso);
                    lock.retention = Some((mode, until));
                }
                if rng.next() % 3 == 0 {
                    req = req.header("x-amz-object-lock-legal-hold", "ON");
                    lock.legal_hold = true;
                }
                let res = req.body(payload).send().await.unwrap();
                assert_eq!(res.status(), StatusCode::OK, "step {step_idx} put failed");
                let vid = res.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
                model.insert(vid, lock);
            }
            1 if !model.is_empty() => {
                // Attempt PutRetention against a random existing version.
                let vids: Vec<String> = model.keys().cloned().collect();
                let vid = vids[(rng.next() as usize) % vids.len()].clone();
                let mode = if rng.next() & 1 == 0 { RetentionMode::Governance } else { RetentionMode::Compliance };
                let delta = ((rng.next() % 6) as i64 - 2) * 100;
                let until = (now_sec as i64 + delta).max(1_700_000_000) as u64;
                let bypass = rng.next() & 1 == 0;
                let days = (until / 86_400) as i64;
                let secs = until % 86_400;
                let z = days + 719_468;
                let era = z.div_euclid(146_097);
                let doe = z - era * 146_097;
                let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
                let y = yoe + era * 400;
                let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
                let mp = (5 * doy + 2) / 153;
                let d = doy - (153 * mp + 2) / 5 + 1;
                let m = if mp < 10 { mp + 3 } else { mp - 9 };
                let year = y + if m <= 2 { 1 } else { 0 };
                let iso = format!("{year:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", secs / 3600, (secs % 3600) / 60, secs % 60);
                let xml = format!(
                    "<Retention><Mode>{}</Mode><RetainUntilDate>{iso}</RetainUntilDate></Retention>",
                    if mode == RetentionMode::Governance { "GOVERNANCE" } else { "COMPLIANCE" }
                );
                let mut req = c.put(format!("{base}/prop-bkt/{key}?retention&versionId={vid}")).body(xml);
                if bypass {
                    req = req.header("x-amz-bypass-governance-retention", "true");
                }
                let res = req.send().await.unwrap();
                let expected = lock_step(model[&vid], LockEvent::PutRetention { mode, until, bypass_governance: bypass }, now_sec);
                match expected {
                    Ok(next) => {
                        assert_eq!(res.status(), StatusCode::OK, "step {step_idx} expected OK");
                        model.insert(vid, next);
                    }
                    Err(refusal) => {
                        assert_eq!(res.status().as_u16(), refusal.status, "step {step_idx} expected {}", refusal.status);
                    }
                }
            }
            2 if !model.is_empty() => {
                // Toggle legal hold ON/OFF.
                let vids: Vec<String> = model.keys().cloned().collect();
                let vid = vids[(rng.next() as usize) % vids.len()].clone();
                let on = rng.next() & 1 == 0;
                let xml = format!("<LegalHold><Status>{}</Status></LegalHold>", if on { "ON" } else { "OFF" });
                let res = c.put(format!("{base}/prop-bkt/{key}?legal-hold&versionId={vid}")).body(xml).send().await.unwrap();
                assert_eq!(res.status(), StatusCode::OK);
                let next = lock_step(model[&vid], LockEvent::PutLegalHold { on }, now_sec).unwrap();
                model.insert(vid, next);
            }
            3 if !model.is_empty() => {
                // Delete random version with random bypass.
                let vids: Vec<String> = model.keys().cloned().collect();
                let vid = vids[(rng.next() as usize) % vids.len()].clone();
                let bypass = rng.next() & 1 == 0;
                let mut req = c.delete(format!("{base}/prop-bkt/{key}?versionId={vid}"));
                if bypass {
                    req = req.header("x-amz-bypass-governance-retention", "true");
                }
                let res = req.send().await.unwrap();
                match model[&vid].may_delete(now_sec, bypass) {
                    Ok(()) => {
                        assert_eq!(res.status(), StatusCode::NO_CONTENT, "step {step_idx} expected delete OK");
                        model.remove(&vid);
                    }
                    Err(_) => {
                        assert_eq!(res.status(), StatusCode::FORBIDDEN, "step {step_idx} expected delete 403");
                    }
                }
            }
            4 => {
                // Advance clock or run lifecycle sweep: neither may remove any locked version.
                clock.advance(250 * 1_000_000_000);
                run_lifecycle_sweep(&state, &state.lifecycle, (clock.now_ns() / 1_000_000) + 10_000_000);
            }
            5 => {
                let _ = state.compact_oldest_volume().await;
            }
            _ => {
                // Unversioned delete (adds delete marker) + periodic server restart.
                assert_eq!(c.delete(format!("{base}/prop-bkt/{key}")).send().await.unwrap().status(), StatusCode::NO_CONTENT);
                if step_idx % 20 == 6 {
                    let cur_ns = clock.now_ns();
                    drop(handle);
                    drop(state);
                    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                    let restarted = spawn_clocked(Some(&dir), cur_ns / 1_000_000_000, None, AuthConfig::open()).await;
                    base = restarted.0;
                    state = restarted.1;
                    clock = restarted.2;
                    handle = restarted.3;
                }
            }
        }

        // Verify every version currently expected in `model` is still readable by GET!
        for vid in model.keys() {
            let head = c.head(format!("{base}/prop-bkt/{key}?versionId={vid}")).send().await.unwrap();
            assert_eq!(head.status(), StatusCode::OK, "step {step_idx}: version {vid} vanished!");
        }
    }
}

#[tokio::test]
async fn pre_versioning_null_object_supports_put_and_get_lock_without_version_id() {
    let dir = temp_dir("null_lock");
    let (base, _state, _clock, handle) = spawn_clocked(Some(&dir), 1_750_000_000, None, AuthConfig::open()).await;
    let c = reqwest::Client::new();

    // Put object before versioning or Object Lock is enabled (unversioned "null" object).
    assert_eq!(c.put(format!("{base}/null-bkt")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{base}/null-bkt/legacy.txt")).body("old").send().await.unwrap().status(),
        StatusCode::OK
    );

    // Enable Versioning, then Object Lock on the bucket.
    assert_eq!(
        c.put(format!("{base}/null-bkt?versioning"))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        c.put(format!("{base}/null-bkt?object-lock"))
            .body("<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // PUT & GET retention and legal hold without ?versionId= must both resolve the legacy null object.
    let ret_xml = "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";
    assert_eq!(
        c.put(format!("{base}/null-bkt/legacy.txt?retention")).body(ret_xml).send().await.unwrap().status(),
        StatusCode::OK
    );
    let get_ret = c.get(format!("{base}/null-bkt/legacy.txt?retention")).send().await.unwrap();
    assert_eq!(get_ret.status(), StatusCode::OK);
    assert!(get_ret.text().await.unwrap().contains("2030-01-01T00:00:00Z"));

    assert_eq!(
        c.put(format!("{base}/null-bkt/legacy.txt?legal-hold"))
            .body("<LegalHold><Status>ON</Status></LegalHold>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let get_lh = c.get(format!("{base}/null-bkt/legacy.txt?legal-hold")).send().await.unwrap();
    assert_eq!(get_lh.status(), StatusCode::OK);
    assert!(get_lh.text().await.unwrap().contains("<Status>ON</Status>"));

    drop(handle);
}

#[test]
fn grep_guard_no_retention_comparisons_outside_fsm_rs() {
    let src_root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
    let mut stack = vec![src_root.to_path_buf()];
    let mut violations = Vec::new();
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            if path.file_name().and_then(|n| n.to_str()) == Some("fsm.rs") {
                continue;
            }
            let rel = path.strip_prefix(src_root).unwrap().to_string_lossy().to_string();
            let text = std::fs::read_to_string(&path).unwrap();
            for (idx, line) in text.lines().enumerate() {
                if line.trim() == "#[cfg(test)]" {
                    break;
                }
                let code = line.split("//").next().unwrap_or("");
                let scrubbed = code.replace("->", "").replace("=>", "");
                if scrubbed.contains("retain_until") && (scrubbed.contains('<') || scrubbed.contains('>')) {
                    violations.push(format!("{}:{}: {}", rel, idx + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "retain_until comparisons must live only in fsm.rs:\n{}",
        violations.join("\n")
    );
}
