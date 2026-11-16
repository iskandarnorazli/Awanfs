//! #158: One clock, real Last-Modified, and real bucket CreationDate.

use bytes::Bytes;
use reqwest::StatusCode;
use s3_server::clock::{format_http_date, Clock, ManualClock, SystemClock};
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("awanfs_clk_{}_{}_{}", name, std::process::id(), seq));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn no_system_time_now_outside_clock_rs() {
    let src_root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
    let allowed = ["clock.rs", "request_log.rs", "trace.rs", "metrics.rs"];
    let mut violations = Vec::new();
    let mut const_date_hits = Vec::new();

    fn visit(dir: &Path, allowed: &[&str], violations: &mut Vec<String>, const_date_hits: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, allowed, violations, const_date_hits);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                let content = std::fs::read_to_string(&path).unwrap();
                if !allowed.contains(&name) && content.contains("SystemTime::now()") {
                    violations.push(path.display().to_string());
                }
                if content.contains("10 Nov 2026") || content.contains("2026-11-10") {
                    const_date_hits.push(path.display().to_string());
                }
            }
        }
    }

    visit(src_root, &allowed, &mut violations, &mut const_date_hits);
    assert!(violations.is_empty(), "SystemTime::now() found outside clock.rs: {:?}", violations);
    assert!(const_date_hits.is_empty(), "Hardcoded 2026-11-10 date still in server/src: {:?}", const_date_hits);
}

#[test]
fn system_clock_and_manual_clock_work() {
    let sys = SystemClock;
    assert!(sys.now_ns() > 1_700_000_000_000_000_000);

    let manual = ManualClock::new(1_700_000_000_000_000_000);
    assert_eq!(manual.now_ns(), 1_700_000_000_000_000_000);
    manual.advance(5_000_000_000);
    assert_eq!(manual.now_ns(), 1_700_000_005_000_000_000);
}

#[tokio::test]
async fn last_modified_survives_restart_and_compaction() {
    let dir = temp_dir("lm_restart");
    let t0_ns: u64 = 1_731_200_000_000_000_000; // 2024-11-10T00:53:20Z
    let expected_http = format_http_date(t0_ns);
    let clock = Arc::new(ManualClock::new(t0_ns));

    {
        let state = Arc::new(S3ServiceState::with_data_dir_and_clock(&dir, clock.clone()));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
        let url = format!("http://{}", handle.local_addr);
        let c = reqwest::Client::new();

        assert_eq!(c.put(format!("{}/bkt-lm", url)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(
            c.put(format!("{}/bkt-lm/obj1", url)).body(Bytes::from_static(b"hello world")).send().await.unwrap().status(),
            StatusCode::OK
        );
        // Advance clock; reading must still report t0_ns.
        clock.advance(3_600_000_000_000);

        let head = c.head(format!("{}/bkt-lm/obj1", url)).send().await.unwrap();
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(head.headers().get("last-modified").unwrap().to_str().unwrap(), expected_http);

        let get = c.get(format!("{}/bkt-lm/obj1", url)).send().await.unwrap();
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.headers().get("last-modified").unwrap().to_str().unwrap(), expected_http);

        let list = c.get(format!("{}/bkt-lm?list-type=2", url)).send().await.unwrap().text().await.unwrap();
        assert!(list.contains(&s3_server::versions::iso8601(t0_ns)), "list xml: {}", list);

        // Compact the volume and verify Last-Modified is unchanged.
        let _ = state.perform_online_compaction().await.unwrap();
        let head2 = c.head(format!("{}/bkt-lm/obj1", url)).send().await.unwrap();
        assert_eq!(head2.headers().get("last-modified").unwrap().to_str().unwrap(), expected_http);

        handle.shutdown_gracefully(std::time::Duration::from_secs(2)).await;
    }

    // Restart with a later clock; replayed needle must keep t0_ns.
    let later_clock = Arc::new(ManualClock::new(t0_ns + 86_400_000_000_000));
    let state2 = Arc::new(S3ServiceState::with_data_dir_and_clock(&dir, later_clock));
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle2 = start_s3_reactor(addr2, Arc::clone(&state2)).await.unwrap();
    let url2 = format!("http://{}", handle2.local_addr);
    let c = reqwest::Client::new();

    let head3 = c.head(format!("{}/bkt-lm/obj1", url2)).send().await.unwrap();
    assert_eq!(head3.status(), StatusCode::OK);
    assert_eq!(head3.headers().get("last-modified").unwrap().to_str().unwrap(), expected_http);

    let buckets = c.get(format!("{}/", url2)).send().await.unwrap().text().await.unwrap();
    assert!(buckets.contains(&s3_server::versions::iso8601(t0_ns)), "buckets xml: {}", buckets);

    handle2.shutdown_gracefully(std::time::Duration::from_secs(2)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn two_objects_in_order_have_non_decreasing_last_modified_and_version_id_equals_creation_time() {
    let dir = temp_dir("non_dec");
    let t0_ns: u64 = 1_731_200_000_000_000_000;
    let clock = Arc::new(ManualClock::new(t0_ns));
    let state = Arc::new(S3ServiceState::with_data_dir_and_clock(&dir, clock.clone()));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/bkt-ver", url)).send().await.unwrap().status(), StatusCode::OK);
    let ver_xml = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(
        c.put(format!("{}/bkt-ver?versioning", url)).body(ver_xml).send().await.unwrap().status(),
        StatusCode::OK
    );

    let r1 = c.put(format!("{}/bkt-ver/k", url)).body("v1").send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let vid1 = r1.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    let expected_vid1 = s3_server::versions::id_string(t0_ns);
    assert_eq!(vid1, expected_vid1);

    clock.advance(2_000_000_000);
    let r2 = c.put(format!("{}/bkt-ver/k", url)).body("v2").send().await.unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let vid2 = r2.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    let expected_vid2 = s3_server::versions::id_string(t0_ns + 2_000_000_000);
    assert_eq!(vid2, expected_vid2);

    let h1 = c.head(format!("{}/bkt-ver/k?versionId={}", url, vid1)).send().await.unwrap();
    let h2 = c.head(format!("{}/bkt-ver/k?versionId={}", url, vid2)).send().await.unwrap();
    assert_eq!(h1.headers().get("last-modified").unwrap().to_str().unwrap(), format_http_date(t0_ns));
    assert_eq!(h2.headers().get("last-modified").unwrap().to_str().unwrap(), format_http_date(t0_ns + 2_000_000_000));

    handle.shutdown_gracefully(std::time::Duration::from_secs(2)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn legacy_bucket_manifest_without_timestamp_defaults_to_started_at() {
    let dir = temp_dir("legacy_bkt");
    std::fs::write(dir.join("buckets.manifest"), "+1:legacy-bkt\n").unwrap();
    let t_start: u64 = 1_731_300_000_000_000_000;
    let clock = Arc::new(ManualClock::new(t_start));
    let state = Arc::new(S3ServiceState::with_data_dir_and_clock(&dir, clock));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    let buckets = c.get(format!("{}/", url)).send().await.unwrap().text().await.unwrap();
    assert!(buckets.contains("legacy-bkt"));
    assert!(buckets.contains(&s3_server::versions::iso8601(t_start)), "buckets xml: {}", buckets);

    handle.shutdown_gracefully(std::time::Duration::from_secs(2)).await;
    let _ = std::fs::remove_dir_all(&dir);
}
