//! #42: /_awanfs/metrics in Prometheus text format.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// A strict reader of the text exposition format: every line is a comment (`# HELP`/`# TYPE`)
/// or `name{label="v",...} value`, every sample's family has a TYPE line before it, and values
/// parse as numbers. Returns samples keyed by `name{labels}`.
fn parse_prometheus(text: &str) -> HashMap<String, f64> {
    let mut typed = std::collections::HashSet::new();
    let mut out = HashMap::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut it = rest.split(' ');
            let name = it.next().unwrap();
            let kind = it.next().unwrap();
            assert!(["counter", "gauge", "histogram"].contains(&kind), "type of {}: {}", name, kind);
            typed.insert(name.to_string());
            continue;
        }
        if line.starts_with("# HELP ") {
            continue;
        }
        assert!(!line.starts_with('#'), "unknown comment: {}", line);
        let (series, value) = line.rsplit_once(' ').unwrap_or_else(|| panic!("no value: {}", line));
        let v: f64 = value.parse().unwrap_or_else(|_| panic!("bad value in {}", line));
        let name = series.split('{').next().unwrap();
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'), "bad name {}", name);
        if let Some(labels) = series.strip_prefix(name) {
            if !labels.is_empty() {
                assert!(labels.starts_with('{') && labels.ends_with('}'), "labels of {}", line);
                for pair in labels[1..labels.len() - 1].split(',') {
                    let (k, v) = pair.split_once('=').unwrap_or_else(|| panic!("label {}", pair));
                    assert!(!k.is_empty() && v.starts_with('"') && v.ends_with('"'), "label {}", pair);
                }
            }
        }
        let family = ["_bucket", "_sum", "_count"].iter().find_map(|s| name.strip_suffix(s).filter(|f| typed.contains(*f))).unwrap_or(name);
        assert!(typed.contains(family), "{} has no TYPE line", name);
        out.insert(series.to_string(), v);
    }
    out
}

#[tokio::test]
async fn metrics_count_requests_and_match_the_work_counters() {
    let state = Arc::new(S3ServiceState::new());
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/metrics", url)).send().await.unwrap().status(), StatusCode::OK, "a bucket named metrics");
    for i in 0..3 {
        assert_eq!(c.put(format!("{}/metrics/k{}", url, i)).body(vec![b'x'; 1000]).send().await.unwrap().status(), StatusCode::OK);
    }
    for i in 0..2 {
        assert_eq!(c.get(format!("{}/metrics/k{}", url, i)).send().await.unwrap().bytes().await.unwrap().len(), 1000);
    }
    assert_eq!(c.get(format!("{}/metrics/missing", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let res = c.get(format!("{}/_awanfs/metrics", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let text = res.text().await.unwrap();
    let m = parse_prometheus(&text);
    let w = state.work.snapshot();

    assert_eq!(m["awanfs_requests_total{operation=\"CreateBucket\",status=\"2xx\"}"], 1.0);
    assert_eq!(m["awanfs_requests_total{operation=\"PutObject\",status=\"2xx\"}"], 3.0);
    assert_eq!(m["awanfs_requests_total{operation=\"GetObject\",status=\"2xx\"}"], 2.0);
    assert_eq!(m["awanfs_requests_total{operation=\"GetObject\",status=\"4xx\"}"], 1.0);
    assert_eq!(m["awanfs_request_duration_seconds_count{operation=\"PutObject\"}"], 3.0);
    assert_eq!(m["awanfs_request_duration_seconds_bucket{operation=\"PutObject\",le=\"+Inf\"}"], 3.0);
    assert_eq!(m["awanfs_request_bytes_total"], 3000.0);
    assert!(m["awanfs_response_bytes_total"] >= 2000.0);
    assert_eq!(m["awanfs_disk_writes_total"], w.disk_writes as f64);
    assert_eq!(m["awanfs_disk_syncs_total"], w.disk_syncs as f64);
    assert_eq!(m["awanfs_payload_copies_total"], w.payload_copies as f64);
    assert_eq!(m["awanfs_payload_cache_hits_total"], 2.0, "both GETs came from the cache");
    assert_eq!(m["awanfs_ready"], 1.0);
    assert!(m["awanfs_volume_used_bytes"] > 0.0);
    assert_eq!(state.metrics.requests("PutObject"), 3);
}
