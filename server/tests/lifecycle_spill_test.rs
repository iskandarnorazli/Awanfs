//! #475: the lifecycle sweep visits every current object, also those whose rows are in the spill
//! index past the in-memory hot set (`OpenLimits::max_objects`, 100,000 by default). Before the
//! fix it listed only the in-memory rows, so on a bucket with more objects than the hot set
//! most objects never expired.

use reqwest::StatusCode;
use s3_server::clock::SystemClock;
use s3_server::{run_lifecycle_sweep, start_s3_reactor, OpenLimits, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_lc_spill_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

const EXPIRE_AFTER_ONE_DAY: &str = "<LifecycleConfiguration><Rule><ID>all</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";

#[tokio::test]
async fn objects_past_the_hot_set_expire_too() {
    let limits = OpenLimits { max_objects: 4, spill_cache_bytes: Some(1 << 20) };
    let state = Arc::new(S3ServiceState::try_with_data_dir_limits(&temp_dir("expire"), Arc::new(SystemClock), limits).unwrap());
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/spill-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for i in 0..12 {
        let res = c.put(format!("{}/spill-bkt/obj-{:02}", url, i)).body(format!("payload {}", i)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "PUT obj-{:02}", i);
    }
    {
        let engine = state.engine.read();
        let b = engine.lookup_bucket("spill-bkt").unwrap();
        let in_memory = engine.buckets.get(&b).unwrap().key_index.len();
        assert!(in_memory < 12, "the test needs rows in the spill index; {} of 12 are in memory", in_memory);
    }
    let res = c.put(format!("{}/spill-bkt?lifecycle", url)).body(EXPIRE_AFTER_ONE_DAY).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "{}", res.text().await.unwrap());

    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    run_lifecycle_sweep(&state, &state.lifecycle, now_ms + 3 * 86_400_000);

    let mut still_there = Vec::new();
    for i in 0..12 {
        let status = c.get(format!("{}/spill-bkt/obj-{:02}", url, i)).send().await.unwrap().status();
        if status != StatusCode::NOT_FOUND {
            still_there.push(format!("obj-{:02}: {}", i, status));
        }
    }
    assert!(still_there.is_empty(), "not expired: {:?}", still_there);
}
