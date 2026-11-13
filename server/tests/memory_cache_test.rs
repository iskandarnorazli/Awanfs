//! What a PUT leaves in memory (#126). AwanFS writes volumes with direct I/O, so written bytes
//! are not in the OS page cache. Reading a payload back through the read-only volume mapping to
//! cache a view of it therefore faults every byte in from disk: measured on GitHub
//! `ubuntu-latest`, that took the server's peak RSS over the ratio workloads from 117.0 MiB
//! (run 43400978818) to 2258.9 MiB (run 43413585215). The cache keeps the request's buffer
//! instead, which the per-object and total caps bound.
//!
//! The ceiling itself is checked by `benchmarks/ratio_check.py` against
//! `rss_ceiling_mib` in `benchmarks/ratio_baseline.json`. This test is the cheap guard that
//! runs on every PR: it fails if a PUT puts volume-mapping bytes in the cache again.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn a_put_does_not_cache_bytes_it_faulted_in_from_the_volume() {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_memcache_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let st = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&st)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let body: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    assert_eq!(c.put(format!("{}/bkt/k", url)).body(body.clone()).send().await.unwrap().status(), StatusCode::OK);

    let b_id = st.engine.read().lookup_bucket("bkt").unwrap();
    let off = st.engine.read().get_object(b_id, b"k").unwrap().vol_offset;
    let cached = st.payload_cache.get(b_id, off).expect("the PUT is cached");
    assert_eq!(&cached[..], &body[..]);
    let vm = st.volume_manager.read().clone().expect("the active volume, where the PUT went");
    assert!(
        !vm.maps(cached.as_ptr()),
        "the PUT cached bytes inside the volume mapping, so it read the payload back from disk"
    );

    let res = c.get(format!("{}/bkt/k", url)).send().await.unwrap();
    assert!(res.bytes().await.unwrap() == body);
    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
