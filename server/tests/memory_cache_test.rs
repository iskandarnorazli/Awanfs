//! #126 (decision 36): the payload cache keeps views of the volume mapping, not heap copies of
//! request bodies. The OS page cache already holds the bytes, so a heap copy only adds RSS.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn a_put_is_cached_as_a_view_of_the_volume_not_the_request_body() {
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
    assert!(vm.maps(cached.as_ptr()), "the cached bytes are a view of the volume mapping, not a heap copy");

    let res = c.get(format!("{}/bkt/k", url)).send().await.unwrap();
    assert!(res.bytes().await.unwrap() == body);
    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
