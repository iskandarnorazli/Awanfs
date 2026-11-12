//! awan-storage/server/tests/compactor_test.rs
//! Online Background Volume Compactor Tests for Issue #475

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awan_compact_test_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

async fn spawn_server_with_dir(dir: &Path) -> (String, s3_server::ServerHandle, Arc<S3ServiceState>) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    let handle = start_s3_reactor(addr, state.clone()).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, handle, state)
}

#[tokio::test]
async fn test_online_compaction_reclaims_dead_space() {
    let dir = temp_dir("online_compaction");
    let client = reqwest::Client::new();
    let bucket = "compaction-bucket";

    let (base_url, handle, state) = spawn_server_with_dir(&dir).await;

    // 1. Create bucket
    let res = client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 2. Put 50 objects of 64 KiB each (~3.2 MiB)
    let payload = vec![0xABu8; 64 * 1024];
    for i in 0..50 {
        let key = format!("object_{:03}.bin", i);
        let res = client
            .put(&format!("{}/{}/{}", base_url, bucket, key))
            .body(payload.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    let vol_1_path = dir.join("volume_0001.dat");
    let initial_size = std::fs::metadata(&vol_1_path).unwrap().len();
    assert!(initial_size >= 50 * 64 * 1024);

    // 3. Delete 30 objects (60% dead space -> fragmentation > 20%)
    for i in 0..30 {
        let key = format!("object_{:03}.bin", i);
        let res = client
            .delete(&format!("{}/{}/{}", base_url, bucket, key))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    // Check fragmentation ratio
    let frag = state.fragmentation_ratio();
    assert!(frag >= 0.20, "Fragmentation ratio must be >= 20%, got {}", frag);

    // 4. Perform online compaction
    let report = state.perform_online_compaction().await.expect("online compaction");
    assert_eq!(report.live_needles_copied, 20);
    // 30 dead data needles + 30 dead tombstone markers = 60 dead needles reclaimed
    assert_eq!(report.dead_needles_reclaimed, 60);

    // 5. Verify disk footprint shrunk
    let vol_2_path = dir.join("volume_0002.dat");
    assert!(vol_2_path.exists(), "volume_0002.dat must exist after compaction");
    let compacted_size = std::fs::metadata(&vol_2_path).unwrap().len();
    assert!(
        compacted_size < initial_size / 2,
        "Compacted volume must be < half initial size (got {} vs initial {})",
        compacted_size,
        initial_size
    );

    // 6. Verify all 20 remaining live objects read back bit-identically
    for i in 30..50 {
        let key = format!("object_{:03}.bin", i);
        let res = client
            .get(&format!("{}/{}/{}", base_url, bucket, key))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "Key {} must be accessible", key);
        let body = res.bytes().await.unwrap();
        assert_eq!(&body[..], &payload[..]);
    }

    // 7. Verify deleted objects return 404
    for i in 0..30 {
        let key = format!("object_{:03}.bin", i);
        let res = client
            .get(&format!("{}/{}/{}", base_url, bucket, key))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    // 8. Verify new writes succeed on compacted volume
    let new_res = client
        .put(&format!("{}/{}/fresh_post_compact.bin", base_url, bucket))
        .body(b"hello_from_volume_2".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(new_res.status(), StatusCode::OK);

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
