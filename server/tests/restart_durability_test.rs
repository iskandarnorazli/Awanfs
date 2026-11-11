//! awan-storage/server/tests/restart_durability_test.rs
//! Restart Durability & Boot Volume Recovery Tests for Issue #473

use reqwest::header::{CONTENT_LENGTH, ETAG, RANGE};
use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awan_durability_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

async fn spawn_server_with_dir(dir: &Path) -> (String, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    let handle = start_s3_reactor(addr, state).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, handle)
}

#[tokio::test]
async fn test_bucket_persistence_across_restart() {
    let dir = temp_dir("bucket_persist");
    let client = reqwest::Client::new();
    let bucket = "persist-bucket";

    // Phase 1: Server instance 1 - Create bucket
    {
        let (base_url, handle) = spawn_server_with_dir(&dir).await;
        let res = client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = client.head(&format!("{}/{}", base_url, bucket)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        handle.shutdown();
    }

    // Phase 2: Server instance 2 - Restart on same data_dir
    {
        let (base_url, handle) = spawn_server_with_dir(&dir).await;

        // Bucket MUST still exist without re-creation
        let res = client.head(&format!("{}/{}", base_url, bucket)).send().await.unwrap();
        assert_eq!(
            res.status(),
            StatusCode::OK,
            "Bucket must survive process restart on existing --data-dir"
        );

        handle.shutdown();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_needle_objects_persistence_across_restart() {
    let dir = temp_dir("needle_persist");
    let client = reqwest::Client::new();
    let bucket = "needle-durability-bucket";

    let mut expected_objects = Vec::new();

    // Phase 1: Write objects, overwrite, and delete
    {
        let (base_url, handle) = spawn_server_with_dir(&dir).await;

        let res = client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Put 20 small objects
        for i in 0..20 {
            let key = format!("obj_{:02}.dat", i);
            let payload: Vec<u8> = (0..(1024 * (i + 1))).map(|b| (b % 251) as u8).collect();
            let res = client
                .put(&format!("{}/{}/{}", base_url, bucket, key))
                .body(payload.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let etag = res.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
            expected_objects.push((key, payload, etag));
        }

        // Put object that will be overwritten
        let res = client
            .put(&format!("{}/{}/overwrite.txt", base_url, bucket))
            .body(b"version_1_initial".to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = client
            .put(&format!("{}/{}/overwrite.txt", base_url, bucket))
            .body(b"version_2_final_update".to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Put object that will be deleted
        let res = client
            .put(&format!("{}/{}/deleted.txt", base_url, bucket))
            .body(b"temporary_secret_to_delete".to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = client
            .delete(&format!("{}/{}/deleted.txt", base_url, bucket))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        handle.shutdown();
    }

    // Phase 2: Restart server and verify durability of all objects
    {
        let (base_url, handle) = spawn_server_with_dir(&dir).await;

        // 1. Verify 20 objects
        for (key, payload, etag) in &expected_objects {
            let res = client
                .get(&format!("{}/{}/{}", base_url, bucket, key))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK, "Object {} should be present", key);
            assert_eq!(
                res.headers().get(ETAG).unwrap().to_str().unwrap(),
                etag.as_str(),
                "ETag mismatch for {}",
                key
            );
            assert_eq!(
                res.headers().get(CONTENT_LENGTH).unwrap().to_str().unwrap(),
                &payload.len().to_string()
            );
            let body = res.bytes().await.unwrap();
            assert_eq!(&body[..], &payload[..], "Payload data mismatch for {}", key);

            // Test partial Range GET (bytes=10-25)
            if payload.len() > 30 {
                let range_res = client
                    .get(&format!("{}/{}/{}", base_url, bucket, key))
                    .header(RANGE, "bytes=10-25")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(range_res.status(), StatusCode::PARTIAL_CONTENT);
                let range_body = range_res.bytes().await.unwrap();
                assert_eq!(&range_body[..], &payload[10..=25]);
            }
        }

        // 2. Verify overwritten object returns version 2
        let res = client
            .get(&format!("{}/{}/overwrite.txt", base_url, bucket))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.bytes().await.unwrap();
        assert_eq!(&body[..], b"version_2_final_update");

        // 3. Verify deleted object returns 404
        let res = client
            .get(&format!("{}/{}/deleted.txt", base_url, bucket))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        handle.shutdown();
    }

    let _ = std::fs::remove_dir_all(&dir);
}
