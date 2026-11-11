//! awan-storage/server/tests/staged_mpu_test.rs
//! Staged Parallel Part Files for Multipart Uploads Tests (#483)

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use reqwest::Client;
use s3_server::{start_s3_reactor, S3ServiceState};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("awan_staged_mpu_{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn test_16_parallel_workers_staged_multipart_upload() {
    let dir = temp_dir("parallel_16_workers");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    let client = Client::new();

    // 1. Create Bucket
    let bucket = "mpu-parallel-bucket";
    let key = "1gb-staged-object.bin";
    client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();

    // 2. Initiate Multipart Upload
    let init_res = client
        .post(&format!("{}/{}/{}?uploads", base_url, bucket, key))
        .send()
        .await
        .unwrap();
    assert_eq!(init_res.status(), reqwest::StatusCode::OK);
    let init_xml = init_res.text().await.unwrap();
    let upload_id = init_xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();

    // Verify staging directory was created
    let staging_upload_dir = state.multipart.staging_dir.join(&upload_id);
    assert!(staging_upload_dir.exists(), "Staging upload dir must exist");

    // 3. Upload 16 parts of 5 MiB concurrently (80 MiB total in test)
    // using 16 concurrent tasks
    let part_size = 5 * 1024 * 1024; // 5 MiB min S3 part size
    let num_parts = 16;
    let mut tasks = Vec::new();

    let start_t = Instant::now();
    for part_num in 1..=num_parts {
        let client_c = client.clone();
        let base_c = base_url.clone();
        let upload_id_c = upload_id.clone();
        let payload = vec![part_num as u8; part_size];

        tasks.push(tokio::spawn(async move {
            let res = client_c
                .put(&format!(
                    "{}/{}/{}?partNumber={}&uploadId={}",
                    base_c, bucket, key, part_num, upload_id_c
                ))
                .body(payload)
                .send()
                .await
                .expect("upload part send");
            assert_eq!(res.status(), reqwest::StatusCode::OK);
            let etag = res
                .headers()
                .get("ETag")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            (part_num, etag)
        }));
    }

    let mut declared_parts = Vec::new();
    for t in tasks {
        declared_parts.push(t.await.unwrap());
    }
    declared_parts.sort_by_key(|(p, _)| *p);
    let elapsed = start_t.elapsed();
    println!("16 parallel parts uploaded in {:?}", elapsed);

    // Verify each part was staged to its own independent file handle
    for part_num in 1..=num_parts {
        let part_file = staging_upload_dir.join(format!("part_{:05}.dat", part_num));
        assert!(part_file.exists(), "Independent staged part file must exist: {:?}", part_file);
        assert_eq!(fs::metadata(&part_file).unwrap().len(), part_size as u64);
    }

    // 4. Complete Multipart Upload
    let mut complete_xml = String::from("<CompleteMultipartUpload>\n");
    for (p_num, etag) in &declared_parts {
        complete_xml.push_str(&format!(
            "  <Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>\n",
            p_num, etag
        ));
    }
    complete_xml.push_str("</CompleteMultipartUpload>");

    let comp_res = client
        .post(&format!("{}/{}/{}?uploadId={}", base_url, bucket, key, upload_id))
        .body(complete_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(comp_res.status(), reqwest::StatusCode::OK);

    // 5. Verify bit-identical GET
    let get_res = client
        .get(&format!("{}/{}/{}", base_url, bucket, key))
        .send()
        .await
        .unwrap();
    assert_eq!(get_res.status(), reqwest::StatusCode::OK);
    let fetched = get_res.bytes().await.unwrap();
    assert_eq!(fetched.len(), num_parts * part_size);

    for part_num in 1..=num_parts {
        let start = (part_num - 1) * part_size;
        let end = start + part_size;
        let expected_byte = part_num as u8;
        assert!(
            fetched[start..end].iter().all(|&b| b == expected_byte),
            "Part {} byte integrity check failed",
            part_num
        );
    }

    handle.shutdown();
}

#[tokio::test]
async fn test_negative_control_abort_cleans_staged_parts() {
    let dir = temp_dir("abort_cleans_staged");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    let client = Client::new();

    let bucket = "mpu-abort-bucket";
    let key = "aborted-obj.bin";
    client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();

    let init_res = client
        .post(&format!("{}/{}/{}?uploads", base_url, bucket, key))
        .send()
        .await
        .unwrap();
    let init_xml = init_res.text().await.unwrap();
    let upload_id = init_xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();

    let staging_upload_dir = state.multipart.staging_dir.join(&upload_id);
    assert!(staging_upload_dir.exists());

    // Upload 2 parts
    for part_num in 1..=2 {
        let res = client
            .put(&format!(
                "{}/{}/{}?partNumber={}&uploadId={}",
                base_url, bucket, key, part_num, upload_id
            ))
            .body(vec![0x42u8; 1024 * 1024 * 5])
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::OK);
    }

    // Verify staged files exist
    assert!(staging_upload_dir.join("part_00001.dat").exists());
    assert!(staging_upload_dir.join("part_00002.dat").exists());

    // Abort Multipart Upload
    let abort_res = client
        .delete(&format!("{}/{}/{}?uploadId={}", base_url, bucket, key, upload_id))
        .send()
        .await
        .unwrap();
    assert_eq!(abort_res.status(), reqwest::StatusCode::NO_CONTENT);

    // Staging directory must be completely cleaned up
    assert!(!staging_upload_dir.exists(), "Staging upload dir must be deleted upon abort");

    handle.shutdown();
}
