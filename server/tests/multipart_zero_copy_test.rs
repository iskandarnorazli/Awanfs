//! awan-storage/server/tests/multipart_zero_copy_test.rs
//! Zero-Copy Direct Extent Slicing for Multipart Range GETs Tests (#485)

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use reqwest::Client;
use s3_server::{start_s3_reactor, S3ServiceState};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("awan_mpu_zero_copy_{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn test_multipart_zero_copy_range_gets_and_boundary_crossing() {
    let dir = temp_dir("zero_copy_range");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    let client = Client::new();

    let bucket = "mpu-zero-copy-bucket";
    let key = "multipart-target.bin";
    client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();

    // 1. Initiate Multipart Upload
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

    // 2. Upload 3 parts of 5 MiB each (15 MiB total)
    // Part 1: all 0x11, Part 2: all 0x22, Part 3: all 0x33
    let part_size = 5 * 1024 * 1024;
    let mut declared_parts = Vec::new();

    for (p_num, pattern) in [(1, 0x11u8), (2, 0x22u8), (3, 0x33u8)] {
        let payload = vec![pattern; part_size];
        let res = client
            .put(&format!(
                "{}/{}/{}?partNumber={}&uploadId={}",
                base_url, bucket, key, p_num, upload_id
            ))
            .body(payload)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::OK);
        let etag = res.headers().get("ETag").unwrap().to_str().unwrap().to_string();
        declared_parts.push((p_num, etag));
    }

    // 3. Complete Multipart Upload
    let mut comp_xml = String::from("<CompleteMultipartUpload>\n");
    for (p, etag) in &declared_parts {
        comp_xml.push_str(&format!(
            "  <Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>\n",
            p, etag
        ));
    }
    comp_xml.push_str("</CompleteMultipartUpload>");

    let comp_res = client
        .post(&format!("{}/{}/{}?uploadId={}", base_url, bucket, key, upload_id))
        .body(comp_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(comp_res.status(), reqwest::StatusCode::OK);

    // 4. Test Single-Extent Zero-Copy Range GET (Part 1 chunk start)
    // bytes=0-1023
    let r1 = client
        .get(&format!("{}/{}/{}", base_url, bucket, key))
        .header("Range", "bytes=0-1023")
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(r1.headers().get("Content-Range").unwrap(), "bytes 0-1023/15728640");
    let b1 = r1.bytes().await.unwrap();
    assert_eq!(b1.len(), 1024);
    assert!(b1.iter().all(|&b| b == 0x11));

    // 5. Test Single-Extent Zero-Copy Range GET (Part 2 mid chunk)
    // bytes=5242880-5243880 (5 MiB to 5 MiB + 1000)
    let r2 = client
        .get(&format!("{}/{}/{}", base_url, bucket, key))
        .header("Range", "bytes=5242880-5243880")
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(r2.headers().get("Content-Range").unwrap(), "bytes 5242880-5243880/15728640");
    let b2 = r2.bytes().await.unwrap();
    assert_eq!(b2.len(), 1001);
    assert!(b2.iter().all(|&b| b == 0x22));

    // 6. Test Multi-Extent Boundary Crossing Range GET (straddling Part 1 and Part 2)
    // Range spanning from 100 bytes before Part 1 end to 100 bytes into Part 2
    let boundary_start = (part_size - 100) as u64;
    let boundary_end = (part_size + 99) as u64;
    let r3 = client
        .get(&format!("{}/{}/{}", base_url, bucket, key))
        .header("Range", format!("bytes={}-{}", boundary_start, boundary_end))
        .send()
        .await
        .unwrap();
    assert_eq!(r3.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let b3 = r3.bytes().await.unwrap();
    assert_eq!(b3.len(), 200);
    assert!(b3[..100].iter().all(|&b| b == 0x11));
    assert!(b3[100..].iter().all(|&b| b == 0x22));

    // 7. Test Multi-Extent Boundary Crossing Range GET (straddling Part 2 and Part 3)
    let boundary2_start = (part_size * 2 - 50) as u64;
    let boundary2_end = (part_size * 2 + 49) as u64;
    let r4 = client
        .get(&format!("{}/{}/{}", base_url, bucket, key))
        .header("Range", format!("bytes={}-{}", boundary2_start, boundary2_end))
        .send()
        .await
        .unwrap();
    assert_eq!(r4.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let b4 = r4.bytes().await.unwrap();
    assert_eq!(b4.len(), 100);
    assert!(b4[..50].iter().all(|&b| b == 0x22));
    assert!(b4[50..].iter().all(|&b| b == 0x33));

    // 8. Negative Control: Out-of-bounds range returns 416 Range Not Satisfiable
    let r_oob = client
        .get(&format!("{}/{}/{}", base_url, bucket, key))
        .header("Range", "bytes=20000000-25000000")
        .send()
        .await
        .unwrap();
    assert_eq!(r_oob.status(), reqwest::StatusCode::RANGE_NOT_SATISFIABLE);

    handle.shutdown();
}
