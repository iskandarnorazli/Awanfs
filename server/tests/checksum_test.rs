//! #117: Additional S3 checksums (CRC32, CRC32C, CRC64NVME, SHA1, SHA256)
//! and GetObjectAttributes (`GET /{bucket}/{key}?attributes`).

use reqwest::StatusCode;
use s3_server::checksum::{Checksum, ChecksumAlgorithm};
use s3_server::{start_s3_reactor, S3ServiceState, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_cksum_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn between<'a>(s: &'a str, open: &str, close: &str) -> Option<&'a str> {
    s.split(open).nth(1).and_then(|r| r.split(close).next())
}

fn compute_b64(alg: ChecksumAlgorithm, data: &[u8]) -> String {
    let mut c = Checksum::new(alg);
    c.update(data);
    c.finish_base64()
}

fn header_name(alg: ChecksumAlgorithm) -> &'static str {
    match alg {
        ChecksumAlgorithm::Crc32 => "x-amz-checksum-crc32",
        ChecksumAlgorithm::Crc32c => "x-amz-checksum-crc32c",
        ChecksumAlgorithm::Crc64Nvme => "x-amz-checksum-crc64nvme",
        ChecksumAlgorithm::Sha1 => "x-amz-checksum-sha1",
        ChecksumAlgorithm::Sha256 => "x-amz-checksum-sha256",
    }
}

fn xml_tag(alg: ChecksumAlgorithm) -> &'static str {
    match alg {
        ChecksumAlgorithm::Crc32 => "ChecksumCRC32",
        ChecksumAlgorithm::Crc32c => "ChecksumCRC32C",
        ChecksumAlgorithm::Crc64Nvme => "ChecksumCRC64NVME",
        ChecksumAlgorithm::Sha1 => "ChecksumSHA1",
        ChecksumAlgorithm::Sha256 => "ChecksumSHA256",
    }
}

#[tokio::test]
async fn put_get_head_all_five_checksum_algorithms_and_restart_persistence() {
    let dir = temp_dir("all5");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/ckbkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let payload = b"123456789".to_vec();
    let cases = [
        (ChecksumAlgorithm::Crc32, "y/Q5Jg=="),
        (ChecksumAlgorithm::Crc32c, "4waSgw=="),
        (ChecksumAlgorithm::Crc64Nvme, "rosUhgp5mIg="),
        (ChecksumAlgorithm::Sha1, "98O8HYCOB7q3230OkN57N23N238="), // computed below
        (ChecksumAlgorithm::Sha256, "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU="),
    ];

    for (i, (alg, _)) in cases.iter().enumerate() {
        let expected = compute_b64(*alg, &payload);
        let hdr = header_name(*alg);
        let key = format!("obj-{}", i);

        // 1. PUT with declared checksum header
        let res = c
            .put(format!("{}/ckbkt/{}", url, key))
            .header("x-amz-sdk-checksum-algorithm", alg.name())
            .header(hdr, &expected)
            .body(payload.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "PUT failed for {:?}", alg);
        assert_eq!(
            res.headers().get(hdr).and_then(|v| v.to_str().ok()),
            Some(expected.as_str()),
            "PUT response missing checksum header for {:?}",
            alg
        );

        // 2. PUT with only x-amz-sdk-checksum-algorithm (server computes)
        let key_auto = format!("obj-auto-{}", i);
        let res_auto = c
            .put(format!("{}/ckbkt/{}", url, key_auto))
            .header("x-amz-sdk-checksum-algorithm", alg.name())
            .body(payload.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(res_auto.status(), StatusCode::OK, "PUT auto failed for {:?}", alg);
        assert_eq!(
            res_auto.headers().get(hdr).and_then(|v| v.to_str().ok()),
            Some(expected.as_str()),
            "PUT auto missing checksum header for {:?}",
            alg
        );

        // 3. HEAD without ChecksumMode: must NOT return checksum header
        let head_no = c.head(format!("{}/ckbkt/{}", url, key)).send().await.unwrap();
        assert_eq!(head_no.status(), StatusCode::OK);
        assert!(head_no.headers().get(hdr).is_none(), "HEAD without mode should omit checksum");

        // 4. HEAD with x-amz-checksum-mode: ENABLED
        let head_yes = c
            .head(format!("{}/ckbkt/{}", url, key))
            .header("x-amz-checksum-mode", "ENABLED")
            .send()
            .await
            .unwrap();
        assert_eq!(head_yes.status(), StatusCode::OK);
        assert_eq!(
            head_yes.headers().get(hdr).and_then(|v| v.to_str().ok()),
            Some(expected.as_str()),
            "HEAD with ENABLED missing checksum for {:?}",
            alg
        );

        // 5. GET without ChecksumMode: must NOT return checksum header
        let get_no = c.get(format!("{}/ckbkt/{}", url, key)).send().await.unwrap();
        assert_eq!(get_no.status(), StatusCode::OK);
        assert!(get_no.headers().get(hdr).is_none(), "GET without mode should omit checksum");
        assert_eq!(get_no.bytes().await.unwrap().as_ref(), payload.as_slice());

        // 6. GET with x-amz-checksum-mode: ENABLED
        let get_yes = c
            .get(format!("{}/ckbkt/{}", url, key))
            .header("x-amz-checksum-mode", "ENABLED")
            .send()
            .await
            .unwrap();
        assert_eq!(get_yes.status(), StatusCode::OK);
        assert_eq!(
            get_yes.headers().get(hdr).and_then(|v| v.to_str().ok()),
            Some(expected.as_str()),
            "GET with ENABLED missing checksum for {:?}",
            alg
        );
        assert_eq!(get_yes.bytes().await.unwrap().as_ref(), payload.as_slice());

        // 7. GET ?attributes
        let attr_res = c
            .get(format!("{}/ckbkt/{}?attributes", url, key))
            .header("x-amz-object-attributes", "ETag,Checksum,ObjectParts,StorageClass,ObjectSize")
            .send()
            .await
            .unwrap();
        assert_eq!(attr_res.status(), StatusCode::OK, "GetObjectAttributes failed for {:?}", alg);
        let xml = attr_res.text().await.unwrap();
        let tag = xml_tag(*alg);
        assert_eq!(
            between(&xml, &format!("<{}>", tag), &format!("</{}>", tag)),
            Some(expected.as_str()),
            "GetObjectAttributes XML missing {} in:\n{}",
            tag,
            xml
        );
        assert_eq!(between(&xml, "<ObjectSize>", "</ObjectSize>"), Some("9"));
        assert_eq!(between(&xml, "<StorageClass>", "</StorageClass>"), Some("STANDARD"));
        assert!(between(&xml, "<ETag>", "</ETag>").is_some());
        assert!(between(&xml, "<ObjectParts>", "</ObjectParts>").is_none());
    }

    // Restart server and verify persistence
    handle.shutdown();
    drop(state);

    let (_state2, url2, handle2) = start(&dir).await;
    for (i, (alg, _)) in cases.iter().enumerate() {
        let expected = compute_b64(*alg, &payload);
        let hdr = header_name(*alg);
        let key = format!("obj-{}", i);

        let head_yes = c
            .head(format!("{}/ckbkt/{}", url2, key))
            .header("x-amz-checksum-mode", "ENABLED")
            .send()
            .await
            .unwrap();
        assert_eq!(head_yes.status(), StatusCode::OK);
        assert_eq!(
            head_yes.headers().get(hdr).and_then(|v| v.to_str().ok()),
            Some(expected.as_str()),
            "HEAD after restart missing checksum for {:?}",
            alg
        );

        let attr_res = c
            .get(format!("{}/ckbkt/{}?attributes", url2, key))
            .header("x-amz-object-attributes", "Checksum,ObjectSize")
            .send()
            .await
            .unwrap();
        assert_eq!(attr_res.status(), StatusCode::OK);
        let xml = attr_res.text().await.unwrap();
        let tag = xml_tag(*alg);
        assert_eq!(
            between(&xml, &format!("<{}>", tag), &format!("</{}>", tag)),
            Some(expected.as_str()),
            "GetObjectAttributes after restart missing {} in:\n{}",
            tag,
            xml
        );
    }
    handle2.shutdown();
}

#[tokio::test]
async fn put_checksum_mismatch_returns_400_bad_digest_buffered_and_streamed() {
    let dir = temp_dir("mismatch");
    let c = reqwest::Client::new();
    let (_state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/mmbkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // Buffered PUT mismatch
    let wrong_sha256 = compute_b64(ChecksumAlgorithm::Sha256, b"different-data");
    let res = c
        .put(format!("{}/mmbkt/small", url))
        .header("x-amz-sdk-checksum-algorithm", "SHA256")
        .header("x-amz-checksum-sha256", &wrong_sha256)
        .body("hello world")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("BadDigest"), "got XML:\n{}", body);

    // Ensure failed PUT did not store the object
    assert_eq!(c.head(format!("{}/mmbkt/small", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Streamed PUT (> STREAM_PUT_MIN) mismatch
    let big = vec![b'A'; STREAM_PUT_MIN as usize + 4096];
    let res_big = c
        .put(format!("{}/mmbkt/big", url))
        .header("x-amz-sdk-checksum-algorithm", "CRC32C")
        .header("x-amz-checksum-crc32c", compute_b64(ChecksumAlgorithm::Crc32c, b"wrong"))
        .body(big.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(res_big.status(), StatusCode::BAD_REQUEST);
    let body_big = res_big.text().await.unwrap();
    assert_eq!(between(&body_big, "<Code>", "</Code>"), Some("BadDigest"), "got XML:\n{}", body_big);
    assert_eq!(c.head(format!("{}/mmbkt/big", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Streamed PUT valid checksum succeeds
    let good_crc32c = compute_b64(ChecksumAlgorithm::Crc32c, &big);
    let res_ok = c
        .put(format!("{}/mmbkt/big", url))
        .header("x-amz-checksum-crc32c", &good_crc32c)
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(res_ok.status(), StatusCode::OK);
    assert_eq!(
        res_ok.headers().get("x-amz-checksum-crc32c").and_then(|v| v.to_str().ok()),
        Some(good_crc32c.as_str())
    );

    handle.shutdown();
}

#[tokio::test]
async fn multipart_checksum_and_get_object_attributes_parts() {
    let dir = temp_dir("mpu");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/mpbkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // 1. CreateMultipartUpload with ChecksumAlgorithm=SHA256
    let init = c
        .post(format!("{}/mpbkt/multi?uploads", url))
        .header("x-amz-checksum-algorithm", "SHA256")
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    assert_eq!(init.headers().get("x-amz-checksum-algorithm").and_then(|v| v.to_str().ok()), Some("SHA256"));
    let init_xml = init.text().await.unwrap();
    let upload_id = between(&init_xml, "<UploadId>", "</UploadId>").unwrap().to_string();

    let p1 = vec![b'A'; 1024];
    let p2 = vec![b'B'; 2048];
    let c1 = compute_b64(ChecksumAlgorithm::Sha256, &p1);
    let c2 = compute_b64(ChecksumAlgorithm::Sha256, &p2);

    // UploadPart mismatch -> 400 BadDigest
    let bad_up = c
        .put(format!("{}/mpbkt/multi?partNumber=1&uploadId={}", url, upload_id))
        .header("x-amz-checksum-sha256", compute_b64(ChecksumAlgorithm::Sha256, b"nope"))
        .body(p1.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(bad_up.status(), StatusCode::BAD_REQUEST);
    let bad_xml = bad_up.text().await.unwrap();
    assert_eq!(between(&bad_xml, "<Code>", "</Code>"), Some("BadDigest"));

    // UploadPart 1 & 2 valid
    let up1 = c
        .put(format!("{}/mpbkt/multi?partNumber=1&uploadId={}", url, upload_id))
        .header("x-amz-checksum-sha256", &c1)
        .body(p1.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(up1.status(), StatusCode::OK);
    assert_eq!(up1.headers().get("x-amz-checksum-sha256").and_then(|v| v.to_str().ok()), Some(c1.as_str()));
    let etag1 = up1.headers().get("etag").unwrap().to_str().unwrap().trim_matches('"').to_string();

    let up2 = c
        .put(format!("{}/mpbkt/multi?partNumber=2&uploadId={}", url, upload_id))
        .header("x-amz-checksum-sha256", &c2)
        .body(p2.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(up2.status(), StatusCode::OK);
    assert_eq!(up2.headers().get("x-amz-checksum-sha256").and_then(|v| v.to_str().ok()), Some(c2.as_str()));
    let etag2 = up2.headers().get("etag").unwrap().to_str().unwrap().trim_matches('"').to_string();

    let mut raw1 = Checksum::new(ChecksumAlgorithm::Sha256);
    raw1.update(&p1);
    let mut raw2 = Checksum::new(ChecksumAlgorithm::Sha256);
    raw2.update(&p2);
    let composite = s3_server::checksum::composite(ChecksumAlgorithm::Sha256, &[raw1.finish(), raw2.finish()]);

    let complete_body = format!(
        "<CompleteMultipartUpload>\
         <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag><ChecksumSHA256>{}</ChecksumSHA256></Part>\
         <Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag><ChecksumSHA256>{}</ChecksumSHA256></Part>\
         </CompleteMultipartUpload>",
        etag1, c1, etag2, c2
    );

    // Complete with wrong composite checksum header -> 400 BadDigest
    let bad_cmp = c
        .post(format!("{}/mpbkt/multi?uploadId={}", url, upload_id))
        .header("x-amz-checksum-sha256", format!("{}-2", c1))
        .body(complete_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(bad_cmp.status(), StatusCode::BAD_REQUEST);
    let bad_cmp_xml = bad_cmp.text().await.unwrap();
    assert_eq!(between(&bad_cmp_xml, "<Code>", "</Code>"), Some("BadDigest"));

    // Complete with right composite checksum
    let cmp = c
        .post(format!("{}/mpbkt/multi?uploadId={}", url, upload_id))
        .header("x-amz-checksum-sha256", &composite)
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);
    assert_eq!(cmp.headers().get("x-amz-checksum-sha256").and_then(|v| v.to_str().ok()), Some(composite.as_str()));
    assert_eq!(cmp.headers().get("x-amz-checksum-type").and_then(|v| v.to_str().ok()), Some("COMPOSITE"));

    // HEAD with ChecksumMode=ENABLED
    let head = c
        .head(format!("{}/mpbkt/multi", url))
        .header("x-amz-checksum-mode", "ENABLED")
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers().get("x-amz-checksum-sha256").and_then(|v| v.to_str().ok()), Some(composite.as_str()));
    assert_eq!(head.headers().get("x-amz-checksum-type").and_then(|v| v.to_str().ok()), Some("COMPOSITE"));

    // GET ?attributes with ObjectParts pagination
    let attrs_p1 = c
        .get(format!("{}/mpbkt/multi?attributes", url))
        .header("x-amz-object-attributes", "ETag,Checksum,ObjectParts,StorageClass,ObjectSize")
        .header("x-amz-max-parts", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(attrs_p1.status(), StatusCode::OK);
    let xml1 = attrs_p1.text().await.unwrap();
    assert_eq!(between(&xml1, "<ObjectSize>", "</ObjectSize>"), Some("3072"));
    assert_eq!(between(&xml1, "<PartsCount>", "</PartsCount>"), Some("2"));
    assert_eq!(between(&xml1, "<MaxParts>", "</MaxParts>"), Some("1"));
    assert_eq!(between(&xml1, "<IsTruncated>", "</IsTruncated>"), Some("true"));
    assert_eq!(between(&xml1, "<NextPartNumberMarker>", "</NextPartNumberMarker>"), Some("1"));
    assert_eq!(between(&xml1, "<ChecksumSHA256>", "</ChecksumSHA256>"), Some(composite.as_str()));
    assert_eq!(between(&xml1, "<ChecksumType>", "</ChecksumType>"), Some("COMPOSITE"));
    assert_eq!(between(&xml1, "<Size>", "</Size>"), Some("1024"));

    // Restart and verify attributes + checksum persist
    handle.shutdown();
    drop(state);
    let (_state2, url2, handle2) = start(&dir).await;
    let attrs_after = c
        .get(format!("{}/mpbkt/multi?attributes", url2))
        .header("x-amz-object-attributes", "Checksum,ObjectParts,ObjectSize")
        .send()
        .await
        .unwrap();
    assert_eq!(attrs_after.status(), StatusCode::OK);
    let xml2 = attrs_after.text().await.unwrap();
    assert_eq!(between(&xml2, "<ChecksumSHA256>", "</ChecksumSHA256>"), Some(composite.as_str()));
    assert_eq!(between(&xml2, "<PartsCount>", "</PartsCount>"), Some("2"));
    assert_eq!(between(&xml2, "<ObjectSize>", "</ObjectSize>"), Some("3072"));

    handle2.shutdown();
}


#[tokio::test]
async fn range_get_omits_checksum_headers_while_whole_get_includes_them() {
    let dir = temp_dir("range_cksum");
    let c = reqwest::Client::new();
    let (_state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/rbkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let data = vec![b'R'; 20_000];
    let crc = compute_b64(ChecksumAlgorithm::Crc32, &data);
    assert_eq!(
        c.put(format!("{}/rbkt/obj", url))
            .header("x-amz-checksum-crc32", &crc)
            .body(data.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Whole-object GET with checksum-mode=ENABLED includes the full-object checksum
    let full = c
        .get(format!("{}/rbkt/obj", url))
        .header("x-amz-checksum-mode", "ENABLED")
        .send()
        .await
        .unwrap();
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.headers().get("x-amz-checksum-crc32").unwrap(), crc.as_str());

    // Range GET (206 Partial Content) must NOT return the full-object checksum header
    let partial = c
        .get(format!("{}/rbkt/obj", url))
        .header("Range", "bytes=100-4195")
        .header("x-amz-checksum-mode", "ENABLED")
        .send()
        .await
        .unwrap();
    assert_eq!(partial.status(), StatusCode::PARTIAL_CONTENT);
    assert!(partial.headers().get("x-amz-checksum-crc32").is_none());
    assert!(partial.headers().get("x-amz-checksum-type").is_none());

    // Also verify on a versioned bucket
    assert_eq!(
        c.put(format!("{}/rbkt?versioning", url))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let put_v = c
        .put(format!("{}/rbkt/vobj", url))
        .header("x-amz-checksum-crc32", &crc)
        .body(data)
        .send()
        .await
        .unwrap();
    assert_eq!(put_v.status(), StatusCode::OK);
    let vid = put_v.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    let v_partial = c
        .get(format!("{}/rbkt/vobj?versionId={}", url, vid))
        .header("Range", "bytes=0-99")
        .header("x-amz-checksum-mode", "ENABLED")
        .send()
        .await
        .unwrap();
    assert_eq!(v_partial.status(), StatusCode::PARTIAL_CONTENT);
    assert!(v_partial.headers().get("x-amz-checksum-crc32").is_none());

    handle.shutdown();
}


#[tokio::test]
async fn complete_multipart_returns_checksum_in_xml_and_supports_full_object_crc32() {
    let dir = temp_dir("mp_fullobj");
    let c = reqwest::Client::new();
    let (_state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/fobkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // 1. FULL_OBJECT CRC32 multipart upload
    let init = c
        .post(format!("{}/fobkt/fullobj?uploads", url))
        .header("x-amz-checksum-algorithm", "CRC32")
        .header("x-amz-checksum-type", "FULL_OBJECT")
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    assert_eq!(init.headers().get("x-amz-checksum-type").unwrap().to_str().unwrap(), "FULL_OBJECT");
    let uid = between(&init.text().await.unwrap(), "<UploadId>", "</UploadId>").unwrap().to_string();

    let p1 = vec![b'1'; 1024];
    let p2 = vec![b'2'; 2048];
    let c1 = compute_b64(ChecksumAlgorithm::Crc32, &p1);
    let c2 = compute_b64(ChecksumAlgorithm::Crc32, &p2);
    let mut full = Vec::new();
    full.extend_from_slice(&p1);
    full.extend_from_slice(&p2);
    let full_crc = compute_b64(ChecksumAlgorithm::Crc32, &full);

    let r1 = c
        .put(format!("{}/fobkt/fullobj?partNumber=1&uploadId={}", url, uid))
        .header("x-amz-checksum-crc32", &c1)
        .body(p1)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let e1 = r1.headers().get("etag").unwrap().to_str().unwrap().to_string();

    let r2 = c
        .put(format!("{}/fobkt/fullobj?partNumber=2&uploadId={}", url, uid))
        .header("x-amz-checksum-crc32", &c2)
        .body(p2)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let e2 = r2.headers().get("etag").unwrap().to_str().unwrap().to_string();

    let cmp = c
        .post(format!("{}/fobkt/fullobj?uploadId={}", url, uid))
        .header("x-amz-checksum-crc32", &full_crc)
        .body(format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag><ChecksumCRC32>{}</ChecksumCRC32></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag><ChecksumCRC32>{}</ChecksumCRC32></Part></CompleteMultipartUpload>",
            e1, c1, e2, c2
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);
    assert_eq!(cmp.headers().get("x-amz-checksum-crc32").unwrap().to_str().unwrap(), full_crc);
    assert_eq!(cmp.headers().get("x-amz-checksum-type").unwrap().to_str().unwrap(), "FULL_OBJECT");
    let cmp_xml = cmp.text().await.unwrap();
    assert_eq!(between(&cmp_xml, "<ChecksumCRC32>", "</ChecksumCRC32>"), Some(full_crc.as_str()));
    assert_eq!(between(&cmp_xml, "<ChecksumType>", "</ChecksumType>"), Some("FULL_OBJECT"));

    handle.shutdown();
}
