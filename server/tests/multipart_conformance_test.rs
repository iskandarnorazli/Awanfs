//! #111: S3 multipart upload conformance tests:
//! - `complete_multipart_upload` XML validation (`MalformedXML`, `InvalidPartOrder`, `InvalidArgument`, `InvalidPart`)
//! - `UploadPartCopy` (full, ranged, versioned, special names, preconditions, range errors)
//! - `GET`/`HEAD ?partNumber=N` (multipart & non-multipart)
//! - `ListParts` and `ListMultipartUploads` pagination, filtering, and `encoding-type=url`

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn between<'a>(s: &'a str, open: &str, close: &str) -> Option<&'a str> {
    s.split(open).nth(1).and_then(|r| r.split(close).next())
}

async fn start_server(tag: &str) -> (s3_server::ServerHandle, String, std::path::PathBuf) {
    let dir = temp_dir(tag);
    let st = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, st).await.unwrap();
    let base = format!("http://{}", handle.local_addr);
    (handle, base, dir)
}

async fn create_upload(c: &reqwest::Client, base: &str, bucket: &str, key: &str) -> String {
    let res = c.post(format!("{}/{}/{}?uploads", base, bucket, key)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml = res.text().await.unwrap();
    between(&xml, "<UploadId>", "</UploadId>").unwrap().to_string()
}

async fn upload_part(c: &reqwest::Client, base: &str, bucket: &str, key: &str, id: &str, part: u32, body: Vec<u8>) -> String {
    let res = c
        .put(format!("{}/{}/{}?partNumber={}&uploadId={}", base, bucket, key, part, id))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    res.headers().get("etag").unwrap().to_str().unwrap().trim_matches('"').to_string()
}

#[tokio::test]
async fn complete_multipart_xml_and_part_validation() {
    let (handle, base, dir) = start_server("mpu_xml").await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let id = create_upload(&c, &base, "bkt", "obj").await;
    let etag1 = upload_part(&c, &base, "bkt", "obj", &id, 1, b"hello ".to_vec()).await;
    let etag2 = upload_part(&c, &base, "bkt", "obj", &id, 2, b"world".to_vec()).await;

    // 1. Bad XML -> MalformedXML
    let res = c
        .post(format!("{}/bkt/obj?uploadId={}", base, id))
        .body("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("MalformedXML"), "{}", body);

    // 2. Empty parts -> MalformedXML
    let res = c
        .post(format!("{}/bkt/obj?uploadId={}", base, id))
        .body("<CompleteMultipartUpload></CompleteMultipartUpload>")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("MalformedXML"), "{}", body);

    // 3. Out-of-range part number -> InvalidArgument
    let res = c
        .post(format!("{}/bkt/obj?uploadId={}", base, id))
        .body(format!("<CompleteMultipartUpload><Part><PartNumber>10001</PartNumber><ETag>\"{}\"</ETag></Part></CompleteMultipartUpload>", etag1))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("InvalidArgument"), "{}", body);

    // 4. Non-ascending part numbers -> InvalidPartOrder
    let res = c
        .post(format!("{}/bkt/obj?uploadId={}", base, id))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag></Part>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            etag2, etag1
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("InvalidPartOrder"), "{}", body);

    // 5. Missing part -> InvalidPart
    let res = c
        .post(format!("{}/bkt/obj?uploadId={}", base, id))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             <Part><PartNumber>3</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            etag1, etag2
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("InvalidPart"), "{}", body);

    // 6. ETag mismatch -> InvalidPart
    let res = c
        .post(format!("{}/bkt/obj?uploadId={}", base, id))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"00000000000000000000000000000000\"</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            etag2
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("InvalidPart"), "{}", body);

    // 7. Valid completion succeeds
    let res = c
        .post(format!("{}/bkt/obj?uploadId={}", base, id))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            etag1, etag2
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let got = c.get(format!("{}/bkt/obj", base)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(&got[..], b"hello world");

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn upload_part_copy_range_preconditions_and_errors() {
    let (handle, base, dir) = start_server("mpu_copy").await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let src_data = b"0123456789abcdefghij".to_vec(); // 20 bytes
    let put_src = c.put(format!("{}/bkt/src", base)).body(src_data.clone()).send().await.unwrap();
    assert_eq!(put_src.status(), StatusCode::OK);
    let src_etag = put_src.headers().get("etag").unwrap().to_str().unwrap().to_string();

    // Special name `?versionId` encoded as `%3FversionId`
    assert_eq!(
        c.put(format!("{}/bkt/%3FversionId", base)).body(b"special!".to_vec()).send().await.unwrap().status(),
        StatusCode::OK
    );

    let id = create_upload(&c, &base, "bkt", "dst").await;

    // Improper ranges -> 400 InvalidArgument
    for bad_range in ["0-2", "bytes=0", "bytes=hello-world", "bytes=0-bar", "bytes=hello-", "bytes=0-2,3-5"] {
        let res = c
            .put(format!("{}/bkt/dst?partNumber=1&uploadId={}", base, id))
            .header("x-amz-copy-source", "/bkt/src")
            .header("x-amz-copy-source-range", bad_range)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "range {}", bad_range);
        let body = res.text().await.unwrap();
        assert_eq!(between(&body, "<Code>", "</Code>"), Some("InvalidArgument"), "range {}: {}", bad_range, body);
    }

    // Out-of-bounds range -> 400 or 416 InvalidRange
    let res = c
        .put(format!("{}/bkt/dst?partNumber=1&uploadId={}", base, id))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-range", "bytes=0-21")
        .send()
        .await
        .unwrap();
    assert!(res.status() == StatusCode::BAD_REQUEST || res.status() == StatusCode::RANGE_NOT_SATISFIABLE);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("InvalidRange"), "{}", body);

    // Precondition failure (x-amz-copy-source-if-match mismatch -> 412)
    let res = c
        .put(format!("{}/bkt/dst?partNumber=1&uploadId={}", base, id))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-match", "\"deadbeef\"")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PRECONDITION_FAILED);

    // Precondition failure (x-amz-copy-source-if-none-match matches -> 412)
    let res = c
        .put(format!("{}/bkt/dst?partNumber=1&uploadId={}", base, id))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-none-match", &src_etag)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PRECONDITION_FAILED);

    // Part 1: ranged copy with matching x-amz-copy-source-if-match
    let res = c
        .put(format!("{}/bkt/dst?partNumber=1&uploadId={}", base, id))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-range", "bytes=10-19")
        .header("x-amz-copy-source-if-match", &src_etag)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml1 = res.text().await.unwrap();
    let etag1 = between(&xml1, "<ETag>", "</ETag>").expect("CopyPartResult ETag").trim_matches('"').to_string();

    // Part 2: full copy without range from special name `%3FversionId`
    let res = c
        .put(format!("{}/bkt/dst?partNumber=2&uploadId={}", base, id))
        .header("x-amz-copy-source", "/bkt/%3FversionId")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml2 = res.text().await.unwrap();
    let etag2 = between(&xml2, "<ETag>", "</ETag>").expect("CopyPartResult ETag").trim_matches('"').to_string();

    // Complete and verify combined bytes
    let res = c
        .post(format!("{}/bkt/dst?uploadId={}", base, id))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            etag1, etag2
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let got = c.get(format!("{}/bkt/dst", base)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(&got[..], b"abcdefghijspecial!");

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn get_and_head_with_part_number() {
    let (handle, base, dir) = start_server("mpu_get_part").await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    // Non-multipart object
    assert_eq!(c.put(format!("{}/bkt/single", base)).body("single-body").send().await.unwrap().status(), StatusCode::OK);
    let res = c.get(format!("{}/bkt/single?partNumber=1", base)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), "single-body");

    let res = c.get(format!("{}/bkt/single?partNumber=2", base)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("InvalidPart"), "{}", body);

    // Multipart object: request before complete -> 404 NoSuchKey
    let id = create_upload(&c, &base, "bkt", "multi").await;
    let res = c.get(format!("{}/bkt/multi?partNumber=1", base)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let p1 = vec![b'A'; 100];
    let p2 = vec![b'B'; 55];
    let etag1 = upload_part(&c, &base, "bkt", "multi", &id, 1, p1.clone()).await;
    let etag2 = upload_part(&c, &base, "bkt", "multi", &id, 2, p2.clone()).await;
    let cmp = c
        .post(format!("{}/bkt/multi?uploadId={}", base, id))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            etag1, etag2
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);
    let obj_etag = cmp.headers().get("etag").unwrap().to_str().unwrap().to_string();

    for (pn, expected) in [(1u32, &p1), (2u32, &p2)] {
        let head = c.head(format!("{}/bkt/multi?partNumber={}", base, pn)).send().await.unwrap();
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(head.headers().get("x-amz-mp-parts-count").unwrap().to_str().unwrap(), "2");
        assert_eq!(head.headers().get("etag").unwrap().to_str().unwrap(), obj_etag);
        assert_eq!(head.headers().get("accept-ranges").unwrap().to_str().unwrap(), "bytes");
        assert_eq!(head.headers().get("content-length").unwrap().to_str().unwrap(), expected.len().to_string());

        let get = c.get(format!("{}/bkt/multi?partNumber={}", base, pn)).send().await.unwrap();
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.headers().get("x-amz-mp-parts-count").unwrap().to_str().unwrap(), "2");
        assert_eq!(get.headers().get("etag").unwrap().to_str().unwrap(), obj_etag);
        assert_eq!(get.headers().get("content-length").unwrap().to_str().unwrap(), expected.len().to_string());
        assert_eq!(get.bytes().await.unwrap().as_ref(), expected.as_slice());
    }

    // Out-of-range partNumber -> 400 InvalidPart
    let res = c.get(format!("{}/bkt/multi?partNumber=3", base)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("InvalidPart"), "{}", body);

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_parts_and_list_multipart_uploads_pagination() {
    let (handle, base, dir) = start_server("mpu_list").await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    // ListParts pagination + encoding-type=url
    let id = create_upload(&c, &base, "bkt", "dir/my%20file").await;
    for pn in 1..=4 {
        upload_part(&c, &base, "bkt", "dir/my%20file", &id, pn, vec![pn as u8; 10]).await;
    }

    let page1 = c
        .get(format!("{}/bkt/dir/my%20file?uploadId={}&max-parts=2&encoding-type=url", base, id))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(between(&page1, "<IsTruncated>", "</IsTruncated>"), Some("true"), "{}", page1);
    assert_eq!(between(&page1, "<NextPartNumberMarker>", "</NextPartNumberMarker>"), Some("2"), "{}", page1);
    assert_eq!(between(&page1, "<EncodingType>", "</EncodingType>"), Some("url"), "{}", page1);
    assert_eq!(between(&page1, "<Key>", "</Key>"), Some("dir/my%20file"), "{}", page1);
    assert_eq!(page1.matches("<Part>").count(), 2);

    let page2 = c
        .get(format!("{}/bkt/dir/my%20file?uploadId={}&part-number-marker=2&max-parts=2", base, id))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(between(&page2, "<IsTruncated>", "</IsTruncated>"), Some("false"), "{}", page2);
    assert_eq!(page2.matches("<Part>").count(), 2);
    assert!(page2.contains("<PartNumber>3</PartNumber>") && page2.contains("<PartNumber>4</PartNumber>"));

    // ListMultipartUploads pagination, prefix, delimiter, encoding-type=url
    let _id_a1 = create_upload(&c, &base, "bkt", "a/1").await;
    let _id_a2 = create_upload(&c, &base, "bkt", "a/2").await;
    let _id_b1 = create_upload(&c, &base, "bkt", "b/1").await;
    let _id_top = create_upload(&c, &base, "bkt", "top%20file").await;

    let delim_xml = c
        .get(format!("{}/bkt?uploads&delimiter=/&encoding-type=url", base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(between(&delim_xml, "<EncodingType>", "</EncodingType>"), Some("url"), "{}", delim_xml);
    assert!(delim_xml.contains("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>"), "{}", delim_xml);
    assert!(delim_xml.contains("<CommonPrefixes><Prefix>b/</Prefix></CommonPrefixes>"), "{}", delim_xml);
    assert_eq!(delim_xml.matches("<Upload>").count(), 1); // top file
    assert!(delim_xml.contains("<Key>top%20file</Key>"), "{}", delim_xml);
    assert!(delim_xml.contains("<CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes>"), "{}", delim_xml);

    let up_p1 = c
        .get(format!("{}/bkt?uploads&prefix=a/&max-uploads=1", base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(between(&up_p1, "<IsTruncated>", "</IsTruncated>"), Some("true"), "{}", up_p1);
    assert_eq!(up_p1.matches("<Upload>").count(), 1);
    let next_km = between(&up_p1, "<NextKeyMarker>", "</NextKeyMarker>").unwrap();
    let next_uim = between(&up_p1, "<NextUploadIdMarker>", "</NextUploadIdMarker>").unwrap();

    let up_p2 = c
        .get(format!("{}/bkt?uploads&prefix=a/&max-uploads=1&key-marker={}&upload-id-marker={}", base, next_km, next_uim))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(between(&up_p2, "<IsTruncated>", "</IsTruncated>"), Some("false"), "{}", up_p2);
    assert_eq!(up_p2.matches("<Upload>").count(), 1);
    assert!(up_p2.contains("<Key>a/2</Key>"));

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}


#[tokio::test]
async fn complete_multipart_is_idempotent_and_abort_after_complete_is_no_such_upload() {
    let dir = temp_dir("idemp");
    let (handle, base, _dir2) = start_server("idemp").await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let init = c.post(format!("{}/bkt/obj?uploads", base)).send().await.unwrap().text().await.unwrap();
    let uid = between(&init, "<UploadId>", "</UploadId>").unwrap().to_string();

    let p1 = c
        .put(format!("{}/bkt/obj?partNumber=1&uploadId={}", base, uid))
        .body("hello-idempotent")
        .send()
        .await
        .unwrap();
    assert_eq!(p1.status(), StatusCode::OK);
    let e1 = p1.headers().get("etag").unwrap().to_str().unwrap().to_string();

    let xml_body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        e1
    );
    let cmp1 = c
        .post(format!("{}/bkt/obj?uploadId={}", base, uid))
        .body(xml_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(cmp1.status(), StatusCode::OK);
    let etag1 = cmp1.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let body1 = cmp1.text().await.unwrap();

    // Second CompleteMultipartUpload on the same uploadId answers the completed result (200 OK).
    let cmp2 = c
        .post(format!("{}/bkt/obj?uploadId={}", base, uid))
        .body(xml_body)
        .send()
        .await
        .unwrap();
    assert_eq!(cmp2.status(), StatusCode::OK);
    assert_eq!(cmp2.headers().get("etag").unwrap().to_str().unwrap(), etag1);
    let body2 = cmp2.text().await.unwrap();
    assert_eq!(between(&body2, "<ETag>", "</ETag>"), between(&body1, "<ETag>", "</ETag>"));

    // Abort after Complete answers 404 NoSuchUpload.
    let abort = c
        .delete(format!("{}/bkt/obj?uploadId={}", base, uid))
        .send()
        .await
        .unwrap();
    assert_eq!(abort.status(), StatusCode::NOT_FOUND);
    let abort_xml = abort.text().await.unwrap();
    assert_eq!(between(&abort_xml, "<Code>", "</Code>"), Some("NoSuchUpload"));

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}


#[tokio::test]
async fn complete_multipart_preserves_create_metadata_and_content_type() {
    let (handle, base, dir) = start_server("mpu_meta").await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let init = c
        .post(format!("{}/bkt/obj?uploads", base))
        .header("content-type", "text/bla")
        .header("x-amz-meta-foo", "bar")
        .header("cache-control", "max-age=60")
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let uid = between(&init.text().await.unwrap(), "<UploadId>", "</UploadId>").unwrap().to_string();

    let _e1_old = upload_part(&c, &base, "bkt", "obj", &uid, 1, b"hello".to_vec()).await;
    let e2 = upload_part(&c, &base, "bkt", "obj", &uid, 2, b"world".to_vec()).await;
    let e1_new = upload_part(&c, &base, "bkt", "obj", &uid, 1, b"HELLO".to_vec()).await;

    let cmp = c
        .post(format!("{}/bkt/obj?uploadId={}", base, uid))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            e1_new, e2
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);

    let get = c.get(format!("{}/bkt/obj", base)).send().await.unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.headers().get("content-type").and_then(|v| v.to_str().ok()), Some("text/bla"));
    assert_eq!(get.headers().get("x-amz-meta-foo").and_then(|v| v.to_str().ok()), Some("bar"));
    assert_eq!(get.headers().get("cache-control").and_then(|v| v.to_str().ok()), Some("max-age=60"));
    assert_eq!(&get.bytes().await.unwrap()[..], b"HELLOworld");

    // CopyObject from the completed multipart object preserves its metadata and Content-Type.
    let copy = c
        .put(format!("{}/bkt/dst", base))
        .header("x-amz-copy-source", "/bkt/obj")
        .send()
        .await
        .unwrap();
    assert_eq!(copy.status(), StatusCode::OK);
    let head = c.head(format!("{}/bkt/dst", base)).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers().get("content-type").and_then(|v| v.to_str().ok()), Some("text/bla"));
    assert_eq!(head.headers().get("x-amz-meta-foo").and_then(|v| v.to_str().ok()), Some("bar"));

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn complete_multipart_resend_same_part_number_finishes_last() {
    let (handle, base, dir) = start_server("mpu_resend_dup").await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);

    let uid = create_upload(&c, &base, "bkt", "obj").await;
    let etag_b = upload_part(&c, &base, "bkt", "obj", &uid, 1, b"BBBB".to_vec()).await;
    let etag_a = upload_part(&c, &base, "bkt", "obj", &uid, 1, b"AAAA".to_vec()).await;

    let cmp = c
        .post(format!("{}/bkt/obj?uploadId={}", base, uid))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            etag_b, etag_a
        ))
        .send()
        .await
        .unwrap();
    let status = cmp.status();
    let body = cmp.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}", body);

    let got = c.get(format!("{}/bkt/obj", base)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(&got[..], b"AAAA");

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn complete_multipart_versioning_return_version_id() {
    let (handle, base, dir) = start_server("mpu_ver").await;
    let c = reqwest::Client::new();

    // 1. Unset (default): no x-amz-version-id
    assert_eq!(c.put(format!("{}/bkt-unset", base)).send().await.unwrap().status(), StatusCode::OK);
    let uid = create_upload(&c, &base, "bkt-unset", "k").await;
    let e1 = upload_part(&c, &base, "bkt-unset", "k", &uid, 1, b"data".to_vec()).await;
    let cmp = c
        .post(format!("{}/bkt-unset/k?uploadId={}", base, uid))
        .body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part></CompleteMultipartUpload>", e1))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);
    assert!(cmp.headers().get("x-amz-version-id").is_none(), "unset bucket must not return x-amz-version-id");

    // 2. Enabled: non-empty x-amz-version-id
    assert_eq!(c.put(format!("{}/bkt-en", base)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{}/bkt-en?versioning", base))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let uid = create_upload(&c, &base, "bkt-en", "k").await;
    let e1 = upload_part(&c, &base, "bkt-en", "k", &uid, 1, b"data".to_vec()).await;
    let cmp = c
        .post(format!("{}/bkt-en/k?uploadId={}", base, uid))
        .body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part></CompleteMultipartUpload>", e1))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);
    let vid = cmp.headers().get("x-amz-version-id").and_then(|v| v.to_str().ok());
    assert!(vid.is_some_and(|s| !s.is_empty() && s != "null"), "enabled bucket must return real version id, got {:?}", vid);

    // 3. Suspended: no x-amz-version-id
    assert_eq!(c.put(format!("{}/bkt-susp", base)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{}/bkt-susp?versioning", base))
            .body("<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let uid = create_upload(&c, &base, "bkt-susp", "k").await;
    let e1 = upload_part(&c, &base, "bkt-susp", "k", &uid, 1, b"data".to_vec()).await;
    let cmp = c
        .post(format!("{}/bkt-susp/k?uploadId={}", base, uid))
        .body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part></CompleteMultipartUpload>", e1))
        .send()
        .await
        .unwrap();
    assert_eq!(cmp.status(), StatusCode::OK);
    assert!(
        cmp.headers().get("x-amz-version-id").is_none(),
        "suspended bucket must not return x-amz-version-id, got {:?}",
        cmp.headers().get("x-amz-version-id")
    );
    assert_eq!(&c.get(format!("{}/bkt-susp/k", base)).send().await.unwrap().bytes().await.unwrap()[..], b"data");

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

struct ChildServer(std::process::Child);

impl Drop for ChildServer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start_binary_server(tag: &str, extra_args: &[&str]) -> (ChildServer, String, std::path::PathBuf) {
    let dir = temp_dir(tag);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_s3-server"))
        .args([
            "--port",
            &port.to_string(),
            "--data-dir",
            dir.to_str().unwrap(),
            "--allow-anonymous",
            "--auto-compaction",
            "off",
        ])
        .args(extra_args)
        .env("AWANFS_LOG", "off")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn s3-server");
    let server = ChildServer(child);
    let base = format!("http://127.0.0.1:{}", port);
    let c = reqwest::Client::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if let Ok(r) = c.get(format!("{}/_awanfs/health/ready", base)).send().await {
            if r.status() == StatusCode::OK {
                return (server, base, dir);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("s3-server binary did not become ready on {}", base);
}

#[test]
fn cli_help_documents_allow_small_parts() {
    let dir = temp_dir("cli_help");
    let out_path = dir.join("help.stdout");
    let err_path = dir.join("help.stderr");
    for flag in ["--help", "-h"] {
        let out_file = std::fs::File::create(&out_path).unwrap();
        let err_file = std::fs::File::create(&err_path).unwrap();
        let mut child = ChildServer(
            std::process::Command::new(env!("CARGO_BIN_EXE_s3-server"))
                .arg(flag)
                .stdout(std::process::Stdio::from(out_file))
                .stderr(std::process::Stdio::from(err_file))
                .spawn()
                .expect("spawn s3-server --help"),
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let status = loop {
            match child.0.try_wait().unwrap() {
                Some(st) => break st,
                None if std::time::Instant::now() >= deadline => {
                    let _ = child.0.kill();
                    let _ = child.0.wait();
                    panic!("s3-server {} did not exit immediately", flag);
                }
                None => std::thread::sleep(std::time::Duration::from_millis(25)),
            }
        };
        let stdout = std::fs::read_to_string(&out_path).unwrap_or_default();
        let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
        assert!(status.success(), "s3-server {} exited {:?}: stderr={}", flag, status.code(), stderr);
        assert!(
            stdout.contains("--allow-small-parts"),
            "s3-server {} stdout must document --allow-small-parts; got:\n{}",
            flag,
            stdout
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn cli_default_rejects_small_parts_and_flag_allows_them() {
    let c = reqwest::Client::new();
    let part_body = vec![b'a'; 256 * 1024];

    // 1. Default s3-server (without --allow-small-parts): < 5 MiB non-final part -> 400 EntityTooSmall.
    let (mut srv_default, base_default, dir_default) = start_binary_server("cli_default_small", &[]).await;
    assert_eq!(c.put(format!("{}/bkt", base_default)).send().await.unwrap().status(), StatusCode::OK);
    let uid = create_upload(&c, &base_default, "bkt", "obj").await;
    let e1 = upload_part(&c, &base_default, "bkt", "obj", &uid, 1, part_body.clone()).await;
    let e2 = upload_part(&c, &base_default, "bkt", "obj", &uid, 2, part_body.clone()).await;
    let cmp = c
        .post(format!("{}/bkt/obj?uploadId={}", base_default, uid))
        .body(format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag></Part>\
             </CompleteMultipartUpload>",
            e1, e2
        ))
        .send()
        .await
        .unwrap();
    let status = cmp.status();
    let body = cmp.text().await.unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "default server must reject <5MiB non-final part: {}", body);
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("EntityTooSmall"), "{}", body);
    let _ = srv_default.0.kill();
    let _ = srv_default.0.wait();
    drop(srv_default);
    let _ = std::fs::remove_dir_all(&dir_default);

    // 2. s3-server --allow-small-parts: 4 x 256 KiB parts (as in mpu_1m) succeed with 200 OK.
    let (mut srv_allow, base_allow, dir_allow) =
        start_binary_server("cli_allow_small", &["--allow-small-parts"]).await;
    assert_eq!(c.put(format!("{}/bkt", base_allow)).send().await.unwrap().status(), StatusCode::OK);
    let uid = create_upload(&c, &base_allow, "bkt", "obj").await;
    let mut parts_xml = String::from("<CompleteMultipartUpload>");
    for p in 1..=4u32 {
        let etag = upload_part(&c, &base_allow, "bkt", "obj", &uid, p, part_body.clone()).await;
        parts_xml.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>\"{}\"</ETag></Part>", p, etag));
    }
    parts_xml.push_str("</CompleteMultipartUpload>");
    let cmp = c
        .post(format!("{}/bkt/obj?uploadId={}", base_allow, uid))
        .body(parts_xml)
        .send()
        .await
        .unwrap();
    let status = cmp.status();
    let body = cmp.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "--allow-small-parts must accept 4x256KiB parts: {}", body);
    let got = c.get(format!("{}/bkt/obj", base_allow)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), 4 * 256 * 1024);
    let _ = srv_allow.0.kill();
    let _ = srv_allow.0.wait();
    drop(srv_allow);
    let _ = std::fs::remove_dir_all(&dir_allow);
}
