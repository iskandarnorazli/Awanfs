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
