//! S3 Object Tagging Tests (#115).

use reqwest::header::CONTENT_TYPE;
use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_tagging_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn_server() -> (String, Arc<S3ServiceState>, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::new());
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, state, handle)
}

fn tagging_xml(pairs: &[(&str, &str)]) -> String {
    let mut s = String::from("<Tagging><TagSet>");
    for (k, v) in pairs {
        s.push_str(&format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", k, v));
    }
    s.push_str("</TagSet></Tagging>");
    s
}

#[tokio::test]
async fn test_put_get_modify_delete_object_tagging() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    // Non-existent bucket -> 404 NoSuchBucket
    let res = client.get(format!("{}/no-such-bkt/obj?tagging", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("NoSuchBucket"));

    // Create bucket
    assert_eq!(client.put(format!("{}/tag-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);

    // Non-existent key -> 404 NoSuchKey
    let res = client.get(format!("{}/tag-bkt/missing?tagging", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("NoSuchKey"));

    // Put object with Content-Type
    let res = client
        .put(format!("{}/tag-bkt/obj1", base_url))
        .header(CONTENT_TYPE, "text/plain")
        .body("hello tagging")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // GET ?tagging before any tags -> 200 OK with empty TagSet, HEAD has no x-amz-tagging-count
    let res = client.get(format!("{}/tag-bkt/obj1?tagging", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<TagSet>"));
    assert!(!xml.contains("<Tag>"));

    let head = client.head(format!("{}/tag-bkt/obj1", base_url)).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert!(head.headers().get("x-amz-tagging-count").is_none());

    // PUT ?tagging with 2 tags -> 200 OK
    let body_xml = tagging_xml(&[("env", "prod"), ("team", "storage")]);
    let res = client
        .put(format!("{}/tag-bkt/obj1?tagging", base_url))
        .body(body_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // GET ?tagging returns both tags
    let res = client.get(format!("{}/tag-bkt/obj1?tagging", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Key>env</Key>"));
    assert!(xml.contains("<Value>prod</Value>"));
    assert!(xml.contains("<Key>team</Key>"));
    assert!(xml.contains("<Value>storage</Value>"));

    // HEAD and GET object include x-amz-tagging-count: 2 and preserve Content-Type + body
    let head = client.head(format!("{}/tag-bkt/obj1", base_url)).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "2");
    assert_eq!(head.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap(), "text/plain");

    let get = client.get(format!("{}/tag-bkt/obj1", base_url)).send().await.unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "2");
    assert_eq!(get.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap(), "text/plain");
    assert_eq!(get.text().await.unwrap(), "hello tagging");

    // Modify tags (PUT ?tagging replaces previous tags)
    let res = client
        .put(format!("{}/tag-bkt/obj1?tagging", base_url))
        .body(tagging_xml(&[("key3", "val3")]))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = client.get(format!("{}/tag-bkt/obj1?tagging", base_url)).send().await.unwrap();
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Key>key3</Key>"));
    assert!(!xml.contains("<Key>env</Key>"));

    let head = client.head(format!("{}/tag-bkt/obj1", base_url)).send().await.unwrap();
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "1");

    // DELETE ?tagging -> 204 No Content
    let res = client.delete(format!("{}/tag-bkt/obj1?tagging", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let res = client.get(format!("{}/tag-bkt/obj1?tagging", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(!res.text().await.unwrap().contains("<Tag>"));

    let head = client.head(format!("{}/tag-bkt/obj1", base_url)).send().await.unwrap();
    assert!(head.headers().get("x-amz-tagging-count").is_none());
    assert_eq!(head.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap(), "text/plain");
}

#[tokio::test]
async fn test_put_object_with_x_amz_tagging_header() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    assert_eq!(client.put(format!("{}/hdr-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);

    // PUT with x-amz-tagging: foo=bar&bar
    let res = client
        .put(format!("{}/hdr-bkt/obj", base_url))
        .header("x-amz-tagging", "foo=bar&bar")
        .body("A".repeat(100))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let get = client.get(format!("{}/hdr-bkt/obj", base_url)).send().await.unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "2");
    assert_eq!(get.text().await.unwrap(), "A".repeat(100));

    let res = client.get(format!("{}/hdr-bkt/obj?tagging", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Key>foo</Key>"));
    assert!(xml.contains("<Value>bar</Value>"));
    assert!(xml.contains("<Key>bar</Key>"));
    assert!(xml.contains("<Value></Value>"));

    // Invalid x-amz-tagging on PUT -> 400 InvalidTag, object is not stored
    let res = client
        .put(format!("{}/hdr-bkt/bad-obj", base_url))
        .header("x-amz-tagging", "aws:reserved=1")
        .body("nope")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("InvalidTag"));

    let head = client.head(format!("{}/hdr-bkt/bad-obj", base_url)).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_tagging_limits_and_errors() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    assert_eq!(client.put(format!("{}/lim-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(client.put(format!("{}/lim-bkt/obj", base_url)).body("data").send().await.unwrap().status(), StatusCode::OK);

    // 10 tags (max) with 128-char keys and 256-char values -> 200 OK
    let mut pairs_storage = Vec::new();
    for i in 0..10 {
        let k = format!("{}{}", i, "k".repeat(127));
        let v = "v".repeat(256);
        pairs_storage.push((k, v));
    }
    let pairs: Vec<(&str, &str)> = pairs_storage.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let res = client
        .put(format!("{}/lim-bkt/obj?tagging", base_url))
        .body(tagging_xml(&pairs))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let head = client.head(format!("{}/lim-bkt/obj", base_url)).send().await.unwrap();
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "10");

    // 11 tags -> 400 InvalidTag, existing 10 tags untouched
    let mut excess = Vec::new();
    for i in 0..11 {
        excess.push((format!("k{}", i), format!("v{}", i)));
    }
    let excess_refs: Vec<(&str, &str)> = excess.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let res = client
        .put(format!("{}/lim-bkt/obj?tagging", base_url))
        .body(tagging_xml(&excess_refs))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("InvalidTag"));

    let head = client.head(format!("{}/lim-bkt/obj", base_url)).send().await.unwrap();
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "10");

    // Key length 129 -> 400 InvalidTag
    let k129 = "k".repeat(129);
    let res = client
        .put(format!("{}/lim-bkt/obj?tagging", base_url))
        .body(tagging_xml(&[(k129.as_str(), "v")]))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("InvalidTag"));

    // Value length 257 -> 400 InvalidTag
    let v257 = "v".repeat(257);
    let res = client
        .put(format!("{}/lim-bkt/obj?tagging", base_url))
        .body(tagging_xml(&[("k", v257.as_str())]))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("InvalidTag"));

    // Malformed XML -> 400 MalformedXML
    let res = client
        .put(format!("{}/lim-bkt/obj?tagging", base_url))
        .body("<Tagging><Broken>")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("MalformedXML"));
}

#[tokio::test]
async fn test_versioned_object_tagging() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    assert_eq!(client.put(format!("{}/ver-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);
    let res = client
        .put(format!("{}/ver-bkt?versioning", base_url))
        .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Put v1 and v2
    let r1 = client.put(format!("{}/ver-bkt/k", base_url)).body("v1").send().await.unwrap();
    let v1 = r1.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();

    let r2 = client.put(format!("{}/ver-bkt/k", base_url)).body("v2").send().await.unwrap();
    let v2 = r2.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    assert_ne!(v1, v2);

    // Tag v1 only
    let res = client
        .put(format!("{}/ver-bkt/k?tagging&versionId={}", base_url, v1))
        .body(tagging_xml(&[("ver", "one")]))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // v1 has tag, v2 has none
    let g1 = client.get(format!("{}/ver-bkt/k?tagging&versionId={}", base_url, v1)).send().await.unwrap();
    assert_eq!(g1.status(), StatusCode::OK);
    assert!(g1.text().await.unwrap().contains("<Value>one</Value>"));

    let g2 = client.get(format!("{}/ver-bkt/k?tagging&versionId={}", base_url, v2)).send().await.unwrap();
    assert_eq!(g2.status(), StatusCode::OK);
    assert!(!g2.text().await.unwrap().contains("<Tag>"));

    // Delete v1 tags
    let d1 = client.delete(format!("{}/ver-bkt/k?tagging&versionId={}", base_url, v1)).send().await.unwrap();
    assert_eq!(d1.status(), StatusCode::NO_CONTENT);

    let g1 = client.get(format!("{}/ver-bkt/k?tagging&versionId={}", base_url, v1)).send().await.unwrap();
    assert!(!g1.text().await.unwrap().contains("<Tag>"));
}

#[tokio::test]
async fn test_tagging_restart_persistence() {
    let dir = temp_dir("restart");
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client = reqwest::Client::new();

    {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir));
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
        let base_url = format!("http://{}", handle.local_addr);

        assert_eq!(client.put(format!("{}/persist-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);

        // Object 1: x-amz-tagging on PUT
        assert_eq!(
            client
                .put(format!("{}/persist-bkt/o1", base_url))
                .header(CONTENT_TYPE, "application/json")
                .header("x-amz-tagging", "a=1&b=2")
                .body("{\"ok\":true}")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        // Object 2: PutObjectTagging after PUT
        assert_eq!(
            client
                .put(format!("{}/persist-bkt/o2", base_url))
                .header(CONTENT_TYPE, "text/plain")
                .body("payload2")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .put(format!("{}/persist-bkt/o2?tagging", base_url))
                .body(tagging_xml(&[("p", "yes")]))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        // Object 3: tagged then deleted
        assert_eq!(
            client
                .put(format!("{}/persist-bkt/o3", base_url))
                .header("x-amz-tagging", "tmp=1")
                .body("payload3")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client.delete(format!("{}/persist-bkt/o3?tagging", base_url)).send().await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
    }

    // Restart from same data dir
    {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir));
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
        let base_url = format!("http://{}", handle.local_addr);

        let r1 = client.get(format!("{}/persist-bkt/o1?tagging", base_url)).send().await.unwrap();
        assert_eq!(r1.status(), StatusCode::OK);
        let xml1 = r1.text().await.unwrap();
        assert!(xml1.contains("<Key>a</Key>") && xml1.contains("<Value>1</Value>"));
        assert!(xml1.contains("<Key>b</Key>") && xml1.contains("<Value>2</Value>"));
        let h1 = client.head(format!("{}/persist-bkt/o1", base_url)).send().await.unwrap();
        assert_eq!(h1.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "2");
        assert_eq!(h1.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap(), "application/json");

        let r2 = client.get(format!("{}/persist-bkt/o2?tagging", base_url)).send().await.unwrap();
        assert_eq!(r2.status(), StatusCode::OK);
        assert!(r2.text().await.unwrap().contains("<Key>p</Key>"));
        let g2 = client.get(format!("{}/persist-bkt/o2", base_url)).send().await.unwrap();
        assert_eq!(g2.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "1");
        assert_eq!(g2.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap(), "text/plain");
        assert_eq!(g2.text().await.unwrap(), "payload2");

        let r3 = client.get(format!("{}/persist-bkt/o3?tagging", base_url)).send().await.unwrap();
        assert_eq!(r3.status(), StatusCode::OK);
        assert!(!r3.text().await.unwrap().contains("<Tag>"));
        let h3 = client.head(format!("{}/persist-bkt/o3", base_url)).send().await.unwrap();
        assert!(h3.headers().get("x-amz-tagging-count").is_none());
    }

    let _ = std::fs::remove_dir_all(&dir);
}


#[tokio::test]
async fn test_copy_object_tagging_directive() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    assert_eq!(client.put(format!("{}/cp-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);

    // Put source object with x-amz-tagging: foo (empty value) and Content-Type
    let res = client
        .put(format!("{}/cp-bkt/src", base_url))
        .header(CONTENT_TYPE, "text/plain")
        .header("x-amz-tagging", "foo")
        .body("copy payload")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Default directive (COPY) copies source tags
    let res = client
        .put(format!("{}/cp-bkt/dst-copy", base_url))
        .header("x-amz-copy-source", "/cp-bkt/src")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let tags = client.get(format!("{}/cp-bkt/dst-copy?tagging", base_url)).send().await.unwrap().text().await.unwrap();
    assert!(tags.contains("<Key>foo</Key>"));
    assert!(tags.contains("<Value></Value>"));

    // Self-copy with x-amz-tagging-directive: REPLACE and x-amz-tagging: bar=bar
    let res = client
        .put(format!("{}/cp-bkt/src", base_url))
        .header("x-amz-copy-source", "/cp-bkt/src")
        .header("x-amz-tagging-directive", "REPLACE")
        .header("x-amz-tagging", "bar=bar")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let tags = client.get(format!("{}/cp-bkt/src?tagging", base_url)).send().await.unwrap().text().await.unwrap();
    assert!(tags.contains("<Key>bar</Key>") && tags.contains("<Value>bar</Value>"));
    assert!(!tags.contains("<Key>foo</Key>"));
    let head = client.head(format!("{}/cp-bkt/src", base_url)).send().await.unwrap();
    assert_eq!(head.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap(), "text/plain");
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "1");

    // REPLACE without x-amz-tagging clears tags
    let res = client
        .put(format!("{}/cp-bkt/dst-empty", base_url))
        .header("x-amz-copy-source", "/cp-bkt/src")
        .header("x-amz-tagging-directive", "REPLACE")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let head = client.head(format!("{}/cp-bkt/dst-empty", base_url)).send().await.unwrap();
    assert!(head.headers().get("x-amz-tagging-count").is_none());

    // Metadata REPLACE + tagging COPY preserves source tags
    let res = client
        .put(format!("{}/cp-bkt/dst-meta", base_url))
        .header("x-amz-copy-source", "/cp-bkt/src")
        .header("x-amz-metadata-directive", "REPLACE")
        .header(CONTENT_TYPE, "application/octet-stream")
        .header("x-amz-tagging-directive", "COPY")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let head = client.head(format!("{}/cp-bkt/dst-meta", base_url)).send().await.unwrap();
    assert_eq!(head.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap(), "application/octet-stream");
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "1");

    // Invalid x-amz-tagging-directive -> 400 InvalidArgument
    let res = client
        .put(format!("{}/cp-bkt/dst-bad", base_url))
        .header("x-amz-copy-source", "/cp-bkt/src")
        .header("x-amz-tagging-directive", "BOGUS")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_multipart_upload_tagging() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    assert_eq!(client.put(format!("{}/mp-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);

    // Invalid x-amz-tagging on CreateMultipartUpload -> 400 InvalidTag
    let res = client
        .post(format!("{}/mp-bkt/bad?uploads", base_url))
        .header("x-amz-tagging", "aws:reserved=1")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("InvalidTag"));

    // CreateMultipartUpload with x-amz-tagging: k1=v1&k2=v2
    let init = client
        .post(format!("{}/mp-bkt/obj?uploads", base_url))
        .header("x-amz-tagging", "k1=v1&k2=v2")
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let init_xml = init.text().await.unwrap();
    let start = init_xml.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = init_xml.find("</UploadId>").unwrap();
    let upload_id = &init_xml[start..end];

    let part = client
        .put(format!("{}/mp-bkt/obj?partNumber=1&uploadId={}", base_url, upload_id))
        .body("hello multipart tagging")
        .send()
        .await
        .unwrap();
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part.headers().get("etag").unwrap().to_str().unwrap().to_string();

    let complete_xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etag
    );
    let comp = client
        .post(format!("{}/mp-bkt/obj?uploadId={}", base_url, upload_id))
        .body(complete_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(comp.status(), StatusCode::OK);

    let head = client.head(format!("{}/mp-bkt/obj", base_url)).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap().to_str().unwrap(), "2");

    let tags = client.get(format!("{}/mp-bkt/obj?tagging", base_url)).send().await.unwrap().text().await.unwrap();
    assert!(tags.contains("<Key>k1</Key>") && tags.contains("<Value>v1</Value>"));
    assert!(tags.contains("<Key>k2</Key>") && tags.contains("<Value>v2</Value>"));
}
