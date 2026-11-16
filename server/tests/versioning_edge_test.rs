//! Edge cases for bucket versioning, multi-object delete, and ListObjectVersions (#113).

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

async fn spawn() -> (String, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::new())).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (url, handle)
}

fn xml_code(body: &str) -> String {
    let s = body.find("<Code>").expect(body) + 6;
    let e = body[s..].find("</Code>").expect(body) + s;
    body[s..e].to_string()
}

fn xml_tags(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(s) = rest.find(&open) {
        let after = &rest[s + open.len()..];
        let e = after.find(&close).expect(body);
        out.push(after[..e].to_string());
        rest = &after[e + close.len()..];
    }
    out
}

#[tokio::test]
async fn put_bucket_versioning_xml_validation() {
    let (url, handle) = spawn().await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/ver-xml-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // Malformed XML -> 400 MalformedXML
    for bad in [
        "not xml",
        "<WrongRoot><Status>Enabled</Status></WrongRoot>",
        "<VersioningConfiguration/>",
        "<VersioningConfiguration><Status>Enabled</Status><Status>Suspended</Status></VersioningConfiguration>",
        "<VersioningConfiguration><MfaDelete>Disabled</MfaDelete></VersioningConfiguration>",
    ] {
        let res = c.put(format!("{}/ver-xml-bkt?versioning", url)).body(bad).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "body={}", bad);
        assert_eq!(xml_code(&res.text().await.unwrap()), "MalformedXML", "body={}", bad);
    }

    // Illegal status -> 400 IllegalVersioningConfigurationException
    for bad_status in [
        "<VersioningConfiguration><Status>Bogus</Status></VersioningConfiguration>",
        "<VersioningConfiguration><Status/></VersioningConfiguration>",
    ] {
        let res = c.put(format!("{}/ver-xml-bkt?versioning", url)).body(bad_status).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "body={}", bad_status);
        assert_eq!(
            xml_code(&res.text().await.unwrap()),
            "IllegalVersioningConfigurationException",
            "body={}",
            bad_status
        );
    }

    // MfaDelete Enabled -> 501 NotImplemented
    for mfa in [
        "<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>",
        "<VersioningConfiguration><Status>Enabled</Status><MFADelete>Enabled</MFADelete></VersioningConfiguration>",
    ] {
        let res = c.put(format!("{}/ver-xml-bkt?versioning", url)).body(mfa).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED, "body={}", mfa);
        assert_eq!(xml_code(&res.text().await.unwrap()), "NotImplemented", "body={}", mfa);
    }

    // MfaDelete Disabled -> 200 OK
    let ok_mfa = "<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Disabled</MfaDelete></VersioningConfiguration>";
    let res = c.put(format!("{}/ver-xml-bkt?versioning", url)).body(ok_mfa).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    handle.shutdown();
}

#[tokio::test]
async fn delete_objects_quiet_malformed_and_delete_markers() {
    let (url, handle) = spawn().await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/del-edge-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let enable = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{}/del-edge-bkt?versioning", url)).body(enable).send().await.unwrap().status(), StatusCode::OK);

    // MalformedXML on invalid <Delete>
    for bad in [
        "not xml",
        "<Delete/>",
        "<Delete><Object></Object></Delete>",
        "<Delete><Quiet>maybe</Quiet><Object><Key>a</Key></Object></Delete>",
    ] {
        let res = c.post(format!("{}/del-edge-bkt?delete", url)).body(bad).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "body={}", bad);
        assert_eq!(xml_code(&res.text().await.unwrap()), "MalformedXML", "body={}", bad);
    }

    // Multi-object delete without VersionId creates a delete marker and reports DeleteMarker + DeleteMarkerVersionId
    let res = c.post(format!("{}/del-edge-bkt?delete", url))
        .body("<Delete><Object><Key>k1</Key></Object></Delete>")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<DeleteMarker>true</DeleteMarker>"), "{}", body);
    let dm_vids = xml_tags(&body, "DeleteMarkerVersionId");
    assert_eq!(dm_vids.len(), 1, "{}", body);
    let dm_vid = dm_vids[0].clone();

    // Deleting that delete marker by VersionId also reports DeleteMarker=true and DeleteMarkerVersionId
    let del_dm = format!("<Delete><Object><Key>k1</Key><VersionId>{}</VersionId></Object></Delete>", dm_vid);
    let res = c.post(format!("{}/del-edge-bkt?delete", url)).body(del_dm).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<DeleteMarker>true</DeleteMarker>"), "{}", body);
    assert_eq!(xml_tags(&body, "DeleteMarkerVersionId"), vec![dm_vid]);

    // Quiet=true omits <Deleted> elements
    assert_eq!(c.put(format!("{}/del-edge-bkt/k2", url)).body("v1").send().await.unwrap().status(), StatusCode::OK);
    let quiet_del = "<Delete><Quiet>true</Quiet><Object><Key>k2</Key></Object></Delete>";
    let res = c.post(format!("{}/del-edge-bkt?delete", url)).body(quiet_del).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(!body.contains("<Deleted>"), "Quiet=true must omit <Deleted>: {}", body);

    handle.shutdown();
}

#[tokio::test]
async fn get_head_delete_version_id_edge_cases() {
    let (url, handle) = spawn().await;
    let c = reqwest::Client::new();

    // Non-versioned bucket: HEAD after DELETE returns 404 with x-amz-delete-marker: false and empty body
    assert_eq!(c.put(format!("{}/nv-edge-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/nv-edge-bkt/f", url)).body("hi").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.delete(format!("{}/nv-edge-bkt/f", url)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let res = c.head(format!("{}/nv-edge-bkt/f", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        res.headers().get("x-amz-delete-marker").and_then(|v| v.to_str().ok()),
        Some("false")
    );
    assert!(res.bytes().await.unwrap().is_empty(), "HEAD 404 must have empty body");

    // Versioned bucket
    assert_eq!(c.put(format!("{}/v-edge-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let enable = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{}/v-edge-bkt?versioning", url)).body(enable).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/v-edge-bkt/k", url)).body("one").send().await.unwrap().status(), StatusCode::OK);

    // GET ?versionId=invalid -> 404 NoSuchVersion (or 400 InvalidArgument)
    let res = c.get(format!("{}/v-edge-bkt/k?versionId=invalid", url)).send().await.unwrap();
    assert!(
        res.status() == StatusCode::NOT_FOUND || res.status() == StatusCode::BAD_REQUEST,
        "unexpected status {}",
        res.status()
    );
    let head_res = c.head(format!("{}/v-edge-bkt/k?versionId=invalid", url)).send().await.unwrap();
    assert_eq!(head_res.status(), res.status());
    assert!(head_res.bytes().await.unwrap().is_empty(), "HEAD error must have empty body");

    // DELETE ?versionId=<delete-marker> returns x-amz-delete-marker: true
    let del_res = c.delete(format!("{}/v-edge-bkt/k", url)).send().await.unwrap();
    assert_eq!(del_res.status(), StatusCode::NO_CONTENT);
    let dm_vid = del_res.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    let del_dm = c.delete(format!("{}/v-edge-bkt/k?versionId={}", url, dm_vid)).send().await.unwrap();
    assert_eq!(del_dm.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        del_dm.headers().get("x-amz-delete-marker").and_then(|v| v.to_str().ok()),
        Some("true")
    );
    assert_eq!(
        del_dm.headers().get("x-amz-version-id").and_then(|v| v.to_str().ok()),
        Some(dm_vid.as_str())
    );

    handle.shutdown();
}

#[tokio::test]
async fn list_object_versions_pagination_delimiter_and_encoding() {
    let (url, handle) = spawn().await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/list-ver-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let enable = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{}/list-ver-bkt?versioning", url)).body(enable).send().await.unwrap().status(), StatusCode::OK);

    // Create 3 versions of "a/one", 1 version of "a/two", 2 versions of "b space", 1 version of "root"
    let mut a_one_vids = Vec::new();
    for i in 0..3 {
        let r = c.put(format!("{}/list-ver-bkt/a/one", url)).body(format!("v{}", i)).send().await.unwrap();
        a_one_vids.push(r.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string());
    }
    c.put(format!("{}/list-ver-bkt/a/two", url)).body("x").send().await.unwrap();
    c.put(format!("{}/list-ver-bkt/b%20space", url)).body("s1").send().await.unwrap();
    c.put(format!("{}/list-ver-bkt/b%20space", url)).body("s2").send().await.unwrap();
    c.put(format!("{}/list-ver-bkt/root", url)).body("r").send().await.unwrap();

    // 1. Delimiter="/" collapses "a/one" and "a/two" into CommonPrefixes "a/"
    let res = c.get(format!("{}/list-ver-bkt?versions&delimiter=/", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Delimiter>/</Delimiter>"), "{}", xml);
    assert_eq!(xml_tags(&xml, "Prefix"), vec!["", "a/"], "{}", xml);
    assert_eq!(xml.matches("<Version>").count(), 3, "only b space (x2) and root (x1): {}", xml);

    // 2. Prefix="a/" lists only keys under "a/"
    let res = c.get(format!("{}/list-ver-bkt?versions&prefix=a/", url)).send().await.unwrap();
    let xml = res.text().await.unwrap();
    assert_eq!(xml.matches("<Version>").count(), 4, "{}", xml);

    // 3. Pagination with max-keys=2 across versions of the same key using key-marker + version-id-marker
    let p1 = c.get(format!("{}/list-ver-bkt?versions&max-keys=2", url)).send().await.unwrap().text().await.unwrap();
    assert!(p1.contains("<IsTruncated>true</IsTruncated>"), "{}", p1);
    assert_eq!(p1.matches("<Version>").count(), 2, "{}", p1);
    let nkm = xml_tags(&p1, "NextKeyMarker")[0].clone();
    let nvim = xml_tags(&p1, "NextVersionIdMarker")[0].clone();
    assert_eq!(nkm, "a/one");
    assert_eq!(nvim, a_one_vids[1]); // newest first: [2], [1], so next marker is [1]

    let p2 = c.get(format!("{}/list-ver-bkt?versions&max-keys=2&key-marker={}&version-id-marker={}", url, nkm, nvim))
        .send().await.unwrap().text().await.unwrap();
    assert!(p2.contains(&format!("<VersionIdMarker>{}</VersionIdMarker>", nvim)), "{}", p2);
    assert_eq!(p2.matches("<Version>").count(), 2, "a/one[0] and a/two: {}", p2);
    let keys_p2 = xml_tags(&p2, "Key");
    assert_eq!(keys_p2, vec!["a/one", "a/two"], "{}", p2);
    let latest_p2 = xml_tags(&p2, "IsLatest");
    assert_eq!(latest_p2, vec!["false", "true"], "a/one[0] is not latest, a/two is latest: {}", p2);

    // 4. max-keys=0 returns 0 entries and IsTruncated=false
    let p0 = c.get(format!("{}/list-ver-bkt?versions&max-keys=0", url)).send().await.unwrap().text().await.unwrap();
    assert!(p0.contains("<IsTruncated>false</IsTruncated>"), "{}", p0);
    assert_eq!(p0.matches("<Version>").count(), 0, "{}", p0);

    // 5. encoding-type=url encodes keys with spaces
    let pe = c.get(format!("{}/list-ver-bkt?versions&encoding-type=url", url)).send().await.unwrap().text().await.unwrap();
    assert!(pe.contains("<EncodingType>url</EncodingType>"), "{}", pe);
    assert!(pe.contains("<Key>b%20space</Key>"), "{}", pe);

    // 6. Invalid max-keys / encoding-type / orphan version-id-marker -> 400 InvalidArgument
    for bad_q in [
        "versions&max-keys=-1",
        "versions&encoding-type=bogus",
        "versions&version-id-marker=0000000000000001",
    ] {
        let r = c.get(format!("{}/list-ver-bkt?{}", url, bad_q)).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST, "q={}", bad_q);
        assert_eq!(xml_code(&r.text().await.unwrap()), "InvalidArgument", "q={}", bad_q);
    }

    handle.shutdown();
}
