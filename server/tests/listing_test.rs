//! Listing edge cases conformance tests (#114).

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

fn tags<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{}>", tag), format!("</{}>", tag));
    xml.split(&open).skip(1).map(|p| p.split(&close).next().unwrap()).collect()
}

fn common_prefixes<'a>(xml: &'a str) -> Vec<&'a str> {
    xml.split("<CommonPrefixes>")
        .skip(1)
        .filter_map(|blk| blk.split("</CommonPrefixes>").next())
        .flat_map(|blk| tags(blk, "Prefix"))
        .collect()
}

async fn server() -> (String, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::new())).await.unwrap();
    (format!("http://{}", handle.local_addr), handle)
}

async fn setup_bucket(c: &reqwest::Client, url: &str, bucket: &str, keys: &[&str]) {
    assert_eq!(c.put(format!("{}/{}", url, bucket)).send().await.unwrap().status(), StatusCode::OK);
    for k in keys {
        let encoded = k
            .replace('%', "%25")
            .replace(' ', "%20")
            .replace('+', "%2B")
            .replace('#', "%23")
            .replace('&', "%26")
            .replace('<', "%3C")
            .replace('>', "%3E");
        assert_eq!(
            c.put(format!("{}/{}/{}", url, bucket, encoded)).body("x").send().await.unwrap().status(),
            StatusCode::OK
        );
    }
}

#[tokio::test]
async fn max_keys_zero_v1_and_v2() {
    let (url, _h) = server().await;
    let c = reqwest::Client::new();
    setup_bucket(&c, &url, "max0-bkt", &["bar", "baz", "foo", "quxx"]).await;

    // V1 max-keys=0
    let resp = c.get(format!("{}/max0-bkt?max-keys=0", url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let xml = resp.text().await.unwrap();
    assert_eq!(tags(&xml, "MaxKeys"), vec!["0"]);
    assert_eq!(tags(&xml, "IsTruncated"), vec!["false"], "V1 max-keys=0 must not be truncated:\n{}", xml);
    assert!(tags(&xml, "Key").is_empty());
    assert!(common_prefixes(&xml).is_empty());
    assert!(tags(&xml, "NextMarker").is_empty());

    // V2 max-keys=0
    let resp = c.get(format!("{}/max0-bkt?list-type=2&max-keys=0", url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let xml = resp.text().await.unwrap();
    assert_eq!(tags(&xml, "MaxKeys"), vec!["0"]);
    assert_eq!(tags(&xml, "KeyCount"), vec!["0"]);
    assert_eq!(tags(&xml, "IsTruncated"), vec!["false"], "V2 max-keys=0 must not be truncated:\n{}", xml);
    assert!(tags(&xml, "Key").is_empty());
    assert!(common_prefixes(&xml).is_empty());
    assert!(tags(&xml, "NextContinuationToken").is_empty());
}

#[tokio::test]
async fn max_keys_invalid_and_negative_returns_400() {
    let (url, _h) = server().await;
    let c = reqwest::Client::new();
    setup_bucket(&c, &url, "maxinv-bkt", &["bar", "baz"]).await;

    for query in [
        "max-keys=blah",
        "max-keys=-1",
        "max-keys=",
        "list-type=2&max-keys=blah",
        "list-type=2&max-keys=-5",
    ] {
        let resp = c.get(format!("{}/maxinv-bkt?{}", url, query)).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "query {} should return 400", query);
        let xml = resp.text().await.unwrap();
        assert_eq!(tags(&xml, "Code"), vec!["InvalidArgument"], "query {} body:\n{}", query, xml);
    }
}

#[tokio::test]
async fn v2_fetch_owner_and_v1_owner() {
    let (url, _h) = server().await;
    let c = reqwest::Client::new();
    setup_bucket(&c, &url, "owner-bkt", &["foo/bar", "foo/baz", "quux"]).await;

    // V2 fetch-owner=true includes Owner
    let xml = c.get(format!("{}/owner-bkt?list-type=2&fetch-owner=true", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Owner").len(), 3, "V2 fetch-owner=true should include Owner:\n{}", xml);

    // V2 default and fetch-owner=false omit Owner
    let xml = c.get(format!("{}/owner-bkt?list-type=2", url)).send().await.unwrap().text().await.unwrap();
    assert!(tags(&xml, "Owner").is_empty(), "V2 default should omit Owner:\n{}", xml);

    let xml = c.get(format!("{}/owner-bkt?list-type=2&fetch-owner=false", url)).send().await.unwrap().text().await.unwrap();
    assert!(tags(&xml, "Owner").is_empty(), "V2 fetch-owner=false should omit Owner:\n{}", xml);

    // V1 always includes Owner
    let xml = c.get(format!("{}/owner-bkt", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Owner").len(), 3, "V1 should include Owner:\n{}", xml);
}

#[tokio::test]
async fn delimiter_not_skip_special_and_common_prefixes_dedup_sorted() {
    let (url, _h) = server().await;
    let c = reqwest::Client::new();
    setup_bucket(
        &c,
        &url,
        "delim-bkt",
        &["0/", "0/1000", "0/1001", "1999", "1999#", "1999+", "2000", "z/1", "a/1", "a/2", "m/1"],
    )
    .await;

    let xml = c.get(format!("{}/delim-bkt?delimiter=/", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Delimiter"), vec!["/"]);
    assert_eq!(tags(&xml, "Key"), vec!["1999", "1999#", "1999+", "2000"]);
    assert_eq!(common_prefixes(&xml), vec!["0/", "a/", "m/", "z/"]);

    // V2 KeyCount = contents.len() + common_prefixes.len()
    let xml = c.get(format!("{}/delim-bkt?list-type=2&delimiter=/", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "KeyCount"), vec!["8"]);
    assert_eq!(common_prefixes(&xml), vec!["0/", "a/", "m/", "z/"]);
}

#[tokio::test]
async fn encoding_type_url_and_xml_escaping() {
    let (url, _h) = server().await;
    let c = reqwest::Client::new();
    setup_bucket(&c, &url, "enc-bkt", &["a b+c", "d&e<f>g", "sp ace/item1", "sp ace/item2"]).await;

    // encoding-type=url on V2
    let xml = c
        .get(format!("{}/enc-bkt?list-type=2&encoding-type=url&delimiter=/&start-after=a%20", url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(tags(&xml, "EncodingType"), vec!["url"]);
    assert_eq!(tags(&xml, "StartAfter"), vec!["a%20"]);
    assert_eq!(tags(&xml, "Delimiter"), vec!["/"]);
    assert_eq!(tags(&xml, "Key"), vec!["a%20b%2Bc", "d%26e%3Cf%3Eg"]);
    assert_eq!(common_prefixes(&xml), vec!["sp%20ace/"]);

    // Without encoding-type=url, special XML chars are XML-escaped
    let xml = c.get(format!("{}/enc-bkt?prefix=d", url)).send().await.unwrap().text().await.unwrap();
    assert!(tags(&xml, "EncodingType").is_empty());
    assert_eq!(tags(&xml, "Key"), vec!["d&amp;e&lt;f&gt;g"]);

    // Invalid encoding-type returns 400 InvalidArgument
    let resp = c.get(format!("{}/enc-bkt?encoding-type=xml", url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let xml = resp.text().await.unwrap();
    assert_eq!(tags(&xml, "Code"), vec!["InvalidArgument"]);
}

#[tokio::test]
async fn prefix_basic_alt_empty() {
    let (url, _h) = server().await;
    let c = reqwest::Client::new();
    setup_bucket(&c, &url, "pfx-bkt", &["bar", "baz", "foo/bar", "foo/baz", "quux"]).await;

    // prefix=foo/
    let xml = c.get(format!("{}/pfx-bkt?prefix=foo/", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Prefix"), vec!["foo/"]);
    assert_eq!(tags(&xml, "Key"), vec!["foo/bar", "foo/baz"]);

    // prefix=ba
    let xml = c.get(format!("{}/pfx-bkt?list-type=2&prefix=ba", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Prefix"), vec!["ba"]);
    assert_eq!(tags(&xml, "Key"), vec!["bar", "baz"]);

    // prefix= (empty)
    let xml = c.get(format!("{}/pfx-bkt?list-type=2&prefix=", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Prefix"), vec![""]);
    assert_eq!(tags(&xml, "Key"), vec!["bar", "baz", "foo/bar", "foo/baz", "quux"]);
}
