//! Listing: delimiter and common prefixes, paging (V2 tokens, V1 markers) that visits every key
//! once, encoding-type=url, and keys that live in the spill index.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

fn tags<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{}>", tag), format!("</{}>", tag));
    xml.split(&open).skip(1).map(|p| p.split(&close).next().unwrap()).collect()
}

async fn server(state: S3ServiceState) -> (String, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(state)).await.unwrap();
    (format!("http://{}", handle.local_addr), handle)
}

#[tokio::test]
async fn delimiter_paging_and_url_encoding() {
    let (url, _h) = server(S3ServiceState::new()).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/ls-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let mut all = BTreeSet::new();
    for d in ["a", "b", "c"] {
        for i in 0..5 {
            let k = format!("{}/f{}", d, i);
            c.put(format!("{}/ls-bkt/{}", url, k)).body("x").send().await.unwrap();
            all.insert(k);
        }
    }
    for k in ["top1", "top2", "sp ace+plus"] {
        c.put(format!("{}/ls-bkt/{}", url, k.replace(' ', "%20").replace('+', "%2B"))).body("x").send().await.unwrap();
        all.insert(k.to_string());
    }

    // Delimiter: three common prefixes and the three top-level keys.
    let xml = c.get(format!("{}/ls-bkt?list-type=2&delimiter=/", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Prefix").into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>(), vec!["a/", "b/", "c/"], "{}", xml);
    assert_eq!(tags(&xml, "Key"), vec!["sp ace+plus", "top1", "top2"]);
    // Prefix and delimiter together.
    let xml = c.get(format!("{}/ls-bkt?list-type=2&prefix=b/&delimiter=/", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Key").len(), 5);

    // V2 paging, 4 per page: every key once, in order, and the last page is not truncated.
    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    for _ in 0..10 {
        let mut u = format!("{}/ls-bkt?list-type=2&max-keys=4", url);
        if let Some(t) = &token {
            u.push_str(&format!("&continuation-token={}", t));
        }
        let xml = c.get(u).send().await.unwrap().text().await.unwrap();
        seen.extend(tags(&xml, "Key").into_iter().map(String::from));
        match tags(&xml, "NextContinuationToken").first() {
            Some(t) => {
                assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
                token = Some(t.to_string());
            }
            None => {
                assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
                break;
            }
        }
    }
    assert_eq!(seen, all.iter().cloned().collect::<Vec<_>>(), "V2 pages");

    // V1 paging with marker and delimiter: NextMarker leads on.
    let mut seen = Vec::new();
    let mut marker = String::new();
    for _ in 0..10 {
        let xml = c.get(format!("{}/ls-bkt?delimiter=/&max-keys=2&marker={}", url, marker)).send().await.unwrap().text().await.unwrap();
        seen.extend(tags(&xml, "Key").into_iter().map(String::from));
        seen.extend(tags(&xml, "Prefix").into_iter().filter(|p| !p.is_empty()).map(String::from));
        match tags(&xml, "NextMarker").first() {
            Some(m) => marker = m.replace('/', "%2F").replace(' ', "%20").replace('+', "%2B"),
            None => break,
        }
    }
    seen.sort();
    assert_eq!(seen, vec!["a/", "b/", "c/", "sp ace+plus", "top1", "top2"], "V1 pages");

    // encoding-type=url: keys come back percent-encoded.
    let xml = c.get(format!("{}/ls-bkt?list-type=2&prefix=sp&encoding-type=url", url)).send().await.unwrap().text().await.unwrap();
    assert!(xml.contains("<EncodingType>url</EncodingType>"));
    assert_eq!(tags(&xml, "Key"), vec!["sp%20ace%2Bplus"]);
}

#[tokio::test]
async fn spilled_keys_are_listed_with_their_own_names() {
    let dir = std::env::temp_dir().join(format!("awanfs_ls_spill_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let state = S3ServiceState::with_data_dir(&dir);
    // A 4-row in-memory table: most keys go to the redb spill index.
    {
        let mut engine = state.engine.write();
        *engine = s3_server::AwanStorageEngine::new(4, 65536, 65536);
        engine.attach_spill_index(s3_server::spill::SpillIndex::open(&dir.join("small.db")).unwrap());
    }
    let (url, _h) = server(state).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/sp-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let keys: Vec<String> = (0..20).map(|i| format!("k{:02}", i)).collect();
    for k in &keys {
        assert_eq!(c.put(format!("{}/sp-bkt/{}", url, k)).body("x").send().await.unwrap().status(), StatusCode::OK);
    }
    let xml = c.get(format!("{}/sp-bkt?list-type=2", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(tags(&xml, "Key"), keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(), "{}", xml);
}
