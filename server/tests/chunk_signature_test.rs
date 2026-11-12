//! Owner decision 14: every chunk signature of a signed aws-chunked body is checked, chained
//! from the seed signature. And every aws-chunked form is decoded, not only the signed one.

use hyper::header::{HeaderMap, HeaderValue};
use reqwest::StatusCode;
use s3_server::request_log::clf_time;
use s3_server::{compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, ChunkVerifier, S3ServiceState, SigV4KeyCache};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const DOC_AK: &str = "AKIAIOSFODNN7EXAMPLE";
const DOC_SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

/// The AWS documentation example "Transferring payload in multiple chunks": 66560 bytes of
/// 'a' in chunks of 65536 and 1024, then the final chunk. Values recomputed with Python's hmac.
#[test]
fn aws_documented_chunk_signatures_are_accepted_in_order_only() {
    let mut h = HeaderMap::new();
    h.insert("x-amz-content-sha256", HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"));
    h.insert("x-amz-date", HeaderValue::from_static("20130524T000000Z"));
    h.insert("authorization", HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"));
    let cfg = AuthConfig { credentials: Some((DOC_AK.into(), DOC_SK.into())), allow_anonymous: false };
    let cache = SigV4KeyCache::new();
    let c1 = vec![b'a'; 65536];
    let c2 = vec![b'a'; 1024];
    let s1 = "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648";
    let s2 = "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497";
    let s3 = "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9";

    let mut v = ChunkVerifier::for_request(&cfg, &h).expect("a signed streaming request");
    assert!(v.check(&cache, &c1, Some(s1)));
    assert!(v.check(&cache, &c2, Some(s2)));
    assert!(v.check(&cache, &[], Some(s3)));

    let mut v = ChunkVerifier::for_request(&cfg, &h).unwrap();
    assert!(!v.check(&cache, &c2, Some(s2)), "the second chunk's signature does not fit first");
    let mut v = ChunkVerifier::for_request(&cfg, &h).unwrap();
    let mut bad = c1.clone();
    bad[100] = b'b';
    assert!(!v.check(&cache, &bad, Some(s1)), "one changed byte");
    let mut v = ChunkVerifier::for_request(&cfg, &h).unwrap();
    assert!(!v.check(&cache, &c1, None), "a missing chunk signature");
}

const AK: &str = "AKCHUNKTEST";
const SK: &str = "chunk-secret";

fn amz_date(secs: u64) -> String {
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    format!("{}{:02}{}T{}{}{}Z", &t[7..11], m, &t[0..2], &t[12..14], &t[15..17], &t[18..20])
}

/// A signed aws-chunked PUT of `chunks`; `tamper` changes one byte of chunk 0 after signing.
fn chunked_put(c: &reqwest::Client, host: &str, path: &str, chunks: &[Vec<u8>], tamper: bool) -> reqwest::RequestBuilder {
    let date = amz_date(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    let decoded: usize = chunks.iter().map(|c| c.len()).sum();
    let canonical = format!(
        "PUT\n{}\n\nhost:{}\nx-amz-content-sha256:STREAMING-AWS4-HMAC-SHA256-PAYLOAD\nx-amz-date:{}\nx-amz-decoded-content-length:{}\n\nhost;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length\nSTREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        path, host, date, decoded
    );
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let cache = SigV4KeyCache::new();
    let sign = |sts: &str| compute_sigv4_signature(&cache, SK, &date[..8], "us-east-1", "s3", sts);
    let seed = sign(&format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canonical.as_bytes())));
    let mut prev = seed.clone();
    let mut body = Vec::new();
    let empty = sha256_hex(b"");
    for (i, chunk) in chunks.iter().chain(std::iter::once(&Vec::new())).enumerate() {
        let sig = sign(&format!("AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}", date, scope, prev, empty, sha256_hex(chunk)));
        body.extend_from_slice(format!("{:x};chunk-signature={}\r\n", chunk.len(), sig).as_bytes());
        let mut data = chunk.clone();
        if tamper && i == 0 {
            data[0] ^= 1;
        }
        body.extend_from_slice(&data);
        body.extend_from_slice(b"\r\n");
        prev = sig;
    }
    c.put(format!("http://{}{}", host, path))
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
        .header("x-amz-decoded-content-length", decoded.to_string())
        .header("content-encoding", "aws-chunked")
        .header("authorization", format!("AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature={}", AK, scope, seed))
        .body(body)
}

#[tokio::test]
async fn a_tampered_chunk_is_refused_and_nothing_is_stored() {
    let mut st = S3ServiceState::new();
    // Anonymous reads let the test check what was stored without signing the GET.
    st.auth = AuthConfig { credentials: Some((AK.into(), SK.into())), allow_anonymous: true };
    let state = Arc::new(st);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, state).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("http://{}/cs-bkt", host)).send().await.unwrap().status(), StatusCode::OK);
    let chunks = vec![vec![b'x'; 8192], vec![b'y'; 100]];

    let res = chunked_put(&c, &host, "/cs-bkt/good", &chunks, false).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "{}", res.text().await.unwrap());
    let got = c.get(format!("http://{}/cs-bkt/good", host)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), 8292);
    assert!(got[..8192].iter().all(|&b| b == b'x') && got[8192..].iter().all(|&b| b == b'y'));

    let res = chunked_put(&c, &host, "/cs-bkt/bad", &chunks, true).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(res.text().await.unwrap().contains("SignatureDoesNotMatch"));
    assert_eq!(c.get(format!("http://{}/cs-bkt/bad", host)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_unsigned_trailer_body_is_decoded_not_stored_with_its_framing() {
    let state = Arc::new(S3ServiceState::new());
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, state).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/tr-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let body = "5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
    let res = c
        .put(format!("{}/tr-bkt/k", url))
        .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
        .header("content-encoding", "aws-chunked")
        .header("x-amz-decoded-content-length", "11")
        .header("x-amz-trailer", "x-amz-checksum-crc32")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(c.get(format!("{}/tr-bkt/k", url)).send().await.unwrap().text().await.unwrap(), "hello world");
}

/// Above 4 MiB the chunked body is decoded and written as it arrives (#36); the signature
/// chain is checked the same way, and a tampered chunk stores nothing.
#[tokio::test]
async fn a_large_signed_chunked_put_is_streamed_and_still_checked() {
    let mut st = S3ServiceState::new();
    st.auth = AuthConfig { credentials: Some((AK.into(), SK.into())), allow_anonymous: true };
    let state = Arc::new(st);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("http://{}/big-bkt", host)).send().await.unwrap().status(), StatusCode::OK);
    let chunks: Vec<Vec<u8>> = (0..3).map(|i| (0..(2 << 20)).map(|j| ((j * 7 + i * 13) % 251) as u8).collect()).collect();
    let want: Vec<u8> = chunks.concat();

    let before = state.work.snapshot();
    let res = chunked_put(&c, &host, "/big-bkt/good", &chunks, false).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "{}", res.text().await.unwrap());
    let d = state.work.snapshot().since(&before);
    assert!(d.disk_writes > 2, "written in pieces as it arrived: {:?}", d);
    assert_eq!((d.payload_copies, d.payload_copy_bytes), (1, want.len() as u64), "one copy: no collected body, no decode buffer");
    let got = c.get(format!("http://{}/big-bkt/good", host)).send().await.unwrap().bytes().await.unwrap();
    assert!(&got[..] == &want[..]);

    let res = chunked_put(&c, &host, "/big-bkt/bad", &chunks, true).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert_eq!(c.get(format!("http://{}/big-bkt/bad", host)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
}
