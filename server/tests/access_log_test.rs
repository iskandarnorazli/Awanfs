//! #46: the S3 server access log, with a signed PUT, a signed GET and a denied request.

use reqwest::StatusCode;
use s3_server::request_log::{clf_time, LogSink};
use s3_server::{compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState, SigV4KeyCache};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const AK: &str = "AKAUDITTEST";
const SK: &str = "audit-secret";

fn amz_date(secs: u64) -> String {
    // clf_time gives dd/Mon/yyyy:HH:MM:SS; rebuild yyyymmddTHHMMSSZ from it.
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    format!("{}{:02}{}T{}{}{}Z", &t[7..11], m, &t[0..2], &t[12..14], &t[15..17], &t[18..20])
}

fn signed(c: &reqwest::Client, method: reqwest::Method, host: &str, path: &str, body: &[u8]) -> reqwest::RequestBuilder {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let date = amz_date(now);
    let hash = sha256_hex(body);
    let canonical = format!(
        "{}\n{}\n\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\n{}",
        method.as_str(), path, host, hash, date, hash
    );
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canonical.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), SK, &date[..8], "us-east-1", "s3", &sts);
    c.request(method, format!("http://{}{}", host, path))
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", hash)
        .header("authorization", format!("AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}", AK, scope, sig))
        .body(body.to_vec())
}

#[test]
fn clf_time_matches_known_dates() {
    assert_eq!(clf_time(0), "01/Jan/1970:00:00:00 +0000");
    assert_eq!(clf_time(1_794_441_600), "12/Nov/2026:00:00:00 +0000");
    assert_eq!(clf_time(951_782_400), "29/Feb/2000:00:00:00 +0000");
    assert_eq!(clf_time(4_102_444_799), "31/Dec/2099:23:59:59 +0000");
}

#[tokio::test]
async fn put_get_and_a_denied_request_write_three_access_log_lines() {
    let mut st = S3ServiceState::new();
    st.auth = AuthConfig { credentials: Some((AK.into(), SK.into())), allow_anonymous: false };
    let state = Arc::new(st);
    let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let lines = Arc::clone(&lines);
        let sink: LogSink = Arc::new(move |l: &str| lines.lock().unwrap().push(l.to_string()));
        *state.audit_log.write() = Some(sink);
    }
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    assert_eq!(signed(&c, reqwest::Method::PUT, &host, "/al-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    lines.lock().unwrap().clear();
    assert_eq!(signed(&c, reqwest::Method::PUT, &host, "/al-bkt/obj", b"logged").send().await.unwrap().status(), StatusCode::OK);
    let res = signed(&c, reqwest::Method::GET, &host, "/al-bkt/obj", b"").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), "logged");
    assert_eq!(c.get(format!("http://{}/al-bkt/obj", host)).send().await.unwrap().status(), StatusCode::FORBIDDEN);

    let got = lines.lock().unwrap().clone();
    assert_eq!(got.len(), 3, "{:#?}", got);
    let fields: Vec<Vec<&str>> = got.iter().map(|l| l.split(' ').collect()).collect();
    // bucket owner, bucket, [time (2 tokens)], remote IP, requester, request ID, operation, key
    for (f, (requester, op, status)) in fields.iter().zip([(AK, "REST.PUT.OBJECT", "200"), (AK, "REST.GET.OBJECT", "200"), ("-", "REST.GET.OBJECT", "403")]) {
        assert_eq!(f[1], "al-bkt");
        assert!(f[2].starts_with('[') && f[3].ends_with(']'));
        assert_eq!(f[4], "127.0.0.1");
        assert_eq!(f[5], requester, "{:?}", f);
        assert_eq!(f[7], op);
        assert_eq!(f[8], "obj");
        assert!(f.iter().any(|x| *x == status), "status {} in {:?}", status, f);
    }
    assert!(got[2].contains(" 403 AccessDenied "), "the denied line names its error: {}", got[2]);
    assert!(got[0].contains("SigV4 - AuthHeader"), "{}", got[0]);
}
