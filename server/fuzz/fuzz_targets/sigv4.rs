#![no_main]
//! SigV4 header and presigned parsing: never panics, never accepts a request whose
//! signature was not made with the secret.
use hyper::header::{HeaderMap, HeaderValue};
use libfuzzer_sys::fuzz_target;
use s3_server::{authenticate, AuthConfig, SigV4KeyCache};

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let mut parts = text.splitn(4, '\n');
    let auth = parts.next().unwrap_or("");
    let date = parts.next().unwrap_or("");
    let query = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");
    let mut h = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(auth) {
        h.insert("authorization", v);
    }
    if let Ok(v) = HeaderValue::from_str(date) {
        h.insert("x-amz-date", v);
    }
    h.insert("host", HeaderValue::from_static("localhost"));
    let cfg = AuthConfig { credentials: Some(("AKIAFUZZ".into(), "fuzz-secret".into())), allow_anonymous: false };
    let r = authenticate(&cfg, &SigV4KeyCache::new(), "GET", path, query, &h, 1_794_441_600);
    // A random input that carries a signature cannot be valid (2^-256 per try).
    if auth.contains("Signature=") || query.contains("X-Amz-Signature") {
        assert!(r.is_err(), "accepted a made-up signature: {:?} {:?}", auth, query);
    }
});
