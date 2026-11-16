#![no_main]
//! S3 target (path-style and virtual-host-style): never panics, never yields an empty
//! or slash-containing bucket, and round-trips valid bucket/key pairs in both styles.
use libfuzzer_sys::fuzz_target;
use s3_server::{parse_s3_target, ParsedTarget};

fn is_valid_s3_bucket(b: &str) -> bool {
    let bytes = b.as_bytes();
    if bytes.len() < 3 || bytes.len() > 63 {
        return false;
    }
    let is_alnum = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    if !is_alnum(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    bytes.iter().all(|&c| is_alnum(c) || c == b'-')
}

fn percent_encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for &b in key.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let mut parts = text.splitn(2, '\n');
    let uri = parts.next().unwrap_or("");
    let host = parts.next();

    match parse_s3_target(uri, host) {
        ParsedTarget::Service => {}
        ParsedTarget::Bucket { bucket } => {
            assert!(!bucket.is_empty(), "empty bucket for {:?} {:?}", uri, host);
            assert!(!bucket.contains('/'), "slash in bucket {:?} for {:?} {:?}", bucket, uri, host);
        }
        ParsedTarget::Object { bucket, .. } => {
            assert!(!bucket.is_empty(), "empty bucket for {:?} {:?}", uri, host);
            assert!(!bucket.contains('/'), "slash in bucket {:?} for {:?} {:?}", bucket, uri, host);
        }
    }

    if data.len() < 4 {
        return;
    }
    let blen = 3 + (data[0] as usize % 61);
    if data.len() <= 1 + blen {
        return;
    }
    let mut bucket = String::with_capacity(blen);
    for (i, &b) in data[1..1 + blen].iter().enumerate() {
        let alphabet: &[u8] = if i == 0 || i + 1 == blen {
            b"abcdefghijklmnopqrstuvwxyz0123456789"
        } else {
            b"abcdefghijklmnopqrstuvwxyz0123456789-"
        };
        bucket.push(alphabet[b as usize % alphabet.len()] as char);
    }
    assert!(is_valid_s3_bucket(&bucket));

    let key = String::from_utf8_lossy(&data[1 + blen..]).into_owned();
    let encoded_key = percent_encode_key(&key);

    // Path-style round-trip
    assert_eq!(
        parse_s3_target(&format!("/{}", bucket), None),
        ParsedTarget::Bucket { bucket: bucket.clone() }
    );
    assert_eq!(
        parse_s3_target(&format!("/{}/{}", bucket, encoded_key), None),
        ParsedTarget::Object { bucket: bucket.clone(), key: key.clone() }
    );

    // Virtual-host-style round-trip
    let vhost = format!("{}.s3.amazonaws.com", bucket);
    assert_eq!(
        parse_s3_target("/", Some(&vhost)),
        ParsedTarget::Bucket { bucket: bucket.clone() }
    );
    if !key.is_empty() {
        assert_eq!(
            parse_s3_target(&format!("/{}", encoded_key), Some(&vhost)),
            ParsedTarget::Object { bucket, key }
        );
    }
});
