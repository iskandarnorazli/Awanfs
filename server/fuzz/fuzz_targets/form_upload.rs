#![no_main]
//! Multipart form upload parser (#193): never panics, and buffered_bytes stays within the
//! configured bound across arbitrary chunk splits.
use libfuzzer_sys::fuzz_target;
use s3_server::form_upload::{extract_boundary, FormLimits, FormParser};

fuzz_target!(|data: &[u8]| {
    // Also exercise extract_boundary on arbitrary UTF-8 lossy input.
    let ct = String::from_utf8_lossy(data);
    let _ = extract_boundary(&ct);

    // Use first byte to pick chunk size (1..=64) and feed the rest with a fixed boundary.
    let Some((&first, rest)) = data.split_first() else {
        return;
    };
    let chunk_size = (first as usize % 64) + 1;
    let boundary = "fuzzboundary";
    let limits = FormLimits::default();
    let bound = limits
        .max_field_bytes
        .max(limits.max_header_bytes)
        .max(boundary.len() + 4)
        + boundary.len()
        + 4;

    let mut parser = FormParser::new(boundary, limits);
    for chunk in rest.chunks(chunk_size) {
        if parser.push(chunk).is_err() {
            assert!(parser.buffered_bytes() <= bound);
            return;
        }
        assert!(parser.buffered_bytes() <= bound);
    }
    let _ = parser.finish();
    assert!(parser.buffered_bytes() <= bound);
});
