#![no_main]
//! aws-chunked decoder: never panics; what it decodes is never longer than its input.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(out) = s3_server::decode_aws_chunked(data) {
        assert!(out.len() <= data.len());
    }
});
