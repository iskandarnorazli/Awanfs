#![no_main]
//! Raw S3 object key extraction and byte-level percent decoding (`raw_s3_key`,
//! `percent_decode_path_bytes`): never panics on invalid `%` or non-UTF8, and
//! round-trips percent-encoded bytes.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    awanfs_fuzz::check_raw_s3_key(data);
});
