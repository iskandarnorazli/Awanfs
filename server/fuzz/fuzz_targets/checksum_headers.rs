#![no_main]
//! Checksum and Content-MD5 base64 decoders (`x-amz-checksum-crc64nvme`,
//! `x-amz-checksum-sha256`, `checksum::base64_decode`, `bucket_config::base64_decode`):
//! never panic, and decoding valid base64 and re-encoding it returns the input.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    awanfs_fuzz::check_checksum_headers(data);
});
