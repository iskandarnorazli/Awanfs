#![no_main]
//! Checksum header decoders (`x-amz-checksum-crc64nvme`, `x-amz-checksum-sha256`,
//! `base64_decode`): never panic, and decoding a valid base64 checksum and
//! re-encoding it returns the input.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    awanfs_fuzz::check_checksum_headers(data);
});
