#![no_main]
//! Request classification (#148, #448): total across methods, buckets, keys,
//! sub-resources, and headers; query-order independent; round-trips every
//! canonical `Operation` in `OPS`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    awanfs_fuzz::check_classify(data);
});
