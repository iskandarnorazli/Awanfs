#![no_main]
//! ListBuckets query parameter parsing (`max-buckets`, `continuation-token`):
//! never panics, rejects `max-buckets` outside `1..=10000` without clamping,
//! and deterministically parses valid pagination tokens.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    awanfs_fuzz::check_list_buckets_query(data);
});
