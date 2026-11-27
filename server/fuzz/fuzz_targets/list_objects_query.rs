#![no_main]
//! ListObjects and ListObjectsV2 query parameter parsing (#114, #444):
//! checks encoding-type, max-keys, continuation-token validation and round-trip.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    awanfs_fuzz::check_list_objects_query(data);
});
