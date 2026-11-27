#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    awanfs_fuzz::check_meta_segment(data);
});
