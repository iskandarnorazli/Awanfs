#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    awan_fuzz::check_object_attributes_header(data);
});
