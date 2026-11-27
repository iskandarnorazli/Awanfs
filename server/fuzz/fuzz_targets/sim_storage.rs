#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Err(err) = s3_server::sim_harness::run_fuzz_input(data) {
        panic!("sim_storage invariant failure: {}", err);
    }
});
