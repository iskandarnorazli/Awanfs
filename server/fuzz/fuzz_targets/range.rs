#![no_main]
//! Range header & large-object segment bounds (#155): never panics; a satisfiable answer
//! lies inside the object and produces bounded segment slices above 4 MiB.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 9 {
        return;
    }
    let raw_size = u64::from_le_bytes(data[..8].try_into().unwrap());
    // Exercise both general sizes and sizes forced above the 4 MiB streaming threshold.
    let size = if data[8] & 1 == 0 {
        raw_size % (1 << 40)
    } else {
        s3_server::VERIFY_WHILE_SENDING_MIN + 1 + (raw_size % (1 << 34))
    };
    let header = String::from_utf8_lossy(&data[9..]);
    if let Ok(Some(r)) = s3_server::parse_range_header(&header, size) {
        assert!(r.start <= r.end && r.end < size, "{:?} for size {}", r, size);
        let len = r.end - r.start + 1;
        assert!(len >= 1 && r.start + len <= size);
    }
});
