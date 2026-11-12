#![no_main]
//! Range header: never panics; a satisfiable answer lies inside the object.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }
    let size = u64::from_le_bytes(data[..8].try_into().unwrap()) % (1 << 40);
    let header = String::from_utf8_lossy(&data[8..]);
    if let Ok(Some(r)) = s3_server::parse_range_header(&header, size) {
        assert!(r.start <= r.end && r.end < size, "{:?} for size {}", r, size);
    }
});
