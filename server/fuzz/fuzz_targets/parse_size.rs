#![no_main]
//! Memory size parser (`memory::parse_size`): never panics and round-trips for printed units.
use libfuzzer_sys::fuzz_target;
use s3_server::memory::parse_size;

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    if let Some(bytes) = parse_size(&s) {
        assert_eq!(parse_size(&bytes.to_string()), Some(bytes));
        assert_eq!(parse_size(&format!("{}B", bytes)), Some(bytes));
    }

    if data.len() >= 8 {
        let n = u64::from_le_bytes(data[..8].try_into().unwrap());
        assert_eq!(parse_size(&n.to_string()), Some(n));
        for (suffix, shift) in [
            ("K", 10u32),
            ("KiB", 10),
            ("M", 20),
            ("MiB", 20),
            ("G", 30),
            ("GiB", 30),
            ("T", 40),
            ("TiB", 40),
        ] {
            if let Some(expected) = n.checked_mul(1u64 << shift) {
                assert_eq!(parse_size(&format!("{}{}", n, suffix)), Some(expected));
            }
        }
    }
});
