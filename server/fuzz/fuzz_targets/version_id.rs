#![no_main]
//! Version ID parser (`versions::parse_id`): never panics and round-trips with `Version::id_string`.
use libfuzzer_sys::fuzz_target;
use s3_server::versions::{id_string, parse_id, Version};

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    if let Some(id) = parse_id(&s) {
        let formatted = id_string(id);
        assert_eq!(parse_id(&formatted), Some(id));
        assert!(formatted.eq_ignore_ascii_case(&s), "{} vs {:?}", formatted, s);
    }

    if data.len() >= 8 {
        let id = u64::from_le_bytes(data[..8].try_into().unwrap());
        let v = Version {
            ts: id,
            modified: id,
            null: id == 0,
            marker: false,
            vol_offset: 0,
            size: 0,
            etag_hi: 0,
            etag_lo: 0,
            part_count: 0,
            flags: 0,
        };
        let formatted = v.id_string();
        assert_eq!(formatted, id_string(id));
        assert_eq!(parse_id(&formatted), Some(id));
    }
});
