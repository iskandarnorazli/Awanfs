#![no_main]
//! Attribute value decoders (#221): never panic, never over-allocate, and round-trip.
use libfuzzer_sys::fuzz_target;
use s3_server::attrs::{decode_tags, encode_tags, Attrs, TAG_CHECKSUM, TAG_PART_SIZES, TAG_USER_META};

fuzz_target!(|data: &[u8]| {
    let Some((&selector, rest)) = data.split_first() else {
        return;
    };
    match selector % 4 {
        0 => {
            if let Ok(tags) = decode_tags(rest) {
                let reencoded = encode_tags(&tags);
                assert_eq!(decode_tags(&reencoded).unwrap(), tags);
            }
        }
        1 => {
            let mut a = Attrs::new();
            a.set(TAG_PART_SIZES, rest);
            if let Some(sizes) = a.part_sizes() {
                let mut b = Attrs::new();
                b.set_part_sizes(&sizes);
                assert_eq!(b.part_sizes(), Some(sizes));
            }
        }
        2 => {
            let mut a = Attrs::new();
            a.set(TAG_USER_META, rest);
            let meta = a.user_meta();
            let mut b = Attrs::new();
            b.set_user_meta(meta.iter().copied());
            assert_eq!(b.user_meta(), meta);
        }
        3 => {
            let mut a = Attrs::new();
            a.set(TAG_CHECKSUM, rest);
            if let Some((alg, cksum_type, val)) = a.checksum() {
                let mut b = Attrs::new();
                b.set_checksum(alg, cksum_type, val);
                assert_eq!(b.checksum(), Some((alg, cksum_type, val)));
            }
        }
        _ => unreachable!(),
    }
});
