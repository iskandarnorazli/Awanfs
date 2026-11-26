#![no_main]
//! The attributes block and 0x200 attribute-update payload (#150, #203): never panics; a block
//! or update payload that decodes is written back byte for byte.
use libfuzzer_sys::fuzz_target;
use s3_server::attrs::{decode_attr_update, encode_attr_update, Attrs};

fuzz_target!(|data: &[u8]| {
    if let Ok(a) = Attrs::decode(data) {
        let again = a.encode().expect("a decoded block fits");
        assert_eq!(&again[..], data, "decode then encode is the identity");
        assert_eq!(Attrs::decode(&again).unwrap(), a);
    }
    if let Ok((vid, upd)) = decode_attr_update(data) {
        let again = encode_attr_update(vid, &upd).expect("a decoded update fits");
        assert_eq!(&again[..], data, "update decode then encode is the identity");
        assert_eq!(decode_attr_update(&again).unwrap(), (vid, upd));
    }
    // Any set of attributes that encodes (as block or 0x200 update) decodes to itself.
    let mut built = Attrs::new();
    for (i, piece) in data.chunks(7).enumerate().take(64) {
        built.set(i as u16 * 3, piece);
    }
    if let Ok(block) = built.encode() {
        assert_eq!(Attrs::decode(&block).unwrap(), built);
    }
    let vid = if data.len() >= 8 {
        u64::from_le_bytes(data[..8].try_into().unwrap())
    } else {
        0
    };
    if let Ok(payload) = encode_attr_update(vid, &built) {
        assert_eq!(decode_attr_update(&payload).unwrap(), (vid, built));
    }
});
