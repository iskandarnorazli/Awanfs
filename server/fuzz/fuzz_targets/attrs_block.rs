#![no_main]
//! The attributes block of a needle (#150): never panics; a block that decodes is written
//! back byte for byte, and the decoded value holds no more bytes than the block.
use libfuzzer_sys::fuzz_target;
use s3_server::attrs::Attrs;

fuzz_target!(|data: &[u8]| {
    if let Ok(a) = Attrs::decode(data) {
        let again = a.encode().expect("a decoded block fits");
        assert_eq!(&again[..], data, "decode then encode is the identity");
        assert_eq!(Attrs::decode(&again).unwrap(), a);
    }
    // Any set of attributes that encodes decodes to itself.
    let mut built = Attrs::new();
    for (i, piece) in data.chunks(7).enumerate().take(64) {
        built.set(i as u16 * 3, piece);
    }
    if let Ok(block) = built.encode() {
        assert_eq!(Attrs::decode(&block).unwrap(), built);
    }
});
