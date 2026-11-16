#![no_main]
//! Tagging XML: never panics; a parsed tag set serialises and parses back to the same tag set.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    for scope in [s3_server::tagging::Scope::Object, s3_server::tagging::Scope::Bucket] {
        if let Ok(set) = s3_server::tagging::parse_tagging_xml(&text, scope) {
            let xml = s3_server::tagging::tagging_xml(&set);
            let again = s3_server::tagging::parse_tagging_xml(&xml, scope)
                .expect("our own output parses");
            assert_eq!(set, again);
        }
    }
    let _ = s3_server::tagging::parse_tagging_header(&text);
});
