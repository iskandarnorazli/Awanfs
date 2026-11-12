#![no_main]
//! Lifecycle XML: never panics; a parsed configuration serialises and parses back to the
//! same rules.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    if let Ok(rules) = s3_server::parse_lifecycle_xml(&text) {
        let xml = s3_server::lifecycle_config_xml(&rules);
        let again = s3_server::parse_lifecycle_xml(&xml).expect("our own output parses");
        assert_eq!(format!("{:?}", rules), format!("{:?}", again));
    }
});
