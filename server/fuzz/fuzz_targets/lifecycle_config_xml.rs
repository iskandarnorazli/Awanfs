#![no_main]
//! Full lifecycle configuration XML: never panics; a parsed configuration serialises and parses
//! back to the exact same `LifecycleConfig`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    if let Ok(cfg) = s3_server::lifecycle_config::parse_lifecycle_config_xml(&text) {
        let xml = s3_server::lifecycle_config::lifecycle_config_xml(&cfg);
        let again = s3_server::lifecycle_config::parse_lifecycle_config_xml(&xml)
            .expect("our own output parses");
        assert_eq!(cfg, again);
    }
});
