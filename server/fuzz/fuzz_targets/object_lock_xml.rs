#![no_main]
//! Object Lock XML: never panics; any parsed document serialises and parses back to the same value.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    if let Ok(cfg) = s3_server::object_lock::parse_lock_configuration(&text) {
        let xml = s3_server::object_lock::lock_configuration_xml(&cfg);
        let again = s3_server::object_lock::parse_lock_configuration(&xml)
            .expect("serialized lock configuration parses");
        assert_eq!(cfg, again);
    }
    if let Ok(ret) = s3_server::object_lock::parse_retention(&text) {
        let xml = s3_server::object_lock::retention_xml(&ret);
        let again = s3_server::object_lock::parse_retention(&xml)
            .expect("serialized retention parses");
        assert_eq!(ret, again);
    }
    if let Ok(hold) = s3_server::object_lock::parse_legal_hold(&text) {
        let xml = s3_server::object_lock::legal_hold_xml(hold);
        let again = s3_server::object_lock::parse_legal_hold(&xml)
            .expect("serialized legal hold parses");
        assert_eq!(hold, again);
    }
});
