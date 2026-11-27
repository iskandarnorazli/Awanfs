#![no_main]
//! ACL, OwnershipControls, and PublicAccessBlock XML parsers: never panic and canonical XML round-trips.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    if let Ok(acp) = s3_server::acl::parse_acl_xml_structural(&text) {
        let xml = s3_server::acl::render_acl_xml(&acp);
        let again = s3_server::acl::parse_acl_xml_structural(&xml).expect("canonical ACL XML reparses");
        assert_eq!(acp, again);
    }
    if let Ok(own) = s3_server::acl::parse_ownership_xml(&text) {
        let xml = s3_server::acl::render_ownership_xml(own);
        assert_eq!(s3_server::acl::parse_ownership_xml(&xml), Ok(own));
    }
    if let Ok(pab) = s3_server::acl::parse_pab_xml(&text) {
        let xml = s3_server::acl::render_pab_xml(pab);
        assert_eq!(s3_server::acl::parse_pab_xml(&xml), Ok(pab));
    }
});
