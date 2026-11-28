#![no_main]
//! ACL, OwnershipControls, and PublicAccessBlock XML parsers: never panic and canonical XML round-trips.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    if let Ok(acp) = s3_server::acl::parse_acl_xml_structural(&text) {
        let xml = s3_server::acl::render_acl_xml(&acp);
        // Rendering lists group grants before user grants, so the grant order of `acp` may
        // change once; after that, parsing and rendering must reach a fixed point.
        let again = s3_server::acl::parse_acl_xml_structural(&xml).expect("canonical ACL XML reparses");
        assert_eq!(again.owner, acp.owner);
        let mut a = acp.grants.clone();
        let mut b = again.grants.clone();
        a.sort_by_key(|g| format!("{:?}", g));
        b.sort_by_key(|g| format!("{:?}", g));
        assert_eq!(a, b, "same grants");
        assert_eq!(s3_server::acl::render_acl_xml(&again), xml, "render is a fixed point");
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
