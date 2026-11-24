#![no_main]
//! Request XML parsers (#194, #195): never panic; a parsed DeleteObjects request serialises and
//! parses back to the same struct.
use libfuzzer_sys::fuzz_target;
use s3_server::request_xml::{
    parse_complete_multipart, parse_delete_objects, parse_versioning, DeleteRequest,
};

fn serialize_delete_objects(req: &DeleteRequest) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    if req.quiet {
        out.push_str("  <Quiet>true</Quiet>\n");
    }
    for obj in &req.objects {
        out.push_str("  <Object>\n");
        out.push_str(&format!("    <Key>{}</Key>\n", s3_server::xml_escape_exact(&obj.key)));
        if let Some(vid) = &obj.version_id {
            out.push_str(&format!("    <VersionId>{}</VersionId>\n", s3_server::xml_escape_exact(vid)));
        }
        if let Some(etag) = &obj.etag {
            out.push_str(&format!("    <ETag>{}</ETag>\n", s3_server::xml_escape_exact(etag)));
        }
        if let Some(lmt) = obj.last_modified_secs {
            out.push_str(&format!(
                "    <LastModifiedTime>{}</LastModifiedTime>\n",
                s3_server::precondition::format_http_date(lmt)
            ));
        }
        if let Some(sz) = obj.size {
            out.push_str(&format!("    <Size>{sz}</Size>\n"));
        }
        out.push_str("  </Object>\n");
    }
    out.push_str("</Delete>");
    out
}

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    if let Ok(req) = parse_delete_objects(&text) {
        let xml = serialize_delete_objects(&req);
        let again = parse_delete_objects(&xml).expect("our own DeleteObjects output parses");
        assert_eq!(req, again);
    }
    let _ = parse_versioning(&text);
    let _ = parse_complete_multipart(&text);
});
