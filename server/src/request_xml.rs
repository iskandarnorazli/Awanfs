//! S3 request XML parsers (#194, #195):
//! - `parse_delete_objects`: `POST /{bucket}?delete` (`<Delete>`)
//! - `parse_versioning`: `PUT /{bucket}?versioning` (`<VersioningConfiguration>`)
//! - `parse_complete_multipart`: `POST /{bucket}/{key}?uploadId=...` (`<CompleteMultipartUpload>`)

use crate::fsm::{Refusal, INVALID_ARGUMENT, MALFORMED_XML};
use crate::versions::Versioning;
use quick_xml::events::Event;
use quick_xml::Reader;

const MAX_DELETE_OBJECTS: usize = 1000;
const MAX_COMPLETE_PARTS: usize = 10_000;

const fn refuse(code: &'static str, status: u16) -> Refusal {
    Refusal { code, status, why: code }
}

const ILLEGAL_VERSIONING: Refusal = refuse("IllegalVersioningConfigurationException", 400);
const NOT_IMPLEMENTED: Refusal = refuse("NotImplemented", 501);
const INVALID_PART_ORDER: Refusal = refuse("InvalidPartOrder", 400);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteObject {
    pub key: String,
    pub version_id: Option<String>,
    pub etag: Option<String>,
    pub last_modified_secs: Option<u64>,
    pub size: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct DeleteRequest {
    pub quiet: bool,
    pub objects: Vec<DeleteObject>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersioningRequest {
    pub status: Versioning,
    pub mfa_delete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedPart {
    pub part_number: u32,
    pub etag: String,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_crc64nvme: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
}

fn unescape_text(t: &quick_xml::events::BytesText<'_>) -> Result<String, Refusal> {
    t.unescape().map(|s| s.into_owned()).map_err(|_| MALFORMED_XML)
}

fn cdata_text(c: quick_xml::events::BytesCData<'_>) -> Result<String, Refusal> {
    String::from_utf8(c.into_inner().into_owned()).map_err(|_| MALFORMED_XML)
}

fn is_delete_leaf(path: &[String]) -> bool {
    matches!(
        path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice(),
        ["Delete", "Quiet"]
            | ["Delete", "Object", "Key"]
            | ["Delete", "Object", "VersionId"]
            | ["Delete", "Object", "ETag"]
            | ["Delete", "Object", "LastModifiedTime"]
            | ["Delete", "Object", "Size"]
    )
}

fn push_delete_text(
    path: &[String],
    quiet: &mut Option<String>,
    cur_key: &mut Option<String>,
    cur_vid: &mut Option<String>,
    cur_etag: &mut Option<String>,
    cur_lmt: &mut Option<String>,
    cur_size: &mut Option<String>,
    v: &str,
) -> Result<(), Refusal> {
    let slot = match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
        ["Delete", "Quiet"] => quiet,
        ["Delete", "Object", "Key"] => cur_key,
        ["Delete", "Object", "VersionId"] => cur_vid,
        ["Delete", "Object", "ETag"] => cur_etag,
        ["Delete", "Object", "LastModifiedTime"] => cur_lmt,
        ["Delete", "Object", "Size"] => cur_size,
        _ => {
            if !v.trim().is_empty() {
                return Err(MALFORMED_XML);
            }
            return Ok(());
        }
    };
    slot.get_or_insert_with(String::new).push_str(v);
    Ok(())
}

pub fn parse_delete_objects(body: &str) -> Result<DeleteRequest, Refusal> {
    let mut reader = Reader::from_str(body);
    let mut path: Vec<String> = Vec::new();
    let mut seen_root = false;
    let mut quiet: Option<String> = None;
    let mut objects: Vec<DeleteObject> = Vec::new();
    let mut cur_key: Option<String> = None;
    let mut cur_vid: Option<String> = None;
    let mut cur_etag: Option<String> = None;
    let mut cur_lmt: Option<String> = None;
    let mut cur_size: Option<String> = None;
    let mut in_object = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8(e.local_name().as_ref().to_vec()).map_err(|_| MALFORMED_XML)?;
                if path.is_empty() {
                    if seen_root || name != "Delete" {
                        return Err(MALFORMED_XML);
                    }
                    seen_root = true;
                }
                path.push(name);
                match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
                    ["Delete"] => {}
                    ["Delete", "Quiet"] => {
                        if quiet.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        quiet = Some(String::new());
                    }
                    ["Delete", "Object"] => {
                        if objects.len() >= MAX_DELETE_OBJECTS {
                            return Err(MALFORMED_XML);
                        }
                        in_object = true;
                        cur_key = None;
                        cur_vid = None;
                        cur_etag = None;
                        cur_lmt = None;
                        cur_size = None;
                    }
                    ["Delete", "Object", "Key"] => {
                        if cur_key.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        cur_key = Some(String::new());
                    }
                    ["Delete", "Object", "VersionId"] => {
                        if cur_vid.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        cur_vid = Some(String::new());
                    }
                    ["Delete", "Object", "ETag"] => {
                        if cur_etag.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        cur_etag = Some(String::new());
                    }
                    ["Delete", "Object", "LastModifiedTime"] => {
                        if cur_lmt.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        cur_lmt = Some(String::new());
                    }
                    ["Delete", "Object", "Size"] => {
                        if cur_size.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        cur_size = Some(String::new());
                    }
                    _ => {
                        if path.len() > 2 && is_delete_leaf(&path[..path.len() - 1]) {
                            return Err(MALFORMED_XML);
                        }
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8(e.local_name().as_ref().to_vec()).map_err(|_| MALFORMED_XML)?;
                if path.is_empty() {
                    return Err(MALFORMED_XML);
                }
                path.push(name);
                match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
                    ["Delete", "Object"] | ["Delete", "Object", "Key"] => return Err(MALFORMED_XML),
                    ["Delete", "Quiet"] => {
                        if quiet.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        quiet = Some(String::new());
                    }
                    ["Delete", "Object", "VersionId"] => {
                        if cur_vid.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        cur_vid = Some(String::new());
                    }
                    ["Delete", "Object", "ETag"] => {
                        if cur_etag.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        cur_etag = Some(String::new());
                    }
                    _ => {}
                }
                path.pop();
            }
            Ok(Event::Text(t)) => {
                let v = unescape_text(&t)?;
                push_delete_text(
                    &path,
                    &mut quiet,
                    &mut cur_key,
                    &mut cur_vid,
                    &mut cur_etag,
                    &mut cur_lmt,
                    &mut cur_size,
                    &v,
                )?;
            }
            Ok(Event::CData(c)) => {
                let v = cdata_text(c)?;
                push_delete_text(
                    &path,
                    &mut quiet,
                    &mut cur_key,
                    &mut cur_vid,
                    &mut cur_etag,
                    &mut cur_lmt,
                    &mut cur_size,
                    &v,
                )?;
            }
            Ok(Event::End(_)) => {
                if in_object && path.len() == 2 && path[1] == "Object" {
                    let key = cur_key.take().filter(|k| !k.is_empty()).ok_or(MALFORMED_XML)?;
                    let etag = cur_etag.take();
                    let last_modified_secs = match cur_lmt.take() {
                        Some(raw) => Some(crate::precondition::parse_http_date_or_iso(&raw).ok_or(MALFORMED_XML)?),
                        None => None,
                    };
                    let size = match cur_size.take() {
                        Some(raw) => Some(raw.trim().parse::<i64>().map_err(|_| MALFORMED_XML)?),
                        None => None,
                    };
                    objects.push(DeleteObject {
                        key,
                        version_id: cur_vid.take(),
                        etag,
                        last_modified_secs,
                        size,
                    });
                    in_object = false;
                }
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(MALFORMED_XML),
            Ok(_) => {}
        }
    }

    if !seen_root || !path.is_empty() || objects.is_empty() {
        return Err(MALFORMED_XML);
    }

    let quiet = match quiet.as_deref().map(str::trim) {
        None => false,
        Some("true") => true,
        Some("false") => false,
        Some(_) => return Err(MALFORMED_XML),
    };

    Ok(DeleteRequest { quiet, objects })
}

pub fn parse_versioning(body: &str) -> Result<VersioningRequest, Refusal> {
    let mut reader = Reader::from_str(body);
    let mut path: Vec<String> = Vec::new();
    let mut seen_root = false;
    let mut status: Option<String> = None;
    let mut mfa_delete: Option<String> = None;

    let is_mfa = |s: &str| s == "MfaDelete" || s == "MFADelete";

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8(e.local_name().as_ref().to_vec()).map_err(|_| MALFORMED_XML)?;
                if path.is_empty() {
                    if seen_root || name != "VersioningConfiguration" {
                        return Err(MALFORMED_XML);
                    }
                    seen_root = true;
                }
                path.push(name);
                match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
                    ["VersioningConfiguration"] => {}
                    ["VersioningConfiguration", "Status"] => {
                        if status.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        status = Some(String::new());
                    }
                    ["VersioningConfiguration", n] if is_mfa(n) => {
                        if mfa_delete.is_some() {
                            return Err(MALFORMED_XML);
                        }
                        mfa_delete = Some(String::new());
                    }
                    _ => return Err(MALFORMED_XML),
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8(e.local_name().as_ref().to_vec()).map_err(|_| MALFORMED_XML)?;
                if path.is_empty() {
                    if seen_root || name != "VersioningConfiguration" {
                        return Err(MALFORMED_XML);
                    }
                    return Err(MALFORMED_XML);
                }
                if path.len() == 1 && name == "Status" {
                    return Err(ILLEGAL_VERSIONING);
                }
                return Err(MALFORMED_XML);
            }
            Ok(Event::Text(t)) => {
                let v = unescape_text(&t)?;
                match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
                    ["VersioningConfiguration", "Status"] => {
                        status.get_or_insert_with(String::new).push_str(&v);
                    }
                    ["VersioningConfiguration", n] if is_mfa(n) => {
                        mfa_delete.get_or_insert_with(String::new).push_str(&v);
                    }
                    _ => {
                        if !v.trim().is_empty() {
                            return Err(MALFORMED_XML);
                        }
                    }
                }
            }
            Ok(Event::CData(c)) => {
                let v = cdata_text(c)?;
                match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
                    ["VersioningConfiguration", "Status"] => {
                        status.get_or_insert_with(String::new).push_str(&v);
                    }
                    ["VersioningConfiguration", n] if is_mfa(n) => {
                        mfa_delete.get_or_insert_with(String::new).push_str(&v);
                    }
                    _ => {
                        if !v.trim().is_empty() {
                            return Err(MALFORMED_XML);
                        }
                    }
                }
            }
            Ok(Event::End(_)) => {
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(MALFORMED_XML),
            Ok(_) => {}
        }
    }

    if !seen_root || !path.is_empty() {
        return Err(MALFORMED_XML);
    }

    let status_str = status.ok_or(MALFORMED_XML)?;
    let status = Versioning::parse(status_str.trim()).ok_or(ILLEGAL_VERSIONING)?;

    let mfa_delete = match mfa_delete.as_deref().map(str::trim) {
        None => false,
        Some("Disabled") => false,
        Some("Enabled") => return Err(NOT_IMPLEMENTED),
        Some(_) => return Err(MALFORMED_XML),
    };

    Ok(VersioningRequest { status, mfa_delete })
}

#[derive(Default)]
struct PartDraft {
    part_number: Option<String>,
    etag: Option<String>,
    checksum_crc32: Option<String>,
    checksum_crc32c: Option<String>,
    checksum_crc64nvme: Option<String>,
    checksum_sha1: Option<String>,
    checksum_sha256: Option<String>,
}

impl PartDraft {
    fn field(&mut self, name: &str) -> Option<&mut Option<String>> {
        match name {
            "PartNumber" => Some(&mut self.part_number),
            "ETag" => Some(&mut self.etag),
            "ChecksumCRC32" => Some(&mut self.checksum_crc32),
            "ChecksumCRC32C" => Some(&mut self.checksum_crc32c),
            "ChecksumCRC64NVME" => Some(&mut self.checksum_crc64nvme),
            "ChecksumSHA1" => Some(&mut self.checksum_sha1),
            "ChecksumSHA256" => Some(&mut self.checksum_sha256),
            _ => None,
        }
    }
}

pub fn parse_complete_multipart(body: &str) -> Result<Vec<CompletedPart>, Refusal> {
    let mut reader = Reader::from_str(body);
    let mut path: Vec<String> = Vec::new();
    let mut seen_root = false;
    let mut parts: Vec<CompletedPart> = Vec::new();
    let mut draft: Option<PartDraft> = None;
    let mut prev_part_number: u32 = 0;

    let append_text = |path: &[String], draft: &mut Option<PartDraft>, v: &str| -> Result<(), Refusal> {
        if path.len() == 3 && path[0] == "CompleteMultipartUpload" && path[1] == "Part" {
            if let Some(d) = draft.as_mut() {
                if let Some(slot) = d.field(&path[2]) {
                    slot.get_or_insert_with(String::new).push_str(v);
                    return Ok(());
                }
            }
        }
        if !v.trim().is_empty() {
            return Err(MALFORMED_XML);
        }
        Ok(())
    };

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8(e.local_name().as_ref().to_vec()).map_err(|_| MALFORMED_XML)?;
                if path.is_empty() {
                    if seen_root || name != "CompleteMultipartUpload" {
                        return Err(MALFORMED_XML);
                    }
                    seen_root = true;
                }
                path.push(name);
                match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
                    ["CompleteMultipartUpload"] => {}
                    ["CompleteMultipartUpload", "Part"] => {
                        if parts.len() >= MAX_COMPLETE_PARTS {
                            return Err(MALFORMED_XML);
                        }
                        draft = Some(PartDraft::default());
                    }
                    ["CompleteMultipartUpload", "Part", field_name] => {
                        let d = draft.as_mut().ok_or(MALFORMED_XML)?;
                        if let Some(slot) = d.field(field_name) {
                            if slot.is_some() {
                                return Err(MALFORMED_XML);
                            }
                            *slot = Some(String::new());
                        }
                    }
                    _ => return Err(MALFORMED_XML),
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8(e.local_name().as_ref().to_vec()).map_err(|_| MALFORMED_XML)?;
                if path.is_empty() {
                    return Err(MALFORMED_XML);
                }
                path.push(name);
                match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
                    ["CompleteMultipartUpload", "Part"]
                    | ["CompleteMultipartUpload", "Part", "PartNumber"]
                    | ["CompleteMultipartUpload", "Part", "ETag"] => return Err(MALFORMED_XML),
                    ["CompleteMultipartUpload", "Part", _] => {}
                    _ => return Err(MALFORMED_XML),
                }
                path.pop();
            }
            Ok(Event::Text(t)) => {
                let v = unescape_text(&t)?;
                append_text(&path, &mut draft, &v)?;
            }
            Ok(Event::CData(c)) => {
                let v = cdata_text(c)?;
                append_text(&path, &mut draft, &v)?;
            }
            Ok(Event::End(_)) => {
                if path.len() == 2 && path[1] == "Part" {
                    let d = draft.take().ok_or(MALFORMED_XML)?;
                    let raw_num = d.part_number.ok_or(MALFORMED_XML)?;
                    let parsed_num: i64 = raw_num.trim().parse().map_err(|_| MALFORMED_XML)?;
                    if !(1..=10_000).contains(&parsed_num) {
                        return Err(INVALID_ARGUMENT);
                    }
                    let part_number = parsed_num as u32;
                    if part_number < prev_part_number {
                        return Err(INVALID_PART_ORDER);
                    }
                    prev_part_number = part_number;

                    let raw_etag = d.etag.ok_or(MALFORMED_XML)?;
                    let etag = raw_etag.trim().trim_matches('"').to_string();
                    if etag.is_empty() {
                        return Err(MALFORMED_XML);
                    }

                    let opt_non_empty = |v: Option<String>| v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
                    parts.push(CompletedPart {
                        part_number,
                        etag,
                        checksum_crc32: opt_non_empty(d.checksum_crc32),
                        checksum_crc32c: opt_non_empty(d.checksum_crc32c),
                        checksum_crc64nvme: opt_non_empty(d.checksum_crc64nvme),
                        checksum_sha1: opt_non_empty(d.checksum_sha1),
                        checksum_sha256: opt_non_empty(d.checksum_sha256),
                    });
                }
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(MALFORMED_XML),
            Ok(_) => {}
        }
    }

    if !seen_root || !path.is_empty() || parts.is_empty() {
        return Err(MALFORMED_XML);
    }

    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Small test serialiser for `DeleteRequest` used in round-trip tests and fuzzing.
    fn serialize_delete_objects(req: &DeleteRequest) -> String {
        let mut out = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
        );
        if req.quiet {
            out.push_str("  <Quiet>true</Quiet>\n");
        }
        for obj in &req.objects {
            out.push_str("  <Object>\n");
            out.push_str(&format!("    <Key>{}</Key>\n", crate::xml::xml_escape_exact(&obj.key)));
            if let Some(vid) = &obj.version_id {
                out.push_str(&format!("    <VersionId>{}</VersionId>\n", crate::xml::xml_escape_exact(vid)));
            }
            if let Some(etag) = &obj.etag {
                out.push_str(&format!("    <ETag>{}</ETag>\n", crate::xml::xml_escape_exact(etag)));
            }
            if let Some(lmt) = obj.last_modified_secs {
                out.push_str(&format!("    <LastModifiedTime>{}</LastModifiedTime>\n", crate::precondition::format_http_date(lmt)));
            }
            if let Some(sz) = obj.size {
                out.push_str(&format!("    <Size>{sz}</Size>\n"));
            }
            out.push_str("  </Object>\n");
        }
        out.push_str("</Delete>");
        out
    }

    #[test]
    fn delete_objects_table() {
        let ok_cases: &[(&str, DeleteRequest)] = &[
            // Existing server test: delete_objects_test.rs
            (
                "<Delete><Object><Key>k1</Key></Object><Object><Key>a&amp;b</Key></Object></Delete>",
                DeleteRequest {
                    quiet: false,
                    objects: vec![
                        DeleteObject { key: "k1".into(), version_id: None, etag: None, last_modified_secs: None, size: None },
                        DeleteObject { key: "a&b".into(), version_id: None, etag: None, last_modified_secs: None, size: None },
                    ],
                },
            ),
            // Existing server test: list_versions_test.rs
            (
                "<Delete><Object><Key>a</Key><VersionId>null</VersionId></Object><Object><Key>b/c</Key><VersionId>null</VersionId></Object></Delete>",
                DeleteRequest {
                    quiet: false,
                    objects: vec![
                        DeleteObject { key: "a".into(), version_id: Some("null".into()), etag: None, last_modified_secs: None, size: None },
                        DeleteObject { key: "b/c".into(), version_id: Some("null".into()), etag: None, last_modified_secs: None, size: None },
                    ],
                },
            ),
            // S3 namespace, Quiet=true, whitespace between elements, entity decoding
            (
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
                   <Quiet>true</Quiet>\n  \
                   <Object>\n    \
                     <Key>dir/file &lt;1&gt;.txt</Key>\n    \
                     <VersionId>v123</VersionId>\n  \
                   </Object>\n\
                 </Delete>",
                DeleteRequest {
                    quiet: true,
                    objects: vec![DeleteObject {
                        key: "dir/file <1>.txt".into(),
                        version_id: Some("v123".into()),
                        etag: None,
                        last_modified_secs: None,
                        size: None,
                    }],
                },
            ),
            // Key with leading/trailing spaces preserved
            (
                "<Delete><Object><Key> spaced </Key></Object></Delete>",
                DeleteRequest {
                    quiet: false,
                    objects: vec![DeleteObject { key: " spaced ".into(), version_id: None, etag: None, last_modified_secs: None, size: None }],
                },
            ),
            // Conditional DeleteObjects fields: ETag, LastModifiedTime, Size
            (
                "<Delete><Object><Key>cond</Key><ETag>\"abc\"</ETag><LastModifiedTime>Wed, 11 Nov 2026 00:00:00 GMT</LastModifiedTime><Size>42</Size></Object></Delete>",
                DeleteRequest {
                    quiet: false,
                    objects: vec![DeleteObject {
                        key: "cond".into(),
                        version_id: None,
                        etag: Some("\"abc\"".into()),
                        last_modified_secs: Some(1_794_355_200),
                        size: Some(42),
                    }],
                },
            ),
        ];

        for (xml, expected) in ok_cases {
            let parsed = parse_delete_objects(xml).unwrap_or_else(|e| panic!("failed on {xml}: {e:?}"));
            assert_eq!(&parsed, expected, "xml: {xml}");
            // Round-trip check
            let reserialized = serialize_delete_objects(&parsed);
            let reparsed = parse_delete_objects(&reserialized).expect("round-trip parse");
            assert_eq!(parsed, reparsed, "round-trip mismatch for {xml}");
        }

        let err_cases: &[(&str, &str, u16)] = &[
            ("", "MalformedXML", 400),
            ("<Delete/>", "MalformedXML", 400),
            ("<Delete></Delete>", "MalformedXML", 400),
            ("<Delete><Quiet>true</Quiet></Delete>", "MalformedXML", 400),
            ("<Delete><Object></Object></Delete>", "MalformedXML", 400),
            ("<Delete><Object><Key></Key></Object></Delete>", "MalformedXML", 400),
            ("<Delete><Object><Key/></Object></Delete>", "MalformedXML", 400),
            ("<Delete><Object><VersionId>v1</VersionId></Object></Delete>", "MalformedXML", 400),
            ("<Delete><Quiet>maybe</Quiet><Object><Key>k</Key></Object></Delete>", "MalformedXML", 400),
            ("<NotDelete><Object><Key>k</Key></Object></NotDelete>", "MalformedXML", 400),
            ("<Delete><Object><Key>k</Key></Object>", "MalformedXML", 400), // unclosed
        ];

        for (xml, code, status) in err_cases {
            let err = parse_delete_objects(xml).unwrap_err();
            assert_eq!(err.code, *code, "xml: {xml}");
            assert_eq!(err.status, *status, "xml: {xml}");
        }
    }

    #[test]
    fn delete_objects_key_limit_1000_and_1001() {
        let mut xml_1000 = String::from("<Delete>");
        for i in 0..1000 {
            xml_1000.push_str(&format!("<Object><Key>key-{i}</Key></Object>"));
        }
        xml_1000.push_str("</Delete>");
        let parsed = parse_delete_objects(&xml_1000).expect("1000 objects allowed");
        assert_eq!(parsed.objects.len(), 1000);

        // test_multi_object_delete_key_limit: 1001 objects -> 400 MalformedXML
        let mut xml_1001 = String::from("<Delete>");
        for i in 0..1001 {
            xml_1001.push_str(&format!("<Object><Key>key-{i}</Key></Object>"));
        }
        xml_1001.push_str("</Delete>");
        let err = parse_delete_objects(&xml_1001).unwrap_err();
        assert_eq!(err.code, "MalformedXML");
        assert_eq!(err.status, 400);
    }

    #[test]
    fn versioning_table() {
        let ok_cases: &[(&str, VersioningRequest)] = &[
            // Existing server tests: compaction_inflight_test.rs, stalled_put_test.rs
            (
                "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>",
                VersioningRequest { status: Versioning::Enabled, mfa_delete: false },
            ),
            // Existing server tests: copy_version_test.rs, versioning_test.rs
            (
                "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
                VersioningRequest { status: Versioning::Enabled, mfa_delete: false },
            ),
            (
                "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>",
                VersioningRequest { status: Versioning::Suspended, mfa_delete: false },
            ),
            // test_versioning_obj_create_read_remove: MFADelete=Disabled
            (
                "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <MfaDelete>Disabled</MfaDelete>\n  <Status>Enabled</Status>\n</VersioningConfiguration>",
                VersioningRequest { status: Versioning::Enabled, mfa_delete: false },
            ),
            (
                "<VersioningConfiguration><MFADelete>Disabled</MFADelete><Status>Suspended</Status></VersioningConfiguration>",
                VersioningRequest { status: Versioning::Suspended, mfa_delete: false },
            ),
        ];

        for (xml, expected) in ok_cases {
            let parsed = parse_versioning(xml).unwrap_or_else(|e| panic!("failed on {xml}: {e:?}"));
            assert_eq!(&parsed, expected, "xml: {xml}");
        }

        let err_cases: &[(&str, &str, u16)] = &[
            ("", "MalformedXML", 400),
            ("<VersioningConfiguration/>", "MalformedXML", 400),
            ("<VersioningConfiguration></VersioningConfiguration>", "MalformedXML", 400),
            ("<VersioningConfiguration><MfaDelete>Disabled</MfaDelete></VersioningConfiguration>", "MalformedXML", 400),
            ("<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>", "NotImplemented", 501),
            ("<VersioningConfiguration><Status>Suspended</Status><MFADelete>Enabled</MFADelete></VersioningConfiguration>", "NotImplemented", 501),
            ("<VersioningConfiguration><Status>Invalid</Status></VersioningConfiguration>", "IllegalVersioningConfigurationException", 400),
            ("<VersioningConfiguration><Status></Status></VersioningConfiguration>", "IllegalVersioningConfigurationException", 400),
            ("<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Bogus</MfaDelete></VersioningConfiguration>", "MalformedXML", 400),
            ("<Other><Status>Enabled</Status></Other>", "MalformedXML", 400),
        ];

        for (xml, code, status) in err_cases {
            let err = parse_versioning(xml).unwrap_err();
            assert_eq!(err.code, *code, "xml: {xml}");
            assert_eq!(err.status, *status, "xml: {xml}");
        }
    }

    #[test]
    fn complete_multipart_table() {
        let ok_cases: &[(&str, Vec<CompletedPart>)] = &[
            // Existing server tests: compaction_safety_test.rs, index_full_test.rs
            (
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>abc123</ETag></Part></CompleteMultipartUpload>",
                vec![CompletedPart {
                    part_number: 1,
                    etag: "abc123".into(),
                    checksum_crc32: None,
                    checksum_crc32c: None,
                    checksum_crc64nvme: None,
                    checksum_sha1: None,
                    checksum_sha256: None,
                }],
            ),
            // Existing server test: part_write_error_test.rs (quoted ETag)
            (
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part></CompleteMultipartUpload>",
                vec![CompletedPart {
                    part_number: 1,
                    etag: "x".into(),
                    checksum_crc32: None,
                    checksum_crc32c: None,
                    checksum_crc64nvme: None,
                    checksum_sha1: None,
                    checksum_sha256: None,
                }],
            ),
            // S3 namespace, whitespace, multiple parts, checksums
            (
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
                   <Part>\n    \
                     <PartNumber>1</PartNumber>\n    \
                     <ETag>\"etag1\"</ETag>\n    \
                     <ChecksumCRC32>AAAA</ChecksumCRC32>\n    \
                     <ChecksumSHA256>BBBB</ChecksumSHA256>\n  \
                   </Part>\n  \
                   <Part>\n    \
                     <PartNumber>5</PartNumber>\n    \
                     <ETag>etag5</ETag>\n    \
                     <ChecksumCRC32C>CCCC</ChecksumCRC32C>\n    \
                     <ChecksumCRC64NVME>DDDD</ChecksumCRC64NVME>\n    \
                     <ChecksumSHA1>EEEE</ChecksumSHA1>\n  \
                   </Part>\n\
                 </CompleteMultipartUpload>",
                vec![
                    CompletedPart {
                        part_number: 1,
                        etag: "etag1".into(),
                        checksum_crc32: Some("AAAA".into()),
                        checksum_crc32c: None,
                        checksum_crc64nvme: None,
                        checksum_sha1: None,
                        checksum_sha256: Some("BBBB".into()),
                    },
                    CompletedPart {
                        part_number: 5,
                        etag: "etag5".into(),
                        checksum_crc32: None,
                        checksum_crc32c: Some("CCCC".into()),
                        checksum_crc64nvme: Some("DDDD".into()),
                        checksum_sha1: Some("EEEE".into()),
                        checksum_sha256: None,
                    },
                ],
            ),
        ];

        for (xml, expected) in ok_cases {
            let parsed = parse_complete_multipart(xml).unwrap_or_else(|e| panic!("failed on {xml}: {e:?}"));
            assert_eq!(&parsed, expected, "xml: {xml}");
        }

        let err_cases: &[(&str, &str, u16)] = &[
            // test_multipart_upload_empty: no body / no parts -> 400 MalformedXML
            ("", "MalformedXML", 400),
            ("<CompleteMultipartUpload/>", "MalformedXML", 400),
            ("<CompleteMultipartUpload></CompleteMultipartUpload>", "MalformedXML", 400),
            // Missing PartNumber or ETag
            ("<CompleteMultipartUpload><Part><ETag>a</ETag></Part></CompleteMultipartUpload>", "MalformedXML", 400),
            ("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>", "MalformedXML", 400),
            ("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag></ETag></Part></CompleteMultipartUpload>", "MalformedXML", 400),
            ("<CompleteMultipartUpload><Part><PartNumber>abc</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>", "MalformedXML", 400),
            // Out of range PartNumber -> 400 InvalidArgument
            ("<CompleteMultipartUpload><Part><PartNumber>0</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>", "InvalidArgument", 400),
            ("<CompleteMultipartUpload><Part><PartNumber>-1</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>", "InvalidArgument", 400),
            ("<CompleteMultipartUpload><Part><PartNumber>10001</PartNumber><ETag>a</ETag></Part></CompleteMultipartUpload>", "InvalidArgument", 400),
            // Decreasing PartNumber -> 400 InvalidPartOrder (consecutive duplicates are allowed for resent parts)
            ("<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>a</ETag></Part><Part><PartNumber>1</PartNumber><ETag>b</ETag></Part></CompleteMultipartUpload>", "InvalidPartOrder", 400),
            // Wrong root / unclosed
            ("<Other><Part><PartNumber>1</PartNumber><ETag>a</ETag></Part></Other>", "MalformedXML", 400),
            ("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>a</ETag></Part>", "MalformedXML", 400),
        ];

        for (xml, code, status) in err_cases {
            let err = parse_complete_multipart(xml).unwrap_err();
            assert_eq!(err.code, *code, "xml: {xml}");
            assert_eq!(err.status, *status, "xml: {xml}");
        }
    }

    #[test]
    fn complete_multipart_10000_and_10001_parts() {
        let mut xml_10000 = String::from("<CompleteMultipartUpload>");
        for i in 1..=10_000 {
            xml_10000.push_str(&format!("<Part><PartNumber>{i}</PartNumber><ETag>e{i}</ETag></Part>"));
        }
        xml_10000.push_str("</CompleteMultipartUpload>");
        let parsed = parse_complete_multipart(&xml_10000).expect("10000 parts allowed");
        assert_eq!(parsed.len(), 10_000);

        let mut xml_10001 = String::from("<CompleteMultipartUpload>");
        for i in 1..=10_001 {
            xml_10001.push_str(&format!("<Part><PartNumber>{i}</PartNumber><ETag>e{i}</ETag></Part>"));
        }
        xml_10001.push_str("</CompleteMultipartUpload>");
        let err = parse_complete_multipart(&xml_10001).unwrap_err();
        assert_eq!(err.status, 400);
    }
}
