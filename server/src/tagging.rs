//! Object and bucket tagging: XML/header parsing, validation, and XML serialization.

use std::collections::HashSet;

use crate::fsm::{Refusal, MALFORMED_XML};
use crate::s3_api::percent_decode;
use crate::xml::xml_escape;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Object,
    Bucket,
}

impl Scope {
    const fn max_tags(self) -> usize {
        match self {
            Scope::Object => 10,
            Scope::Bucket => 50,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

impl Tag {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TagSet {
    pub tags: Vec<Tag>,
}

impl TagSet {
    pub fn new(tags: Vec<Tag>) -> Self {
        Self { tags }
    }
}

const fn invalid_tag(why: &'static str) -> Refusal {
    Refusal {
        code: "InvalidTag",
        status: 400,
        why,
    }
}

fn validate_tags(tags: &[Tag], scope: Scope) -> Result<(), Refusal> {
    if tags.len() > scope.max_tags() {
        return Err(invalid_tag("too many tags"));
    }
    let mut seen = HashSet::with_capacity(tags.len());
    for tag in tags {
        let klen = tag.key.chars().count();
        if klen == 0 || klen > 128 {
            return Err(invalid_tag("tag key length must be 1 to 128 characters"));
        }
        if tag.key.starts_with("aws:") {
            return Err(invalid_tag("tag key cannot start with aws:"));
        }
        if tag.value.chars().count() > 256 {
            return Err(invalid_tag("tag value length must be 0 to 256 characters"));
        }
        if !seen.insert(tag.key.as_str()) {
            return Err(invalid_tag("duplicate tag key"));
        }
    }
    Ok(())
}

pub fn parse_tagging_xml(xml: &str, scope: Scope) -> Result<TagSet, Refusal> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(xml);
    let mut path: Vec<String> = Vec::new();
    let mut saw_tagging = false;
    let mut saw_tagset = false;
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;
    let mut tags = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                path.push(name);
                let p: Vec<&str> = path.iter().map(String::as_str).collect();
                match p.as_slice() {
                    ["Tagging"] if !saw_tagging => saw_tagging = true,
                    ["Tagging", "TagSet"] if !saw_tagset => saw_tagset = true,
                    ["Tagging", "TagSet", "Tag"] => {
                        key = None;
                        value = None;
                    }
                    ["Tagging", "TagSet", "Tag", "Key"] if key.is_none() => {
                        key = Some(String::new());
                    }
                    ["Tagging", "TagSet", "Tag", "Value"] if value.is_none() => {
                        value = Some(String::new());
                    }
                    _ => return Err(MALFORMED_XML),
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                path.push(name);
                let p: Vec<&str> = path.iter().map(String::as_str).collect();
                match p.as_slice() {
                    ["Tagging"] if !saw_tagging => saw_tagging = true,
                    ["Tagging", "TagSet"] if !saw_tagset => saw_tagset = true,
                    ["Tagging", "TagSet", "Tag", "Key"] if key.is_none() => {
                        key = Some(String::new());
                    }
                    ["Tagging", "TagSet", "Tag", "Value"] if value.is_none() => {
                        value = Some(String::new());
                    }
                    _ => return Err(MALFORMED_XML),
                }
                path.pop();
            }
            Ok(Event::Text(t)) => {
                let v = t.unescape().map_err(|_| MALFORMED_XML)?;
                let p: Vec<&str> = path.iter().map(String::as_str).collect();
                match p.as_slice() {
                    ["Tagging", "TagSet", "Tag", "Key"] => {
                        key.get_or_insert_with(String::new).push_str(&v);
                    }
                    ["Tagging", "TagSet", "Tag", "Value"] => {
                        value.get_or_insert_with(String::new).push_str(&v);
                    }
                    _ if v.trim().is_empty() => {}
                    _ => return Err(MALFORMED_XML),
                }
            }
            Ok(Event::CData(c)) => {
                let v = String::from_utf8_lossy(&c.into_inner()).into_owned();
                let p: Vec<&str> = path.iter().map(String::as_str).collect();
                match p.as_slice() {
                    ["Tagging", "TagSet", "Tag", "Key"] => {
                        key.get_or_insert_with(String::new).push_str(&v);
                    }
                    ["Tagging", "TagSet", "Tag", "Value"] => {
                        value.get_or_insert_with(String::new).push_str(&v);
                    }
                    _ => return Err(MALFORMED_XML),
                }
            }
            Ok(Event::End(_)) => {
                let p: Vec<&str> = path.iter().map(String::as_str).collect();
                if p.as_slice() == ["Tagging", "TagSet", "Tag"] {
                    let k = key.take().ok_or(MALFORMED_XML)?;
                    let v = value.take().ok_or(MALFORMED_XML)?;
                    tags.push(Tag { key: k, value: v });
                }
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(MALFORMED_XML),
            Ok(_) => {}
        }
    }

    if !path.is_empty() || !saw_tagging || !saw_tagset {
        return Err(MALFORMED_XML);
    }

    validate_tags(&tags, scope)?;
    Ok(TagSet { tags })
}

pub fn parse_tagging_header(header: &str) -> Result<TagSet, Refusal> {
    let mut tags = Vec::new();
    for pair in header.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(pair), String::new()),
        };
        tags.push(Tag { key: k, value: v });
    }
    validate_tags(&tags, Scope::Object)?;
    Ok(TagSet { tags })
}

pub fn tagging_xml(set: &TagSet) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <TagSet>\n",
    );
    for tag in &set.tags {
        out.push_str("    <Tag>\n");
        out.push_str(&format!("      <Key>{}</Key>\n", xml_escape(&tag.key)));
        out.push_str(&format!("      <Value>{}</Value>\n", xml_escape(&tag.value)));
        out.push_str("    </Tag>\n");
    }
    out.push_str("  </TagSet>\n</Tagging>");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn tag_pairs(set: &TagSet) -> Vec<(&str, &str)> {
        set.tags
            .iter()
            .map(|t| (t.key.as_str(), t.value.as_str()))
            .collect()
    }

    fn make_xml(pairs: &[(&str, &str)]) -> String {
        let mut s = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>",
        );
        for (k, v) in pairs {
            s.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                crate::xml::xml_escape(k),
                crate::xml::xml_escape(v)
            ));
        }
        s.push_str("</TagSet></Tagging>");
        s
    }

    #[test]
    fn table_limits_and_validation() {
        let k128 = "k".repeat(128);
        let k129 = "k".repeat(129);
        let v256 = "v".repeat(256);
        let v257 = "v".repeat(257);
        // 128 Unicode scalar values (4-byte emoji each = 512 UTF-8 bytes)
        let k128_unicode = "\u{1F980}".repeat(128);
        let k129_unicode = "\u{1F980}".repeat(129);
        let v256_unicode = "\u{1F980}".repeat(256);
        let v257_unicode = "\u{1F980}".repeat(257);

        let obj_10: Vec<(String, String)> =
            (0..10).map(|i| (format!("k{i}"), format!("v{i}"))).collect();
        let obj_10_refs: Vec<(&str, &str)> =
            obj_10.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let obj_11: Vec<(String, String)> =
            (0..11).map(|i| (format!("k{i}"), format!("v{i}"))).collect();
        let obj_11_refs: Vec<(&str, &str)> =
            obj_11.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        let bkt_50: Vec<(String, String)> =
            (0..50).map(|i| (format!("k{i}"), format!("v{i}"))).collect();
        let bkt_50_refs: Vec<(&str, &str)> =
            bkt_50.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let bkt_51: Vec<(String, String)> =
            (0..51).map(|i| (format!("k{i}"), format!("v{i}"))).collect();
        let bkt_51_refs: Vec<(&str, &str)> =
            bkt_51.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        struct Case<'a> {
            name: &'a str,
            pairs: &'a [(&'a str, &'a str)],
            scope: Scope,
            expected_err: Option<(&'static str, u16)>,
        }

        let cases = [
            Case {
                name: "empty tagset",
                pairs: &[],
                scope: Scope::Object,
                expected_err: None,
            },
            Case {
                name: "key len 1, val len 0",
                pairs: &[("a", "")],
                scope: Scope::Object,
                expected_err: None,
            },
            Case {
                name: "key len 0 refused",
                pairs: &[("", "v")],
                scope: Scope::Object,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "key len 128 accepted",
                pairs: &[(&k128, "v")],
                scope: Scope::Object,
                expected_err: None,
            },
            Case {
                name: "key len 129 refused",
                pairs: &[(&k129, "v")],
                scope: Scope::Object,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "key len 128 unicode scalar values accepted",
                pairs: &[(&k128_unicode, "v")],
                scope: Scope::Object,
                expected_err: None,
            },
            Case {
                name: "key len 129 unicode scalar values refused",
                pairs: &[(&k129_unicode, "v")],
                scope: Scope::Object,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "val len 256 accepted",
                pairs: &[("k", &v256)],
                scope: Scope::Object,
                expected_err: None,
            },
            Case {
                name: "val len 257 refused",
                pairs: &[("k", &v257)],
                scope: Scope::Object,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "val len 256 unicode scalar values accepted",
                pairs: &[("k", &v256_unicode)],
                scope: Scope::Object,
                expected_err: None,
            },
            Case {
                name: "val len 257 unicode scalar values refused",
                pairs: &[("k", &v257_unicode)],
                scope: Scope::Object,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "duplicate keys refused",
                pairs: &[("dup", "1"), ("other", "2"), ("dup", "3")],
                scope: Scope::Object,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "aws: prefix refused",
                pairs: &[("aws:createdBy", "user")],
                scope: Scope::Object,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "aws: prefix in bucket scope refused",
                pairs: &[("aws:cloudformation:stack-name", "s")],
                scope: Scope::Bucket,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "object 10 tags accepted",
                pairs: &obj_10_refs,
                scope: Scope::Object,
                expected_err: None,
            },
            Case {
                name: "object 11 tags refused",
                pairs: &obj_11_refs,
                scope: Scope::Object,
                expected_err: Some(("InvalidTag", 400)),
            },
            Case {
                name: "bucket 50 tags accepted",
                pairs: &bkt_50_refs,
                scope: Scope::Bucket,
                expected_err: None,
            },
            Case {
                name: "bucket 51 tags refused",
                pairs: &bkt_51_refs,
                scope: Scope::Bucket,
                expected_err: Some(("InvalidTag", 400)),
            },
        ];

        for c in &cases {
            let xml = make_xml(c.pairs);
            let res = parse_tagging_xml(&xml, c.scope);
            match c.expected_err {
                None => {
                    let parsed = res.unwrap_or_else(|e| panic!("{}: unexpected err {:?}", c.name, e));
                    assert_eq!(tag_pairs(&parsed), c.pairs, "{}", c.name);
                }
                Some((code, status)) => {
                    let err = res.expect_err(c.name);
                    assert_eq!((err.code, err.status), (code, status), "{}", c.name);
                }
            }
        }
    }

    #[test]
    fn malformed_xml_cases() {
        let bad_docs = [
            "",
            "not xml at all",
            "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet>",
            "<Tagging></Tagging>",
            "<Tagging/>",
            "<Other><TagSet/></Other>",
            "<Tagging><TagSet><Tag><Value>v</Value></Tag></TagSet></Tagging>",
            "<Tagging><TagSet><Tag><Key>k</Key></Tag></TagSet></Tagging>",
        ];
        for doc in bad_docs {
            let err = parse_tagging_xml(doc, Scope::Object)
                .unwrap_err();
            assert_eq!(err.code, "MalformedXML", "doc: {doc}");
            assert_eq!(err.status, 400, "doc: {doc}");
        }
    }

    #[test]
    fn order_preserved() {
        let xml = make_xml(&[("z", "1"), ("a", "2"), ("m", "3")]);
        let set = parse_tagging_xml(&xml, Scope::Object).unwrap();
        assert_eq!(tag_pairs(&set), vec![("z", "1"), ("a", "2"), ("m", "3")]);
        let out = tagging_xml(&set);
        let again = parse_tagging_xml(&out, Scope::Object).unwrap();
        assert_eq!(set, again);
    }

    #[test]
    fn header_vs_xml_equivalence() {
        let cases: &[(&str, &[(&str, &str)])] = &[
            ("", &[]),
            ("foo=bar&bar", &[("foo", "bar"), ("bar", "")]),
            ("foo=bar&baz=", &[("foo", "bar"), ("baz", "")]),
            (
                "a%20b=c%26d&x+y=z%3D1",
                &[("a b", "c&d"), ("x y", "z=1")],
            ),
            ("z=1&a=2&m=3", &[("z", "1"), ("a", "2"), ("m", "3")]),
        ];
        for (hdr, expected) in cases {
            let from_hdr = parse_tagging_header(hdr).unwrap();
            let from_xml = parse_tagging_xml(&make_xml(expected), Scope::Object).unwrap();
            assert_eq!(from_hdr, from_xml, "header: {hdr}");
            assert_eq!(tag_pairs(&from_hdr), *expected, "header: {hdr}");
        }

        // Invalid header cases
        let bad_headers = [
            "aws:key=val",
            "dup=1&dup=2",
            "=val",
            "k1=v1&k2=v2&k3=v3&k4=v4&k5=v5&k6=v6&k7=v7&k8=v8&k9=v9&k10=v10&k11=v11",
        ];
        for hdr in bad_headers {
            let err = parse_tagging_header(hdr).unwrap_err();
            assert_eq!(err.code, "InvalidTag", "header: {hdr}");
            assert_eq!(err.status, 400, "header: {hdr}");
        }
    }

    #[test]
    fn tagging_xml_escapes_and_has_namespace() {
        let set = TagSet::new(vec![Tag::new("k&<>\"'", "v&<>\"'")]);
        let xml = tagging_xml(&set);
        assert!(xml.contains("http://s3.amazonaws.com/doc/2006-03-01/"));
        assert!(xml.contains("<Key>k&amp;&lt;&gt;&quot;&apos;</Key>"));
        assert!(xml.contains("<Value>v&amp;&lt;&gt;&quot;&apos;</Value>"));
    }

    fn xml_safe_char() -> impl Strategy<Value = char> {
        prop_oneof![
            Just(' '),
            Just('\t'),
            Just('\n'),
            Just('\r'),
            Just('&'),
            Just('<'),
            Just('>'),
            Just('"'),
            Just('\''),
            proptest::char::range('a', 'z'),
            proptest::char::range('0', '9'),
            proptest::char::range('\u{00A0}', '\u{1F9FF}'),
        ]
    }

    fn valid_key_strategy() -> impl Strategy<Value = String> {
        proptest::collection::vec(xml_safe_char(), 1..=128)
            .prop_map(|chars| chars.into_iter().collect::<String>())
            .prop_filter("must not start with aws:", |s| !s.starts_with("aws:"))
    }

    fn valid_value_strategy() -> impl Strategy<Value = String> {
        proptest::collection::vec(xml_safe_char(), 0..=256)
            .prop_map(|chars| chars.into_iter().collect::<String>())
    }

    fn tagset_strategy(max_tags: usize) -> impl Strategy<Value = TagSet> {
        proptest::collection::vec((valid_key_strategy(), valid_value_strategy()), 0..=max_tags)
            .prop_map(|pairs| {
                let mut seen = std::collections::HashSet::new();
                let tags = pairs
                    .into_iter()
                    .filter(|(k, _)| seen.insert(k.clone()))
                    .map(|(k, v)| Tag::new(k, v))
                    .collect();
                TagSet::new(tags)
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn roundtrip_object_scope(set in tagset_strategy(10)) {
            let xml = tagging_xml(&set);
            let parsed = parse_tagging_xml(&xml, Scope::Object);
            prop_assert_eq!(parsed, Ok(set));
        }

        #[test]
        fn roundtrip_bucket_scope(set in tagset_strategy(50)) {
            let xml = tagging_xml(&set);
            let parsed = parse_tagging_xml(&xml, Scope::Bucket);
            prop_assert_eq!(parsed, Ok(set));
        }
    }
}
