//! Bucket CORS configuration parsing, serialization, and rule matching.

use crate::fsm::Refusal;
use crate::xml::xml_escape_exact;

/// Allowed HTTP methods in an S3 CORS rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CorsMethod {
    Get,
    Put,
    Post,
    Delete,
    Head,
}

impl CorsMethod {
    /// Returns the uppercase HTTP method name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put => "PUT",
            Self::Post => "POST",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
        }
    }

    /// Parses an exact S3 CORS method name (`GET`, `PUT`, `POST`, `DELETE`, `HEAD`).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "GET" => Some(Self::Get),
            "PUT" => Some(Self::Put),
            "POST" => Some(Self::Post),
            "DELETE" => Some(Self::Delete),
            "HEAD" => Some(Self::Head),
            _ => None,
        }
    }
}

/// A single `<CORSRule>` entry in a bucket's CORS configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorsRule {
    pub id: Option<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<CorsMethod>,
    pub allowed_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
    pub expose_headers: Vec<String>,
}

/// The CORS response headers derived from the first matching rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorsAnswer {
    pub allow_origin: String,
    pub allow_methods: Vec<CorsMethod>,
    pub allow_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
    pub expose_headers: Vec<String>,
    pub vary_origin: bool,
}

const fn malformed_xml(why: &'static str) -> Refusal {
    Refusal {
        code: "MalformedXML",
        status: 400,
        why,
    }
}

const fn invalid_request(why: &'static str) -> Refusal {
    Refusal {
        code: "InvalidRequest",
        status: 400,
        why,
    }
}

#[derive(Default)]
struct Draft {
    id: Option<String>,
    allowed_origins: Vec<String>,
    allowed_methods: Vec<String>,
    allowed_headers: Vec<String>,
    max_age_seconds: Option<String>,
    expose_headers: Vec<String>,
    active_text: Option<String>,
}

impl Draft {
    fn push_field(&mut self, tag: &str, value: String) {
        match tag {
            "ID" => self.id = Some(value),
            "AllowedOrigin" => self.allowed_origins.push(value),
            "AllowedMethod" => self.allowed_methods.push(value),
            "AllowedHeader" => self.allowed_headers.push(value),
            "MaxAgeSeconds" => self.max_age_seconds = Some(value),
            "ExposeHeader" => self.expose_headers.push(value),
            _ => {}
        }
    }
}

/// Parses an S3 `<CORSConfiguration>` XML document into validated `CorsRule`s.
pub fn parse_cors_xml(xml_str: &str) -> Result<Vec<CorsRule>, Refusal> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(xml_str);
    reader.config_mut().trim_text(true);

    let mut path: Vec<String> = Vec::new();
    let mut saw_root = false;
    let mut rules = Vec::new();
    let mut draft: Option<Draft> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if path.is_empty() {
                    if name != "CORSConfiguration" {
                        return Err(malformed_xml("root element must be CORSConfiguration"));
                    }
                    saw_root = true;
                } else if path.len() == 1 && path[0] == "CORSConfiguration" && name == "CORSRule" {
                    draft = Some(Draft::default());
                } else if path.len() == 2 && path[1] == "CORSRule" {
                    if let Some(d) = draft.as_mut() {
                        d.active_text = Some(String::new());
                    }
                }
                path.push(name);
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if path.is_empty() {
                    if name != "CORSConfiguration" {
                        return Err(malformed_xml("root element must be CORSConfiguration"));
                    }
                    saw_root = true;
                } else if path.len() == 1 && path[0] == "CORSConfiguration" && name == "CORSRule" {
                    return Err(malformed_xml("CORSRule missing AllowedOrigin"));
                } else if path.len() == 2 && path[1] == "CORSRule" {
                    if let Some(d) = draft.as_mut() {
                        d.push_field(&name, String::new());
                    }
                }
            }
            Ok(Event::Text(t)) => {
                let v = t
                    .unescape()
                    .map_err(|_| malformed_xml("invalid XML entity"))?;
                if path.len() == 3 && path[1] == "CORSRule" {
                    if let Some(d) = draft.as_mut() {
                        if let Some(buf) = d.active_text.as_mut() {
                            buf.push_str(&v);
                        }
                    }
                }
            }
            Ok(Event::CData(c)) => {
                let v = String::from_utf8_lossy(&c.into_inner()).into_owned();
                if path.len() == 3 && path[1] == "CORSRule" {
                    if let Some(d) = draft.as_mut() {
                        if let Some(buf) = d.active_text.as_mut() {
                            buf.push_str(&v);
                        }
                    }
                }
            }
            Ok(Event::End(_)) => {
                let closed = path.pop();
                if let Some(tag) = closed {
                    if path.len() == 2 && path[1] == "CORSRule" {
                        if let Some(d) = draft.as_mut() {
                            let val = d.active_text.take().unwrap_or_default();
                            d.push_field(&tag, val);
                        }
                    } else if path.len() == 1 && path[0] == "CORSConfiguration" && tag == "CORSRule" {
                        if let Some(d) = draft.take() {
                            rules.push(validate_rule(d)?);
                            if rules.len() > 100 {
                                return Err(invalid_request(
                                    "CORSConfiguration cannot exceed 100 rules",
                                ));
                            }
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(malformed_xml("malformed XML")),
            Ok(_) => {}
        }
    }

    if !saw_root || rules.is_empty() {
        return Err(malformed_xml(
            "CORSConfiguration must contain at least one CORSRule",
        ));
    }

    Ok(rules)
}

fn validate_rule(d: Draft) -> Result<CorsRule, Refusal> {
    let Draft {
        id,
        allowed_origins,
        allowed_methods: raw_methods,
        allowed_headers,
        max_age_seconds: raw_max_age,
        expose_headers,
        ..
    } = d;
    if allowed_origins.is_empty() {
        return Err(malformed_xml("CORSRule missing AllowedOrigin"));
    }
    if raw_methods.is_empty() {
        return Err(malformed_xml("CORSRule missing AllowedMethod"));
    }
    if let Some(ref id_val) = id {
        if id_val.chars().count() > 255 {
            return Err(invalid_request("ID exceeds 255 characters"));
        }
    }
    for origin in &allowed_origins {
        if origin.bytes().filter(|&b| b == b'*').count() > 1 {
            return Err(invalid_request(
                "AllowedOrigin can contain at most one '*'",
            ));
        }
    }
    for header in &allowed_headers {
        if header.bytes().filter(|&b| b == b'*').count() > 1 {
            return Err(invalid_request(
                "AllowedHeader can contain at most one '*'",
            ));
        }
    }
    let mut allowed_methods = Vec::with_capacity(raw_methods.len());
    for m in &raw_methods {
        let parsed = CorsMethod::parse(m)
            .ok_or_else(|| invalid_request("unsupported HTTP method in AllowedMethod"))?;
        allowed_methods.push(parsed);
    }
    let max_age_seconds = match raw_max_age {
        Some(s) => Some(
            s.trim()
                .parse::<u32>()
                .map_err(|_| malformed_xml("invalid MaxAgeSeconds"))?,
        ),
        None => None,
    };
    Ok(CorsRule {
        id,
        allowed_origins,
        allowed_methods,
        allowed_headers,
        max_age_seconds,
        expose_headers,
    })
}

/// Serializes `CorsRule`s into an S3 `<CORSConfiguration>` XML document.
pub fn cors_xml(rules: &[CorsRule]) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CORSConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for rule in rules {
        out.push_str("  <CORSRule>\n");
        if let Some(ref id) = rule.id {
            out.push_str(&format!("    <ID>{}</ID>\n", xml_escape_exact(id)));
        }
        for origin in &rule.allowed_origins {
            out.push_str(&format!(
                "    <AllowedOrigin>{}</AllowedOrigin>\n",
                xml_escape_exact(origin)
            ));
        }
        for method in &rule.allowed_methods {
            out.push_str(&format!(
                "    <AllowedMethod>{}</AllowedMethod>\n",
                method.as_str()
            ));
        }
        for header in &rule.allowed_headers {
            out.push_str(&format!(
                "    <AllowedHeader>{}</AllowedHeader>\n",
                xml_escape_exact(header)
            ));
        }
        if let Some(max_age) = rule.max_age_seconds {
            out.push_str(&format!("    <MaxAgeSeconds>{}</MaxAgeSeconds>\n", max_age));
        }
        for expose in &rule.expose_headers {
            out.push_str(&format!(
                "    <ExposeHeader>{}</ExposeHeader>\n",
                xml_escape_exact(expose)
            ));
        }
        out.push_str("  </CORSRule>\n");
    }
    out.push_str("</CORSConfiguration>");
    out
}

/// Matches a CORS preflight (`OPTIONS`) request against `rules`.
pub fn match_preflight(
    rules: &[CorsRule],
    origin: &str,
    method: &str,
    request_headers: &[&str],
) -> Option<CorsAnswer> {
    let req_method = CorsMethod::parse(method)?;
    match_rules(rules, origin, req_method, Some(request_headers))
}

/// Matches a non-preflight CORS request against `rules` (request headers are not checked).
pub fn match_actual(rules: &[CorsRule], origin: &str, method: &str) -> Option<CorsAnswer> {
    let req_method = CorsMethod::parse(method)?;
    match_rules(rules, origin, req_method, None)
}

fn match_rules(
    rules: &[CorsRule],
    origin: &str,
    req_method: CorsMethod,
    request_headers: Option<&[&str]>,
) -> Option<CorsAnswer> {
    for rule in rules {
        if !rule
            .allowed_origins
            .iter()
            .any(|pat| wildcard_match(pat, origin, false))
        {
            continue;
        }
        if !rule.allowed_methods.contains(&req_method) {
            continue;
        }
        if let Some(req_hdrs) = request_headers {
            let headers_ok = req_hdrs.iter().all(|req_h| {
                rule.allowed_headers
                    .iter()
                    .any(|pat| wildcard_match(pat, req_h, true))
            });
            if !headers_ok {
                continue;
            }
        }
        let star_only = rule.allowed_origins.len() == 1 && rule.allowed_origins[0] == "*";
        return Some(CorsAnswer {
            allow_origin: if star_only {
                "*".to_string()
            } else {
                origin.to_string()
            },
            allow_methods: rule.allowed_methods.clone(),
            allow_headers: request_headers
                .map(|hs| hs.iter().map(|s| (*s).to_string()).collect())
                .unwrap_or_default(),
            max_age_seconds: rule.max_age_seconds,
            expose_headers: rule.expose_headers.clone(),
            vary_origin: !star_only,
        });
    }
    None
}

fn wildcard_match(pattern: &str, text: &str, case_insensitive: bool) -> bool {
    let mut comparisons = 0usize;
    wildcard_match_counted(pattern, text, case_insensitive, &mut comparisons)
}

fn bytes_eq_counted(
    a: &[u8],
    b: &[u8],
    case_insensitive: bool,
    comparisons: &mut usize,
) -> bool {
    for (&x, &y) in a.iter().zip(b.iter()) {
        *comparisons += 1;
        let eq = if case_insensitive {
            x.eq_ignore_ascii_case(&y)
        } else {
            x == y
        };
        if !eq {
            return false;
        }
    }
    true
}

/// Matches `text` against `pattern` (at most one `*`), counting byte comparisons in `comparisons`.
pub fn wildcard_match_counted(
    pattern: &str,
    text: &str,
    case_insensitive: bool,
    comparisons: &mut usize,
) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    match p.iter().position(|&b| b == b'*') {
        None => p.len() == t.len() && bytes_eq_counted(p, t, case_insensitive, comparisons),
        Some(star) => {
            let prefix = &p[..star];
            let suffix = &p[star + 1..];
            if t.len() < prefix.len() + suffix.len() {
                return false;
            }
            bytes_eq_counted(prefix, &t[..prefix.len()], case_insensitive, comparisons)
                && bytes_eq_counted(
                    suffix,
                    &t[t.len() - suffix.len()..],
                    case_insensitive,
                    comparisons,
                )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn suite_cors_table() {
        // 1. test_set_cors: round-trip AllowedMethods + AllowedOrigins
        let set_cors_xml = r#"<CORSConfiguration>
            <CORSRule>
                <AllowedMethod>GET</AllowedMethod>
                <AllowedMethod>PUT</AllowedMethod>
                <AllowedOrigin>*.get</AllowedOrigin>
                <AllowedOrigin>*.put</AllowedOrigin>
            </CORSRule>
        </CORSConfiguration>"#;
        let parsed = parse_cors_xml(set_cors_xml).expect("test_set_cors parses");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].allowed_methods, vec![CorsMethod::Get, CorsMethod::Put]);
        assert_eq!(parsed[0].allowed_origins, vec!["*.get", "*.put"]);
        assert_eq!(parse_cors_xml(&cors_xml(&parsed)), Ok(parsed));

        // 2. test_cors_origin_response
        let origin_response_rules = parse_cors_xml(
            r#"<CORSConfiguration>
                <CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*suffix</AllowedOrigin></CORSRule>
                <CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>start*end</AllowedOrigin></CORSRule>
                <CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>prefix*</AllowedOrigin></CORSRule>
                <CORSRule><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>*.put</AllowedOrigin></CORSRule>
            </CORSConfiguration>"#,
        )
        .expect("test_cors_origin_response parses");

        struct Case<'a> {
            suite_test: &'a str,
            preflight: bool,
            origin: &'a str,
            method: &'a str,
            req_headers: &'a [&'a str],
            expect_origin: Option<&'a str>,
            expect_methods: Option<&'a [CorsMethod]>,
            expect_vary: bool,
        }

        let cases = [
            // test_cors_origin_response (actual requests)
            Case { suite_test: "test_cors_origin_response: GET foo.suffix", preflight: false, origin: "foo.suffix", method: "GET", req_headers: &[], expect_origin: Some("foo.suffix"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: GET foo.bar", preflight: false, origin: "foo.bar", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: GET foo.suffix.get", preflight: false, origin: "foo.suffix.get", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: GET startend", preflight: false, origin: "startend", method: "GET", req_headers: &[], expect_origin: Some("startend"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: GET start1end", preflight: false, origin: "start1end", method: "GET", req_headers: &[], expect_origin: Some("start1end"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: GET start12end", preflight: false, origin: "start12end", method: "GET", req_headers: &[], expect_origin: Some("start12end"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: GET 0start12end", preflight: false, origin: "0start12end", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: GET prefix", preflight: false, origin: "prefix", method: "GET", req_headers: &[], expect_origin: Some("prefix"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: GET prefix.suffix", preflight: false, origin: "prefix.suffix", method: "GET", req_headers: &[], expect_origin: Some("prefix.suffix"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: GET bla.prefix", preflight: false, origin: "bla.prefix", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: PUT foo.suffix", preflight: false, origin: "foo.suffix", method: "PUT", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: PUT foo.put", preflight: false, origin: "foo.put", method: "PUT", req_headers: &[], expect_origin: Some("foo.put"), expect_methods: Some(&[CorsMethod::Put]), expect_vary: true },
            // test_cors_origin_response (preflight OPTIONS)
            Case { suite_test: "test_cors_origin_response: OPTIONS foo.suffix GET", preflight: true, origin: "foo.suffix", method: "GET", req_headers: &[], expect_origin: Some("foo.suffix"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: OPTIONS foo.bar GET", preflight: true, origin: "foo.bar", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: OPTIONS foo.suffix.get GET", preflight: true, origin: "foo.suffix.get", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: OPTIONS startend GET", preflight: true, origin: "startend", method: "GET", req_headers: &[], expect_origin: Some("startend"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: OPTIONS start1end GET", preflight: true, origin: "start1end", method: "GET", req_headers: &[], expect_origin: Some("start1end"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: OPTIONS start12end GET", preflight: true, origin: "start12end", method: "GET", req_headers: &[], expect_origin: Some("start12end"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: OPTIONS 0start12end GET", preflight: true, origin: "0start12end", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: OPTIONS prefix GET", preflight: true, origin: "prefix", method: "GET", req_headers: &[], expect_origin: Some("prefix"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: OPTIONS prefix.suffix GET", preflight: true, origin: "prefix.suffix", method: "GET", req_headers: &[], expect_origin: Some("prefix.suffix"), expect_methods: Some(&[CorsMethod::Get]), expect_vary: true },
            Case { suite_test: "test_cors_origin_response: OPTIONS bla.prefix GET", preflight: true, origin: "bla.prefix", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: OPTIONS foo.put GET", preflight: true, origin: "foo.put", method: "GET", req_headers: &[], expect_origin: None, expect_methods: None, expect_vary: false },
            Case { suite_test: "test_cors_origin_response: OPTIONS foo.put PUT", preflight: true, origin: "foo.put", method: "PUT", req_headers: &[], expect_origin: Some("foo.put"), expect_methods: Some(&[CorsMethod::Put]), expect_vary: true },
        ];

        for c in cases {
            let ans = if c.preflight {
                match_preflight(&origin_response_rules, c.origin, c.method, c.req_headers)
            } else {
                match_actual(&origin_response_rules, c.origin, c.method)
            };
            match (c.expect_origin, c.expect_methods) {
                (Some(exp_o), Some(exp_m)) => {
                    let a = ans.unwrap_or_else(|| panic!("expected match for {}", c.suite_test));
                    assert_eq!(a.allow_origin, exp_o, "{}", c.suite_test);
                    assert_eq!(a.allow_methods, exp_m, "{}", c.suite_test);
                    assert_eq!(a.vary_origin, c.expect_vary, "{}", c.suite_test);
                }
                _ => assert!(ans.is_none(), "expected no match for {}", c.suite_test),
            }
        }

        // 3. test_cors_origin_wildcard
        let wildcard_rules = parse_cors_xml(
            r#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>"#,
        )
        .expect("test_cors_origin_wildcard parses");
        let ans = match_actual(&wildcard_rules, "example.origin", "GET")
            .expect("test_cors_origin_wildcard matches");
        assert_eq!(ans.allow_origin, "*");
        assert_eq!(ans.allow_methods, vec![CorsMethod::Get]);
        assert!(!ans.vary_origin);

        // 4. test_cors_header_option
        let header_option_rules = parse_cors_xml(
            r#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin><ExposeHeader>x-amz-meta-header1</ExposeHeader></CORSRule></CORSConfiguration>"#,
        )
        .expect("test_cors_header_option parses");
        assert!(
            match_preflight(
                &header_option_rules,
                "example.origin",
                "GET",
                &["x-amz-meta-header2"],
            )
            .is_none(),
            "test_cors_header_option rejects unallowed request header"
        );

        // 5. test_cors_presigned_* (get_object, put_object, tenant, v2, with_acl variants)
        let presigned_suites = [
            ("test_cors_presigned_get_object", CorsMethod::Get),
            ("test_cors_presigned_get_object_tenant", CorsMethod::Get),
            ("test_cors_presigned_get_object_v2", CorsMethod::Get),
            ("test_cors_presigned_get_object_tenant_v2", CorsMethod::Get),
            ("test_cors_presigned_put_object", CorsMethod::Put),
            ("test_cors_presigned_put_object_with_acl", CorsMethod::Put),
            ("test_cors_presigned_put_object_v2", CorsMethod::Put),
            ("test_cors_presigned_put_object_tenant_v2", CorsMethod::Put),
            ("test_cors_presigned_put_object_tenant", CorsMethod::Put),
            ("test_cors_presigned_put_object_tenant_with_acl", CorsMethod::Put),
        ];
        for (name, method) in presigned_suites {
            let xml = format!(
                "<CORSConfiguration><CORSRule><AllowedMethod>{}</AllowedMethod><AllowedOrigin>example</AllowedOrigin></CORSRule></CORSConfiguration>",
                method.as_str()
            );
            let rules = parse_cors_xml(&xml).unwrap_or_else(|e| panic!("{name}: {e:?}"));
            let ans = match_preflight(&rules, "example", method.as_str(), &[])
                .unwrap_or_else(|| panic!("{name}: expected preflight match"));
            assert_eq!(ans.allow_origin, "example", "{name}");
            assert_eq!(ans.allow_methods, vec![method], "{name}");
            assert!(ans.vary_origin, "{name}");
        }
    }

    #[test]
    fn validation_errors_match_s3_codes() {
        // 0 rules
        let err = parse_cors_xml("<CORSConfiguration></CORSConfiguration>").unwrap_err();
        assert_eq!(err.code, "MalformedXML");
        assert_eq!(err.status, 400);

        // 101 rules -> rejected
        let mut many = String::from("<CORSConfiguration>");
        for _ in 0..101 {
            many.push_str("<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>");
        }
        many.push_str("</CORSConfiguration>");
        let err = parse_cors_xml(&many).unwrap_err();
        assert_eq!(err.status, 400);
        assert!(err.code == "MalformedXML" || err.code == "InvalidRequest");

        // 100 rules -> accepted
        let mut hundred = String::from("<CORSConfiguration>");
        for _ in 0..100 {
            hundred.push_str("<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>");
        }
        hundred.push_str("</CORSConfiguration>");
        assert_eq!(parse_cors_xml(&hundred).unwrap().len(), 100);

        // Missing AllowedOrigin
        let err = parse_cors_xml("<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>").unwrap_err();
        assert_eq!(err.code, "MalformedXML");
        assert_eq!(err.status, 400);

        // Missing AllowedMethod
        let err = parse_cors_xml("<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>").unwrap_err();
        assert_eq!(err.code, "MalformedXML");
        assert_eq!(err.status, 400);

        // Unsupported method
        let err = parse_cors_xml("<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>PATCH</AllowedMethod></CORSRule></CORSConfiguration>").unwrap_err();
        assert_eq!(err.code, "InvalidRequest");
        assert_eq!(err.status, 400);

        // Multiple * in AllowedOrigin
        let err = parse_cors_xml("<CORSConfiguration><CORSRule><AllowedOrigin>*foo*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>").unwrap_err();
        assert_eq!(err.code, "InvalidRequest");
        assert_eq!(err.status, 400);

        // Multiple * in AllowedHeader
        let err = parse_cors_xml("<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedHeader>x-*-*</AllowedHeader></CORSRule></CORSConfiguration>").unwrap_err();
        assert_eq!(err.code, "InvalidRequest");
        assert_eq!(err.status, 400);

        // ID > 255 chars
        let long_id = "a".repeat(256);
        let xml = format!("<CORSConfiguration><CORSRule><ID>{long_id}</ID><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>");
        let err = parse_cors_xml(&xml).unwrap_err();
        assert_eq!(err.code, "InvalidRequest");
        assert_eq!(err.status, 400);

        // ID == 255 chars -> Ok
        let ok_id = "a".repeat(255);
        let xml = format!("<CORSConfiguration><CORSRule><ID>{ok_id}</ID><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>");
        assert!(parse_cors_xml(&xml).is_ok());

        // Invalid MaxAgeSeconds
        let err = parse_cors_xml("<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><MaxAgeSeconds>-1</MaxAgeSeconds></CORSRule></CORSConfiguration>").unwrap_err();
        assert_eq!(err.code, "MalformedXML");
        assert_eq!(err.status, 400);

        // Wrong root element
        let err = parse_cors_xml("<NotCORS><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></NotCORS>").unwrap_err();
        assert_eq!(err.code, "MalformedXML");
        assert_eq!(err.status, 400);

        // Unclosed tag
        let err = parse_cors_xml("<CORSConfiguration><CORSRule>").unwrap_err();
        assert_eq!(err.code, "MalformedXML");
        assert_eq!(err.status, 400);
    }

    #[test]
    fn preflight_headers_case_insensitive_and_first_rule_wins() {
        let rules = parse_cors_xml(
            r#"<CORSConfiguration>
                <CORSRule>
                    <ID>rule-1</ID>
                    <AllowedOrigin>https://*.example.com</AllowedOrigin>
                    <AllowedMethod>GET</AllowedMethod>
                    <AllowedHeader>X-Amz-*</AllowedHeader>
                    <MaxAgeSeconds>3600</MaxAgeSeconds>
                    <ExposeHeader>ETag</ExposeHeader>
                </CORSRule>
                <CORSRule>
                    <ID>rule-2</ID>
                    <AllowedOrigin>*</AllowedOrigin>
                    <AllowedMethod>GET</AllowedMethod>
                    <AllowedMethod>POST</AllowedMethod>
                    <AllowedHeader>*</AllowedHeader>
                </CORSRule>
            </CORSConfiguration>"#,
        )
        .unwrap();

        let ans = match_preflight(
            &rules,
            "https://app.example.com",
            "GET",
            &["x-amz-date", "X-Amz-Meta-Foo"],
        )
        .expect("rule-1 matches");
        assert_eq!(ans.allow_origin, "https://app.example.com");
        assert_eq!(ans.allow_methods, vec![CorsMethod::Get]);
        assert_eq!(ans.allow_headers, vec!["x-amz-date", "X-Amz-Meta-Foo"]);
        assert_eq!(ans.max_age_seconds, Some(3600));
        assert_eq!(ans.expose_headers, vec!["ETag"]);
        assert!(ans.vary_origin);

        // Falls through to rule-2 when rule-1 doesn't cover Content-Type
        let ans2 = match_preflight(
            &rules,
            "https://app.example.com",
            "GET",
            &["content-type"],
        )
        .expect("rule-2 matches");
        assert_eq!(ans2.allow_origin, "*");
        assert!(!ans2.vary_origin);
        assert_eq!(ans2.allow_headers, vec!["content-type"]);

        // Two origins including "*" -> echoes request origin and sets Vary: Origin
        let multi_origin = parse_cors_xml(
            r#"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedOrigin>https://a.test</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"#,
        )
        .unwrap();
        let ans3 = match_actual(&multi_origin, "https://b.test", "GET").unwrap();
        assert_eq!(ans3.allow_origin, "https://b.test");
        assert!(ans3.vary_origin);
    }

    #[test]
    fn wildcard_matching_is_linear_time_on_4k_origin() {
        let prefix = "a".repeat(2040);
        let suffix = "b".repeat(2040);
        let pattern = format!("{prefix}*{suffix}");
        let origin_match = format!("{prefix}xyz123{suffix}");
        let origin_mismatch = format!("{prefix}xyz123{}c", "b".repeat(2039));

        let mut cmp_match = 0usize;
        assert!(wildcard_match_counted(&pattern, &origin_match, false, &mut cmp_match));
        assert!(
            cmp_match <= pattern.len() + 1,
            "expected linear comparisons <= {}, got {}",
            pattern.len() + 1,
            cmp_match
        );

        let mut cmp_mismatch = 0usize;
        assert!(!wildcard_match_counted(&pattern, &origin_mismatch, false, &mut cmp_mismatch));
        assert!(
            cmp_mismatch <= pattern.len() + 1,
            "expected linear comparisons <= {}, got {}",
            pattern.len() + 1,
            cmp_mismatch
        );
    }

    fn single_star_string() -> impl Strategy<Value = String> {
        prop_oneof![
            "[^*\\x00]{1,24}".prop_map(|s| s),
            ("[^*\\x00]{0,12}", "[^*\\x00]{0,12}").prop_map(|(a, b)| format!("{a}*{b}")),
        ]
    }

    fn cors_method_strategy() -> impl Strategy<Value = CorsMethod> {
        prop_oneof![
            Just(CorsMethod::Get),
            Just(CorsMethod::Put),
            Just(CorsMethod::Post),
            Just(CorsMethod::Delete),
            Just(CorsMethod::Head),
        ]
    }

    fn cors_rule_strategy() -> impl Strategy<Value = CorsRule> {
        (
            prop::option::of("[^\\x00]{0,40}"),
            prop::collection::vec(single_star_string(), 1..=4),
            prop::collection::vec(cors_method_strategy(), 1..=5),
            prop::collection::vec(single_star_string(), 0..=4),
            prop::option::of(any::<u32>()),
            prop::collection::vec("[^\\x00]{0,24}", 0..=4),
        )
            .prop_map(
                |(id, allowed_origins, allowed_methods, allowed_headers, max_age_seconds, expose_headers)| {
                    CorsRule {
                        id,
                        allowed_origins,
                        allowed_methods,
                        allowed_headers,
                        max_age_seconds,
                        expose_headers,
                    }
                },
            )
    }

    proptest! {
        #[test]
        fn round_trip_cors_xml(rules in prop::collection::vec(cors_rule_strategy(), 1..=10)) {
            let xml = cors_xml(&rules);
            let parsed = parse_cors_xml(&xml);
            prop_assert_eq!(parsed, Ok(rules));
        }
    }
}
