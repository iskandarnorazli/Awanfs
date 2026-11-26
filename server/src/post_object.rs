//! POST object (#119): browser form uploads, signed with SigV4 (owner decision, #119
//! issuecomment-7191269534; AwanFS accepts SigV4 only, `CONFORMANCE_DESIGN.md` section 9).
//!
//! The pure half: the policy document, its signature, its conditions, and the answer. The
//! request side (reading the form, streaming the file) is `ops/post_object.rs`. Nothing here
//! does I/O or reads a clock; callers pass `now`.
//!
//! The policy is base64 JSON: `{"expiration": "2026-12-01T00:00:00Z", "conditions": [...]}`.
//! A condition is `{"field": "value"}`, `["eq", "$field", "value"]`,
//! `["starts-with", "$field", "prefix"]` or `["content-length-range", min, max]`. Operator and
//! field names compare without case; `expiration` and `conditions` do not (the suite checks
//! both). Every form field must be covered by a condition, except `policy`, `x-amz-signature`,
//! `file`, fields starting `x-ignore-`, and `x-amz-checksum-*` (the suite's checksum test sends
//! one without a condition). The bucket always needs a condition, form field or not.

use crate::fsm::Refusal;
use crate::sigv4::{compute_sigv4_signature, parse_iso8601_epoch, SigV4KeyCache};

const fn refuse(code: &'static str, status: u16, why: &'static str) -> Refusal {
    Refusal { code, status, why }
}

pub const MISSING_KEY: Refusal = refuse("InvalidArgument", 400, "Bucket POST must contain a field named 'key'.");
pub const EMPTY_KEY: Refusal = refuse("InvalidArgument", 400, "The key field is empty.");
pub const SIGV2_FORM: Refusal = refuse(
    "InvalidArgument",
    400,
    "POST forms signed with Signature Version 2 are not supported; sign the policy with x-amz-algorithm AWS4-HMAC-SHA256.",
);
pub const MISSING_SIGNATURE: Refusal = refuse(
    "InvalidArgument",
    400,
    "Bucket POST with a policy must contain the fields x-amz-algorithm, x-amz-credential, x-amz-date and x-amz-signature.",
);
pub const SIGNATURE_WITHOUT_POLICY: Refusal =
    refuse("InvalidArgument", 400, "Bucket POST with a signature must contain a field named 'policy'.");
pub const BAD_ALGORITHM: Refusal = refuse("InvalidArgument", 400, "x-amz-algorithm must be AWS4-HMAC-SHA256.");
pub const BAD_CREDENTIAL: Refusal = refuse("InvalidArgument", 400, "x-amz-credential is not <key>/<date>/<region>/s3/aws4_request.");
pub const BAD_DATE: Refusal = refuse("InvalidArgument", 400, "x-amz-date is not a date like 20261126T000000Z matching the credential.");
pub const UNKNOWN_KEY: Refusal = refuse("InvalidAccessKeyId", 403, "The AWS access key Id you provided does not exist in our records.");
pub const BAD_SIGNATURE: Refusal = refuse("SignatureDoesNotMatch", 403, "The policy signature does not match.");
pub const NOT_BASE64: Refusal = refuse("InvalidPolicyDocument", 400, "Invalid Policy: the policy is not base64.");
pub const NOT_JSON: Refusal = refuse("InvalidPolicyDocument", 400, "Invalid Policy: Invalid JSON.");
pub const NO_EXPIRATION: Refusal = refuse("InvalidPolicyDocument", 400, "Invalid Policy: Policy missing expiration.");
pub const BAD_EXPIRATION: Refusal =
    refuse("InvalidPolicyDocument", 400, "Invalid Policy: Invalid 'expiration' value, expected YYYY-MM-DDTHH:MM:SSZ.");
pub const NO_CONDITIONS: Refusal = refuse("InvalidPolicyDocument", 400, "Invalid Policy: Policy missing conditions.");
pub const BAD_CONDITION: Refusal = refuse("InvalidPolicyDocument", 400, "Invalid Policy: Invalid Simple-Condition.");
pub const BAD_RANGE: Refusal = refuse("InvalidPolicyDocument", 400, "Invalid Policy: Invalid content-length-range.");
pub const EXPIRED: Refusal = refuse("AccessDenied", 403, "Invalid according to Policy: Policy expired.");
pub const CONDITION_FAILED: Refusal = refuse("AccessDenied", 403, "Invalid according to Policy: Policy Condition failed.");
pub const EXTRA_FIELD: Refusal = refuse("AccessDenied", 403, "Invalid according to Policy: Extra input fields.");
pub const NO_BUCKET_CONDITION: Refusal =
    refuse("AccessDenied", 403, "Invalid according to Policy: Policy Condition failed: [\"eq\", \"$bucket\"]");
pub const TOO_LARGE: Refusal = refuse("EntityTooLarge", 400, "Your proposed upload exceeds the maximum allowed size.");
pub const TOO_SMALL: Refusal = refuse("EntityTooSmall", 400, "Your proposed upload is smaller than the minimum allowed size.");

/// One condition of the policy. Field names are lower case, without the `$`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Condition {
    Eq { field: String, value: String },
    StartsWith { field: String, prefix: String },
    ContentLengthRange { min: u64, max: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Unix seconds.
    pub expiration: u64,
    pub conditions: Vec<Condition>,
}

/// The form's fields in the order they came, names as sent. Lookups ignore case.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FormFields {
    pub fields: Vec<(String, String)>,
}

impl FormFields {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn has(&self, name: &str) -> bool {
        self.get(name).is_some()
    }
}

/// `YYYY-MM-DDTHH:MM:SS(.fraction)?Z` to Unix seconds; anything else is `None`.
pub fn parse_expiration(s: &str) -> Option<u64> {
    if !s.is_ascii() {
        return None;
    }
    let b = s.as_bytes();
    if b.len() < 20 || *b.last()? != b'Z' {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let rest = &s[19..s.len() - 1];
    if !rest.is_empty() {
        let frac = rest.strip_prefix('.')?;
        if frac.is_empty() || !frac.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
    }
    let basic = format!("{}{}{}T{}{}{}Z", &s[0..4], &s[5..7], &s[8..10], &s[11..13], &s[14..16], &s[17..19]);
    parse_iso8601_epoch(&basic)
}

/// Decodes and validates the policy document. Only its shape is checked here; expiration
/// and conditions are checked against the request by [`check_policy`].
pub fn parse_policy(b64: &str) -> Result<Policy, Refusal> {
    let compact: String = b64.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    let raw = crate::checksum::base64_decode(&compact).ok_or(NOT_BASE64)?;
    let doc: serde_json::Value = serde_json::from_slice(&raw).map_err(|_| NOT_JSON)?;
    let obj = doc.as_object().ok_or(NOT_JSON)?;
    let exp = obj.get("expiration").ok_or(NO_EXPIRATION)?;
    let expiration = exp.as_str().and_then(parse_expiration).ok_or(BAD_EXPIRATION)?;
    let list = obj.get("conditions").and_then(|c| c.as_array()).ok_or(NO_CONDITIONS)?;
    let mut conditions = Vec::with_capacity(list.len());
    for c in list {
        conditions.push(parse_condition(c)?);
    }
    Ok(Policy { expiration, conditions })
}

fn field_name(v: &serde_json::Value) -> Result<String, Refusal> {
    let s = v.as_str().ok_or(BAD_CONDITION)?;
    let name = s.strip_prefix('$').ok_or(BAD_CONDITION)?;
    if name.is_empty() {
        return Err(BAD_CONDITION);
    }
    Ok(name.to_ascii_lowercase())
}

fn range_bound(v: &serde_json::Value) -> Result<u64, Refusal> {
    if let Some(n) = v.as_u64() {
        return Ok(n);
    }
    if v.as_i64().is_some() || v.as_f64().is_some() {
        return Err(BAD_RANGE);
    }
    v.as_str().and_then(|s| s.trim().parse::<u64>().ok()).ok_or(BAD_RANGE)
}

fn parse_condition(c: &serde_json::Value) -> Result<Condition, Refusal> {
    if let Some(map) = c.as_object() {
        if map.len() != 1 {
            return Err(BAD_CONDITION);
        }
        let (k, v) = map.iter().next().ok_or(BAD_CONDITION)?;
        let value = v.as_str().ok_or(BAD_CONDITION)?;
        let field = k.strip_prefix('$').unwrap_or(k);
        if field.is_empty() {
            return Err(BAD_CONDITION);
        }
        return Ok(Condition::Eq { field: field.to_ascii_lowercase(), value: value.to_string() });
    }
    let arr = c.as_array().ok_or(BAD_CONDITION)?;
    let op = arr.first().and_then(|o| o.as_str()).ok_or(BAD_CONDITION)?.to_ascii_lowercase();
    match op.as_str() {
        "eq" | "starts-with" => {
            if arr.len() != 3 {
                return Err(BAD_CONDITION);
            }
            let field = field_name(&arr[1])?;
            let value = arr[2].as_str().ok_or(BAD_CONDITION)?.to_string();
            Ok(if op == "eq" {
                Condition::Eq { field, value }
            } else {
                Condition::StartsWith { field, prefix: value }
            })
        }
        "content-length-range" => {
            if arr.len() != 3 {
                return Err(BAD_RANGE);
            }
            let (min, max) = (range_bound(&arr[1])?, range_bound(&arr[2])?);
            if min > max {
                return Err(BAD_RANGE);
            }
            Ok(Condition::ContentLengthRange { min, max })
        }
        _ => Err(BAD_CONDITION),
    }
}

/// Fields that need no condition.
pub fn exempt_from_conditions(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    n == "policy" || n == "x-amz-signature" || n == "file" || n.starts_with("x-ignore-") || n.starts_with("x-amz-checksum-")
}

/// Checks a signed form's policy against the request: not expired, every condition holds,
/// every field is covered, and the bucket has a condition. `key` is the key after
/// `${filename}` is replaced. The file's size is checked by [`check_size`] once it is known.
pub fn check_policy(policy: &Policy, form: &FormFields, bucket: &str, key: &str, now: u64) -> Result<(), Refusal> {
    if now >= policy.expiration {
        return Err(EXPIRED);
    }
    let value_of = |field: &str| -> Option<String> {
        match field {
            "bucket" => Some(bucket.to_string()),
            "key" => Some(key.to_string()),
            f => form.get(f).map(str::to_string),
        }
    };
    let mut covered: Vec<&str> = Vec::new();
    for c in &policy.conditions {
        match c {
            Condition::Eq { field, value } => {
                if value_of(field).as_deref() != Some(value.as_str()) {
                    return Err(CONDITION_FAILED);
                }
                covered.push(field);
            }
            Condition::StartsWith { field, prefix } => {
                match value_of(field) {
                    Some(v) if v.starts_with(prefix.as_str()) => {}
                    _ => return Err(CONDITION_FAILED),
                }
                covered.push(field);
            }
            Condition::ContentLengthRange { .. } => {}
        }
    }
    if !covered.contains(&"bucket") {
        return Err(NO_BUCKET_CONDITION);
    }
    for (name, _) in &form.fields {
        if exempt_from_conditions(name) {
            continue;
        }
        let lower = name.to_ascii_lowercase();
        if !covered.contains(&lower.as_str()) {
            return Err(EXTRA_FIELD);
        }
    }
    Ok(())
}

/// The file's size against the policy's `content-length-range`, if it has one.
pub fn check_size(policy: Option<&Policy>, size: u64) -> Result<(), Refusal> {
    let Some(p) = policy else {
        return Ok(());
    };
    for c in &p.conditions {
        if let Condition::ContentLengthRange { min, max } = c {
            if size > *max {
                return Err(TOO_LARGE);
            }
            if size < *min {
                return Err(TOO_SMALL);
            }
        }
    }
    Ok(())
}

/// The object key: the `key` field with `${filename}` replaced by the file's name (its last
/// path segment, as browsers on Windows send the whole path).
pub fn object_key(form: &FormFields, filename: Option<&str>) -> Result<String, Refusal> {
    let raw = form.get("key").ok_or(MISSING_KEY)?;
    let name = filename.map(|f| f.rsplit(['/', '\\']).next().unwrap_or(f)).unwrap_or("");
    let key = raw.replace("${filename}", name);
    if key.is_empty() {
        return Err(EMPTY_KEY);
    }
    Ok(key)
}

/// Who signed the form, or `None` for an unsigned form (no policy, no signature fields).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormSignature {
    pub access_key: String,
    pub date8: String,
    pub region: String,
    pub service: String,
    pub signature: String,
    pub policy_b64: String,
}

/// Reads the SigV4 fields. `Ok(None)` is an unsigned form; a SigV2 form is refused.
pub fn read_signature(form: &FormFields) -> Result<Option<FormSignature>, Refusal> {
    let policy = form.get("policy");
    let v4 = ["x-amz-algorithm", "x-amz-credential", "x-amz-date", "x-amz-signature"];
    let has_v4 = v4.iter().any(|f| form.has(f));
    let has_v2 = form.has("awsaccesskeyid") || form.has("signature");
    let Some(policy) = policy else {
        if has_v4 || has_v2 {
            return Err(SIGNATURE_WITHOUT_POLICY);
        }
        return Ok(None);
    };
    if has_v2 && !has_v4 {
        return Err(SIGV2_FORM);
    }
    let (Some(alg), Some(cred), Some(date), Some(sig)) =
        (form.get(v4[0]), form.get(v4[1]), form.get(v4[2]), form.get(v4[3]))
    else {
        return Err(MISSING_SIGNATURE);
    };
    if alg != "AWS4-HMAC-SHA256" {
        return Err(BAD_ALGORITHM);
    }
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" || parts.iter().take(4).any(|p| p.is_empty()) {
        return Err(BAD_CREDENTIAL);
    }
    if parse_iso8601_epoch(date).is_none() || !date.starts_with(parts[1]) || parts[1].len() != 8 {
        return Err(BAD_DATE);
    }
    Ok(Some(FormSignature {
        access_key: parts[0].to_string(),
        date8: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
        signature: sig.trim().to_ascii_lowercase(),
        policy_b64: policy.to_string(),
    }))
}

/// The signature a form signed with `secret` must carry: hex HMAC-SHA256 of the base64
/// policy, with the SigV4 signing key of the credential's date, region and service.
pub fn expected_signature(cache: &SigV4KeyCache, secret: &str, sig: &FormSignature) -> String {
    compute_sigv4_signature(cache, secret, &sig.date8, &sig.region, &sig.service, &sig.policy_b64)
}

/// How a successful POST answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Success {
    /// 303 to this URL.
    Redirect(String),
    /// 200 or 204 with an empty body.
    Empty(u16),
    /// 201 with the `PostResponse` XML.
    Created,
}

/// `success_action_redirect` (or the older `redirect`) wins; then `success_action_status`
/// 200, 201 or 204; anything else is 204.
pub fn success(form: &FormFields, bucket: &str, key: &str, etag: &str) -> Success {
    if let Some(url) = form.get("success_action_redirect").or_else(|| form.get("redirect")) {
        if url.starts_with("http://") || url.starts_with("https://") {
            let sep = if url.contains('?') { '&' } else { '?' };
            let quoted = format!("\"{}\"", etag.trim_matches('"'));
            return Success::Redirect(format!(
                "{}{}bucket={}&key={}&etag={}",
                url,
                sep,
                query_encode(bucket),
                query_encode(key),
                query_encode(&quoted)
            ));
        }
    }
    match form.get("success_action_status").map(str::trim) {
        Some("200") => Success::Empty(200),
        Some("201") => Success::Created,
        _ => Success::Empty(204),
    }
}

/// The 201 body.
pub fn post_response_xml(location: &str, bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PostResponse><Location>{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></PostResponse>",
        crate::xml::xml_escape(location),
        crate::xml::xml_escape(bucket),
        crate::xml::xml_escape(key),
        crate::xml::xml_escape(&format!("\"{}\"", etag.trim_matches('"')))
    )
}

/// Percent-encodes everything but unreserved characters.
pub fn query_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// The form's `tagging` XML as the `x-amz-tagging` header PUT reads.
pub fn tagging_header(xml: &str) -> Result<String, Refusal> {
    let set = crate::tagging::parse_tagging_xml(xml, crate::tagging::Scope::Object)?;
    Ok(set
        .tags
        .iter()
        .map(|t| format!("{}={}", query_encode(&t.key), query_encode(&t.value)))
        .collect::<Vec<_>>()
        .join("&"))
}

/// Form fields that become request headers of the stored object, as PUT would read them.
pub fn object_header_fields(form: &FormFields) -> Vec<(String, String)> {
    const PLAIN: [&str; 5] = ["content-type", "cache-control", "content-disposition", "content-encoding", "expires"];
    let mut out = Vec::new();
    for (name, value) in &form.fields {
        let n = name.to_ascii_lowercase();
        if PLAIN.contains(&n.as_str()) || n.starts_with("x-amz-meta-") || n.starts_with("x-amz-checksum-") {
            out.push((n, value.clone()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b64(s: &str) -> String {
        crate::bucket_config::base64_encode(s.as_bytes())
    }

    fn form(pairs: &[(&str, &str)]) -> FormFields {
        FormFields { fields: pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect() }
    }

    const FAR: &str = "2099-01-01T00:00:00Z";
    const NOW: u64 = 1_800_000_000;

    #[test]
    fn expiration_needs_the_extended_utc_form() {
        assert_eq!(parse_expiration("1970-01-01T00:00:10Z"), Some(10));
        assert_eq!(parse_expiration("1970-01-01T00:00:10.123Z"), Some(10));
        for bad in ["2026-11-26 13:00:00.123+00:00", "20261126T000000Z", "2026-11-26T00:00:00", "2026-11-26T00:00:00.Z", ""] {
            assert_eq!(parse_expiration(bad), None, "{:?}", bad);
        }
    }

    #[test]
    fn a_policy_without_expiration_or_conditions_is_refused_and_both_names_are_case_sensitive() {
        let c = r#"[{"bucket": "b"}]"#;
        assert_eq!(parse_policy(&b64(&format!(r#"{{"conditions": {}}}"#, c))), Err(NO_EXPIRATION));
        assert_eq!(parse_policy(&b64(&format!(r#"{{"EXPIRATION": "{}", "conditions": {}}}"#, FAR, c))), Err(NO_EXPIRATION));
        assert_eq!(parse_policy(&b64(&format!(r#"{{"expiration": "{}"}}"#, FAR))), Err(NO_CONDITIONS));
        assert_eq!(parse_policy(&b64(&format!(r#"{{"expiration": "{}", "CONDITIONS": {}}}"#, FAR, c))), Err(NO_CONDITIONS));
        assert_eq!(parse_policy(&b64(&format!(r#"{{"expiration": "2026-11-26 13:00:00", "conditions": {}}}"#, c))), Err(BAD_EXPIRATION));
        assert_eq!(parse_policy("not base64 !!"), Err(NOT_BASE64));
        assert_eq!(parse_policy(&b64("{not json")), Err(NOT_JSON));
    }

    #[test]
    fn conditions_parse_in_every_form_and_bad_ones_are_refused() {
        let p = parse_policy(&b64(&format!(
            r#"{{"expiration": "{}", "conditions": [{{"bUcKeT": "b"}}, ["StArTs-WiTh", "$KeY", "foo"], ["eq", "$acl", "private"], ["content-length-range", 0, 1024]]}}"#,
            FAR
        )))
        .unwrap();
        assert_eq!(
            p.conditions,
            vec![
                Condition::Eq { field: "bucket".into(), value: "b".into() },
                Condition::StartsWith { field: "key".into(), prefix: "foo".into() },
                Condition::Eq { field: "acl".into(), value: "private".into() },
                Condition::ContentLengthRange { min: 0, max: 1024 },
            ]
        );
        for (bad, want) in [
            ("[{}]", BAD_CONDITION),
            (r#"[["content-length-range", 0]]"#, BAD_RANGE),
            (r#"[["content-length-range", -1, 0]]"#, BAD_RANGE),
            (r#"[["content-length-range", 10, 5]]"#, BAD_RANGE),
            (r#"[["starts-with", "key", "foo"]]"#, BAD_CONDITION),
            (r#"[["matches", "$key", "foo"]]"#, BAD_CONDITION),
            (r#"[{"a": "1", "b": "2"}]"#, BAD_CONDITION),
        ] {
            let doc = format!(r#"{{"expiration": "{}", "conditions": {}}}"#, FAR, bad);
            assert_eq!(parse_policy(&b64(&doc)), Err(want), "{}", bad);
        }
    }

    fn policy(conds: &str) -> Policy {
        parse_policy(&b64(&format!(r#"{{"expiration": "{}", "conditions": [{}]}}"#, FAR, conds))).unwrap()
    }

    const SIGNED: [(&str, &str); 4] = [
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-credential", "AK/20261126/us-east-1/s3/aws4_request"),
        ("x-amz-date", "20261126T000000Z"),
        ("x-amz-signature", "00"),
    ];
    const SIGNED_CONDS: &str = r#"{"x-amz-algorithm": "AWS4-HMAC-SHA256"}, {"x-amz-credential": "AK/20261126/us-east-1/s3/aws4_request"}, {"x-amz-date": "20261126T000000Z"}"#;

    fn signed_form(extra: &[(&str, &str)]) -> FormFields {
        let mut f = form(extra);
        f.fields.extend(SIGNED.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        f.fields.push(("policy".into(), "p".into()));
        f
    }

    #[test]
    fn a_policy_that_holds_passes_and_each_failure_has_its_own_refusal() {
        let p = policy(&format!(r#"{{"bucket": "b"}}, ["starts-with", "$key", "foo"], {{"acl": "private"}}, {}"#, SIGNED_CONDS));
        let f = signed_form(&[("key", "foo.txt"), ("acl", "private")]);
        assert_eq!(check_policy(&p, &f, "b", "foo.txt", NOW), Ok(()));
        assert_eq!(check_policy(&p, &f, "other", "foo.txt", NOW), Err(CONDITION_FAILED), "wrong bucket");
        assert_eq!(check_policy(&p, &f, "b", "bar.txt", NOW), Err(CONDITION_FAILED), "key prefix");
        let extra = signed_form(&[("key", "foo.txt"), ("acl", "private"), ("x-amz-meta-foo", "x")]);
        assert_eq!(check_policy(&p, &extra, "b", "foo.txt", NOW), Err(EXTRA_FIELD), "uncovered field");
        let ignored = signed_form(&[("key", "foo.txt"), ("acl", "private"), ("x-ignore-foo", "x"), ("x-amz-checksum-sha256", "y")]);
        assert_eq!(check_policy(&p, &ignored, "b", "foo.txt", NOW), Ok(()), "x-ignore- and checksum fields need no condition");
        let expired = Policy { expiration: NOW, ..p.clone() };
        assert_eq!(check_policy(&expired, &f, "b", "foo.txt", NOW), Err(EXPIRED));
    }

    #[test]
    fn a_policy_without_a_bucket_condition_is_refused() {
        let p = policy(&format!(r#"["starts-with", "$key", "foo"], {}"#, SIGNED_CONDS));
        assert_eq!(check_policy(&p, &signed_form(&[("key", "foo.txt")]), "b", "foo.txt", NOW), Err(NO_BUCKET_CONDITION));
    }

    #[test]
    fn a_condition_on_a_field_the_form_lacks_fails() {
        let p = policy(&format!(r#"{{"bucket": "b"}}, ["starts-with", "$key", ""], ["starts-with", "$x-amz-meta-foo", "bar"], {}"#, SIGNED_CONDS));
        assert_eq!(check_policy(&p, &signed_form(&[("key", "k")]), "b", "k", NOW), Err(CONDITION_FAILED));
        let eq_empty = policy(&format!(r#"{{"bucket": "b"}}, ["starts-with", "$key", ""], ["eq", "$x-amz-meta-foo", ""], {}"#, SIGNED_CONDS));
        let f = signed_form(&[("key", "k"), ("x-amz-meta-foo", "barclamp")]);
        assert_eq!(check_policy(&eq_empty, &f, "b", "k", NOW), Err(CONDITION_FAILED));
    }

    #[test]
    fn size_is_checked_against_the_range() {
        let p = policy(r#"{"bucket": "b"}, ["content-length-range", 512, 1000]"#);
        assert_eq!(check_size(Some(&p), 3), Err(TOO_SMALL));
        assert_eq!(check_size(Some(&p), 1001), Err(TOO_LARGE));
        assert_eq!(check_size(Some(&p), 512), Ok(()));
        assert_eq!(check_size(Some(&p), 1000), Ok(()));
        assert_eq!(check_size(None, u64::MAX), Ok(()));
        let zero = policy(r#"{"bucket": "b"}, ["content-length-range", 0, 0]"#);
        assert_eq!(check_size(Some(&zero), 3), Err(TOO_LARGE));
    }

    #[test]
    fn the_key_takes_the_filename_and_a_missing_key_is_refused() {
        assert_eq!(object_key(&form(&[("key", "up/${filename}")]), Some("foo.txt")).as_deref(), Ok("up/foo.txt"));
        assert_eq!(object_key(&form(&[("KeY", "${filename}")]), Some(r"C:\docs\foo.txt")).as_deref(), Ok("foo.txt"));
        assert_eq!(object_key(&form(&[("key", r"\$foo.txt")]), None).as_deref(), Ok(r"\$foo.txt"));
        assert_eq!(object_key(&form(&[("acl", "private")]), None), Err(MISSING_KEY));
        assert_eq!(object_key(&form(&[("key", "${filename}")]), None), Err(EMPTY_KEY));
    }

    #[test]
    fn signature_fields_are_read_and_sigv2_is_refused() {
        assert_eq!(read_signature(&form(&[("key", "k")])), Ok(None), "an unsigned form");
        let v2 = form(&[("key", "k"), ("AWSAccessKeyId", "AK"), ("signature", "s"), ("policy", "p")]);
        assert_eq!(read_signature(&v2), Err(SIGV2_FORM));
        assert_eq!(read_signature(&form(&[("x-amz-signature", "s")])), Err(SIGNATURE_WITHOUT_POLICY));
        let missing = form(&[("policy", "p"), ("x-amz-algorithm", "AWS4-HMAC-SHA256"), ("x-amz-credential", "AK/20261126/r/s3/aws4_request"), ("x-amz-date", "20261126T000000Z")]);
        assert_eq!(read_signature(&missing), Err(MISSING_SIGNATURE));
        let ok = signed_form(&[]);
        let sig = read_signature(&ok).unwrap().unwrap();
        assert_eq!((sig.access_key.as_str(), sig.date8.as_str(), sig.region.as_str()), ("AK", "20261126", "us-east-1"));
        let mut wrong_day = signed_form(&[]);
        wrong_day.fields.retain(|(k, _)| k != "x-amz-date");
        wrong_day.fields.push(("x-amz-date".into(), "20261127T000000Z".into()));
        assert_eq!(read_signature(&wrong_day), Err(BAD_DATE));
    }

    /// AWS's documented SigV4 POST example ("Example: Browser-Based Upload using HTTP POST
    /// (Using AWS Signature Version 4)", bucket sigv4examplebucket): its policy parses, and
    /// its signature is the one AWS lists.
    #[test]
    fn the_aws_documented_post_example_signs_and_parses() {
        let policy_b64 = "eyAiZXhwaXJhdGlvbiI6ICIyMDE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLA0KICAiY29uZGl0aW9ucyI6IFsNCiAgICB7ImJ1Y2tldCI6ICJzaWd2NGV4YW1wbGVidWNrZXQifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS8iXSwNCiAgICB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LA0KICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL3NpZ3Y0ZXhhbXBsZWJ1Y2tldC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRDb250ZW50LVR5cGUiLCAiaW1hZ2UvIl0sDQogICAgeyJ4LWFtei1tZXRhLXV1aWQiOiAiMTQzNjUxMjM2NTEyNzQifSwNCiAgICB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sDQogICAgWyJzdGFydHMtd2l0aCIsICIkeC1hbXotbWV0YS10YWciLCAiIl0sDQoNCiAgICB7IngtYW16LWNyZWRlbnRpYWwiOiAiQUtJQUlPU0ZPRE5ON0VYQU1QTEUvMjAxNTEyMjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LA0KICAgIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwNCiAgICB7IngtYW16LWRhdGUiOiAiMjAxNTEyMjlUMDAwMDAwWiIgfQ0KICBdDQp9";
        let sig = FormSignature {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            date8: "20151229".into(),
            region: "us-east-1".into(),
            service: "s3".into(),
            signature: String::new(),
            policy_b64: policy_b64.into(),
        };
        assert_eq!(
            expected_signature(&SigV4KeyCache::new(), "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", &sig),
            "8afdbf4008c03f22c2cd3cdb72e4afbb1f6a588f3255ac628749a66d7f09699e"
        );
        let p = parse_policy(policy_b64).unwrap();
        assert_eq!(p.expiration, parse_expiration("2015-12-30T12:00:00Z").unwrap());
        assert_eq!(p.conditions.len(), 11);
    }

    #[test]
    fn success_picks_redirect_then_status() {
        let etag = "\"abc\"";
        let r = success(&form(&[("success_action_redirect", "http://h/b")]), "b", "foo.txt", etag);
        assert_eq!(r, Success::Redirect("http://h/b?bucket=b&key=foo.txt&etag=%22abc%22".into()));
        assert_eq!(success(&form(&[("success_action_status", "201")]), "b", "k", etag), Success::Created);
        assert_eq!(success(&form(&[("success_action_status", "200")]), "b", "k", etag), Success::Empty(200));
        assert_eq!(success(&form(&[("success_action_status", "404")]), "b", "k", etag), Success::Empty(204));
        assert_eq!(success(&form(&[]), "b", "k", etag), Success::Empty(204));
    }

    #[test]
    fn tagging_xml_becomes_the_put_header() {
        let xml = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>a b</Key><Value>1</Value></Tag></TagSet></Tagging>";
        assert_eq!(tagging_header(xml).as_deref(), Ok("0=0&a%20b=1"));
        assert!(tagging_header("<Tagging>").is_err());
    }
}
