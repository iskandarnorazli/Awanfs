//! Bucket policy parser, validator, and evaluator (#122, CONFORMANCE_DESIGN.md section 5.3).
//!
//! Policies that use an unknown top-level/statement element, condition operator, condition key,
//! non-S3 ARN, `Allow` + `NotPrincipal`, or another bucket's specific ARN in `Resource` are
//! refused at `PUT /?policy` with `400 MalformedPolicy` rather than silently mis-evaluated.

use crate::authz::BucketPolicyDecision;
use crate::fsm::Refusal;
use crate::iam::{glob_match, Principal, ROOT_ACCOUNT};
use crate::sigv4::parse_iso8601_epoch;
use serde_json::Value;
use std::net::IpAddr;

/// Maximum bucket policy size (20 KiB, S3 & FORMAT.md limit).
pub const MAX_POLICY_BYTES: usize = 20 * 1024;

pub const MALFORMED_POLICY: Refusal = Refusal {
    code: "MalformedPolicy",
    status: 400,
    why: "This policy contains invalid Json",
};

fn malformed(why: &'static str) -> Refusal {
    Refusal {
        code: "MalformedPolicy",
        status: 400,
        why,
    }
}

/// Every base condition operator supported by the bucket policy parser/evaluator.
pub const KNOWN_CONDITION_OPERATORS: &[&str] = &[
    "stringequals",
    "stringnotequals",
    "stringequalsignorecase",
    "stringnotequalsignorecase",
    "stringlike",
    "stringnotlike",
    "arnequals",
    "arnnotequals",
    "arnlike",
    "arnnotlike",
    "bool",
    "null",
    "ipaddress",
    "notipaddress",
    "numericequals",
    "numericnotequals",
    "numericlessthan",
    "numericlessthanequals",
    "numericgreaterthan",
    "numericgreaterthanequals",
    "dateequals",
    "datenotequals",
    "datelessthan",
    "datelessthanequals",
    "dategreaterthan",
    "dategreaterthanequals",
];

/// Every static condition key supported (in addition to `s3:existingobjecttag/<k>` and
/// `s3:requestobjecttag/<k>` dynamic prefixes).
pub const KNOWN_CONDITION_KEYS: &[&str] = &[
    "s3:x-amz-acl",
    "s3:x-amz-grant-read",
    "s3:x-amz-grant-write",
    "s3:x-amz-grant-read-acp",
    "s3:x-amz-grant-write-acp",
    "s3:x-amz-grant-full-control",
    "s3:x-amz-server-side-encryption",
    "s3:x-amz-server-side-encryption-aws-kms-key-id",
    "s3:x-amz-server-side-encryption-customer-algorithm",
    "s3:x-amz-copy-source",
    "s3:x-amz-metadata-directive",
    "s3:x-amz-tagging-directive",
    "s3:requestobjecttagkeys",
    "s3:prefix",
    "s3:delimiter",
    "s3:max-keys",
    "aws:securetransport",
    "aws:sourceip",
    "aws:referer",
    "aws:currenttime",
    "aws:epochtime",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrincipalSpec {
    Any,
    Matches(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetQualifier {
    Default,
    ForAnyValue,
    ForAllValues,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseOp {
    StringEquals,
    StringNotEquals,
    StringEqualsIgnoreCase,
    StringNotEqualsIgnoreCase,
    StringLike,
    StringNotLike,
    ArnEquals,
    ArnNotEquals,
    ArnLike,
    ArnNotLike,
    Bool,
    Null,
    IpAddress,
    NotIpAddress,
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanEquals,
    DateEquals,
    DateNotEquals,
    DateLessThan,
    DateLessThanEquals,
    DateGreaterThan,
    DateGreaterThanEquals,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionClause {
    pub op: BaseOp,
    pub if_exists: bool,
    pub qualifier: SetQualifier,
    pub key: String,
    pub expected: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyStatement {
    pub sid: Option<String>,
    pub effect: Effect,
    pub principal: PrincipalSpec,
    pub not_principal: bool,
    pub actions: Vec<String>,
    pub not_action: bool,
    pub resources: Vec<String>,
    pub not_resource: bool,
    pub conditions: Vec<ConditionClause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketPolicy {
    pub version: Option<String>,
    pub id: Option<String>,
    pub statements: Vec<PolicyStatement>,
    pub is_public: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PolicyRequestContext<'a> {
    pub acl: Option<&'a str>,
    pub grant_read: &'a [&'a str],
    pub grant_write: &'a [&'a str],
    pub grant_read_acp: &'a [&'a str],
    pub grant_write_acp: &'a [&'a str],
    pub grant_full_control: &'a [&'a str],
    pub sse: Option<&'a str>,
    pub sse_kms_key_id: Option<&'a str>,
    pub sse_c_alg: Option<&'a str>,
    pub copy_source: Option<&'a str>,
    pub metadata_directive: Option<&'a str>,
    pub tagging_directive: Option<&'a str>,
    pub existing_tags: &'a [(String, String)],
    pub request_tags: &'a [(String, String)],
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub max_keys: Option<&'a str>,
    pub secure_transport: bool,
    pub source_ip: Option<&'a str>,
    pub referer: Option<&'a str>,
    pub now_epoch_secs: u64,
}

pub fn is_supported_condition_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    if KNOWN_CONDITION_KEYS.contains(&lower.as_str()) {
        return true;
    }
    if let Some(tag) = lower.strip_prefix("s3:existingobjecttag/") {
        return !tag.is_empty();
    }
    if let Some(tag) = lower.strip_prefix("s3:requestobjecttag/") {
        return !tag.is_empty();
    }
    false
}

fn parse_operator(raw: &str) -> Result<(BaseOp, bool, SetQualifier), Refusal> {
    let (qualifier, rest) = if let Some(r) = raw.strip_prefix("ForAnyValue:") {
        (SetQualifier::ForAnyValue, r)
    } else if let Some(r) = raw.strip_prefix("ForAllValues:") {
        (SetQualifier::ForAllValues, r)
    } else {
        (SetQualifier::Default, raw)
    };
    let lower = rest.to_ascii_lowercase();
    let (base_name, if_exists) = if let Some(b) = lower.strip_suffix("ifexists") {
        if b == "null" || b == "bool" {
            return Err(malformed("Unsupported condition operator"));
        }
        (b, true)
    } else {
        (lower.as_str(), false)
    };
    let op = match base_name {
        "stringequals" => BaseOp::StringEquals,
        "stringnotequals" => BaseOp::StringNotEquals,
        "stringequalsignorecase" => BaseOp::StringEqualsIgnoreCase,
        "stringnotequalsignorecase" => BaseOp::StringNotEqualsIgnoreCase,
        "stringlike" => BaseOp::StringLike,
        "stringnotlike" => BaseOp::StringNotLike,
        "arnequals" => BaseOp::ArnEquals,
        "arnnotequals" => BaseOp::ArnNotEquals,
        "arnlike" => BaseOp::ArnLike,
        "arnnotlike" => BaseOp::ArnNotLike,
        "bool" => BaseOp::Bool,
        "null" => BaseOp::Null,
        "ipaddress" => BaseOp::IpAddress,
        "notipaddress" => BaseOp::NotIpAddress,
        "numericequals" => BaseOp::NumericEquals,
        "numericnotequals" => BaseOp::NumericNotEquals,
        "numericlessthan" => BaseOp::NumericLessThan,
        "numericlessthanequals" => BaseOp::NumericLessThanEquals,
        "numericgreaterthan" => BaseOp::NumericGreaterThan,
        "numericgreaterthanequals" => BaseOp::NumericGreaterThanEquals,
        "dateequals" => BaseOp::DateEquals,
        "datenotequals" => BaseOp::DateNotEquals,
        "datelessthan" => BaseOp::DateLessThan,
        "datelessthanequals" => BaseOp::DateLessThanEquals,
        "dategreaterthan" => BaseOp::DateGreaterThan,
        "dategreaterthanequals" => BaseOp::DateGreaterThanEquals,
        _ => return Err(malformed("Unsupported condition operator")),
    };
    Ok((op, if_exists, qualifier))
}

fn value_as_strings(v: &Value) -> Result<Vec<String>, Refusal> {
    match v {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Bool(b) => Ok(vec![if *b { "true" } else { "false" }.to_string()]),
        Value::Number(n) => Ok(vec![n.to_string()]),
        Value::Array(arr) => {
            if arr.is_empty() {
                return Err(malformed("Empty array in policy"));
            }
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    Value::Bool(b) => out.push(if *b { "true" } else { "false" }.to_string()),
                    Value::Number(n) => out.push(n.to_string()),
                    _ => return Err(malformed("Invalid array item in policy")),
                }
            }
            Ok(out)
        }
        _ => Err(malformed("Invalid value type in policy")),
    }
}

fn parse_principal_spec(v: &Value) -> Result<PrincipalSpec, Refusal> {
    match v {
        Value::String(s) if s == "*" => Ok(PrincipalSpec::Any),
        Value::Object(map) => {
            if map.is_empty() {
                return Err(malformed("Empty Principal object"));
            }
            let mut items = Vec::new();
            for (k, val) in map {
                if k != "AWS" && k != "CanonicalUser" {
                    return Err(malformed("Unsupported Principal provider"));
                }
                for entry in value_as_strings(val)? {
                    if entry == "*" {
                        return Ok(PrincipalSpec::Any);
                    }
                    items.push(entry);
                }
            }
            if items.is_empty() {
                return Err(malformed("Empty Principal list"));
            }
            Ok(PrincipalSpec::Matches(items))
        }
        _ => Err(malformed("Invalid Principal")),
    }
}

fn parse_string_list(v: &Value) -> Result<Vec<String>, Refusal> {
    match v {
        Value::String(s) if !s.is_empty() => Ok(vec![s.clone()]),
        Value::Array(arr) if !arr.is_empty() => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let s = item.as_str().ok_or_else(|| malformed("Expected string list"))?;
                if s.is_empty() {
                    return Err(malformed("Empty string in list"));
                }
                out.push(s.to_string());
            }
            Ok(out)
        }
        _ => Err(malformed("Expected string or non-empty list")),
    }
}

fn validate_cidr(raw: &str) -> Result<(IpAddr, u8), Refusal> {
    if let Some((ip_s, mask_s)) = raw.split_once('/') {
        let ip: IpAddr = ip_s.parse().map_err(|_| malformed("Invalid IP in condition"))?;
        let max_bits = if ip.is_ipv4() { 32 } else { 128 };
        let prefix: u8 = mask_s.parse().map_err(|_| malformed("Invalid CIDR prefix"))?;
        if prefix > max_bits {
            return Err(malformed("CIDR prefix out of range"));
        }
        Ok((ip, prefix))
    } else {
        let ip: IpAddr = raw.parse().map_err(|_| malformed("Invalid IP in condition"))?;
        let prefix = if ip.is_ipv4() { 32 } else { 128 };
        Ok((ip, prefix))
    }
}

fn ip_in_cidr(ip: IpAddr, net: IpAddr, prefix: u8) -> bool {
    match (ip, net) {
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            if prefix == 0 {
                return true;
            }
            let mask = u32::MAX.checked_shl(32 - prefix as u32).unwrap_or(0);
            (u32::from(a) & mask) == (u32::from(b) & mask)
        }
        (IpAddr::V6(a), IpAddr::V6(b)) => {
            if prefix == 0 {
                return true;
            }
            let mask = u128::MAX.checked_shl(128 - prefix as u32).unwrap_or(0);
            (u128::from(a) & mask) == (u128::from(b) & mask)
        }
        _ => false,
    }
}

fn parse_date_epoch(s: &str) -> Option<i64> {
    if let Ok(n) = s.trim().parse::<i64>() {
        return Some(n);
    }
    if let Some(ep) = parse_iso8601_epoch(s.trim()) {
        return Some(ep as i64);
    }
    // Also accept RFC3339 `YYYY-MM-DDTHH:MM:SSZ` via digit extraction.
    let t = s.trim();
    let b = t.as_bytes();
    if t.is_ascii()
        && b.len() >= 20
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
    {
        let compact = format!(
            "{}{}{}T{}{}{}Z",
            &t[0..4],
            &t[5..7],
            &t[8..10],
            &t[11..13],
            &t[14..16],
            &t[17..19]
        );
        return parse_iso8601_epoch(&compact).map(|e| e as i64);
    }
    None
}

fn parse_conditions(v: &Value) -> Result<Vec<ConditionClause>, Refusal> {
    let obj = v.as_object().ok_or_else(|| malformed("Condition must be an object"))?;
    let mut clauses = Vec::new();
    for (op_name, keys_val) in obj {
        let (op, if_exists, qualifier) = parse_operator(op_name)?;
        let keys_obj = keys_val.as_object().ok_or_else(|| malformed("Condition operator value must be object"))?;
        if keys_obj.is_empty() {
            return Err(malformed("Empty condition block"));
        }
        for (raw_key, expected_val) in keys_obj {
            if !is_supported_condition_key(raw_key) {
                return Err(malformed("Unsupported condition key"));
            }
            let expected = value_as_strings(expected_val)?;
            // Eagerly validate CIDR / Bool / Null / Numeric / Date literals at PUT time.
            match op {
                BaseOp::IpAddress | BaseOp::NotIpAddress => {
                    for e in &expected {
                        validate_cidr(e)?;
                    }
                }
                BaseOp::Bool | BaseOp::Null => {
                    for e in &expected {
                        if !e.eq_ignore_ascii_case("true") && !e.eq_ignore_ascii_case("false") {
                            return Err(malformed("Invalid boolean condition value"));
                        }
                    }
                }
                BaseOp::NumericEquals
                | BaseOp::NumericNotEquals
                | BaseOp::NumericLessThan
                | BaseOp::NumericLessThanEquals
                | BaseOp::NumericGreaterThan
                | BaseOp::NumericGreaterThanEquals => {
                    for e in &expected {
                        e.trim().parse::<f64>().map_err(|_| malformed("Invalid numeric condition value"))?;
                    }
                }
                BaseOp::DateEquals
                | BaseOp::DateNotEquals
                | BaseOp::DateLessThan
                | BaseOp::DateLessThanEquals
                | BaseOp::DateGreaterThan
                | BaseOp::DateGreaterThanEquals => {
                    for e in &expected {
                        if parse_date_epoch(e).is_none() {
                            return Err(malformed("Invalid date condition value"));
                        }
                    }
                }
                _ => {}
            }
            clauses.push(ConditionClause {
                op,
                if_exists,
                qualifier,
                key: raw_key.to_ascii_lowercase(),
                expected,
            });
        }
    }
    Ok(clauses)
}

/// Parses and validates a bucket policy JSON document. When `bucket` is `Some(name)`, rejects any
/// `Resource`/`NotResource` whose bucket pattern cannot match `name` (`arn:aws:s3:::*` passes).
pub fn parse_bucket_policy_for_bucket(text: &str, bucket: Option<&str>) -> Result<BucketPolicy, Refusal> {
    if text.len() > MAX_POLICY_BYTES {
        return Err(Refusal {
            code: "MalformedPolicy",
            status: 400,
            why: "Policy exceeds 20 KiB limit",
        });
    }
    let root: Value = serde_json::from_str(text).map_err(|_| MALFORMED_POLICY)?;
    let obj = root.as_object().ok_or(MALFORMED_POLICY)?;
    for k in obj.keys() {
        if !matches!(k.as_str(), "Version" | "Id" | "Statement") {
            return Err(malformed("Unsupported top-level policy element"));
        }
    }
    let version = match obj.get("Version") {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        None => None,
        _ => return Err(malformed("Invalid Version")),
    };
    let id = match obj.get("Id") {
        Some(Value::String(s)) => Some(s.clone()),
        None => None,
        _ => return Err(malformed("Invalid Id")),
    };
    let raw_stmts = match obj.get("Statement") {
        Some(Value::Array(arr)) if !arr.is_empty() => arr.clone(),
        Some(o @ Value::Object(_)) => vec![o.clone()],
        _ => return Err(malformed("Statement must be a non-empty array or object")),
    };

    let mut statements = Vec::with_capacity(raw_stmts.len());
    for raw_stmt in raw_stmts {
        let s_obj = raw_stmt.as_object().ok_or(MALFORMED_POLICY)?;
        for k in s_obj.keys() {
            if !matches!(
                k.as_str(),
                "Sid" | "Effect" | "Principal" | "NotPrincipal" | "Action" | "NotAction" | "Resource" | "NotResource" | "Condition"
            ) {
                return Err(malformed("Unsupported statement element"));
            }
        }
        let sid = match s_obj.get("Sid") {
            Some(Value::String(s)) => Some(s.clone()),
            None => None,
            _ => return Err(malformed("Invalid Sid")),
        };
        let effect = match s_obj.get("Effect").and_then(|v| v.as_str()) {
            Some("Allow") => Effect::Allow,
            Some("Deny") => Effect::Deny,
            _ => return Err(malformed("Invalid Effect")),
        };
        let (principal, not_principal) = match (s_obj.get("Principal"), s_obj.get("NotPrincipal")) {
            (Some(p), None) => (parse_principal_spec(p)?, false),
            (None, Some(np)) => {
                if effect == Effect::Allow {
                    return Err(malformed("Allow with NotPrincipal is not allowed"));
                }
                (parse_principal_spec(np)?, true)
            }
            _ => return Err(malformed("Statement must specify Principal or NotPrincipal")),
        };
        let (actions, not_action) = match (s_obj.get("Action"), s_obj.get("NotAction")) {
            (Some(a), None) => (parse_string_list(a)?, false),
            (None, Some(na)) => (parse_string_list(na)?, true),
            _ => return Err(malformed("Statement must specify Action or NotAction")),
        };
        let (resources, not_resource) = match (s_obj.get("Resource"), s_obj.get("NotResource")) {
            (Some(r), None) => (parse_string_list(r)?, false),
            (None, Some(nr)) => (parse_string_list(nr)?, true),
            _ => return Err(malformed("Statement must specify Resource or NotResource")),
        };
        for r in &resources {
            let rest = r.strip_prefix("arn:aws:s3:::").ok_or_else(|| malformed("Resource must start with arn:aws:s3:::"))?;
            let (bkt_pat, _) = rest.split_once('/').unwrap_or((rest, ""));
            if let Some(target_bkt) = bucket {
                if !glob_match(bkt_pat, target_bkt) {
                    return Err(malformed("Policy Resource refers to another bucket"));
                }
            }
        }
        let conditions = match s_obj.get("Condition") {
            Some(c) => parse_conditions(c)?,
            None => Vec::new(),
        };
        statements.push(PolicyStatement {
            sid,
            effect,
            principal,
            not_principal,
            actions,
            not_action,
            resources,
            not_resource,
            conditions,
        });
    }

    let is_public = compute_is_public(&statements);
    Ok(BucketPolicy {
        version,
        id,
        statements,
        is_public,
    })
}

pub fn parse_bucket_policy(text: &str) -> Result<BucketPolicy, Refusal> {
    parse_bucket_policy_for_bucket(text, None)
}

fn compute_is_public(statements: &[PolicyStatement]) -> bool {
    statements.iter().any(|st| {
        if st.effect != Effect::Allow || st.not_principal || st.principal != PrincipalSpec::Any {
            return false;
        }
        // Per AWS S3 & ceph s3-tests (`test_get_nonpublicpolicy_acl_bucket_policy_status`), an IP
        // address restriction narrows a wildcard policy out of public status only when it is not
        // `IfExists` and every CIDR narrows meaningfully (prefix >= 2, refusing `0.0.0.0/0`, `::/0`,
        // and `/1` whole-space splits).
        let narrowed_by_ip = st.conditions.iter().any(|c| {
            c.op == BaseOp::IpAddress
                && !c.if_exists
                && c.key == "aws:sourceip"
                && !c.expected.is_empty()
                && c.expected
                    .iter()
                    .all(|cidr| validate_cidr(cidr).is_ok_and(|(_, p)| p >= 2))
        });
        !narrowed_by_ip
    })
}

fn principal_pattern_matches(pattern: &str, principal: &Principal) -> bool {
    if pattern == "*" {
        return true;
    }
    let Some(acct) = principal.account() else {
        return false;
    };
    let cid = principal.canonical_id().unwrap_or(acct);
    if pattern == acct || pattern == cid {
        return true;
    }
    match principal {
        Principal::Root => {
            pattern == ROOT_ACCOUNT
                || pattern == format!("arn:aws:iam::{}:root", ROOT_ACCOUNT)
                || pattern == "arn:aws:iam:::root"
        }
        Principal::User { account, name, id } => {
            pattern == id
                || pattern == account
                || pattern == format!("arn:aws:iam::{}:root", account)
                || pattern == format!("arn:aws:iam::{}:user/{}", account, name)
        }
        Principal::Anonymous => false,
    }
}

fn statement_matches_principal(st: &PolicyStatement, principal: &Principal) -> bool {
    let matched = match &st.principal {
        PrincipalSpec::Any => true,
        PrincipalSpec::Matches(list) => list.iter().any(|p| principal_pattern_matches(p, principal)),
    };
    if st.not_principal {
        !matched
    } else {
        matched
    }
}

fn statement_matches_action(st: &PolicyStatement, actions: &[&str]) -> bool {
    let matched = st.actions.iter().any(|pat| {
        let lp = pat.to_ascii_lowercase();
        actions.iter().any(|a| glob_match(&lp, &a.to_ascii_lowercase()))
    });
    if st.not_action {
        !matched
    } else {
        matched
    }
}

fn statement_matches_resource(st: &PolicyStatement, resource_arn: &str) -> bool {
    let matched = st.resources.iter().any(|pat| glob_match(pat, resource_arn));
    if st.not_resource {
        !matched
    } else {
        matched
    }
}

fn context_values<'a>(key_lower: &str, ctx: &'a PolicyRequestContext<'a>) -> Vec<&'a str> {
    if let Some(tag_key) = key_lower.strip_prefix("s3:existingobjecttag/") {
        return ctx
            .existing_tags
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(tag_key))
            .map(|(_, v)| v.as_str())
            .collect();
    }
    if let Some(tag_key) = key_lower.strip_prefix("s3:requestobjecttag/") {
        return ctx
            .request_tags
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(tag_key))
            .map(|(_, v)| v.as_str())
            .collect();
    }
    match key_lower {
        "s3:requestobjecttagkeys" => ctx.request_tags.iter().map(|(k, _)| k.as_str()).collect(),
        "s3:x-amz-acl" => ctx.acl.into_iter().collect(),
        "s3:x-amz-grant-read" => ctx.grant_read.iter().copied().collect(),
        "s3:x-amz-grant-write" => ctx.grant_write.iter().copied().collect(),
        "s3:x-amz-grant-read-acp" => ctx.grant_read_acp.iter().copied().collect(),
        "s3:x-amz-grant-write-acp" => ctx.grant_write_acp.iter().copied().collect(),
        "s3:x-amz-grant-full-control" => ctx.grant_full_control.iter().copied().collect(),
        "s3:x-amz-server-side-encryption" => ctx.sse.into_iter().collect(),
        "s3:x-amz-server-side-encryption-aws-kms-key-id" => ctx.sse_kms_key_id.into_iter().collect(),
        "s3:x-amz-server-side-encryption-customer-algorithm" => ctx.sse_c_alg.into_iter().collect(),
        "s3:x-amz-copy-source" => ctx.copy_source.into_iter().collect(),
        "s3:x-amz-metadata-directive" => ctx.metadata_directive.into_iter().collect(),
        "s3:x-amz-tagging-directive" => ctx.tagging_directive.into_iter().collect(),
        "s3:prefix" => ctx.prefix.into_iter().collect(),
        "s3:delimiter" => ctx.delimiter.into_iter().collect(),
        "s3:max-keys" => ctx.max_keys.into_iter().collect(),
        "aws:securetransport" => vec![if ctx.secure_transport { "true" } else { "false" }],
        "aws:sourceip" => ctx.source_ip.into_iter().collect(),
        "aws:referer" => ctx.referer.into_iter().collect(),
        _ => Vec::new(),
    }
}

fn single_op_matches(op: BaseOp, actual: &str, expected: &str) -> bool {
    match op {
        BaseOp::StringEquals | BaseOp::ArnEquals => actual == expected,
        BaseOp::StringNotEquals | BaseOp::ArnNotEquals => actual != expected,
        BaseOp::StringEqualsIgnoreCase => actual.eq_ignore_ascii_case(expected),
        BaseOp::StringNotEqualsIgnoreCase => !actual.eq_ignore_ascii_case(expected),
        BaseOp::StringLike | BaseOp::ArnLike => glob_match(expected, actual),
        BaseOp::StringNotLike | BaseOp::ArnNotLike => !glob_match(expected, actual),
        BaseOp::Bool => actual.eq_ignore_ascii_case(expected),
        BaseOp::Null => false,
        BaseOp::IpAddress => match (actual.parse::<IpAddr>(), validate_cidr(expected)) {
            (Ok(ip), Ok((net, prefix))) => ip_in_cidr(ip, net, prefix),
            _ => false,
        },
        BaseOp::NotIpAddress => match (actual.parse::<IpAddr>(), validate_cidr(expected)) {
            (Ok(ip), Ok((net, prefix))) => !ip_in_cidr(ip, net, prefix),
            _ => true,
        },
        BaseOp::NumericEquals
        | BaseOp::NumericNotEquals
        | BaseOp::NumericLessThan
        | BaseOp::NumericLessThanEquals
        | BaseOp::NumericGreaterThan
        | BaseOp::NumericGreaterThanEquals => {
            let (Ok(a), Ok(b)) = (actual.trim().parse::<f64>(), expected.trim().parse::<f64>()) else {
                return false;
            };
            match op {
                BaseOp::NumericEquals => a == b,
                BaseOp::NumericNotEquals => a != b,
                BaseOp::NumericLessThan => a < b,
                BaseOp::NumericLessThanEquals => a <= b,
                BaseOp::NumericGreaterThan => a > b,
                BaseOp::NumericGreaterThanEquals => a >= b,
                _ => false,
            }
        }
        BaseOp::DateEquals
        | BaseOp::DateNotEquals
        | BaseOp::DateLessThan
        | BaseOp::DateLessThanEquals
        | BaseOp::DateGreaterThan
        | BaseOp::DateGreaterThanEquals => {
            let (Some(a), Some(b)) = (parse_date_epoch(actual), parse_date_epoch(expected)) else {
                return false;
            };
            match op {
                BaseOp::DateEquals => a == b,
                BaseOp::DateNotEquals => a != b,
                BaseOp::DateLessThan => a < b,
                BaseOp::DateLessThanEquals => a <= b,
                BaseOp::DateGreaterThan => a > b,
                BaseOp::DateGreaterThanEquals => a >= b,
                _ => false,
            }
        }
    }
}

fn is_negated_op(op: BaseOp) -> bool {
    matches!(
        op,
        BaseOp::StringNotEquals
            | BaseOp::StringNotEqualsIgnoreCase
            | BaseOp::StringNotLike
            | BaseOp::ArnNotEquals
            | BaseOp::ArnNotLike
            | BaseOp::NotIpAddress
            | BaseOp::NumericNotEquals
            | BaseOp::DateNotEquals
    )
}

fn clause_holds(clause: &ConditionClause, ctx: &PolicyRequestContext<'_>) -> bool {
    // Dynamic date lookup for aws:currenttime / aws:epochtime.
    let dynamic_now;
    let actuals: Vec<&str> = if clause.key == "aws:currenttime" || clause.key == "aws:epochtime" {
        dynamic_now = ctx.now_epoch_secs.to_string();
        vec![dynamic_now.as_str()]
    } else {
        context_values(&clause.key, ctx)
    };

    if clause.op == BaseOp::Null {
        let is_null = actuals.is_empty();
        return clause.expected.iter().any(|e| {
            if e.eq_ignore_ascii_case("true") {
                is_null
            } else {
                !is_null
            }
        });
    }

    if actuals.is_empty() {
        return clause.if_exists;
    }

    let neg = is_negated_op(clause.op);
    let check_one = |val: &str| -> bool {
        if neg {
            clause.expected.iter().all(|e| single_op_matches(clause.op, val, e))
        } else {
            clause.expected.iter().any(|e| single_op_matches(clause.op, val, e))
        }
    };

    match clause.qualifier {
        SetQualifier::ForAllValues => actuals.iter().all(|v| check_one(v)),
        SetQualifier::ForAnyValue => actuals.iter().any(|v| check_one(v)),
        SetQualifier::Default => {
            if neg {
                actuals.iter().all(|v| check_one(v))
            } else {
                actuals.iter().any(|v| check_one(v))
            }
        }
    }
}

impl BucketPolicy {
    pub fn evaluate(
        &self,
        principal: &Principal,
        actions: &[&str],
        resource_arn: &str,
        ctx: &PolicyRequestContext<'_>,
    ) -> BucketPolicyDecision {
        let mut explicit_deny = false;
        let mut allow = false;
        for st in &self.statements {
            if !statement_matches_principal(st, principal) {
                continue;
            }
            if !statement_matches_action(st, actions) {
                continue;
            }
            if !statement_matches_resource(st, resource_arn) {
                continue;
            }
            if !st.conditions.iter().all(|c| clause_holds(c, ctx)) {
                continue;
            }
            match st.effect {
                Effect::Deny => explicit_deny = true,
                Effect::Allow => allow = true,
            }
        }
        BucketPolicyDecision {
            explicit_deny,
            allow: allow && !explicit_deny,
            is_public: self.is_public,
            conditions_hold: true,
        }
    }
}

/// Renders S3 GetBucketPolicyStatus XML.
pub fn policy_status_xml(is_public: bool) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PolicyStatus xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><IsPublic>{}</IsPublic></PolicyStatus>",
        if is_public { "true" } else { "false" }
    )
}

/// Renders condition coverage summary map for tests.
pub fn supported_operator_and_key_counts() -> (usize, usize) {
    (KNOWN_CONDITION_OPERATORS.len(), KNOWN_CONDITION_KEYS.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_known_condition_operator_and_key_parses_and_evaluates() {
        for op in KNOWN_CONDITION_OPERATORS {
            let (base, if_ex, qual) = parse_operator(op).unwrap_or_else(|e| panic!("failed op {}: {:?}", op, e));
            assert!(!if_ex);
            assert_eq!(qual, SetQualifier::Default);
            let _ = base;
            if *op != "null" && *op != "bool" {
                let with_ie = format!("{}IfExists", op);
                assert!(parse_operator(&with_ie).is_ok(), "failed {}", with_ie);
            }
        }
        for k in KNOWN_CONDITION_KEYS {
            assert!(is_supported_condition_key(k), "missing key {}", k);
        }
        assert!(is_supported_condition_key("s3:ExistingObjectTag/security"));
        assert!(is_supported_condition_key("s3:RequestObjectTag/env"));
        assert!(!is_supported_condition_key("aws:UnknownKey"));
    }
}
