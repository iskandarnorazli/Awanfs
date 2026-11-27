//! Bucket and object ACLs, ownership controls, and public access block parsing/rendering (#122).

pub use crate::authz::BucketOwnership;
use crate::authz::{AclGrant, AclGrants, Grantee, PublicAccessBlock};
use crate::fsm::Refusal;
use crate::iam::{Iam, ROOT_ACCOUNT};
use crate::ops::AclPermission;
use crate::xml::xml_escape;
use hyper::HeaderMap;
use quick_xml::events::Event;
use quick_xml::Reader;

pub const ALL_USERS_URI: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
pub const AUTHENTICATED_USERS_URI: &str = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclOwner {
    pub id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredGrantee {
    CanonicalUser { id: String, display_name: String },
    AuthenticatedUsers,
    AllUsers,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAclGrant {
    pub grantee: StoredGrantee,
    pub permission: AclPermission,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessControlPolicy {
    pub owner: AclOwner,
    pub grants: Vec<StoredAclGrant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CannedAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
    BucketOwnerRead,
    BucketOwnerFullControl,
}

pub fn owner_for_account(account: &str, iam: Option<&Iam>) -> AclOwner {
    let eff = if account.is_empty() { ROOT_ACCOUNT } else { account };
    if eff == ROOT_ACCOUNT {
        return AclOwner {
            id: ROOT_ACCOUNT.to_string(),
            display_name: ROOT_ACCOUNT.to_string(),
        };
    }
    if let Some(u) = iam.and_then(|i| i.user_by_account(eff)) {
        return AclOwner {
            id: u.id.clone(),
            display_name: u.display_name.clone(),
        };
    }
    AclOwner {
        id: eff.to_string(),
        display_name: eff.to_string(),
    }
}

pub fn parse_canned_acl(raw: &str) -> Result<CannedAcl, Refusal> {
    match raw.trim() {
        "private" => Ok(CannedAcl::Private),
        "public-read" => Ok(CannedAcl::PublicRead),
        "public-read-write" => Ok(CannedAcl::PublicReadWrite),
        "authenticated-read" => Ok(CannedAcl::AuthenticatedRead),
        "bucket-owner-read" => Ok(CannedAcl::BucketOwnerRead),
        "bucket-owner-full-control" => Ok(CannedAcl::BucketOwnerFullControl),
        _ => Err(Refusal {
            code: "InvalidArgument",
            status: 400,
            why: "Invalid canned ACL.",
        }),
    }
}

impl CannedAcl {
    pub fn expand(self, owner: &AclOwner, bucket_owner: Option<&AclOwner>) -> AccessControlPolicy {
        let self_grant = StoredAclGrant {
            grantee: StoredGrantee::CanonicalUser {
                id: owner.id.clone(),
                display_name: owner.display_name.clone(),
            },
            permission: AclPermission::FullControl,
        };
        let mut grants = Vec::new();
        match self {
            CannedAcl::Private => grants.push(self_grant),
            CannedAcl::PublicRead => {
                grants.push(StoredAclGrant {
                    grantee: StoredGrantee::AllUsers,
                    permission: AclPermission::Read,
                });
                grants.push(self_grant);
            }
            CannedAcl::PublicReadWrite => {
                grants.push(StoredAclGrant {
                    grantee: StoredGrantee::AllUsers,
                    permission: AclPermission::Read,
                });
                grants.push(StoredAclGrant {
                    grantee: StoredGrantee::AllUsers,
                    permission: AclPermission::Write,
                });
                grants.push(self_grant);
            }
            CannedAcl::AuthenticatedRead => {
                grants.push(StoredAclGrant {
                    grantee: StoredGrantee::AuthenticatedUsers,
                    permission: AclPermission::Read,
                });
                grants.push(self_grant);
            }
            CannedAcl::BucketOwnerRead => {
                grants.push(self_grant);
                if let Some(bo) = bucket_owner {
                    if bo.id != owner.id {
                        grants.push(StoredAclGrant {
                            grantee: StoredGrantee::CanonicalUser {
                                id: bo.id.clone(),
                                display_name: bo.display_name.clone(),
                            },
                            permission: AclPermission::Read,
                        });
                    }
                }
            }
            CannedAcl::BucketOwnerFullControl => {
                grants.push(self_grant);
                if let Some(bo) = bucket_owner {
                    if bo.id != owner.id {
                        grants.push(StoredAclGrant {
                            grantee: StoredGrantee::CanonicalUser {
                                id: bo.id.clone(),
                                display_name: bo.display_name.clone(),
                            },
                            permission: AclPermission::FullControl,
                        });
                    }
                }
            }
        }
        AccessControlPolicy {
            owner: owner.clone(),
            grants,
        }
    }
}

fn resolve_canonical_id(id: &str, iam: Option<&Iam>) -> Option<StoredGrantee> {
    if id == ROOT_ACCOUNT {
        return Some(StoredGrantee::CanonicalUser {
            id: ROOT_ACCOUNT.to_string(),
            display_name: ROOT_ACCOUNT.to_string(),
        });
    }
    let u = iam.and_then(|i| i.user_by_id(id))?;
    Some(StoredGrantee::CanonicalUser {
        id: u.id.clone(),
        display_name: u.display_name.clone(),
    })
}

fn resolve_email(email: &str, iam: Option<&Iam>) -> Option<StoredGrantee> {
    let u = iam.and_then(|i| i.user_by_email(email))?;
    Some(StoredGrantee::CanonicalUser {
        id: u.id.clone(),
        display_name: u.display_name.clone(),
    })
}

fn resolve_group_uri(uri: &str) -> Option<StoredGrantee> {
    if uri == ALL_USERS_URI {
        Some(StoredGrantee::AllUsers)
    } else if uri == AUTHENTICATED_USERS_URI {
        Some(StoredGrantee::AuthenticatedUsers)
    } else {
        None
    }
}

const GRANT_HEADERS: &[(&str, AclPermission)] = &[
    ("x-amz-grant-read", AclPermission::Read),
    ("x-amz-grant-write", AclPermission::Write),
    ("x-amz-grant-read-acp", AclPermission::ReadAcp),
    ("x-amz-grant-write-acp", AclPermission::WriteAcp),
    ("x-amz-grant-full-control", AclPermission::FullControl),
];

pub fn has_grant_headers(headers: &HeaderMap) -> bool {
    GRANT_HEADERS.iter().any(|(h, _)| headers.contains_key(*h))
}

fn parse_grant_header_list(raw: &str, perm: AclPermission, iam: Option<&Iam>, out: &mut Vec<StoredAclGrant>) -> Result<(), Refusal> {
    for part in raw.split(',') {
        let item = part.trim();
        if item.is_empty() {
            continue;
        }
        let Some((k, v)) = item.split_once('=') else {
            return Err(Refusal { code: "InvalidArgument", status: 400, why: "Malformed x-amz-grant header." });
        };
        let k = k.trim();
        let val = v.trim().trim_matches('"');
        let grantee = if k.eq_ignore_ascii_case("id") {
            resolve_canonical_id(val, iam).ok_or(Refusal {
                code: "InvalidArgument",
                status: 400,
                why: "Invalid id",
            })?
        } else if k.eq_ignore_ascii_case("emailAddress") {
            resolve_email(val, iam).ok_or(Refusal {
                code: "UnresolvableGrantByEmailAddress",
                status: 400,
                why: "The e-mail address you provided does not match any account on record.",
            })?
        } else if k.eq_ignore_ascii_case("uri") {
            resolve_group_uri(val).ok_or(Refusal {
                code: "InvalidArgument",
                status: 400,
                why: "Invalid group uri",
            })?
        } else {
            return Err(Refusal { code: "InvalidArgument", status: 400, why: "Unknown grant specifier" });
        };
        out.push(StoredAclGrant { grantee, permission: perm });
    }
    Ok(())
}

/// Builds an ACL from `x-amz-acl` or `x-amz-grant-*` headers if present.
/// Returns `Ok(None)` if neither canned ACL nor explicit grant headers were sent.
pub fn acl_from_headers(
    headers: &HeaderMap,
    owner: &AclOwner,
    bucket_owner: Option<&AclOwner>,
    iam: Option<&Iam>,
) -> Result<Option<AccessControlPolicy>, Refusal> {
    let canned = match headers.get("x-amz-acl") {
        None => None,
        Some(v) => Some(v.to_str().map_err(|_| Refusal {
            code: "InvalidArgument",
            status: 400,
            why: "Invalid header value",
        })?),
    };
    let has_grants = has_grant_headers(headers);
    if canned.is_some() && has_grants {
        return Err(Refusal {
            code: "InvalidRequest",
            status: 400,
            why: "Specifying both Canned ACLs and Header Grants is not allowed.",
        });
    }
    if let Some(c) = canned {
        return Ok(Some(parse_canned_acl(c)?.expand(owner, bucket_owner)));
    }
    if has_grants {
        let mut grants = Vec::new();
        for (hdr, perm) in GRANT_HEADERS {
            for val in headers.get_all(*hdr) {
                let s = val.to_str().map_err(|_| Refusal {
                    code: "InvalidArgument",
                    status: 400,
                    why: "Invalid header value",
                })?;
                parse_grant_header_list(s, *perm, iam, &mut grants)?;
            }
        }
        return Ok(Some(AccessControlPolicy {
            owner: owner.clone(),
            grants,
        }));
    }
    Ok(None)
}

fn parse_permission(s: &str) -> Result<AclPermission, Refusal> {
    match s.trim() {
        "READ" => Ok(AclPermission::Read),
        "WRITE" => Ok(AclPermission::Write),
        "READ_ACP" => Ok(AclPermission::ReadAcp),
        "WRITE_ACP" => Ok(AclPermission::WriteAcp),
        "FULL_CONTROL" => Ok(AclPermission::FullControl),
        _ => Err(Refusal {
            code: "MalformedACLError",
            status: 400,
            why: "The XML you provided was not well-formed or did not validate against our published schema.",
        }),
    }
}

pub fn parse_acl_xml(xml: &str, iam: Option<&Iam>) -> Result<AccessControlPolicy, Refusal> {
    parse_acl_xml_mode(xml, iam, true)
}

/// Parses canonical or stored ACL XML structurally without validating CanonicalUser IDs against IAM.
/// Used at manifest replay before IAM is loaded (#122).
pub fn parse_acl_xml_structural(xml: &str) -> Result<AccessControlPolicy, Refusal> {
    parse_acl_xml_mode(xml, None, false)
}

fn parse_acl_xml_mode(xml: &str, iam: Option<&Iam>, strict_users: bool) -> Result<AccessControlPolicy, Refusal> {
    let bad = || Refusal {
        code: "MalformedACLError",
        status: 400,
        why: "The XML you provided was not well-formed or did not validate against our published schema.",
    };
    let mut reader = Reader::from_str(xml.trim());
    reader.config_mut().trim_text(true);

    let mut seen_root = false;
    let mut in_owner = false;
    let mut in_grant = false;
    let mut in_grantee = false;
    let mut tag_stack: Vec<String> = Vec::new();

    let mut owner_id: Option<String> = None;
    let mut owner_name: Option<String> = None;
    let mut grants: Vec<StoredAclGrant> = Vec::new();

    let mut cur_xsi_type: Option<String> = None;
    let mut cur_id: Option<String> = None;
    let mut cur_email: Option<String> = None;
    let mut cur_uri: Option<String> = None;
    let mut cur_perm: Option<AclPermission> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if !seen_root {
                    if name != "AccessControlPolicy" {
                        return Err(bad());
                    }
                    seen_root = true;
                } else if name == "Owner" && tag_stack.last().map(String::as_str) == Some("AccessControlPolicy") {
                    in_owner = true;
                } else if name == "Grant" && tag_stack.last().map(String::as_str) == Some("AccessControlList") {
                    in_grant = true;
                    cur_xsi_type = None;
                    cur_id = None;
                    cur_email = None;
                    cur_uri = None;
                    cur_perm = None;
                } else if name == "Grantee" && in_grant {
                    in_grantee = true;
                    for attr in e.attributes().flatten() {
                        if attr.key.local_name().as_ref() == b"type" {
                            cur_xsi_type = String::from_utf8(attr.value.into_owned()).ok();
                        }
                    }
                }
                tag_stack.push(name);
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if !seen_root && name != "AccessControlPolicy" {
                    return Err(bad());
                }
                seen_root = true;
            }
            Ok(Event::Text(e)) => {
                let t = e.unescape().map_err(|_| bad())?.into_owned();
                match tag_stack.last().map(String::as_str) {
                    Some("ID") if in_owner => owner_id = Some(t),
                    Some("DisplayName") if in_owner => owner_name = Some(t),
                    Some("ID") if in_grantee => cur_id = Some(t),
                    Some("EmailAddress") if in_grantee => cur_email = Some(t),
                    Some("URI") if in_grantee => cur_uri = Some(t),
                    Some("Permission") if in_grant && !in_grantee => cur_perm = Some(parse_permission(&t)?),
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if name == "Owner" {
                    in_owner = false;
                } else if name == "Grantee" {
                    in_grantee = false;
                } else if name == "Grant" && in_grant {
                    in_grant = false;
                    let perm = cur_perm.ok_or_else(bad)?;
                    let g = match cur_xsi_type.as_deref() {
                        Some("CanonicalUser") | None if cur_id.is_some() => {
                            let id = cur_id.take().unwrap();
                            if strict_users {
                                resolve_canonical_id(&id, iam).ok_or(Refusal {
                                    code: "InvalidArgument",
                                    status: 400,
                                    why: "Invalid id",
                                })?
                            } else {
                                StoredGrantee::CanonicalUser { display_name: id.clone(), id }
                            }
                        }
                        Some("AmazonCustomerByEmail") | None if cur_email.is_some() => {
                            let em = cur_email.take().unwrap();
                            resolve_email(&em, iam).ok_or(Refusal {
                                code: "UnresolvableGrantByEmailAddress",
                                status: 400,
                                why: "The e-mail address you provided does not match any account on record.",
                            })?
                        }
                        Some("Group") | None if cur_uri.is_some() => {
                            let uri = cur_uri.take().unwrap();
                            resolve_group_uri(&uri).ok_or(Refusal {
                                code: "InvalidArgument",
                                status: 400,
                                why: "Invalid group uri",
                            })?
                        }
                        _ => return Err(bad()),
                    };
                    grants.push(StoredAclGrant { grantee: g, permission: perm });
                }
                tag_stack.pop();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(bad()),
            _ => {}
        }
    }
    if !seen_root {
        return Err(bad());
    }
    let id = owner_id.unwrap_or_else(|| ROOT_ACCOUNT.to_string());
    let resolved_owner = match resolve_canonical_id(&id, iam) {
        Some(StoredGrantee::CanonicalUser { id, display_name }) => AclOwner { id, display_name },
        _ => AclOwner {
            display_name: owner_name.unwrap_or_else(|| id.clone()),
            id,
        },
    };
    Ok(AccessControlPolicy {
        owner: resolved_owner,
        grants,
    })
}

pub fn render_acl_xml(acp: &AccessControlPolicy) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <Owner>\n    ",
    );
    out.push_str(&format!("<ID>{}</ID>\n    ", xml_escape(&acp.owner.id)));
    out.push_str(&format!("<DisplayName>{}</DisplayName>\n  </Owner>\n  <AccessControlList>\n", xml_escape(&acp.owner.display_name)));
    let ordered_grants = acp
        .grants
        .iter()
        .filter(|g| !matches!(g.grantee, StoredGrantee::CanonicalUser { .. }))
        .chain(
            acp.grants
                .iter()
                .filter(|g| matches!(g.grantee, StoredGrantee::CanonicalUser { .. })),
        );
    for g in ordered_grants {
        let perm = match g.permission {
            AclPermission::Read => "READ",
            AclPermission::Write => "WRITE",
            AclPermission::ReadAcp => "READ_ACP",
            AclPermission::WriteAcp => "WRITE_ACP",
            AclPermission::FullControl => "FULL_CONTROL",
        };
        out.push_str("    <Grant>\n");
        match &g.grantee {
            StoredGrantee::CanonicalUser { id, display_name } => {
                out.push_str("      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">");
                out.push_str(&format!("<ID>{}</ID><DisplayName>{}</DisplayName>", xml_escape(id), xml_escape(display_name)));
                out.push_str("</Grantee>\n");
            }
            StoredGrantee::AllUsers => {
                out.push_str("      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">");
                out.push_str(&format!("<URI>{}</URI>", ALL_USERS_URI));
                out.push_str("</Grantee>\n");
            }
            StoredGrantee::AuthenticatedUsers => {
                out.push_str("      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">");
                out.push_str(&format!("<URI>{}</URI>", AUTHENTICATED_USERS_URI));
                out.push_str("</Grantee>\n");
            }
        }
        out.push_str(&format!("      <Permission>{}</Permission>\n    </Grant>\n", perm));
    }
    out.push_str("  </AccessControlList>\n</AccessControlPolicy>");
    out
}

impl AccessControlPolicy {
    pub fn is_public(&self) -> bool {
        self.grants.iter().any(|g| {
            matches!(g.grantee, StoredGrantee::AllUsers | StoredGrantee::AuthenticatedUsers)
        })
    }

    pub fn is_private_bucket_acl(&self, bucket_owner_id: &str) -> bool {
        self.grants.iter().all(|g| {
            matches!(&g.grantee, StoredGrantee::CanonicalUser { id, .. } if id == bucket_owner_id)
        })
    }

    pub fn to_authz_grants(&self) -> AclGrants {
        AclGrants {
            grants: self
                .grants
                .iter()
                .map(|g| AclGrant {
                    grantee: match &g.grantee {
                        StoredGrantee::CanonicalUser { id, .. } => Grantee::CanonicalUser(id.clone()),
                        StoredGrantee::AuthenticatedUsers => Grantee::AuthenticatedUsers,
                        StoredGrantee::AllUsers => Grantee::AllUsers,
                    },
                    permission: g.permission,
                })
                .collect(),
        }
    }
}

pub fn parse_ownership_str(s: &str) -> Option<BucketOwnership> {
    match s.trim() {
        "BucketOwnerEnforced" => Some(BucketOwnership::BucketOwnerEnforced),
        "BucketOwnerPreferred" => Some(BucketOwnership::BucketOwnerPreferred),
        "ObjectWriter" => Some(BucketOwnership::ObjectWriter),
        _ => None,
    }
}

pub fn ownership_as_str(o: BucketOwnership) -> &'static str {
    match o {
        BucketOwnership::BucketOwnerEnforced => "BucketOwnerEnforced",
        BucketOwnership::BucketOwnerPreferred => "BucketOwnerPreferred",
        BucketOwnership::ObjectWriter => "ObjectWriter",
    }
}

pub fn parse_ownership_xml(xml: &str) -> Result<BucketOwnership, Refusal> {
    let bad = || Refusal {
        code: "MalformedXML",
        status: 400,
        why: "The XML you provided was not well-formed or did not validate against our published schema.",
    };
    let mut reader = Reader::from_str(xml.trim());
    reader.config_mut().trim_text(true);
    let mut seen_root = false;
    let mut in_val = false;
    let mut found: Option<BucketOwnership> = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if !seen_root {
                    if name != "OwnershipControls" {
                        return Err(bad());
                    }
                    seen_root = true;
                } else if name == "ObjectOwnership" {
                    in_val = true;
                }
            }
            Ok(Event::Text(e)) if in_val => {
                let t = e.unescape().map_err(|_| bad())?;
                found = Some(parse_ownership_str(&t).ok_or_else(bad)?);
            }
            Ok(Event::End(e)) => {
                if e.local_name().as_ref() == b"ObjectOwnership" {
                    in_val = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(bad()),
            _ => {}
        }
    }
    found.ok_or_else(bad)
}

pub fn render_ownership_xml(o: BucketOwnership) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <OwnershipControls xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <Rule><ObjectOwnership>{}</ObjectOwnership></Rule>\n\
        </OwnershipControls>",
        ownership_as_str(o)
    )
}

pub fn parse_pab_xml(xml: &str) -> Result<PublicAccessBlock, Refusal> {
    let bad = || Refusal {
        code: "MalformedXML",
        status: 400,
        why: "The XML you provided was not well-formed or did not validate against our published schema.",
    };
    let mut reader = Reader::from_str(xml.trim());
    reader.config_mut().trim_text(true);
    let mut seen_root = false;
    let mut cur_tag = String::new();
    let mut pab = PublicAccessBlock::default();
    let parse_bool = |s: &str| -> Result<bool, Refusal> {
        if s.eq_ignore_ascii_case("true") {
            Ok(true)
        } else if s.eq_ignore_ascii_case("false") {
            Ok(false)
        } else {
            Err(bad())
        }
    };
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if !seen_root {
                    if name != "PublicAccessBlockConfiguration" {
                        return Err(bad());
                    }
                    seen_root = true;
                } else {
                    cur_tag = name;
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if !seen_root && name != "PublicAccessBlockConfiguration" {
                    return Err(bad());
                }
                seen_root = true;
            }
            Ok(Event::Text(e)) => {
                let t = e.unescape().map_err(|_| bad())?;
                match cur_tag.as_str() {
                    "BlockPublicAcls" => pab.block_public_acls = parse_bool(&t)?,
                    "IgnorePublicAcls" => pab.ignore_public_acls = parse_bool(&t)?,
                    "BlockPublicPolicy" => pab.block_public_policy = parse_bool(&t)?,
                    "RestrictPublicBuckets" => pab.restrict_public_buckets = parse_bool(&t)?,
                    _ => {}
                }
            }
            Ok(Event::End(_)) => cur_tag.clear(),
            Ok(Event::Eof) => break,
            Err(_) => return Err(bad()),
            _ => {}
        }
    }
    if !seen_root {
        return Err(bad());
    }
    Ok(pab)
}

pub fn render_pab_xml(pab: PublicAccessBlock) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <PublicAccessBlockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <BlockPublicAcls>{}</BlockPublicAcls>\n  \
          <IgnorePublicAcls>{}</IgnorePublicAcls>\n  \
          <BlockPublicPolicy>{}</BlockPublicPolicy>\n  \
          <RestrictPublicBuckets>{}</RestrictPublicBuckets>\n\
        </PublicAccessBlockConfiguration>",
        pab.block_public_acls,
        pab.ignore_public_acls,
        pab.block_public_policy,
        pab.restrict_public_buckets
    )
}
