//! #122: 100,000-case differential test of the bucket policy evaluator + `authz::authorize`
//! against an independent reference evaluator over random policies, ACLs, PAB, ownership,
//! principals, operations, and request conditions (logs seed on start and failure).

use s3_server::acl::{AccessControlPolicy, AclOwner, CannedAcl, StoredAclGrant, StoredGrantee};
use s3_server::authz::{
    authorize, AclGrants, AuthzContext, BucketOwnership, BucketTarget, Decision, Grantee,
    ObjectTarget, PolicyMatch, PublicAccessBlock, Reason,
};
use s3_server::iam::{glob_match, Principal, ROOT_ACCOUNT};
use s3_server::ops::{spec, AclPermission, Operation, TargetKind, OPS};
use s3_server::policy::{parse_bucket_policy_for_bucket, BucketPolicy, PolicyRequestContext};
use std::marker::PhantomData;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        self.0 = x ^ (x >> 31);
        self.0
    }
    fn pick(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
    fn bool(&mut self) -> bool {
        (self.next() & 1) == 1
    }
}

#[derive(Clone)]
struct RefStmt {
    allow: bool,
    wildcard_principal: bool,
    principal_id: Option<&'static str>,
    not_principal_id: Option<&'static str>,
    action: &'static str,
    resource: String,
    ip_cidr: Option<&'static str>,
    tag_sec: Option<&'static str>,
    prefix_like: Option<&'static str>,
    referer_if_exists: Option<&'static str>,
}

fn ref_eval_policy(
    stmts: &[RefStmt],
    principal: &Principal,
    action: &str,
    resource: &str,
    source_ip: Option<&str>,
    sec_tag: Option<&str>,
    prefix: Option<&str>,
    referer: Option<&str>,
) -> (bool, bool, bool) {
    let is_public = stmts.iter().any(|s| s.allow && s.wildcard_principal && s.ip_cidr.is_none());
    let mut deny = false;
    let mut allow = false;
    for s in stmts {
        let p_ok = if let Some(np) = s.not_principal_id {
            match principal {
                Principal::Root => np != "root",
                Principal::User { id, .. } => id != np,
                Principal::Anonymous => true,
            }
        } else if s.wildcard_principal {
            true
        } else if let Some(pid) = s.principal_id {
            match principal {
                Principal::Root => pid == "root",
                Principal::User { id, .. } => id == pid,
                Principal::Anonymous => false,
            }
        } else {
            false
        };
        if !p_ok {
            continue;
        }
        if !glob_match(s.action, action) || !glob_match(&s.resource, resource) {
            continue;
        }
        if let Some(cidr) = s.ip_cidr {
            let ok = matches!((cidr, source_ip), ("10.0.0.0/24", Some("10.0.0.5")));
            if !ok {
                continue;
            }
        }
        if let Some(want_tag) = s.tag_sec {
            if sec_tag != Some(want_tag) {
                continue;
            }
        }
        if let Some(pfx_pat) = s.prefix_like {
            if !prefix.is_some_and(|p| glob_match(pfx_pat, p)) {
                continue;
            }
        }
        if let Some(ref_pat) = s.referer_if_exists {
            if let Some(actual) = referer {
                if !glob_match(ref_pat, actual) {
                    continue;
                }
            }
        }
        if s.allow {
            allow = true;
        } else {
            deny = true;
        }
    }
    (deny, allow && !deny, is_public)
}

#[test]
fn differential_100k_policy_and_authz_against_reference() {
    let seed: u64 = 0x1220_CAFE_BABE_0001;
    eprintln!("policy_differential_test seed = {:#018x}", seed);
    let mut rng = Rng(seed);

    let principals = [
        Principal::Anonymous,
        Principal::Root,
        Principal::User {
            account: "alt".to_string(),
            name: "alt".to_string(),
            id: "alt-id".to_string(),
        },
        Principal::User {
            account: "other".to_string(),
            name: "other".to_string(),
            id: "other-id".to_string(),
        },
    ];
    let ops = [
        Operation::ListObjects,
        Operation::GetObject,
        Operation::PutObject,
        Operation::DeleteObject,
        Operation::GetBucketAcl,
        Operation::PutBucketPolicy,
    ];
    let keys = ["public/a", "private/b"];

    for iter in 0..100_000usize {
        let stmt_count = 1 + rng.pick(3);
        let mut ref_stmts = Vec::with_capacity(stmt_count);
        let mut json_stmts = Vec::with_capacity(stmt_count);
        for _ in 0..stmt_count {
            let allow = rng.bool();
            let (wildcard_principal, principal_id, not_principal_id, p_json_k, p_json_v) = match rng.pick(4) {
                0 => (true, None, None, "Principal", serde_json::json!("*")),
                1 => (false, Some("alt-id"), None, "Principal", serde_json::json!({"AWS": "alt-id"})),
                2 => (false, Some("other-id"), None, "Principal", serde_json::json!({"AWS": "arn:aws:iam::other:user/other"})),
                _ if !allow => (false, None, Some("alt-id"), "NotPrincipal", serde_json::json!({"AWS": "alt-id"})),
                _ => (false, Some("root"), None, "Principal", serde_json::json!({"AWS": "root"})),
            };
            let action = match rng.pick(4) {
                0 => "s3:*",
                1 => "s3:GetObject",
                2 => "s3:ListBucket",
                _ => "s3:PutObject",
            };
            let is_obj_res = rng.bool();
            let resource = if is_obj_res {
                if rng.bool() { "arn:aws:s3:::diff-bkt/public/*".to_string() } else { "arn:aws:s3:::diff-bkt/*".to_string() }
            } else {
                "arn:aws:s3:::diff-bkt".to_string()
            };
            let mut cond = serde_json::Map::new();
            let ip_cidr = if rng.pick(4) == 0 {
                cond.insert("IpAddress".into(), serde_json::json!({"aws:SourceIp": "10.0.0.0/24"}));
                Some("10.0.0.0/24")
            } else {
                None
            };
            let tag_sec = if rng.pick(4) == 0 {
                cond.insert("StringEquals".into(), serde_json::json!({"s3:ExistingObjectTag/security": "public"}));
                Some("public")
            } else {
                None
            };
            let prefix_like = if rng.pick(4) == 0 {
                cond.insert("StringLike".into(), serde_json::json!({"s3:prefix": "public/*"}));
                Some("public/*")
            } else {
                None
            };
            let referer_if_exists = if rng.pick(4) == 0 {
                cond.insert("StringLikeIfExists".into(), serde_json::json!({"aws:Referer": "https://good.example/*"}));
                Some("https://good.example/*")
            } else {
                None
            };
            let mut st_obj = serde_json::Map::new();
            st_obj.insert("Effect".into(), serde_json::json!(if allow { "Allow" } else { "Deny" }));
            st_obj.insert(p_json_k.into(), p_json_v);
            st_obj.insert("Action".into(), serde_json::json!(action));
            st_obj.insert("Resource".into(), serde_json::json!(resource));
            if !cond.is_empty() {
                st_obj.insert("Condition".into(), serde_json::Value::Object(cond));
            }
            json_stmts.push(serde_json::Value::Object(st_obj));
            ref_stmts.push(RefStmt {
                allow,
                wildcard_principal,
                principal_id,
                not_principal_id,
                action,
                resource,
                ip_cidr,
                tag_sec,
                prefix_like,
                referer_if_exists,
            });
        }
        let doc = serde_json::json!({ "Version": "2012-10-17", "Statement": json_stmts }).to_string();
        let policy: BucketPolicy = parse_bucket_policy_for_bucket(&doc, Some("diff-bkt"))
            .unwrap_or_else(|e| panic!("seed {:#x} iter {} parse failed: {:?}", seed, iter, e));

        let op = ops[rng.pick(ops.len())];
        let sp = spec(op);
        let principal = &principals[rng.pick(principals.len())];
        let key = keys[rng.pick(keys.len())];
        let source_ip = if rng.bool() { Some("10.0.0.5") } else { Some("203.0.113.9") };
        let sec_tag = if rng.bool() { Some("public") } else { Some("private") };
        let prefix = if rng.bool() { Some("public/a") } else { Some("private/b") };
        let referer = match rng.pick(3) {
            0 => None,
            1 => Some("https://good.example/home"),
            _ => Some("https://evil.example/"),
        };

        let existing_tags = vec![("security".to_string(), sec_tag.unwrap().to_string())];
        let pctx = PolicyRequestContext {
            existing_tags: &existing_tags,
            prefix,
            source_ip,
            referer,
            ..Default::default()
        };
        let res_arn = match sp.target {
            TargetKind::Object => format!("arn:aws:s3:::diff-bkt/{}", key),
            _ => "arn:aws:s3:::diff-bkt".to_string(),
        };
        let action_str = sp.iam_action.unwrap();
        let got_pol = policy.evaluate(principal, &[action_str], &res_arn, &pctx);
        let (want_deny, want_allow, want_pub) = ref_eval_policy(
            &ref_stmts,
            principal,
            action_str,
            &res_arn,
            source_ip,
            sec_tag,
            prefix,
            referer,
        );
        assert_eq!(got_pol.explicit_deny, want_deny, "seed {:#x} iter {} deny", seed, iter);
        assert_eq!(got_pol.allow, want_allow, "seed {:#x} iter {} allow", seed, iter);
        assert_eq!(got_pol.is_public, want_pub, "seed {:#x} iter {} is_public", seed, iter);

        // Also check full authz::authorize default-deny & explicit-deny properties with random ACL/PAB.
        let ownership = [BucketOwnership::BucketOwnerEnforced, BucketOwnership::ObjectWriter][rng.pick(2)];
        let restrict_public_buckets = rng.bool();
        let ignore_public_acls = rng.bool();
        let pab = PublicAccessBlock {
            block_public_acls: false,
            ignore_public_acls,
            block_public_policy: false,
            restrict_public_buckets,
        };
        let acl_owner = AclOwner { id: "root".into(), display_name: "root".into() };
        let acl_mode = rng.pick(4);
        let acp = match acl_mode {
            0 => CannedAcl::Private.expand(&acl_owner, None),
            1 => CannedAcl::PublicRead.expand(&acl_owner, None),
            2 => CannedAcl::AuthenticatedRead.expand(&acl_owner, None),
            _ => AccessControlPolicy {
                owner: acl_owner,
                grants: vec![StoredAclGrant {
                    grantee: StoredGrantee::CanonicalUser { id: "alt-id".into(), display_name: "alt".into() },
                    permission: AclPermission::FullControl,
                }],
            },
        };
        let acl_grants: AclGrants = acp.to_authz_grants();
        let b_target = BucketTarget {
            name: "diff-bkt",
            owner_account: ROOT_ACCOUNT,
            ownership,
            public_access_block: pab,
            policy: got_pol,
            acl: &acl_grants,
        };
        let o_target = if sp.target == TargetKind::Object {
            Some(ObjectTarget {
                key,
                owner_account: ROOT_ACCOUNT,
                acl: &acl_grants,
            })
        } else {
            None
        };
        let id_pol = match principal {
            Principal::User { .. } => PolicyMatch::Allow,
            _ => PolicyMatch::NoOpinion,
        };
        let ctx = AuthzContext { identity_policy: id_pol, _marker: PhantomData };
        let dec = authorize(principal, op, Some(&b_target), o_target.as_ref(), &ctx);

        // Invariant 1: explicit Deny always beats everything.
        if want_deny {
            assert!(matches!(dec, Decision::Deny { reason: Reason::BucketPolicyDeny, .. }), "seed {:#x} iter {} deny", seed, iter);
            continue;
        }
        // Invariant 2: independent authz reference agrees on every allow/deny.
        let is_owner = matches!(principal, Principal::Root);
        let pol_ok = got_pol.allow && !(got_pol.is_public && restrict_public_buckets && !is_owner);
        let acl_ok = ownership != BucketOwnership::BucketOwnerEnforced
            && acl_grants.grants.iter().any(|g| {
                let p_ok = sp.acl_permission.is_some_and(|req| g.permission == AclPermission::FullControl || g.permission == req);
                let w_ok = match &g.grantee {
                    Grantee::AllUsers => !ignore_public_acls,
                    Grantee::AuthenticatedUsers => !ignore_public_acls && !matches!(principal, Principal::Anonymous),
                    Grantee::CanonicalUser(id) => principal.canonical_id() == Some(id.as_str()),
                };
                p_ok && w_ok
            });
        let want_total = is_owner || pol_ok || acl_ok;
        assert_eq!(dec.is_allowed(), want_total, "seed {:#x} iter {} total mismatch", seed, iter);
    }
    let _ = OPS.len();
}
