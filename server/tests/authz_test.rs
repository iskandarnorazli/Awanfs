//! #151: Accounts, owners, and single `authorize` decision tests (TDD).

use reqwest::{Method, StatusCode};
use s3_server::attrs::{Attrs, TAG_OWNER};
use s3_server::authz::{
    authorize, AclGrant, AclGrants, AuthzContext, BucketOwnership, BucketPolicyDecision,
    BucketTarget, Decision, Grantee, ObjectTarget, PolicyMatch, PublicAccessBlock, Reason,
    DEFAULT_BUCKET_OWNERSHIP,
};
use s3_server::iam::{Principal, ReloadingIam};
use s3_server::ops::{spec, AclPermission, Operation, TargetKind, OPS};
use s3_server::request_log::{clf_time, LogSink};
use s3_server::{
    compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState,
    SigV4KeyCache,
};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpClass {
    Read,
    Write,
    ReadAcl,
    WriteAcl,
    BucketAdmin,
}

/// Exhaustive mapping of every `OPS` row to its operation class.
/// Adding a new `Operation` variant without classifying it here fails to compile.
fn op_class(op: Operation) -> OpClass {
    match op {
        Operation::ListBuckets
        | Operation::HeadBucket
        | Operation::ListObjects
        | Operation::ListObjectsV2
        | Operation::ListObjectVersions
        | Operation::GetBucketLocation
        | Operation::GetBucketVersioning
        | Operation::GetBucketLifecycle
        | Operation::GetBucketCors
        | Operation::GetBucketTagging
        | Operation::CorsPreflight
        | Operation::ListMultipartUploads
        | Operation::GetObject
        | Operation::GetObjectAttributes
        | Operation::HeadObject
        | Operation::ListParts
        | Operation::GetObjectTagging
        | Operation::GetObjectLockConfiguration
        | Operation::GetObjectRetention
        | Operation::GetObjectLegalHold => OpClass::Read,
        Operation::PutObject
        | Operation::CopyObject
        | Operation::DeleteObject
        | Operation::DeleteObjects
        | Operation::CreateMultipartUpload
        | Operation::UploadPart
        | Operation::UploadPartCopy
        | Operation::CompleteMultipartUpload
        | Operation::AbortMultipartUpload
        | Operation::PutObjectTagging
        | Operation::DeleteObjectTagging
        | Operation::PutObjectRetention
        | Operation::PutObjectLegalHold => OpClass::Write,
        Operation::CreateBucket
        | Operation::DeleteBucket
        | Operation::PutBucketVersioning
        | Operation::PutBucketLifecycle
        | Operation::DeleteBucketLifecycle
        | Operation::PutBucketCors
        | Operation::DeleteBucketCors
        | Operation::PutBucketTagging
        | Operation::DeleteBucketTagging
        | Operation::PutObjectLockConfiguration => OpClass::BucketAdmin,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrincipalKind {
    Root,
    OwnerUser,
    SameAccountUser,
    OtherAccountUser,
    Anonymous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketPolKind {
    None,
    AllowThisPrincipal,
    AllowAllPublic,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AclKind {
    Private,
    GrantThisPrincipal,
    AuthenticatedUsers,
    AllUsers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PabKind {
    Off,
    BlockPublicAcls,
    IgnorePublicAcls,
    BlockPublicPolicy,
    RestrictPublicBuckets,
}

fn make_principal(kind: PrincipalKind, owner_account: &str) -> Principal {
    match kind {
        PrincipalKind::Root => Principal::Root,
        PrincipalKind::OwnerUser => Principal::User {
            account: owner_account.to_string(),
            name: "owner-user".to_string(),
            id: "owner-user-id".to_string(),
        },
        PrincipalKind::SameAccountUser => Principal::User {
            account: owner_account.to_string(),
            name: "peer-user".to_string(),
            id: "peer-user-id".to_string(),
        },
        PrincipalKind::OtherAccountUser => Principal::User {
            account: "other-acct".to_string(),
            name: "outsider".to_string(),
            id: "outsider-id".to_string(),
        },
        PrincipalKind::Anonymous => Principal::Anonymous,
    }
}

fn make_pab(kind: PabKind) -> PublicAccessBlock {
    match kind {
        PabKind::Off => PublicAccessBlock::default(),
        PabKind::BlockPublicAcls => PublicAccessBlock {
            block_public_acls: true,
            ..Default::default()
        },
        PabKind::IgnorePublicAcls => PublicAccessBlock {
            ignore_public_acls: true,
            ..Default::default()
        },
        PabKind::BlockPublicPolicy => PublicAccessBlock {
            block_public_policy: true,
            ..Default::default()
        },
        PabKind::RestrictPublicBuckets => PublicAccessBlock {
            restrict_public_buckets: true,
            ..Default::default()
        },
    }
}

fn make_bucket_pol(kind: BucketPolKind) -> BucketPolicyDecision {
    match kind {
        BucketPolKind::None => BucketPolicyDecision::none(),
        BucketPolKind::AllowThisPrincipal => BucketPolicyDecision {
            explicit_deny: false,
            allow: true,
            is_public: false,
            conditions_hold: true,
        },
        BucketPolKind::AllowAllPublic => BucketPolicyDecision {
            explicit_deny: false,
            allow: true,
            is_public: true,
            conditions_hold: true,
        },
        BucketPolKind::Deny => BucketPolicyDecision {
            explicit_deny: true,
            allow: false,
            is_public: false,
            conditions_hold: true,
        },
    }
}

fn make_acl(kind: AclKind, principal: &Principal, perm: AclPermission) -> AclGrants {
    match kind {
        AclKind::Private => AclGrants::default(),
        AclKind::GrantThisPrincipal => match principal {
            Principal::Root => AclGrants {
                grants: vec![AclGrant {
                    grantee: Grantee::CanonicalUser("root".to_string()),
                    permission: perm,
                }],
            },
            Principal::User { id, .. } => AclGrants {
                grants: vec![AclGrant {
                    grantee: Grantee::CanonicalUser(id.clone()),
                    permission: perm,
                }],
            },
            Principal::Anonymous => AclGrants::default(),
        },
        AclKind::AuthenticatedUsers => AclGrants {
            grants: vec![AclGrant {
                grantee: Grantee::AuthenticatedUsers,
                permission: perm,
            }],
        },
        AclKind::AllUsers => AclGrants {
            grants: vec![AclGrant {
                grantee: Grantee::AllUsers,
                permission: perm,
            }],
        },
    }
}

/// Deliberately simple, independent reference evaluator of CONFORMANCE_DESIGN.md Section 5.2.
fn reference_authorize(
    principal: &Principal,
    op: Operation,
    bucket: Option<&BucketTarget<'_>>,
    object: Option<&ObjectTarget<'_>>,
    identity_policy: PolicyMatch,
) -> bool {
    let sp = spec(op);

    // Step 1: Explicit Deny in bucket policy or caller's identity policy.
    if matches!(principal, Principal::User { .. }) && identity_policy == PolicyMatch::ExplicitDeny {
        return false;
    }
    if bucket.is_some_and(|b| b.policy.explicit_deny) {
        return false;
    }

    // Step 2: Public access block flags.
    let pab = bucket.map(|b| b.public_access_block).unwrap_or_default();

    let caller_account = match principal {
        Principal::Root => Some("root"),
        Principal::User { account, .. } => Some(account.as_str()),
        Principal::Anonymous => None,
    };

    let bucket_owner = bucket.map(|b| {
        if b.owner_account.is_empty() {
            "root"
        } else {
            b.owner_account
        }
    });

    let caller_owns_bucket = match (caller_account, bucket_owner) {
        (Some(ca), Some(bo)) => ca == bo,
        _ => false,
    };

    let caller_owns_resource = match sp.target {
        TargetKind::Service => caller_account.is_some(),
        TargetKind::Bucket => {
            if op == Operation::CreateBucket && bucket.is_none() {
                caller_account.is_some()
            } else {
                caller_owns_bucket
            }
        }
        TargetKind::Object => {
            let Some(b) = bucket else { return false };
            let eff_owner = match (b.ownership, object) {
                (BucketOwnership::BucketOwnerEnforced, _) | (_, None) => {
                    if b.owner_account.is_empty() {
                        "root"
                    } else {
                        b.owner_account
                    }
                }
                (_, Some(o)) => {
                    if o.owner_account.is_empty() {
                        if b.owner_account.is_empty() {
                            "root"
                        } else {
                            b.owner_account
                        }
                    } else {
                        o.owner_account
                    }
                }
            };
            caller_account.is_some_and(|ca| ca == eff_owner)
        }
    };

    // Step 3: Caller's account owns resource, and identity policy allows (root: always).
    let id_allows = match principal {
        Principal::Root => true,
        Principal::User { .. } => identity_policy == PolicyMatch::Allow,
        Principal::Anonymous => false,
    };
    if caller_owns_resource && id_allows {
        return true;
    }

    // Step 4: Bucket policy allows for this principal and conditions hold (with RestrictPublicBuckets).
    if let Some(b) = bucket {
        let policy_blocked = b.policy.is_public && pab.restrict_public_buckets && !caller_owns_bucket;
        if b.policy.allow && b.policy.conditions_hold && !policy_blocked {
            return true;
        }
    }

    // Step 5: ACL grants permission (unless BucketOwnerEnforced).
    if let Some(b) = bucket {
        if b.ownership != BucketOwnership::BucketOwnerEnforced {
            let acl = match sp.target {
                TargetKind::Object => object.map(|o| o.acl).unwrap_or(b.acl),
                _ => b.acl,
            };
            for g in &acl.grants {
                let perm_ok = g.permission == AclPermission::FullControl
                    || g.permission == sp.acl_permission;
                if !perm_ok {
                    continue;
                }
                let grantee_ok = match &g.grantee {
                    Grantee::CanonicalUser(id) => match principal {
                        Principal::Root => id == "root",
                        Principal::User { id: uid, .. } => uid == id,
                        Principal::Anonymous => false,
                    },
                    Grantee::AuthenticatedUsers => {
                        !pab.ignore_public_acls && !matches!(principal, Principal::Anonymous)
                    }
                    Grantee::AllUsers => !pab.ignore_public_acls,
                };
                if grantee_ok {
                    return true;
                }
            }
        }
    }

    // Step 6: Default deny.
    false
}

#[test]
fn decision_table_covers_every_op_row_and_matches_reference() {
    let mut covered_ops = HashSet::new();
    for s in OPS {
        let _ = op_class(s.op);
        covered_ops.insert(s.op);
    }
    assert_eq!(covered_ops.len(), OPS.len(), "every OPS row must be covered");
    // Ensure ReadAcl/WriteAcl enum variants are exercised conceptually in class coverage too.
    let _ = (OpClass::ReadAcl, OpClass::WriteAcl);

    let principals = [
        PrincipalKind::Root,
        PrincipalKind::OwnerUser,
        PrincipalKind::SameAccountUser,
        PrincipalKind::OtherAccountUser,
        PrincipalKind::Anonymous,
    ];
    let id_pols = [
        PolicyMatch::NoOpinion,
        PolicyMatch::Allow,
        PolicyMatch::ExplicitDeny,
    ];
    let bkt_pols = [
        BucketPolKind::None,
        BucketPolKind::AllowThisPrincipal,
        BucketPolKind::AllowAllPublic,
        BucketPolKind::Deny,
    ];
    let acls = [
        AclKind::Private,
        AclKind::GrantThisPrincipal,
        AclKind::AuthenticatedUsers,
        AclKind::AllUsers,
    ];
    let pabs = [
        PabKind::Off,
        PabKind::BlockPublicAcls,
        PabKind::IgnorePublicAcls,
        PabKind::BlockPublicPolicy,
        PabKind::RestrictPublicBuckets,
    ];
    let ownerships = [
        BucketOwnership::BucketOwnerEnforced,
        BucketOwnership::BucketOwnerPreferred,
        BucketOwnership::ObjectWriter,
    ];

    for sp in OPS {
        let op = sp.op;
        for &pk in &principals {
            let bucket_owner = if pk == PrincipalKind::Root { "root" } else { "acct-owner" };
            let principal = make_principal(pk, bucket_owner);
            for &id_pol in &id_pols {
                for &bkt_pol in &bkt_pols {
                    for &acl_kind in &acls {
                        for &pab_kind in &pabs {
                            for &ownership in &ownerships {
                                let acl = make_acl(acl_kind, &principal, sp.acl_permission);
                                let b_target = BucketTarget {
                                    name: "test-bkt",
                                    owner_account: bucket_owner,
                                    ownership,
                                    public_access_block: make_pab(pab_kind),
                                    policy: make_bucket_pol(bkt_pol),
                                    acl: &acl,
                                };
                                let o_target = ObjectTarget {
                                    key: "obj",
                                    owner_account: bucket_owner,
                                    acl: &acl,
                                };
                                let (b_opt, o_opt) = match sp.target {
                                    TargetKind::Service => (None, None),
                                    TargetKind::Bucket => {
                                        if op == Operation::CreateBucket {
                                            (None, None)
                                        } else {
                                            (Some(&b_target), None)
                                        }
                                    }
                                    TargetKind::Object => (Some(&b_target), Some(&o_target)),
                                };
                                let ctx = AuthzContext {
                                    identity_policy: id_pol,
                                    _marker: PhantomData,
                                };
                                let got = authorize(&principal, op, b_opt, o_opt, &ctx);
                                let want = reference_authorize(&principal, op, b_opt, o_opt, id_pol);
                                assert_eq!(
                                    got.is_allowed(),
                                    want,
                                    "mismatch for op={:?} pk={:?} id={:?} bkt={:?} acl={:?} pab={:?} own={:?}: got {:?}",
                                    op, pk, id_pol, bkt_pol, acl_kind, pab_kind, ownership, got
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    // Spot-check key Section 5.2 rows by hand.
    let outsider = make_principal(PrincipalKind::OtherAccountUser, "acct-a");
    let empty_acl = AclGrants::default();
    let pab_ignore = PublicAccessBlock {
        ignore_public_acls: true,
        ..Default::default()
    };
    let all_users_acl = make_acl(AclKind::AllUsers, &outsider, AclPermission::Read);
    let bkt_ignored = BucketTarget {
        name: "b",
        owner_account: "acct-a",
        ownership: BucketOwnership::ObjectWriter,
        public_access_block: pab_ignore,
        policy: BucketPolicyDecision::none(),
        acl: &all_users_acl,
    };
    assert!(!authorize(
        &outsider,
        Operation::GetObject,
        Some(&bkt_ignored),
        None,
        &AuthzContext { identity_policy: PolicyMatch::Allow, _marker: PhantomData }
    )
    .is_allowed());

    let bkt_enforced = BucketTarget {
        name: "b",
        owner_account: "acct-a",
        ownership: BucketOwnership::BucketOwnerEnforced,
        public_access_block: PublicAccessBlock::default(),
        policy: BucketPolicyDecision::none(),
        acl: &all_users_acl,
    };
    assert!(!authorize(
        &outsider,
        Operation::GetObject,
        Some(&bkt_enforced),
        None,
        &AuthzContext { identity_policy: PolicyMatch::Allow, _marker: PhantomData }
    )
    .is_allowed());

    let bkt_restrict_pub = BucketTarget {
        name: "b",
        owner_account: "acct-a",
        ownership: BucketOwnership::BucketOwnerEnforced,
        public_access_block: PublicAccessBlock {
            restrict_public_buckets: true,
            ..Default::default()
        },
        policy: make_bucket_pol(BucketPolKind::AllowAllPublic),
        acl: &empty_acl,
    };
    assert!(!authorize(
        &outsider,
        Operation::GetObject,
        Some(&bkt_restrict_pub),
        None,
        &AuthzContext { identity_policy: PolicyMatch::Allow, _marker: PhantomData }
    )
    .is_allowed());
}

struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn pick<T: Copy>(&mut self, slice: &[T]) -> T {
        slice[(self.next() as usize) % slice.len()]
    }
}

#[test]
fn differential_100k_random_inputs_against_reference() {
    let seed = 0x151A_CC00_2026_1124u64;
    eprintln!("authz differential seed: 0x{:016x}", seed);
    let mut rng = SplitMix64(seed);
    let accounts = ["root", "acct-a", "acct-b", ""];
    let ownerships = [
        BucketOwnership::BucketOwnerEnforced,
        BucketOwnership::BucketOwnerPreferred,
        BucketOwnership::ObjectWriter,
    ];
    let pols = [PolicyMatch::NoOpinion, PolicyMatch::Allow, PolicyMatch::ExplicitDeny];
    let perms = [
        AclPermission::Read,
        AclPermission::Write,
        AclPermission::ReadAcp,
        AclPermission::WriteAcp,
        AclPermission::FullControl,
    ];

    for i in 0..100_000usize {
        let op = OPS[(rng.next() as usize) % OPS.len()].op;
        let pk_idx = (rng.next() as usize) % 5;
        let p_acct = rng.pick(&accounts[..3]);
        let principal = match pk_idx {
            0 => Principal::Root,
            1 | 2 => Principal::User {
                account: p_acct.to_string(),
                name: format!("u{}", pk_idx),
                id: format!("id-{}", pk_idx),
            },
            3 => Principal::User {
                account: "other".to_string(),
                name: "other".to_string(),
                id: "id-other".to_string(),
            },
            _ => Principal::Anonymous,
        };
        let b_owner = rng.pick(&accounts);
        let o_owner = rng.pick(&accounts);
        let ownership = rng.pick(&ownerships);
        let pab = PublicAccessBlock {
            block_public_acls: rng.next() & 1 == 1,
            ignore_public_acls: rng.next() & 1 == 1,
            block_public_policy: rng.next() & 1 == 1,
            restrict_public_buckets: rng.next() & 1 == 1,
        };
        let b_policy = BucketPolicyDecision {
            explicit_deny: (rng.next() % 8) == 0,
            allow: (rng.next() % 3) == 0,
            is_public: rng.next() & 1 == 1,
            conditions_hold: (rng.next() % 4) != 0,
        };
        let mut b_grants = Vec::new();
        let mut o_grants = Vec::new();
        for (grants, _tag) in [(&mut b_grants, "b"), (&mut o_grants, "o")] {
            let n = (rng.next() % 3) as usize;
            for _ in 0..n {
                let grantee = match rng.next() % 4 {
                    0 => Grantee::CanonicalUser("root".to_string()),
                    1 => Grantee::CanonicalUser(format!("id-{}", rng.next() % 4)),
                    2 => Grantee::AuthenticatedUsers,
                    _ => Grantee::AllUsers,
                };
                grants.push(AclGrant {
                    grantee,
                    permission: rng.pick(&perms),
                });
            }
        }
        let b_acl = AclGrants { grants: b_grants };
        let o_acl = AclGrants { grants: o_grants };
        let b_target = BucketTarget {
            name: "bkt",
            owner_account: b_owner,
            ownership,
            public_access_block: pab,
            policy: b_policy,
            acl: &b_acl,
        };
        let o_target = ObjectTarget {
            key: "key",
            owner_account: o_owner,
            acl: &o_acl,
        };
        let has_b = op != Operation::ListBuckets && !(op == Operation::CreateBucket && (rng.next() & 1 == 0));
        let has_o = spec(op).target == TargetKind::Object && (rng.next() & 1 == 0);
        let b_opt = has_b.then_some(&b_target);
        let o_opt = has_o.then_some(&o_target);
        let id_pol = rng.pick(&pols);
        let ctx = AuthzContext {
            identity_policy: id_pol,
            _marker: PhantomData,
        };
        let got = authorize(&principal, op, b_opt, o_opt, &ctx);
        let want = reference_authorize(&principal, op, b_opt, o_opt, id_pol);
        assert_eq!(got.is_allowed(), want, "iteration {} failed", i);
    }
}

#[test]
fn all_inputs_no_opinion_denies_every_non_owner_or_unpermitted_user() {
    let empty_acl = AclGrants::default();
    let ctx = AuthzContext {
        identity_policy: PolicyMatch::NoOpinion,
        _marker: PhantomData,
    };
    let owner_user = Principal::User {
        account: "acct-a".to_string(),
        name: "alice".to_string(),
        id: "alice-id".to_string(),
    };
    let anon = Principal::Anonymous;
    let bkt = BucketTarget {
        name: "bkt",
        owner_account: "acct-a",
        ownership: BucketOwnership::ObjectWriter,
        public_access_block: PublicAccessBlock::default(),
        policy: BucketPolicyDecision::none(),
        acl: &empty_acl,
    };
    let obj = ObjectTarget {
        key: "obj",
        owner_account: "acct-a",
        acl: &empty_acl,
    };
    for s in OPS {
        let (b_opt, o_opt) = match s.target {
            TargetKind::Service => (None, None),
            TargetKind::Bucket => (Some(&bkt), None),
            TargetKind::Object => (Some(&bkt), Some(&obj)),
        };
        for p in [&owner_user, &anon, &Principal::Root] {
            if matches!(p, Principal::Root) && s.target != TargetKind::Service {
                // Root does not own acct-a's bucket/object, so with no opinion it is denied too.
            } else if matches!(p, Principal::Root) {
                continue;
            }
            let d = authorize(p, s.op, b_opt, o_opt, &ctx);
            assert!(
                matches!(d, Decision::Deny { code: "AccessDenied", reason: Reason::DefaultDeny }),
                "expected DefaultDeny for {:?} {:?}, got {:?}",
                p, s.op, d
            );
        }
    }
}

#[test]
fn default_deny_property_across_all_operation_table_rows() {
    let empty_acl = AclGrants::default();
    let owner_bkt = BucketTarget {
        name: "owned-bkt",
        owner_account: "owner-acct",
        ownership: BucketOwnership::BucketOwnerEnforced,
        public_access_block: PublicAccessBlock::default(),
        policy: BucketPolicyDecision::none(),
        acl: &empty_acl,
    };
    let owner_obj = ObjectTarget {
        key: "owned-obj",
        owner_account: "owner-acct",
        acl: &empty_acl,
    };
    let non_owners = [
        Principal::Anonymous,
        Principal::Root,
        Principal::User {
            account: "other-acct".to_string(),
            name: "outsider".to_string(),
            id: "outsider-id".to_string(),
        },
    ];
    for s in OPS {
        if s.target == TargetKind::Service || s.op == Operation::CreateBucket {
            // Anonymous is still denied for Service/CreateBucket when no policy grants it.
            let d = authorize(
                &Principal::Anonymous,
                s.op,
                None,
                None,
                &AuthzContext { identity_policy: PolicyMatch::Allow, _marker: PhantomData },
            );
            assert!(!d.is_allowed(), "anonymous allowed for {:?}", s.op);
            continue;
        }
        let (b_opt, o_opt) = match s.target {
            TargetKind::Bucket => (Some(&owner_bkt), None),
            TargetKind::Object => (Some(&owner_bkt), Some(&owner_obj)),
            TargetKind::Service => unreachable!(),
        };
        for p in &non_owners {
            for id_pol in [PolicyMatch::NoOpinion, PolicyMatch::Allow] {
                let d = authorize(
                    p,
                    s.op,
                    b_opt,
                    o_opt,
                    &AuthzContext { identity_policy: id_pol, _marker: PhantomData },
                );
                assert!(!d.is_allowed(), "{:?} unexpectedly allowed {:?} with {:?}", p, s.op, id_pol);
            }
        }
    }
}

#[test]
fn monotonicity_adding_deny_or_removing_allow_never_turns_deny_into_allow() {
    let mut rng = SplitMix64(0xDEAD_BEEF_1510_0001);
    for _ in 0..10_000 {
        let op = OPS[(rng.next() as usize) % OPS.len()].op;
        let sp = spec(op);
        let principal = match rng.next() % 3 {
            0 => Principal::Root,
            1 => Principal::User {
                account: "a".to_string(),
                name: "u".to_string(),
                id: "uid".to_string(),
            },
            _ => Principal::Anonymous,
        };
        let b_acl = make_acl(
            if rng.next() & 1 == 1 { AclKind::AllUsers } else { AclKind::Private },
            &principal,
            sp.acl_permission,
        );
        let bkt = BucketTarget {
            name: "b",
            owner_account: "a",
            ownership: BucketOwnership::ObjectWriter,
            public_access_block: PublicAccessBlock::default(),
            policy: BucketPolicyDecision {
                explicit_deny: false,
                allow: rng.next() & 1 == 1,
                is_public: rng.next() & 1 == 1,
                conditions_hold: true,
            },
            acl: &b_acl,
        };
        let id_pol = if rng.next() & 1 == 1 { PolicyMatch::Allow } else { PolicyMatch::NoOpinion };
        let b_opt = (sp.target != TargetKind::Service).then_some(&bkt);
        let base = authorize(&principal, op, b_opt, None, &AuthzContext { identity_policy: id_pol, _marker: PhantomData });
        // Adding explicit deny in identity policy or bucket policy must always Deny and never turn Deny into Allow.
        let id_deny = authorize(&principal, op, b_opt, None, &AuthzContext { identity_policy: PolicyMatch::ExplicitDeny, _marker: PhantomData });
        assert!(!(id_deny.is_allowed() && !base.is_allowed()), "identity deny flipped Deny->Allow");
        if matches!(principal, Principal::User { .. }) {
            assert!(!id_deny.is_allowed(), "identity explicit deny must deny User");
        }
        let mut bkt_deny = bkt.clone();
        bkt_deny.policy.explicit_deny = true;
        let b_deny = authorize(&principal, op, (sp.target != TargetKind::Service).then_some(&bkt_deny), None, &AuthzContext { identity_policy: id_pol, _marker: PhantomData });
        assert!(!(b_deny.is_allowed() && !base.is_allowed()), "bucket deny flipped Deny->Allow");
        if sp.target != TargetKind::Service {
            assert!(!b_deny.is_allowed(), "bucket explicit deny must deny bucket/object op");
        }
        // Removing allows must satisfy tightened <= base across both Allow and Deny bases.
        let empty_acl = AclGrants::default();
        let bkt_no_allow = BucketTarget {
            policy: BucketPolicyDecision::none(),
            acl: &empty_acl,
            ..bkt
        };
        let tightened = authorize(
            &principal,
            op,
            (sp.target != TargetKind::Service).then_some(&bkt_no_allow),
            None,
            &AuthzContext { identity_policy: PolicyMatch::NoOpinion, _marker: PhantomData },
        );
        assert!(
            !(tightened.is_allowed() && !base.is_allowed()),
            "removing allow turned Deny into Allow for {:?} {:?} (base={:?}, tightened={:?})",
            principal,
            op,
            base,
            tightened
        );
        // Adding allows must satisfy base <= loosened (adding Allow never turns Allow into Deny).
        let all_acl = make_acl(AclKind::AllUsers, &principal, sp.acl_permission);
        let bkt_allow = BucketTarget {
            policy: BucketPolicyDecision {
                explicit_deny: false,
                allow: true,
                is_public: false,
                conditions_hold: true,
            },
            acl: &all_acl,
            ..bkt
        };
        let loosened = authorize(
            &principal,
            op,
            (sp.target != TargetKind::Service).then_some(&bkt_allow),
            None,
            &AuthzContext { identity_policy: PolicyMatch::Allow, _marker: PhantomData },
        );
        assert!(
            !(base.is_allowed() && !loosened.is_allowed()),
            "adding allow turned Allow into Deny for {:?} {:?} (base={:?}, loosened={:?})",
            principal,
            op,
            base,
            loosened
        );
    }
}

#[test]
fn no_handler_in_ops_decides_access_outside_pipeline() {
    let ops_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("ops");
    let forbidden = ["allows(", "authz::", "authorize(", "Principal::", "evaluate_user"];
    for entry in std::fs::read_dir(&ops_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        if path.file_name().and_then(|s| s.to_str()) == Some("mod.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        for pat in forbidden {
            assert!(
                !text.contains(pat),
                "{} contains forbidden access-check helper {:?}; only ops/mod.rs pipeline may decide access",
                path.display(),
                pat
            );
        }
    }
}

fn amz_date(secs: u64) -> String {
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    format!("{}{:02}{}T{}{}{}Z", &t[7..11], m, &t[0..2], &t[12..14], &t[15..17], &t[18..20])
}

fn signed(c: &reqwest::Client, ak: &str, sk: &str, method: Method, host: &str, path_and_query: &str, body: &[u8]) -> reqwest::RequestBuilder {
    let (path, query) = path_and_query.split_once('?').unwrap_or((path_and_query, ""));
    let mut q: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (p.to_string(), String::new()),
        })
        .collect();
    q.sort();
    let canonical_query = q.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&");
    let date = amz_date(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    let hash = sha256_hex(body);
    let canonical = format!(
        "{}\n{}\n{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\n{}",
        method.as_str(), path, canonical_query, host, hash, date, hash
    );
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canonical.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), sk, &date[..8], "us-east-1", "s3", &sts);
    c.request(method, format!("http://{}{}", host, path_and_query))
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", hash)
        .header("authorization", format!("AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}", ak, scope, sig))
        .body(body.to_vec())
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_authz_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_users_toml(dir: &Path, text: &str) -> PathBuf {
    let p = dir.join("users.toml");
    std::fs::write(&p, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    p
}

#[tokio::test]
async fn missing_bucket_404_before_authz_and_unauthorized_existing_bucket_403() {
    let dir = temp_dir("404_vs_403");
    let cfg = r#"
[[users]]
name = "alice"
access_key = "AKALICE"
secret_key = "alice-secret"
policy = '{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}'

[[users]]
name = "bob"
access_key = "AKBOB"
secret_key = "bob-secret"
policy = '{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}'
"#;
    let iam = ReloadingIam::open(&write_users_toml(&dir, cfg)).unwrap();
    let mut st = S3ServiceState::new();
    st.auth = AuthConfig { credentials: None, allow_anonymous: false };
    *st.iam.write() = Some(iam);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(st)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    // Missing bucket returns 404 NoSuchBucket even for Bob.
    let res = signed(&c, "AKBOB", "bob-secret", Method::GET, &host, "/no-such-bucket/k", b"").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("NoSuchBucket"));
    assert_eq!(signed(&c, "AKBOB", "bob-secret", Method::HEAD, &host, "/no-such-bucket", b"").send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Once Alice creates it, Bob gets 403 AccessDenied.
    assert_eq!(signed(&c, "AKALICE", "alice-secret", Method::PUT, &host, "/alice-only", b"").send().await.unwrap().status(), StatusCode::OK);
    let res = signed(&c, "AKBOB", "bob-secret", Method::GET, &host, "/alice-only/k", b"").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(res.text().await.unwrap().contains("AccessDenied"));
}

#[tokio::test]
async fn single_key_root_setup_works_unchanged() {
    let mut st = S3ServiceState::new();
    st.auth = AuthConfig {
        credentials: Some(("AKROOT".into(), "root-secret".into())),
        allow_anonymous: false,
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(st)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    assert_eq!(signed(&c, "AKROOT", "root-secret", Method::PUT, &host, "/root-only", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, "AKROOT", "root-secret", Method::PUT, &host, "/root-only/k", b"hello").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, "AKROOT", "root-secret", Method::GET, &host, "/root-only/k", b"").send().await.unwrap().text().await.unwrap(), "hello");
    assert_eq!(signed(&c, "AKROOT", "root-secret", Method::DELETE, &host, "/root-only/k", b"").send().await.unwrap().status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn legacy_buckets_and_objects_without_owners_belong_to_root() {
    let dir = temp_dir("legacy_root");
    // Create bucket and object with an open server (writes legacy empty owner in manifest/no TAG_OWNER).
    {
        let st = S3ServiceState::with_data_dir(&dir);
        st.engine.write().create_bucket("legacy-bkt").unwrap();
        let b_id = st.engine.read().lookup_bucket("legacy-bkt").unwrap();
        st.bucket_configs.lock().apply_bucket_create(b_id, "legacy-bkt", 0, "", "").unwrap();
        // Write a needle with no attributes (no TAG_OWNER).
        let data = bytes::Bytes::from_static(b"legacy-payload");
        let off = st.store_payload(b_id, "old-key", &data, s3_server::AWAN_FLAG_ACTIVE, 1, 2).await.unwrap();
        st.engine.write().put_object_at(b_id, b"old-key", off, data.len() as u64, 1, 2, 0, s3_server::AWAN_FLAG_ACTIVE, 1).unwrap();
    }

    // Reopen with root + non-root user.
    let cfg = r#"
[[users]]
name = "alt"
access_key = "AKALT"
secret_key = "alt-secret"
policy = '{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}'
"#;
    let iam = ReloadingIam::open(&write_users_toml(&dir, cfg)).unwrap();
    let mut st = S3ServiceState::with_data_dir(&dir);
    st.auth = AuthConfig { credentials: Some(("AKROOT".into(), "root-secret".into())), allow_anonymous: false };
    *st.iam.write() = Some(iam);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(st)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    // Root owns the legacy bucket and object.
    assert_eq!(signed(&c, "AKROOT", "root-secret", Method::GET, &host, "/legacy-bkt/old-key", b"").send().await.unwrap().text().await.unwrap(), "legacy-payload");
    // Alt is denied.
    assert_eq!(signed(&c, "AKALT", "alt-secret", Method::GET, &host, "/legacy-bkt/old-key", b"").send().await.unwrap().status(), StatusCode::FORBIDDEN);
}

#[test]
fn tag_owner_constant_and_attrs_round_trip() {
    assert_eq!(TAG_OWNER, 0x0030);
    assert_eq!(DEFAULT_BUCKET_OWNERSHIP, BucketOwnership::BucketOwnerEnforced);

    let mut a = Attrs::new();
    assert_eq!(a.owner(), None);
    a.set_owner("acct-writer");
    assert_eq!(a.owner(), Some("acct-writer"));
    let encoded = a.encode().unwrap();
    let decoded = Attrs::decode(&encoded).unwrap();
    assert_eq!(decoded.owner(), Some("acct-writer"));
}

#[tokio::test]
async fn audit_reason_logged_to_request_log_and_never_sent_to_client() {
    let dir = temp_dir("audit_reason");
    let cfg = r#"
[[users]]
name = "alice"
access_key = "AKALICE"
secret_key = "alice-secret"
policy = '{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}'

[[users]]
name = "bob"
access_key = "AKBOB"
secret_key = "bob-secret"
policy = '{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}'
"#;
    let iam = ReloadingIam::open(&write_users_toml(&dir, cfg)).unwrap();
    let mut st = S3ServiceState::new();
    st.auth = AuthConfig { credentials: None, allow_anonymous: false };
    *st.iam.write() = Some(iam);
    let log_lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let lines = Arc::clone(&log_lines);
        let sink: LogSink = Arc::new(move |l: &str| lines.lock().unwrap().push(l.to_string()));
        *st.access_log.write() = Some(sink);
    }
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(st)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    assert_eq!(signed(&c, "AKALICE", "alice-secret", Method::PUT, &host, "/log-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    let deny_res = signed(&c, "AKBOB", "bob-secret", Method::GET, &host, "/log-bkt/x", b"").send().await.unwrap();
    assert_eq!(deny_res.status(), StatusCode::FORBIDDEN);
    let headers_str = format!("{:?}", deny_res.headers());
    let body = deny_res.text().await.unwrap();
    assert!(!headers_str.contains("authz_reason") && !headers_str.contains("default-deny"));
    assert!(!body.contains("default-deny") && !body.contains("step") && !body.contains("owner"));

    let lines = log_lines.lock().unwrap().clone();
    assert_eq!(lines.len(), 2, "{:?}", lines);
    assert!(lines[0].contains("\"authz_reason\":"), "allow log missing authz_reason: {}", lines[0]);
    assert!(lines[1].contains("\"authz_reason\":\"step6-default-deny\""), "deny log missing authz_reason: {}", lines[1]);
}
