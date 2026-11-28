//! Accounts, owners, and the single `authorize` decision function (#151, CONFORMANCE_DESIGN.md 5.2).
//!
//! `authorize` is the only place access is decided. Handlers receive an `Authorized` request
//! from the pipeline in `server/src/ops/mod.rs` and never check credentials or access.

use crate::iam::{Principal, ROOT_ACCOUNT};
use crate::ops::{spec, AclPermission, Operation, TargetKind};
use std::marker::PhantomData;

/// Bucket ownership mode (owner decision 48, CONFORMANCE_DESIGN.md 5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketOwnership {
    BucketOwnerEnforced,
    BucketOwnerPreferred,
    ObjectWriter,
}

/// D5 (owner decision 48): default ownership setting of new buckets.
pub const DEFAULT_BUCKET_OWNERSHIP: BucketOwnership = BucketOwnership::BucketOwnerEnforced;

/// Result of evaluating an IAM identity policy for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyMatch {
    NoOpinion,
    Allow,
    ExplicitDeny,
}

/// Per-bucket Public Access Block settings (#122 / #151).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PublicAccessBlock {
    pub block_public_acls: bool,
    pub ignore_public_acls: bool,
    pub block_public_policy: bool,
    pub restrict_public_buckets: bool,
}

/// Result of evaluating a bucket policy for the current principal, operation, resource, and context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketPolicyDecision {
    pub explicit_deny: bool,
    pub allow: bool,
    pub is_public: bool,
    pub conditions_hold: bool,
}

impl BucketPolicyDecision {
    pub const fn none() -> Self {
        Self {
            explicit_deny: false,
            allow: false,
            is_public: false,
            conditions_hold: false,
        }
    }
}

/// ACL grantee kind (#122 / #151).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grantee {
    CanonicalUser(String),
    AuthenticatedUsers,
    AllUsers,
}

/// One ACL grant entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclGrant {
    pub grantee: Grantee,
    pub permission: AclPermission,
}

/// ACL grant list on a bucket or object.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AclGrants {
    pub grants: Vec<AclGrant>,
}

/// Bucket inputs to `authorize`.
#[derive(Debug, Clone)]
pub struct BucketTarget<'a> {
    pub name: &'a str,
    pub owner_account: &'a str,
    pub ownership: BucketOwnership,
    pub public_access_block: PublicAccessBlock,
    pub policy: BucketPolicyDecision,
    pub acl: &'a AclGrants,
}

/// Object inputs to `authorize`.
#[derive(Debug, Clone)]
pub struct ObjectTarget<'a> {
    pub key: &'a str,
    pub owner_account: &'a str,
    pub acl: &'a AclGrants,
}

/// Caller context inputs to `authorize`.
#[derive(Debug, Clone)]
pub struct AuthzContext<'a> {
    pub identity_policy: PolicyMatch,
    pub _marker: PhantomData<&'a ()>,
}

/// Audit reason naming which step of CONFORMANCE_DESIGN.md 5.2 allowed or denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    IdentityPolicyDeny,
    BucketPolicyDeny,
    RootOwner,
    OwnerAndIdentityPolicy,
    BucketPolicy,
    Acl,
    DefaultDeny,
}

impl Reason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Reason::IdentityPolicyDeny => "step1-identity-policy-deny",
            Reason::BucketPolicyDeny => "step1-bucket-policy-deny",
            Reason::RootOwner => "step3-root-owner",
            Reason::OwnerAndIdentityPolicy => "step3-owner-identity-policy",
            Reason::BucketPolicy => "step4-bucket-policy",
            Reason::Acl => "step5-acl",
            Reason::DefaultDeny => "step6-default-deny",
        }
    }
}

/// Access decision. Deliberately has no `Default` implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow(Reason),
    Deny { code: &'static str, reason: Reason },
}

impl Decision {
    pub const fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allow(_))
    }

    pub const fn reason(&self) -> Reason {
        match self {
            Decision::Allow(r) | Decision::Deny { reason: r, .. } => *r,
        }
    }
}

fn normalize_owner(owner: &str) -> &str {
    if owner.is_empty() {
        ROOT_ACCOUNT
    } else {
        owner
    }
}

fn acl_grants_permission(
    acl: &AclGrants,
    principal: &Principal,
    required: AclPermission,
    ignore_public_acls: bool,
) -> bool {
    acl.grants.iter().any(|g| {
        let perm_ok = g.permission == AclPermission::FullControl || g.permission == required;
        if !perm_ok {
            return false;
        }
        match &g.grantee {
            Grantee::CanonicalUser(id) => principal.canonical_id().is_some_and(|pid| pid == id),
            Grantee::AuthenticatedUsers => {
                !ignore_public_acls && !matches!(principal, Principal::Anonymous)
            }
            Grantee::AllUsers => !ignore_public_acls,
        }
    })
}

/// Single authorization decision function (CONFORMANCE_DESIGN.md 5.2).
///
/// Evaluation order:
/// 1. Explicit `Deny` in the bucket policy or caller's identity policy -> Deny (except
///    `Principal::Root` on `GetBucketPolicy`/`PutBucketPolicy`/`DeleteBucketPolicy` for lockout
///    recovery, #544).
/// 2. Public access block (`IgnorePublicAcls`, `RestrictPublicBuckets`).
/// 3. Caller's account owns the resource and its identity policy allows (root: always) -> Allow.
/// 4. Bucket policy allows for this principal and conditions hold (for another account's user,
///    its identity policy must also allow) -> Allow.
/// 5. ACL on the resource grants the operation's permission (unless `BucketOwnerEnforced`) -> Allow.
/// 6. Otherwise -> Deny (`AccessDenied`).
pub fn authorize(
    principal: &Principal,
    op: Operation,
    bucket: Option<&BucketTarget<'_>>,
    object: Option<&ObjectTarget<'_>>,
    context: &AuthzContext<'_>,
) -> Decision {
    let op_spec = spec(op);
    // Lockout recovery, as in AWS (#544, owner decision Option A): a bucket policy explicit deny
    // cannot lock Principal::Root out of reading, replacing, or deleting that policy.
    let recovering = matches!(principal, Principal::Root)
        && matches!(
            op,
            Operation::GetBucketPolicy | Operation::PutBucketPolicy | Operation::DeleteBucketPolicy
        );
    let unlocked;
    let bucket = match bucket {
        Some(b) if recovering && b.policy.explicit_deny => {
            let mut t = b.clone();
            t.policy.explicit_deny = false;
            unlocked = t;
            Some(&unlocked)
        }
        other => other,
    };
    authorize_for(
        principal,
        op_spec.target,
        op_spec.acl_permission,
        op == Operation::CreateBucket && bucket.is_none(),
        bucket,
        object,
        context,
    )
}

/// Evaluates `s3:BypassGovernanceRetention` through the same 6-step model as `authorize`.
pub fn authorize_bypass_governance(
    principal: &Principal,
    bucket: Option<&BucketTarget<'_>>,
    object: Option<&ObjectTarget<'_>>,
    context: &AuthzContext<'_>,
) -> Decision {
    authorize_for(
        principal,
        TargetKind::Object,
        Some(AclPermission::Write),
        false,
        bucket,
        object,
        context,
    )
}

fn authorize_for(
    principal: &Principal,
    target_kind: TargetKind,
    acl_permission: Option<AclPermission>,
    create_new_bucket: bool,
    bucket: Option<&BucketTarget<'_>>,
    object: Option<&ObjectTarget<'_>>,
    context: &AuthzContext<'_>,
) -> Decision {
    // Step 1: Explicit Deny in identity policy or bucket policy.
    if matches!(principal, Principal::User { .. })
        && context.identity_policy == PolicyMatch::ExplicitDeny
    {
        return Decision::Deny {
            code: "AccessDenied",
            reason: Reason::IdentityPolicyDeny,
        };
    }
    if bucket.is_some_and(|b| b.policy.explicit_deny) {
        return Decision::Deny {
            code: "AccessDenied",
            reason: Reason::BucketPolicyDeny,
        };
    }

    // Step 2: Public access block (per bucket).
    let pab = bucket.map(|b| b.public_access_block).unwrap_or_default();

    let caller_account = principal.account();
    let bucket_owner = bucket.map(|b| normalize_owner(b.owner_account));
    let caller_owns_bucket = caller_account.zip(bucket_owner).is_some_and(|(ca, bo)| ca == bo);

    let caller_owns_resource = match target_kind {
        TargetKind::Service => caller_account.is_some(),
        TargetKind::Bucket => {
            if create_new_bucket {
                caller_account.is_some()
            } else {
                caller_owns_bucket
            }
        }
        TargetKind::Object => match bucket {
            None => false,
            Some(b) => {
                let b_owner = normalize_owner(b.owner_account);
                let eff_owner = match (b.ownership, object) {
                    (BucketOwnership::BucketOwnerEnforced, _) | (_, None) => b_owner,
                    (_, Some(o)) => {
                        if o.owner_account.is_empty() {
                            b_owner
                        } else {
                            o.owner_account
                        }
                    }
                };
                caller_account.is_some_and(|ca| ca == eff_owner)
            }
        },
    };

    // Step 3: Caller's account owns the resource, and its identity policy allows (root: always).
    if caller_owns_resource {
        match principal {
            Principal::Root => return Decision::Allow(Reason::RootOwner),
            Principal::User { .. } if context.identity_policy == PolicyMatch::Allow => {
                return Decision::Allow(Reason::OwnerAndIdentityPolicy);
            }
            Principal::User { .. } | Principal::Anonymous => {}
        }
    }

    // Step 4: Bucket policy allows the action for this principal and its conditions hold.
    // For a user of another account, its identity policy must also allow (#541).
    let cross_account_id_ok = match principal {
        Principal::User { .. } if !caller_owns_bucket => {
            context.identity_policy == PolicyMatch::Allow
        }
        _ => true,
    };
    if let Some(b) = bucket {
        let public_blocked = b.policy.is_public && pab.restrict_public_buckets && !caller_owns_bucket;
        if b.policy.allow && b.policy.conditions_hold && !public_blocked && cross_account_id_ok {
            return Decision::Allow(Reason::BucketPolicy);
        }
    }

    // Step 5: ACL on the resource grants the permission (unless BucketOwnerEnforced).
    if let Some(b) = bucket {
        if b.ownership != BucketOwnership::BucketOwnerEnforced {
            let acl = match target_kind {
                TargetKind::Object => object.map(|o| o.acl).unwrap_or(b.acl),
                TargetKind::Service | TargetKind::Bucket => b.acl,
            };
            if let Some(req_perm) = acl_permission {
                if acl_grants_permission(acl, principal, req_perm, pab.ignore_public_acls) {
                    return Decision::Allow(Reason::Acl);
                }
            }
        }
    }

    // Step 6: Default deny.
    Decision::Deny {
        code: "AccessDenied",
        reason: Reason::DefaultDeny,
    }
}
