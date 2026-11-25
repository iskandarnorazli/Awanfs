# Security policy

AwanFS is not production-ready yet (see the status in README.md). Reports are still welcome.

## Reporting a vulnerability

Report it privately through GitHub: **Security → Report a vulnerability** on this repository. Do not open a public issue.

Please include the version (commit), the platform, and the smallest request sequence that shows the problem. You will get an answer within 7 days.

## Scope

In scope: the `s3-server` binary and its crate, including request parsing, SigV4 checks, multi-account authorization, IAM policies, TLS, and the on-disk format (a crafted volume or index that crashes or corrupts recovery).

Out of scope: the benchmark scripts, and deployments that disable authentication (`--allow-anonymous`).

## Multi-Account Threat Model and Authorization Order

Every request follows a single pipeline: `authenticate` produces a `Principal` (`Root`, `User { account, name, id }`, or `Anonymous`), `resolve_bucket` loads the bucket manifest (returning `404 NoSuchBucket` before authorization if the bucket does not exist, except service-level operations and `CreateBucket`), and `authz::authorize` returns a typed `Decision` (`Allow` or `Deny`) with an audit `Reason`. Handlers in `server/src/ops/` receive only `Authorized` requests and never inspect credentials or evaluate access rules directly.

- **Account isolation:** Each bucket has an owner account stored in `buckets.manifest` (legacy buckets without an owner belong to `"root"`). Objects written by an account other than the bucket owner carry `TAG_OWNER` (`0x0030`) in their attribute blob. Same-account IAM users share ownership; cross-account access requires an explicit grant (bucket policy or ACL) and is otherwise denied even when a user has wildcard identity permissions (`s3:*`).
- **Evaluation order:** `authz::authorize` enforces explicit deny before allow across identity policies, bucket policies, ACLs, and Public Access Block settings, ending with default deny:
  1. Explicit `Deny` in identity policy or bucket policy -> `Deny`
  2. Public Access Block filtering (`block_public_acls`, `ignore_public_acls`, `block_public_policy`, `restrict_public_buckets`) -> rejects or ignores public ACLs and public bucket policies
  3. Caller's account owns the resource -> `Allow` when `Root` (always unless denied in step 1) or when identity policy allows
  4. Bucket policy allows the action for this principal and its conditions hold (when not blocked by Public Access Block `restrict_public_buckets`) -> `Allow`
  5. ACL on the resource grants the permission (when bucket ownership is not `BucketOwnerEnforced` and not ignored by Public Access Block `ignore_public_acls`) -> `Allow`
  6. Default deny -> `Deny`
- **Anonymous access (Decision 40):** `--allow-anonymous` is refused at startup (exit code 2) whenever users are configured in `--config`, preventing unauthenticated callers from bypassing IAM or multi-account restrictions.
