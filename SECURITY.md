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

## Server-Side Encryption (#121) Threat Model

Server-side encryption (`SSE-C` in PR 1; `SSE-S3` and `SSE-KMS` in PR 2/PR 3) protects object payload bytes at rest in volume needles and staged multipart part files using AES-256-GCM (`AWE1` 64 KiB chunk framing, `FORMAT.md`).

- **Disk thief (offline access to volume `.dat`/`.idx`, `.staging/`, `buckets.manifest`):**
  - Learns: bucket names, object keys, logical `plaintext_size`, timestamps, attributes (`x-amz-meta-*`, tags, owners, retention/legal hold — stored unencrypted as in S3), opaque ciphertext ETags, and `TAG_SSE` (`0x0050`) metadata (`mode`, `key_md5`, 128-bit `key_salt`, HMAC `verifier_mac`, 32-bit `nonce_prefix`).
  - Cannot learn: object plaintext bytes or the SSE-C customer key (never written to disk).
  - Cannot forge/tamper: every 64 KiB chunk carries a 16-byte GCM tag whose AAD binds `"AWE1"`, `part_number`, `chunk_index`, and `is_last_chunk`, rejecting bit-flips, truncation, extension, or chunk/part reordering before releasing bytes.
- **Log reader (access to stderr request logs, `--access-log`, or OTLP traces):**
  - Learns: request paths, bucket/key names, HTTP status, timing, error codes, and `key_md5` response headers if logged.
  - Cannot learn: the raw SSE-C customer key (`x-amz-server-side-encryption-customer-key` / `x-amz-copy-source-server-side-encryption-customer-key`) or plaintext bytes; customer keys are excluded from `Debug` formatting, logs, and traces, and key, DEK, and `Awe1EncryptWriter` plaintext chunk buffers are zeroized on drop.
- **Malicious client (network attacker or unprivileged caller):**
  - Cannot decrypt without possessing the exact 32-byte key: verification checks both `key_md5` and the salted `HMAC-SHA256` `verifier_mac` using volatile constant-time equality (`sse::ct_eq`), and runs before conditional preconditions (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, `x-amz-copy-source-if-*`) so unauthorized callers cannot probe ETags or timestamps via `304`/`412`.
  - Cannot extract plaintext from caches or shared memory: SSE-C plaintext never enters `payload_cache` or `single_flight`, and encrypted payloads never serve from volume or staging `mmap` slices.
  - Plaintext compression is skipped on encrypted objects (no CRIME/BREACH length leakage). Per-object `HKDF-SHA256` with random 128-bit `key_salt` derives an object-specific 256-bit DEK so reusing a customer key across objects does not accumulate GCM invocations under one key. Production deployments must enable TLS (`--profile production` refuses plain HTTP); ceph `s3-tests` runs SSE-C over HTTP in the conformance profile (`is_secure = False`).
