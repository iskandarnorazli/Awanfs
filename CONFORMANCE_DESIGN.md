# Design: passing the ceph s3-tests suite

Status: **plan of record, 2026-11-14.** The owner decided D1, D2, D3 and the large-GET rule
on 2026-11-14, then D4, D5, D6 and the target the same day (section 11, `ARCHITECTURE.md`
decisions 43 to 50). Nothing is open.

The companion file [`tests/s3tests/CONFORMANCE_MAP.md`](tests/s3tests/CONFORMANCE_MAP.md)
assigns every test in the pinned suite to one area below, with its current status and failure
line. It is generated (`tests/s3tests/conformance_map.py`); this file is the reasoning.

## 1. Goal

Every test in `s3tests/functional/test_s3.py` at the pinned commit passes against AwanFS,
except the tests section 9 lists as out of scope with a reason. The work is split so that
several people or agents can build areas in parallel without designing storage or security
on their own: the five foundations in sections 3 to 5 are designed here, and each area in
sections 6 to 8 is a consumer of them.

Constraints that hold for every change:

- **Speed and memory.** The nightly ratio check stays in tolerance and the memory budget gate
  stays under its ceiling. A request for an object that uses none of the new features does
  the same work as today: HEAD stays index-only, GET stays zero-copy.
- **Durability and recovery.** Anything a 200 acknowledges survives a restart. New record
  kinds are replayed by recovery and kept by compaction, with a test for each.
- **Multi-node later (#52).** All new state is an append-only record in the volume log or
  the bucket manifest. Nothing new lives only in memory or in a side file that a replica
  could not rebuild from those two logs.
- **Both platforms.** Windows and Linux.

## 2. Where it stands

Run 43537724429 (main, 2026-11-14): 838 test cases, 246 pass, 498 fail, 94 skipped. By test
function (760), the map's table gives the split by area. Three facts explain most failures:

1. **Objects carry no attributes.** Content-Type, user metadata, Cache-Control, tags, ACLs,
   checksums, encryption details and retention have nowhere to live: the needle header is 64
   fixed bytes and the index row is 64 bytes. The areas that need a stored attribute (object
   metadata, tagging, checksums, encryption, object lock, ACLs) hold about 200 tests that do
   not pass.
2. **Buckets carry no configuration** except a versioning status line. Policy, ACL, CORS,
   lifecycle, encryption defaults, object lock, logging and ownership controls need a store.
   The areas that need one hold about 330 such tests, 113 of them bucket logging.
3. **There are no owners.** Any signed user may do anything, and anonymous access is all or
   nothing. Policies, ACLs, ownership and anonymous access hold about 100 tests that do not
   pass.

## 3. Foundations: request pipeline, bucket configuration, clock

### 3.1 One operation table (issue #148)

`s3_api.rs` routes requests in one 3,600-line match. A query string it does not know falls
through to the plain object or bucket operation: `PUT ?partNumber&uploadId` with
`x-amz-copy-source` stored the request body as a part until #111 refused it. With twelve
areas adding sub-resources, that class of bug and merge conflicts in one file are certain.

Design:

- An `Operation` enum with one variant per S3 operation, and one function
  `classify(method, bucket, key, query, headers) -> Result<Operation, S3Error>`. A sub-resource
  or header combination with no variant is `NotImplemented` (501). No fall-through.
- One table row per operation: its name, the IAM action (`s3:GetObject`), the ACL permission
  it needs (`READ`, `WRITE`, `READ_ACP`, `WRITE_ACP`, `FULL_CONTROL`), whether it targets a
  bucket or an object, and its handler. `iam::requirements` moves into this table.
- Handlers move to one module per area (`ops/object.rs`, `ops/multipart.rs`, `ops/acl.rs`
  and so on). The move is mechanical: no behaviour changes, the existing tests are the gate.

Every request then runs the same steps in the same order. The order decides which error a
client sees when several apply, and the suite checks it:

1. **Classify.** Unknown operation: 501.
2. **Authenticate.** Header SigV4, presigned query, or anonymous. A presigned request with
   `X-Amz-Expires` outside 1..604800 is 400 `AuthorizationQueryParametersError`; an expired
   one is 403 `AccessDenied`.
3. **Resolve the bucket.** Missing: 404 `NoSuchBucket`, before authorization. (The suite
   expects 404, not 403, for anonymous requests to a bucket that is gone.)
4. **Authorize** (section 5). One call.
5. **Validate** the request's headers and body shape: checksum headers, encryption headers,
   tagging, ACL and lock headers. Errors here are 400s.
6. **Resolve the object or version and check preconditions**, under the key lock when the
   operation writes.
7. **Execute.**
8. **Respond.** Object headers are rendered by one function from one `ObjectView` (index row,
   attributes, overlay). GET, HEAD, copy results and GetObjectAttributes all use it, so a new
   attribute shows up everywhere at once.

### 3.2 Bucket configuration store (issue #149)

`buckets.manifest` is a text log: `+id:name`, `-id:name`, `~id:status`. It gains one record:

    =<id>:<kind>:<base64 of the document>

- `kind` is one of `owner`, `policy`, `acl`, `ownership`, `public-access-block`, `cors`,
  `lifecycle`, `logging`, `encryption`, `object-lock`, `tagging`. The existing `~` line stays
  as it is and is read as kind `versioning`.
- The document is the bytes the client sent, after validation. Get returns them as stored
  (GetBucketPolicy must round-trip). An empty document deletes the configuration.
- The latest record per (id, kind) wins. Deleting the bucket drops them all.
- A write appends the record and syncs the manifest before the 200. Configuration writes
  are rare, so there is no group commit.
- At startup, when dead records outnumber live ones, the manifest is rewritten to a
  temporary file and renamed.
- In memory each bucket holds one immutable `BucketConfig` (parsed forms of every kind)
  behind an atomically swapped pointer. A request loads the pointer once and never takes a
  lock for configuration.
- Limits: 20 KiB per policy, 64 KiB for any other kind. Over the limit: 400.
- An unknown kind in the manifest stops startup, like a newer volume format does.

### 3.3 Clock and background work (issue #158)

Lifecycle and bucket logging are tested by waiting. The suite's lifecycle tests assume a
"day" lasts `lc_debug_interval` seconds (10 in our configuration).

- One `Clock` trait gives the time to everything that stamps or compares times. Production
  uses the system clock.
- `--lifecycle-day-seconds N` shortens a lifecycle day. It is for tests; the server prints a
  warning at startup when it is set.
- One scheduler runs sweeps (lifecycle expiration, incomplete multipart uploads, log
  delivery). A sweep runs off the request path, does a bounded amount of work per tick and
  takes the same key locks as a client DELETE. Its cost is checked by the ratio run.
- **Last-Modified is real.** Responses send a constant date today. It comes from the
  needle's creation time through the `Clock`. Conditional requests, lifecycle and listing
  depend on it.

## 4. Foundation: object attributes (issue #150)

### 4.1 What is stored

| Group | Fields | Set when |
|---|---|---|
| Headers | Content-Type, Content-Encoding, Content-Disposition, Content-Language, Cache-Control, Expires, website redirect | at write |
| User metadata | `x-amz-meta-*` | at write |
| Owner | canonical id of the writer | at write |
| Checksums | algorithm, value, full-object or composite; per part for multipart | at write |
| Encryption | algorithm, key id or key MD5, wrapped data key, nonce prefix, chunk size | at write |
| Storage class | a label | at write |
| Tags | up to 10 pairs | at write, replaced later |
| ACL | grants | at write, replaced later |
| Retention | mode, retain-until | at write, replaced later |
| Legal hold | on or off | at write, replaced later |

### 4.2 On disk: volume format 3

**In the needle.** The reserved bytes 18..20 of the needle header become `attr_len`. The
attributes block sits between the key and the payload:

    header (64) | key | attributes block (attr_len) | payload | padding

- `attr_len` 0 means no block: a needle without attributes is byte-for-byte what format 2
  writes, and every format 2 needle is a valid format 3 needle.
- The block is at most 65,535 bytes. A request whose attributes do not fit is 400
  `MetadataTooLarge`. (S3 allows 2 KiB of user metadata and 8 KiB of headers.)
- The block starts with its own CRC32C. The payload CRC stays payload-only, so the streaming
  write path and the read path do not change for the payload.
- Encoding: one version byte (1), then records of `tag (u16) | length (u32) | bytes`.
  Readers skip tags they do not know; compaction copies the block verbatim. Tags are listed
  in `FORMAT.md`. No JSON: parsing is bounded and the parser is a fuzz target.
- A needle with a block sets flag `0x400`. The journal record already carries the flags.

**Changes after the write.** Tags, ACL, retention and legal hold change without rewriting
the object. Each change appends an *attribute update* needle: flag `0x200`, the object's key,
and a payload of the target version id (8 bytes, as in a version delete) followed by an
attributes block holding only the replaced groups.

- Replay applies updates in creation-time order, the rule versions already use. The latest
  update per (key, version, group) wins.
- Deleting a version makes its updates dead. Compaction keeps the latest live update per
  group and drops the rest. It never folds an update into the object's needle.
- The object-version model in `verification/tla` gains attribute state and one invariant:
  what recovery rebuilds equals the live state, attributes included.

A volume with a superblock format above 3 still stops startup. A format 2 binary refuses a
format 3 volume. There is no migration: old needles are read as they are.

### 4.3 In memory

- The index row stays 64 bytes. It gains one flag bit, "has attributes".
- Objects without the flag are served exactly as today.
- For objects with it, the block is read from the needle's first sector, which GET maps
  anyway. HEAD would become a disk read, so parsed blocks go into a small LRU keyed by volume
  offset. The LRU is charged to the cache half of `--memory-budget`.
- Updates live in a side map from (bucket, key hash, version) to the offsets of the latest
  update per group. Only objects that were updated have an entry. Recovery rebuilds it from
  the journal's `0x200` records.
- Listing must not read needles. `ListObjects` needs the owner per key: with
  `BucketOwnerEnforced` (the default, section 5) it is the bucket owner. Under other
  ownership settings a row flag marks objects owned by someone else, and only those are read.

### 4.4 Multipart and copy

- Attributes sent with CreateMultipartUpload are saved in the upload's staging directory
  (`attrs`, written and synced before the 200) and written into the manifest needle's block
  at Complete.
- Per-part sizes and MD5s are already in the manifest. `GET ?partNumber=N`, `x-amz-mp-parts-count`
  and GetObjectAttributes read them from there.
- CopyObject follows `x-amz-metadata-directive`: COPY carries the source block over, REPLACE
  takes the request's. The ACL is never copied. Tags follow `x-amz-tagging-directive`.
  Copying an object onto itself without REPLACE is 400.

## 5. Foundation: owners and authorization (issue #151)

This section is security. A mistake here passes tests and still exposes data, so it gets a
decision-table test, a default-deny property test and a review by someone who did not write it.

### 5.1 Who

- **Accounts.** The root key is one account. Each user in the users file is its own account
  unless it names `account = "<name>"`. Users gain `id`, `display_name` and `email` (ACL
  grants name them). Anonymous is its own principal.
- **Owners.** A bucket's owner is the account that created it (`owner` record, section 3.2).
  An object's owner is its writer, stored in its attributes. Buckets and objects that
  predate owners belong to root.
- **Tenants** (RGW's `tenant:bucket` names) are out of scope (section 9).

### 5.2 The decision

`authorize(principal, operation, bucket, object, context) -> Allow | Deny(code)` is the only
place access is decided. Handlers do not check access themselves. In order:

1. An explicit `Deny` in the bucket policy or the caller's identity policy: deny.
2. Public access block (per bucket): with `IgnorePublicAcls`, grants to AllUsers and
   AuthenticatedUsers do not count; with `RestrictPublicBuckets`, a public policy only
   applies to the owner's account.
3. The caller's account owns the resource, and its identity policy allows the action
   (root: always): allow.
4. The bucket policy allows the action for this principal and its conditions hold: allow.
5. An ACL on the resource grants the permission the operation table names, to this
   principal, to AuthenticatedUsers (signed callers) or to AllUsers: allow. With
   `BucketOwnerEnforced` ownership, ACLs are not consulted.
6. Otherwise deny: 403 `AccessDenied`.

An identity policy only reaches the caller's own account's resources. Reaching another
account's bucket needs a grant on the resource (steps 4 and 5). That is the behaviour change:
today's test users hold `s3:*` on `*` and may touch anything.

`--allow-anonymous` keeps its meaning (every anonymous request is allowed) and stays refused
together with a users file (decision 40). With owners, a public bucket is expressed by an
ACL or a policy.

### 5.3 Policies, ACLs, public access block

- A bucket policy is parsed once at PUT into statements: effect, principals, actions,
  resources, conditions. **A policy with an element, condition operator or condition key the
  parser does not implement is rejected** (400 `MalformedPolicy`). Ignoring an unknown
  condition would turn a restriction into a grant.
- Condition keys come from a `context` built in step 5 of the pipeline (request headers such
  as `x-amz-acl` and `x-amz-server-side-encryption`, `s3:prefix`, existing and requested
  object tags, the copy source). The area issue lists the keys the suite uses.
- ACLs: canned ACLs and grant headers on PUT, `?acl` documents on buckets and objects, the
  three grantee kinds. Per-grantee grants are built (decision 48). `BlockPublicAcls` rejects a PUT that sets a public ACL;
  `BlockPublicPolicy` rejects a public policy.
- Ownership controls: `BucketOwnerEnforced` (default for new buckets, decision 48),
  `BucketOwnerPreferred`, `ObjectWriter`. The suite's profile (#161) sets the default to
  `ObjectWriter`, because its ACL tests expect ACLs to work on a new bucket.

### 5.4 Tests this foundation ships with

- A table test: principal (owner, other account, anonymous) x policy (none, allow, deny) x
  ACL (private, public-read, grant) x public access block, against the expected decision.
- A property test: with no policy and no ACL beyond the owner's, no other principal is ever
  allowed any operation.
- Every operation table row is covered by the table test; a new row without an expectation
  fails the build.

## 6. Areas: making existing features conform

Each area is one issue. Section 10 gives the order. Section 6.1 is the contract they share.

### 6.1 How an area is worked

- **Gate:** every test the map lists under the area is in `tests/s3tests/passing.txt`, the
  nightly ratchet line shows them, and no listed test stopped passing.
- **No indirect state machines** (owner, 2026-11-14). Anything with states and rules for
  moving between them is a pure function in `server/src/fsm.rs` with a test over every
  (state, event) pair and a row in [`STATE_MACHINES.md`](STATE_MACHINES.md), checked in CI.
  State shared between concurrent requests or rebuilt by recovery is also modelled in
  `verification/tla`, with a broken variant the model checker must reject. Access decisions
  and request classification are tables, tested as tables.
- **Tests first.** Each behaviour gets a Rust test in `server/tests/` that fails before the
  change, and a journey line in `scripts/verify_drive.sh` when a user can see it. The suite
  is the acceptance check, not the only test.
- **Uses the foundations.** No new side files, no access checks in handlers, no new record
  kinds. If an area needs one, stop and ask: that is a change to this design.
- **Stop and ask** before changing what an existing test expects, a default a user can see,
  or the on-disk format.
- **Performance.** The ratio check and the budget gate run on the PR. An area that slows a
  row beyond tolerance is not done.

### 6.2 Tagging (#115)
Tags are an attribute group (section 4). `x-amz-tagging` on PUT, copy and multipart create;
`?tagging` GET, PUT and DELETE on an object or a version; `x-amz-tagging-count` on GET.
Limits: 10 tags, 128-character keys, 256-character values. Needs #150.

### 6.3 Conditional requests (#112)
GET and HEAD: `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` with
RFC 7232 precedence. Conditional writes: `If-Match` and `If-None-Match` on PUT, DELETE and
CompleteMultipartUpload, checked under the key lock so two writers cannot both pass. Copy
source conditions. Needs real Last-Modified (section 3.3).

### 6.4 Multipart (#111)
UploadPartCopy with a range and a source version; `GET ?partNumber`; resending a part;
Content-Type and metadata from create; the version id in the Complete response. Parts under
5 MiB, except the last, are rejected at Complete (decision 50); the benchmarks move to 5 MiB
parts.

### 6.5 Versioning (#113), 6.6 Listing (#114), 6.7 Basics (#140)
Edge cases with no storage design: delete-marker headers, `encoding-type=url`, `max-keys` 0
and invalid values, owners in listings (needs #151), paginated ListBuckets with real
creation dates, bucket naming rules, the 1,000-key limit of DeleteObjects, answering
`Expect: 100-continue` with the error instead of 100 when the request will be refused.

`test_atomic_dual_write_8mb` times out, and it points at a real fault, tracked as #154:
a streamed PUT holds its key's lock while it reads the body, so a client that stops sending
blocks every other writer of that key. Taking the lock only at commit is the fix, but the
TLA+ model shows it is unsafe while recovery orders an unversioned key's needles by offset:
#154 moves recovery to commit-stamp order for every key, model first.

## 7. Areas: new request features

### 7.1 Checksums and GetObjectAttributes (#117)
`x-amz-checksum-*` for CRC32, CRC32C, CRC64NVME, SHA-1 and SHA-256, as a header or an
aws-chunked trailer; verified on write, stored as an attribute, returned when the request
asks (`x-amz-checksum-mode`). Multipart: composite and full-object forms. All digests of a
streamed body are computed in the one pass that computes MD5. When the algorithm is CRC32C
over the whole object, the needle's payload CRC is the value: do not compute it twice.
Needs #150.

### 7.2 CORS (#116)
A bucket configuration kind. `OPTIONS` preflight is answered without authentication.
Matching rules add the response headers to actual requests that carry `Origin`. Rules are
compiled when the configuration is stored. Needs #149.

### 7.3 POST object (#119)
Browser form uploads: a streaming, bounded `multipart/form-data` parser; the policy document
and its signature; conditions (`eq`, `starts-with`, `content-length-range`);
`success_action_status` and redirect. The parser reads unauthenticated network input, so it
ships with a fuzz target. Needs #150 for the form's metadata fields and #151 for the
anonymous cases.

## 8. Areas: data protection and background work

### 8.1 Object lock (#120)
A bucket created with object lock has versioning on for good. A default retention is a
bucket configuration; a version's retention and legal hold are attribute groups. Deleting
or overwriting a version checks them under the key lock. `x-amz-bypass-governance-retention`
needs its own permission. COMPLIANCE retention is never shortened. Lifecycle and compaction
obey the same check. The TLA+ model gains the invariant: a locked version is not removed
before its retain-until date. Needs #150, #149, #151.

### 8.2 Encryption (#121)
Order: SSE-C, then SSE-S3, then SSE-KMS.

- **Cipher.** AES-256-GCM over 64 KiB chunks. The nonce is a random per-object prefix plus
  the chunk counter, and the last chunk is marked in the authenticated data, so truncating
  or reordering chunks fails authentication. `ring` is already a dependency (TLS).
- **Keys.** SSE-C: the key arrives with each request and is never stored; its MD5 is. SSE-S3:
  a master key from a file wraps a per-object data key. SSE-KMS: named keys in a local key
  file stand in for a KMS; the key id is stored and checked. (Decision 49.)
- **Layout.** The stored payload is longer than the object (one tag per chunk). The index
  keeps the object's size and the attributes keep the chunk size, the pattern compressed
  payloads already use. A range GET decrypts only the chunks it covers.
- **Read path.** Encrypted objects cannot be served from the mapping. They use the bounded
  streaming reader that #155 introduces. Plaintext of SSE-C objects is never cached.
- **Multipart.** The part number is part of the nonce.
- Bucket default encryption is a bucket configuration. Copy re-encrypts when source and
  destination settings differ.

Needs #150, #149. Reviewed as security work.

### 8.3 Lifecycle (#118)
The full rule model (filters by prefix, tag and size; expiration by days or date; noncurrent
versions; expired delete markers; incomplete uploads), validated at PUT. The scheduler of
section 3.3 runs it. `x-amz-expiration` on GET, HEAD and PUT. Needs #149, the clock of section 3.3,
tags from #150.

### 8.4 Bucket logging (#153)
113 tests, the largest area, none passing. They cover S3 server access logging plus RGW's
additions (journal mode, rollover, a flush call). Built last; its scope is decided after
wave 3 (decision 45).

## 9. Out of scope (decision 45)

| Group | Tests | Why |
|---|---|---|
| RGW-only | 11 | `X-RGW-*` usage headers, account usage, unordered listing, torrents, tenants: ceph extensions, not S3 |
| Storage classes and cloud tiering | 19 | Need more than one storage class or a remote tier. AwanFS has one |

The map lists them under the areas `rgw-only` and `storage-classes`, owner "out of scope", so
they are counted apart from the work that is left.

## 10. Order

| Wave | Work | Runs in parallel | Unlocks (test functions not passing today) |
|---|---|---|---|
| 0 | #148 operation table, #149 bucket configuration, #150 attributes, #158 clock and Last-Modified, #154 commit point, #159 commit events and counters | no: each touches the core once | none directly |
| 1 | object metadata and copy (#152), tagging, conditional, multipart, versioning, listing, basics, checksums, CORS, request headers (#160), the conformance profile (#161) | yes | about 140, plus `test_headers.py` |
| 2 | #151, then policies, ACLs, ownership, anonymous and presigned (#122), POST object | #122's parts in parallel after #151 | about 130 |
| 3 | object lock, encryption, lifecycle | yes | about 115 |
| 4 | bucket logging (#153), storage classes (#162), RGW-only behaviours (#163), the suite's other files (#164): outlines until the owner sets the scope | | 113 + 19 + 11 + 171 |

Wave 0 is small in code and decides everything after it, so it is built and reviewed with
the most care. Waves 1 to 3 are where parallel work pays.

## 10a. Milestones

Each wave is a GitHub milestone: "Conformance 0: foundations" to "Conformance 4: beyond S3
(decide first)". Every issue named in this file is in one.

## 11. Decisions

Decided by the owner on 2026-11-14:

| | Decision | Recorded as |
|---|---|---|
| D1 | Volume format 3: attributes inside the needle, changes as attribute-update needles (section 4) | `ARCHITECTURE.md` decision 43 |
| D2 | Accounts and owners are enforced: each user is its own account unless it names one, and reaches another account's buckets only through a grant (section 5) | decision 44 |
| D3 | Out of scope: RGW-only tests and storage classes or cloud tiering. Bucket logging is built last and its scope decided after wave 3 | decision 45 |
| | Large GETs stream. A full GET is checksummed as it streams and the connection is aborted on a mismatch; a range GET is not verified and a background scrub checks whole objects (#155) | decision 46 |
| D4 | Parts under 5 MiB, except the last, are rejected at Complete | decision 50 |
| D5 | Per-grantee ACL grants are built; new buckets have ACLs off (`BucketOwnerEnforced`), and the suite's profile turns them on | decision 48 (replaces part of 39) |
| D6 | SSE-KMS from a local key file of named keys | decision 49 (replaces part of 38) |
| | The target is S3 behaviour only: `test_s3.py` and `test_headers.py`, without RGW-only tests, storage classes and the suite's other files | decision 47 |

## 12. What a 100% target would add

The suite is written for ceph's gateway (RGW). Three things follow.

**It contains ceph-only API.** 11 tests need RGW extensions: `X-RGW-Object-Count` and
`X-RGW-Bytes-Used` headers, `GET /?usage`, `allow-unordered` listing, torrents and tenants
(`tenant:bucket`). 13 bucket-logging tests skip unless ceph's extension to the client's
service model is installed, and much of bucket logging tests RGW's journal mode and flush
call. 200 of the 760 tests are marked `fails_on_aws`: they assert RGW behaviour that AWS
does not have (ACLs working on new buckets, conditional deletes, RGW's naming rules, a
shortened lifecycle day). Most of those are still reasonable S3-style behaviour and are in
the areas above; the 11 RGW-only tests are not (#163).

**Some tests only run under settings.** 94 tests are skipped today because the configuration
lacks something: KMS key ids, two or three storage classes, a cloud tier endpoint, the ceph
client extension. The pass count is therefore always "under a profile" (#161).

**The suite has more files.** `test_s3.py` is 760 of 980 test functions. `test_headers.py`
(48) is plain S3 and joins the target (#160). IAM (90), STS (37), S3 Select (38), SNS (5) and
S3 Control (1) are separate services (#164).

What the decisions already made cover, and what 100% of `test_s3.py` would add:

| Need | Covered by D1 to D4? | Architecture consequence |
|---|---|---|
| Per-grantee ACLs, ACLs on by default | Decided since: decision 48 (grants yes, off by default, on in the profile) | None beyond section 5; a default setting |
| SSE-KMS key ids | Decided since: decision 49 (local key file) | None: the attribute group already holds a key id |
| 5 MiB minimum part | Decided since: decision 50 | None |
| Storage class changes after write | No | The storage class is a **replaceable** attribute group, not write-time. Changed in #150 now, so format 3 does not need a successor |
| Usage headers and account usage | No | Durable per-bucket counters, rebuilt by recovery (#159) |
| Tenants | No | Bucket identity is (namespace, name). The field is reserved now in the manifest record (#159), empty for all buckets |
| Bucket logging, later notifications and replication | No | One ordered commit event per change, from the single commit point (#159). No consumer yet |
| Cloud tiering | No | An S3 client inside the server, stub objects, restore state: a new subsystem (#162). Not reserved for |
| IAM and STS | No | The identity store changes from a file to a durable store behind an API. Section 5 does not depend on where users come from, so it does not block this |

The three reservations (replaceable storage class, bucket counters, namespace field) and the
commit event are cheap while the foundations are being built and expensive after, so they
are in wave 0 whatever target is chosen. Nothing else in D1 to D4 changes.

The owner chose the S3-only target (decision 47): #162, #163 and #164 stay outlines.
