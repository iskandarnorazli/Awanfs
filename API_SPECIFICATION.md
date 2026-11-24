# Awan Storage REST & S3 API Specification

> **Wire Protocol: Implemented S3 REST Surface, Host Routing, `aws-chunked` Framing, Checksums, and Operator Endpoints**

---

## 1. Scope & Client Compatibility

AwanFS (`s3-server`) implements the subset of the **Amazon S3 REST API (`2006-03-01`)** routed in [`server/src/ops/mod.rs`](server/src/ops/mod.rs) and tracked in [`features.toml`](features.toml) ([`operation-table`](features.toml#L1153)).

- **Tested S3 clients:** AWS CLI (`scripts/verify_drive.sh`, `server/tests/test_aws_cli.py`), Python `boto3` (`server/tests/test_boto3_compat.py` and the `ceph/s3-tests` suite in `tests/s3tests/`), Rust `reqwest` (`server/tests/`), and MinIO `warp put` (`.github/workflows/ci.yml`).
- **Unimplemented sub-resources and verbs:** Requests carrying an unimplemented sub-resource (such as `?acl`, `?policy`, `?encryption`, `?object-lock`, `?retention`, `?legal-hold`, `?logging`, `?website`, `?notification`, `?replication`, `?select`, `?parts`) answer `501 NotImplemented` per [`ARCHITECTURE.md`](ARCHITECTURE.md) Decision 61 (`server/src/ops/mod.rs:413-465`). Unsupported HTTP verbs on a known resource answer `405 MethodNotAllowed`.

### 1.1 Dual Addressing Styles (Path-Style & Virtual-Hosted-Style)

`parse_s3_target` ([`server/src/s3_api.rs:466-496`](server/src/s3_api.rs#L466-L496)) inspects the `Host` header on every request:

1. **Path-Style (`http://127.0.0.1:9000/{bucket}/{key}`):**
   - Used when `Host` does not match one of the recognized virtual-hosted suffixes below.
   - `/` targets the service (`ListBuckets`); `/{bucket}` or `/{bucket}/` targets the bucket; `/{bucket}/{key}` decodes `%XX` bytes (preserving `+` literally) as `{key}`.
2. **Virtual-Hosted-Style (`http://{bucket}.localhost:9000/{key}`):**
   - `vhost_bucket` (`server/src/s3_api.rs:446-456`) extracts a single subdomain label (no `.` or `/` inside `{bucket}`) only when `Host` (ignoring `:port`) ends with `.s3.localhost`, `.localhost`, `.s3.internal`, `.s3.amazonaws.com`, or `.127.0.0.1`.
   - Arbitrary custom domains are treated as path-style rather than virtual-hosted-style.

### 1.2 `Expect: 100-continue` Wire Handshake

For `Expect: 100-continue` (`server/tests/s3_details_test.rs`), request classification, SigV4 authentication, bucket resolution, IAM authorization, key/body-limit validation (`<= 5 GiB` on object `PUT`), `If-None-Match: *` check, `Content-Length` check, `Content-MD5` base64 syntax validation, and `x-amz-checksum-*` header parsing run **before** polling the request body:

1. If any header/metadata pre-check fails, the server sends the error (`403 Forbidden`, `404 NoSuchBucket`, `400 InvalidDigest`, `400 KeyTooLongError`, `400 EntityTooLarge`, `411 MissingContentLength`, `412 PreconditionFailed`) immediately without writing `HTTP/1.1 100 Continue`.
2. Otherwise Hyper emits `HTTP/1.1 100 Continue\r\n\r\n` when body reading starts. `<= 4 MiB` bodies are admitted against the `--memory-budget` in-flight bodies half (`503 SlowDown` with `Retry-After: 1` if exhausted); `> 4 MiB` object bodies stream directly to disk (`400`, `408`, or `500` on body failure).
3. `Expect` header values other than `100-continue` (e.g. `200` or empty) are accepted without `417 ExpectationFailed`.

---

## 2. Implemented S3 & Operator Endpoint Surface

### A. Service & Bucket Operations ([`buckets`](features.toml#L847), [`listing`](features.toml#L743))

| HTTP Verb | Path & Query | S3 Operation | Wire Contract & Status Codes |
| :--- | :--- | :--- | :--- |
| **`GET`** | `/` | `ListBuckets` | Returns `200 OK` `<ListAllMyBucketsResult>` XML. Supports `prefix`, `max-buckets` (`1..=10000`, `400 InvalidArgument` outside range), and `continuation-token`. Note: returns all active buckets in the store (`<Owner><ID>s3-storage-root</ID>`). |
| **`PUT`** | `/{bucket}` | `CreateBucket` | Validates bucket name (`3..=63` chars, `[a-z0-9.-]`, starts/ends `[a-z0-9]`, non-IP, no `..`/`.-`/`-.`, no `xn--`/`sthree-`/`-s3alias`/`--ol-s3`; `400 InvalidBucketName`). Validates optional `<LocationConstraint>` against `--region` (`400 InvalidLocationConstraint`). Creates bucket entry + `buckets.manifest` record and returns `200 OK`. If bucket already exists: returns `200 OK` only when caller owns it AND server `--region` is `us-east-1` or `default`; otherwise `409 BucketAlreadyOwnedByYou` (same owner) or `409 BucketAlreadyExists` (other owner). |
| **`HEAD`** | `/{bucket}` | `HeadBucket` | Returns `200 OK` (empty body) if bucket exists, `404 Not Found` (empty body) otherwise. |
| **`DELETE`** | `/{bucket}` | `DeleteBucket` | If bucket has 0 live objects, removes bucket from index and `buckets.manifest` and returns `204 No Content`. If live objects remain, returns `409 Conflict` (`BucketNotEmpty`). |
| **`GET`** | `/{bucket}?location` | `GetBucketLocation` | Returns `200 OK` `<LocationConstraint xmlns="...">` XML: empty `<LocationConstraint></LocationConstraint>` when `--region` is `us-east-1`, or `<LocationConstraint>{region}</LocationConstraint>` otherwise. |
| **`GET`** | `/{bucket}?list-type=2` | `ListObjectsV2` | Lexicographical UTF-8 key scan from the in-memory `BTreeMap` merged with leveled `SpillIndex` disk segments ([`metadata-index`](features.toml#L1130)). Supports `prefix`, `delimiter`, `max-keys` (`0..=2147483647` parsed, capped at `1000` per page, default `1000`), `start-after`, `continuation-token`, `encoding-type=url`, and `fetch-owner`. Returns `200 OK` XML. |
| **`GET`** | `/{bucket}` | `ListObjects` (V1) | Legacy V1 listing (`marker`, `prefix`, `delimiter`, `max-keys`, `encoding-type=url`). Returns `200 OK` XML. |
| **`GET`** | `/{bucket}?versions` | `ListObjectVersions` | Lists object versions and delete markers (`prefix`, `delimiter`, `key-marker`, `version-id-marker`, `max-keys`, `encoding-type=url`). Returns `200 OK` `<ListVersionsResult>` XML ([`versioning`](features.toml#L692)). |
| **`POST`** | `/{bucket}?delete` | `DeleteObjects` | Batch delete of `1..=1000` `<Object>` items per `<Delete>` XML request (`Quiet`, optional `<VersionId>`). Tombstones or records delete markers and returns `200 OK` `<DeleteResult>` XML. Does not evaluate `Content-MD5`. |

### B. Object Operations ([`objects`](features.toml#L930), [`copy`](features.toml#L663), [`ranges`](features.toml#L909), [`object-metadata`](features.toml#L599), [`checksums`](features.toml#L220), [`object-attributes-api`](features.toml#L251))

| HTTP Verb | Path & Query | S3 Operation | Wire Characteristics & Response Headers |
| :--- | :--- | :--- | :--- |
| **`PUT`** | `/{bucket}/{key}` | `PutObject` | Maximum object body `5 GiB` (`400 EntityTooLarge` if exceeded); requires `Content-Length`, `Transfer-Encoding: chunked`, or `aws-chunked` (`411 MissingContentLength` otherwise). Bodies `<= 4 MiB` are admitted against `--memory-budget` (`503 SlowDown` with `Retry-After: 1` if exhausted); bodies `> 4 MiB` stream directly to 4096B-aligned format-3 needles in growing volumes ([`volume-format`](features.toml#L1138)), optionally compressed with `zstd` per bucket config ([`compression`](features.toml#L1037)). Verifies `Content-MD5` (`400 InvalidDigest` / `BadDigest`), `x-amz-content-sha256` (`400 XAmzContentSHA256Mismatch`), and `x-amz-checksum-*` (`CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256`; `400 BadDigest`). Stores standard metadata headers, `x-amz-meta-*` (`<= 2048` bytes total -> `400 MetadataTooLarge`), and `x-amz-tagging`. Supports `If-None-Match: *` (`412 PreconditionFailed`). Returns `200 OK` with `ETag: "<32-hex-md5>"`, optional `x-amz-version-id`, and requested `x-amz-checksum-*`. |
| **`PUT`** | `/{bucket}/{key}` (+ `x-amz-copy-source`) | `CopyObject` | Copies source object (or specific `?versionId`) bytes into a new needle; supports `x-amz-metadata-directive` (`COPY` / `REPLACE`), `x-amz-tagging-directive` (`COPY` / `REPLACE`), and `x-amz-copy-source-if-*` preconditions (`412 PreconditionFailed`). Returns `200 OK` `<CopyObjectResult>` XML, `ETag`, and optional `x-amz-version-id` / `x-amz-copy-source-version-id`. |
| **`GET`** | `/{bucket}/{key}` | `GetObject` | Objects `<= 4 MiB` use `PayloadCache` + `SingleFlight` (and mmap for small multipart); single objects and multipart objects `> 4 MiB` stream via positioned page-cache reads (`PreadBody`). Supports `Range: bytes=...` (`206 Partial Content`), `?partNumber=N` (`1..=10000`), `?versionId=ID`, conditional headers (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`), `x-amz-checksum-mode: ENABLED` (whole GET/HEAD only), and `response-*` query overrides. Returns `ETag`, `Content-Length`, `Content-Type`, `Last-Modified`, `Accept-Ranges: bytes`, standard/user metadata, `x-amz-tagging-count`, and optional `x-amz-version-id`. |
| **`HEAD`** | `/{bucket}/{key}` | `HeadObject` | Shares the `get_or_head_object` metadata, version, precondition, `?partNumber`, and `Range` evaluation path (reading needle attributes if present), returning the same headers with a 0-byte body (and empty body on `304`/`400`/`404`/`412`/`416`). |
| **`GET`** | `/{bucket}/{key}?attributes` (+ `?versionId`) | `GetObjectAttributes` | Requires `x-amz-object-attributes` (`ETag`, `Checksum`, `ObjectParts`, `StorageClass`, `ObjectSize`), supports `x-amz-max-parts` and `x-amz-part-number-marker`. Returns `200 OK` `<GetObjectAttributesResponse>` XML ([`object-attributes-api`](features.toml#L251)). |
| **`DELETE`** | `/{bucket}/{key}` (+ `?versionId`) | `DeleteObject` | In unversioned buckets, appends a tombstone needle and removes the key from the live index, returning `204 No Content` (idempotent if absent). In versioned/suspended buckets without `?versionId`, writes a delete marker and returns `204` with `x-amz-delete-marker: true` and `x-amz-version-id`. With `?versionId`, permanently deletes that version (`x-amz-delete-marker: true` when removing a marker). |

### C. Multipart Upload Operations ([`multipart`](features.toml#L536))

| HTTP Verb | Path & Query Parameters | S3 Operation | Execution & Validation Contract |
| :--- | :--- | :--- | :--- |
| **`POST`** | `/{bucket}/{key}?uploads` | `CreateMultipartUpload` | Generates upload ID `upload-{:016x}` (`MultipartManager::create_upload`) and creates `.staging/upload-<hex>/`. Captures `Content-Type`, metadata, `x-amz-tagging`, and optional `x-amz-checksum-algorithm` / `x-amz-checksum-type`. Returns `200 OK` `<InitiateMultipartUploadResult>` XML. |
| **`PUT`** | `/{bucket}/{key}?partNumber={N}&uploadId={ID}` | `UploadPart` | Validates `1 <= partNumber <= 10,000` (`400 InvalidArgument`; for buffered parts checked after reading body, for `> 4 MiB` streamed parts after streaming to staging) and `404 NoSuchUpload` if unknown. Writes part payload to `.staging/<uploadId>/part_{NNNNN}.dat` via atomic `.partial-*` rename. Supports `aws-chunked`, `Content-MD5`, and `x-amz-checksum-*`. Returns `200 OK` with `ETag: "<part-md5>"`. |
| **`PUT`** | `/{bucket}/{key}?partNumber={N}&uploadId={ID}` (+ `x-amz-copy-source`) | `UploadPartCopy` | Copies a full source object or `x-amz-copy-source-range: bytes=start-end` byte slice (including `?versionId`) and `x-amz-copy-source-if-*` preconditions into `.staging/<uploadId>/part_{NNNNN}.dat`. Returns `200 OK` `<CopyPartResult>` XML. |
| **`POST`** | `/{bucket}/{key}?uploadId={ID}` | `CompleteMultipartUpload` | Parses `<CompleteMultipartUpload>` XML (`<= 16 MiB`; `1..=10000` `<Part>` elements). Accepts non-decreasing `PartNumber` (`400 InvalidPartOrder` if decreasing; consecutive duplicate `PartNumber` uses last ETag), verifies each part exists and `<ETag>` matches (`400 InvalidPart`), enforces every part except the final part `>= 5 MiB` unless `--allow-small-parts` (`400 EntityTooSmall`), computes `"<md5(raw_part1_md5 || ... || raw_partK_md5)>-<K>"` plus composite/full-object checksum, writes an `AWNMPU1` manifest needle to the active volume (keeping part payloads in `.staging/`), and returns `200 OK` `<CompleteMultipartUploadResult>` XML (idempotent on duplicate `CompleteMultipartUpload` for the same `uploadId`). |
| **`DELETE`** | `/{bucket}/{key}?uploadId={ID}` | `AbortMultipartUpload` | Removes staged `.staging/<uploadId>/` directory and marks upload aborted. Returns `204 No Content` (`404 NoSuchUpload` if unknown or already completed/aborted). |
| **`GET`** | `/{bucket}/{key}?uploadId={ID}` | `ListParts` | Paginates uploaded parts (`part-number-marker`, `max-parts`, `encoding-type=url`). Returns `200 OK` `<ListPartsResult>` XML. |
| **`GET`** | `/{bucket}?uploads` | `ListMultipartUploads` | Lists in-progress uploads in `{bucket}` (`prefix`, `delimiter`, `key-marker`, `upload-id-marker`, `max-uploads`, `encoding-type=url`). Returns `200 OK` `<ListMultipartUploadsResult>` XML. |

### D. Bucket Versioning, Tagging, CORS & Lifecycle Configuration ([`versioning`](features.toml#L692), [`tagging`](features.toml#L269), [`cors`](features.toml#L191), [`lifecycle`](features.toml#L150))

| HTTP Verb | Path & Query | S3 Operation | Behavior |
| :--- | :--- | :--- | :--- |
| **`PUT`/`GET`** | `/{bucket}?versioning` | `PutBucketVersioning` / `GetBucketVersioning` | Persists `Enabled` / `Suspended` status in `buckets.manifest`. |
| **`PUT`/`GET`/`DELETE`** | `/{bucket}?tagging` | `PutBucketTagging` / `GetBucketTagging` / `DeleteBucketTagging` | Bucket tags (`<= 50` tags) persisted in `buckets.manifest`; `GET` returns `404 NoSuchTagSet` when unset; `PUT` returns `204`, `DELETE` returns `204`. |
| **`PUT`/`GET`/`DELETE`** | `/{bucket}/{key}?tagging` (+ `?versionId`) | `PutObjectTagging` / `GetObjectTagging` / `DeleteObjectTagging` | Object tags (`<= 10` tags) stored in needle attributes block (`FLAG_ATTRS`). Note: attribute-update rewrite/replay stage 2 (`#150`) is still partial for some update cases (`tag_count` / `GetObjectTagging` tested in `tagging_test.rs`). |
| **`PUT`/`GET`/`DELETE`** | `/{bucket}?cors` & `OPTIONS` preflight | `PutBucketCors` / `GetBucketCors` / `DeleteBucketCors` / `CorsPreflight` | Persisted in `buckets.manifest`; `GET` returns `404 NoSuchCORSConfiguration` when unset. Evaluates `OPTIONS` preflight (`Origin` + `Access-Control-Request-Method`) unauthenticated (`403 AccessDenied` on no matching rule, `400 InvalidRequest` if headers missing) and attaches `Access-Control-*` headers on `GET`/`PUT`/`POST`/`DELETE`/`HEAD`. |
| **`PUT`/`GET`/`DELETE`** | `/{bucket}?lifecycle` | `PutBucketLifecycleConfiguration` / `GetBucketLifecycleConfiguration` / `DeleteBucketLifecycle` | Parses `<LifecycleConfiguration>` XML (`<= 1000` rules), stores rules **in memory** (`state.lifecycle`), and returns `200 OK` / `404 NoSuchLifecycleConfiguration` / `204 No Content`. See [Not yet](#not-yet) for automated TTL sweep & persistence (`#118`). |

### E. Operator Health & Metrics Endpoints ([`health`](features.toml#L1060), [`metrics`](features.toml#L1069))

Paths under `/_awanfs/` are answered without S3 authentication (`_` is invalid in bucket names, so no bucket is shadowed; [`ARCHITECTURE.md`](ARCHITECTURE.md) Decision 9):

- **`GET`/`HEAD /_awanfs/health/live`** -> `200 OK` (`live\n`).
- **`GET`/`HEAD /_awanfs/health/ready`** -> `200 OK` (`ready\n`), or `503 Service Unavailable` (`not ready: ...`) when shutting down, no volume is open, or volume span cannot fit a needle.
- **`GET`/`HEAD /_awanfs/metrics`** -> `200 OK` (`text/plain; version=0.0.4; charset=utf-8`) Prometheus metrics (request counts/latencies, cache hits/misses/bytes, `--memory-budget` split & rejections, dropped log lines, compaction, volume usage). Non-`GET`/`HEAD` returns `405`; unknown `/_awanfs/*` path returns `404`.

---

## 3. AWS Signature Version 4 & Payload Integrity ([`sigv4-auth`](features.toml#L1014), [`anonymous-and-presigned`](features.toml#L631))

When credentials (`AWANFS_ACCESS_KEY`/`AWANFS_SECRET_KEY` or `--config`) are configured, requests (except `OPTIONS` CORS preflight and `/_awanfs/*`) must authenticate using **AWS Signature Version 4 (`AWS4-HMAC-SHA256`)** via `Authorization` header or Query String Pre-Signed parameters (`X-Amz-Algorithm=AWS4-HMAC-SHA256`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires` `<= 604800`, `X-Amz-SignedHeaders`, `X-Amz-Signature`), or the server is started with `--allow-anonymous`.

### 3.1 Derived Signing Key Cache

`SigV4KeyCache` (`server/src/sigv4.rs:20-75`) is a process-wide `RwLock<HashMap<(String, String, String, String), [u8; 32]>>` keyed by `(secret_key, date_yyyymmdd, region, service)` that caches the 4-step `HMAC-SHA256` derived `kSigning` key across requests.

### 3.2 Payload Integrity Modes (`x-amz-content-sha256`)

1. **Single-Chunk Pre-Hashed Payload (`x-amz-content-sha256: <64-hex-sha256>`):**
   - Server hashes the decoded payload with SHA-256 and returns `400 Bad Request` (`XAmzContentSHA256Mismatch`) if it differs (`server/src/ops/object.rs:1495-1507`, `server/tests/auth_test.rs::a_body_must_match_its_declared_sha256`).
2. **Unsigned Payload (`x-amz-content-sha256: UNSIGNED-PAYLOAD`, or omitted on presigned URLs):**
   - Header signature is verified using `"UNSIGNED-PAYLOAD"` in the Canonical Request while MD5 ETag and Castagnoli CRC32C are still computed over the payload.
3. **Chunked Streaming Payload (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` / `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`, or `Content-Encoding: aws-chunked` + `x-amz-decoded-content-length`):**
   - `ChunkedDecoder` ([`server/src/aws_chunked.rs`](server/src/aws_chunked.rs)) strips `<hex-size>[;chunk-signature=<sig>]\r\n<payload>\r\n` framing incrementally and verifies each chunk's chained `HMAC-SHA256` signature via `ChunkVerifier` (including the final `0`-byte chunk; trailing headers after the final chunk are ignored).
   - Bad chunk signatures return `403 Forbidden` (`SignatureDoesNotMatch`); invalid framing returns `400 Bad Request` (`MalformedChunkHeader` / `IncompleteBody`).

---

## 4. Conditional Headers & Optimistic Concurrency ([`conditional`](features.toml#L470))

| Header | Supported Operations | Behavior on Match / Hold | Behavior on Mismatch |
| :--- | :--- | :--- | :--- |
| **`If-None-Match: "*"`** | `PutObject` | Succeeds only if no active object exists for `(bucket, key)` under the key lock. | `412 PreconditionFailed` (`PreconditionFailed`). |
| **`If-Match` / `If-None-Match` / `If-Modified-Since` / `If-Unmodified-Since`** | `GetObject`, `HeadObject` | Proceeds when preconditions hold (missing key + `If-Match` -> `412`). | `304 NotModified` (`If-None-Match` / `If-Modified-Since`, 0-byte body) or `412 PreconditionFailed` (`If-Match` / `If-Unmodified-Since`). |
| **`x-amz-copy-source-if-match` / `-none-match` / `-modified-since` / `-unmodified-since`** | `CopyObject`, `UploadPartCopy` | Proceeds with copy when source preconditions hold. | `412 PreconditionFailed` (including for `NotModified`). |

---

## 5. RFC 9110 / S3 Byte-Range Semantics (`Range: bytes=...`) ([`ranges`](features.toml#L909))

`parse_range_header` (`server/src/s3_api.rs:387-444`) evaluates single-span byte ranges on non-empty objects (multi-range comma-separated specs and `?partNumber` combined with `Range` are rejected with `416 InvalidRange` / `400 InvalidRequest`):

1. **Explicit Range (`Range: bytes=start-end`):** Requires `0 <= start <= end` and `start < object_size`; if `end >= object_size`, clamps `end` to `object_size - 1`. Returns `206 Partial Content`, `Content-Length: end - start + 1`, and `Content-Range: bytes start-end/object_size`.
2. **Prefix-Offset Range (`Range: bytes=start-`):** Requires `0 <= start < object_size`; serves `[start..object_size - 1]`.
3. **Suffix Range (`Range: bytes=-suffix_len`):** Requires `suffix_len > 0`; serves the final `min(suffix_len, object_size)` bytes.
4. **Unsatisfiable Range (`start >= object_size`, `start > end`, `suffix_len == 0`, multi-range, or any `Range` on a `0`-byte object):** Returns **`416 Range Not Satisfiable`** with `Content-Range: bytes */{object_size}` and `<Code>InvalidRange</Code>` XML (0-byte body for `HEAD`).

---

## 6. Retries & Backoff (`503 SlowDown`)

Every S3 `503 SlowDown` response (`BodyBudget` admission timeout or a full bounded index without spill) carries `Retry-After` in whole seconds (`1` today, `RETRY_AFTER_SECS` in [`server/src/memory.rs`](server/src/memory.rs)). `Retry-After` is the **minimum wait** before retrying. Clients should wait at least that many seconds and may add jittered exponential backoff. Operator probe `/_awanfs/health/ready` is not an S3 operation and returns `503` without `Retry-After`.

A `503 SlowDown` means nothing was written to object storage and no upload or version state was mutated. Index capacity (`CapacityReservation`) and body admission are acquired before writing payloads, committing multipart state, or updating `VersionStore`. The following operations are safe to retry directly after `503`:

- **`PutObject` (buffered and streamed) and `CopyObject`:** No needle header or index/version entry is committed on `503`.
- **Conditional `PUT` (`If-None-Match: *` / `If-Match`):** Preconditions and stored state are untouched on `503`, so retrying preserves optimistic concurrency semantics.
- **`DeleteObject` and `DeleteObjects`:** Deletes are idempotent and do not allocate new index rows; a `503` on `POST ?delete` body admission rejects before parsing or mutating any key.
- **`UploadPart` and `UploadPartCopy`:** Body admission rejects before writing `.staging/` part files or recording part metadata.
- **`CompleteMultipartUpload`:** Capacity is reserved before writing the manifest needle or consuming `uploadId`, so the upload stays open and retrying the same `CompleteMultipartUpload` succeeds once capacity or body budget clears.

---

## Not yet

- **Multipart conformance remainder ([`multipart`](features.toml#L536), `#111`):** Remaining `ceph/s3-tests` multipart cases (`features.toml` lists 24/30 passing in `FEATURES.md`).
- **Conditional `PUT If-Match` / `DELETE` ([`conditional`](features.toml#L470), `#112`):** Full RFC 7232 `If-Match`/`If-Modified-Since`/`If-Unmodified-Since` on `PutObject` and conditional headers on `DeleteObject`/`DeleteObjects`.
- **Lifecycle persistence & periodic TTL sweep ([`lifecycle`](features.toml#L150), `#118`):** Currently `PUT`/`GET`/`DELETE ?lifecycle` are in-memory only (`buckets.manifest` does not persist lifecycle rules) and `run_lifecycle_sweep` is not registered on the background `Scheduler` (only a 7-day `multipart_cleanup` task runs automatically).
- **POST Object browser form uploads ([`post-object`](features.toml#L180), `#119`):** `form_upload.rs` parser exists, not wired into S3 request routing (`501`/`405`).
- **Object Lock (`?object-lock`, `?retention`, `?legal-hold`) ([`object-lock`](features.toml#L162), `#120`):** Returns `501 NotImplemented`.
- **Server-Side Encryption (SSE-S3, SSE-C, SSE-KMS) ([`encryption`](features.toml#L133), `#121`):** Returns `501 NotImplemented` for `?encryption` (per `ARCHITECTURE.md` Decisions 38 & 49).
- **Bucket Policies, ACLs, Ownership Controls, Public Access Block, & Anonymous Bucket Access ([`policy`](features.toml#L327), [`acl-ownership`](features.toml#L379), [`anonymous-and-presigned`](features.toml#L631), [`anonymous-bucket-gone`](features.toml#L70), [`anonymous-cors`](features.toml#L79), `#122`):** Return `501 NotImplemented`; identity policies in `--config` ([`iam`](features.toml#L1029), `#53`) are enforced on authenticated users today.
- **Bucket Server Access Logging (`?logging`) ([`bucket-logging`](features.toml#L88), `#153`):** Returns `501 NotImplemented` (operator access logs use `--access-log`, [`access-log`](features.toml#L1086)).
- **Background payload scrubbing on large objects (`#155`)** and **Multi-node replication / erasure coding ([`erasure-coding`](features.toml#L1161), `#52`, `#27`)**.
- **Out of scope (`501 NotImplemented` / unsupported):** AWS SigV2 ([`sigv2`](features.toml#L7)), Storage Classes / tiering ([`storage-classes`](features.toml#L103), `#162`), RGW-specific extensions ([`rgw-only`](features.toml#L48), `#163`), and IAM/STS/SNS/Select/Control APIs (`#164`).
