# Awan Storage Edge Cases, Failure Semantics & Error Contracts

> **Production Hardening: Stream Timeouts, Validation, Collisions, Concurrent Isolation, Recovery, Compaction, and S3 Error Matrix**

---

## 1. Edge Case Operational Matrix (12 Core Scenarios)

| # | Failure / Edge Scenario | Failure Risk | Implemented Contract |
| :-: | :--- | :--- | :--- |
| **1** | Mid-Stream Disconnect / Idle Stall | Leased Resource Leak / Torn Object | `--body-idle-timeout` (`20s` -> `408 RequestTimeout`), `IncompleteBody`, Gap-Skipping Journal |
| **2** | Zero-Byte Objects & Folder Keys | Corrupt Extent / Bad Range | `FLAG_ZERO_BYTE`, Empty MD5 Constant (`d41d8cd9...`), 0-Payload Persistent Needle |
| **3** | Thundering Herd on Cold Small Keys | Repeated Disk Reads | `SingleFlight` + `PayloadCache` (`<= 4 MiB`); Positioned `PreadBody` (`> 4 MiB`) |
| **4** | Key Traversal (`../`) & Bad UTF-8 | Path Escape / XML Injection | Strict Key Validator (`InvalidURI`/`InvalidObjectName`/`KeyTooLongError`) & XML Escaping |
| **5** | Hash Collision / Key Aliasing | Returning Wrong Key's Payload | Keyed `XXH3` per `bucket_id` + `key_arena` Byte Equality + `BTreeMap`/`SpillIndex` |
| **6** | Concurrent Overwrite During `GET` | Reader Sees Half-Written Object | Immutable Append-Only Needles + Compaction Key Lock & Volume Sealing |
| **7** | `Expect: 100-continue` Upload | Wasted Payload on Header Error | Pre-Body Auth/Bucket/Header Gate Before Polling Body (`400`/`403`/`404`/`411`/`412`) |
| **8** | `aws-chunked` Streaming Upload | Chunk Framing Stored as Payload | Incremental `ChunkedDecoder` & Rolling Chained `HMAC-SHA256` Verifier |
| **9** | Multipart Part Anomalies | Out-of-Order / Small / Resent Parts | Staging Part Files (`.staging/`), Non-Decreasing Order, `5 MiB` Min (`--allow-small-parts`) |
| **10**| Byte-Range Corner Cases | `end >= size`, `0B` Object, Suffix `-N` | RFC 9110 Clamping & `416 Content-Range: bytes */{size}` |
| **11**| Power Loss Mid-Write (Torn Needle) | Corrupt Volume Tail on Reboot | Volume `.dat` Scan + Needle `CRC32C` Tail Truncation + Manifest/Volume Replay |
| **12**| Volume Full & Dead Space Growth | Exhausting Single Volume / Fragmentation | Automatic 32 GiB Volume Rollover + `>= 20%` Dead Space Auto-Compaction |

---

## 2. Deep Dive: Edge Cases & Operational Contracts

### Edge Case 1: Client Network Disconnect Mid-Upload & Idle Timeout ([`objects`](features.toml#L930), [`memory-budget`](features.toml#L1111))
- **Scenario:** A client declares a large `PUT` or `UploadPart`, sends fewer or more bytes than `Content-Length` (or `x-amz-decoded-content-length`), stalls mid-upload, or drops the TCP socket.
- **Contract:**
  1. Buffered requests (`<= 4 MiB`) and XML requests are bounded by `--memory-budget`'s bodies half (`read_body_admitted`, returning `503 SlowDown` after 100 ms wait if exhausted, [`server/src/memory.rs`](server/src/memory.rs)). Requests above `4 MiB` (`STREAM_PUT_MIN`) stream directly to disk (`NeedleWriter` / `PartWriter`).
  2. If the client sends fewer bytes than `Content-Length`, truncates an `aws-chunked` stream before its zero-length chunk, or sends more bytes than declared (`BODY_TOO_LONG` / `BODY_TOO_SHORT`), the server rejects with `400 Bad Request` (`IncompleteBody`).
  3. If no header or body frame arrives within the idle deadline (`DEFAULT_HEADER_IDLE_TIMEOUT = 20s`, `--body-idle-timeout` default `20s`, [`server/src/body_timeout.rs`](server/src/body_timeout.rs)), the server closes the connection with `Connection: close` and `408 RequestTimeout` XML (for body stalls; header stalls close the socket), dropping any uncommitted writer and key lock (`server/tests/body_timeout_test.rs`).
  4. If the client drops the TCP connection mid-stream after an offset was reserved on disk, the server drops the writer (`volume_gap_test.rs`); index recovery steps over uncommitted gaps via `.idx` and live index rows are updated only after payload + digest complete (with late failures *after* the needle header is on disk indexed identically to recovery per [`ARCHITECTURE.md`](ARCHITECTURE.md) Decision 62, [`server/tests/late_failure_test.rs`](server/tests/late_failure_test.rs)).

### Edge Case 2: Zero-Byte Objects & Directory Markers (`Content-Length: 0`) ([`objects`](features.toml#L930))
- **Scenario:** Clients upload `0`-byte objects or folder markers ending in `/` (e.g., `PUT /bucket/logs/2026/`).
- **Contract:**
  1. Buffered `PutObject` with empty body or key ending in `/` sets `FLAG_ZERO_BYTE` (`0x0010`), writes a persistent `0`-payload needle to the active volume (so empty objects, keys ending in `/`, and their attributes survive restart; `PayloadCache` skips 0-byte payloads).
  2. Records `size = 0` and MD5 of `""` (`d41d8cd98f00b204e9800998ecf8427e`).
  3. A subsequent `GET` returns `200 OK` with `Content-Length: 0`; any `Range:` header on a 0-byte object returns `416 Range Not Satisfiable` with `Content-Range: bytes */0` (`server/tests/edge_cases_test.rs::test_zero_byte_object_and_folder_marker`).

### Edge Case 3: Concurrent Reads, Single-Flight & Positioned Streaming ([`memory-budget`](features.toml#L1111), [`ranges`](features.toml#L909))
- **Scenario:** Many concurrent clients request the same object or range.
- **Contract:**
  1. For full `GetObject` requests `<= 4 MiB` (`VERIFY_WHILE_SENDING_MIN`), [`PayloadCache`](server/src/cache.rs) serves hot bytes from RAM (capped by `--memory-budget`'s cache half minus up to `64 MiB` spill cache), and misses coalesce on [`SingleFlight`](server/src/coalesce.rs) keyed by `"{bucket}/{key}/{vol_offset}"` (`server/tests/edge_cases_test.rs::test_single_flight_coalescing_barrier`, `server/src/ops/object.rs:2019-2031`).
  2. Per [`ARCHITECTURE.md`](ARCHITECTURE.md) Decisions 46, 53, and 58, objects and multipart reads `> 4 MiB` bypass `PayloadCache` and `SingleFlight` and stream per-request via positioned page-cache reads (`PreadBody`, [`server/src/stream_read.rs`](server/src/stream_read.rs), `server/tests/large_get_test.rs`, `server/tests/stream_read_test.rs`, `server/tests/multipart_get_reads_test.rs`), keeping RSS bounded independently of object size (`AWAN_DIRECT_READ_MIN` direct-I/O read threshold is off by default).

### Edge Case 4: Key Path Traversal, URI Encoding & XML Escaping ([`objects`](features.toml#L930), [`request-headers`](features.toml#L34))
- **Scenario:** A client sends `PUT /bucket/../../etc/passwd`, null bytes `%00`, control chars, invalid UTF-8 percent-escapes, keys `> 1,024` UTF-8 bytes, or XML characters (`&`, `<`, `>`).
- **Unified Two-Layer Contract:**
  1. **Layer 1 — Input Validation (`400 Bad Request`):**
     - `validate` (`server/src/ops/mod.rs:746-769`) decodes percent-escapes once (keeping `+` literal in path keys).
     - Invalid UTF-8 byte sequences in the decoded key and C1/DEL characters (`0x7F`, `0x80..=0x9F`) return `400 Bad Request` (`InvalidURI`).
     - Empty keys, keys containing `\0` or C0 control bytes (`< 0x20`), or keys with a `..` path segment return `400 Bad Request` (`InvalidObjectName`).
     - Keys exceeding `1,024` UTF-8 bytes return `400 Bad Request` (`KeyTooLongError`). (`server/tests/s3_details_test.rs`, `server/tests/edge_cases_test.rs::test_key_validation_and_path_traversal_defenses`).
  2. **Layer 2 — Host Filesystem Isolation & XML Escaping:**
     - Object keys are never passed to OS filesystem path APIs (`open()`, `CreateFile()`); objects live inside numbered volume files (`volume_NNNN.dat`) or staged multipart ID directories (`.staging/upload-<hex>/part_NNNNN.dat`).
     - Keys, prefixes, upload IDs, tags, and error fields serialized into XML are escaped by `xml_escape` (`server/src/xml.rs:32-45`, `server/tests/delete_objects_test.rs`).

### Edge Case 5: Hash Collisions & Key Equality ([`metadata-index`](features.toml#L1130))
- **Scenario:** Two distinct keys produce the same 64-bit `XXH3` hash.
- **Contract:**
  1. `meta_index.rs` computes `xxh3_64_with_seed(key, bucket_id as u64)` (deterministic per `bucket_id`, not a per-cluster random secret or SipHash).
  2. `hash_slots` (`HashMap<(u32, u64), Vec<usize>>`) and ordered `BTreeMap` / leveled [`SpillIndex`](server/src/spill.rs) segments resolve collisions by comparing the full key byte slice stored in `key_arena` (`key_slice`), preventing false-positive lookups (`server/tests/meta_index_test.rs::test_hash_collision_full_key_check`).

### Edge Case 6: Concurrent Overwrite vs In-Flight Readers ([`objects`](features.toml#L930), [`compaction`](features.toml#L1045))
- **Scenario:** Client A downloads `GET /bucket/weights.bin` while Client B issues `PUT /bucket/weights.bin` or `DELETE /bucket/weights.bin`.
- **Contract:**
  1. Overwrites and deletes never mutate an existing volume needle in place; every `PUT` appends a new needle to the active volume and updates the metadata index under a striped per-key async mutex (`key_locks`, 256 stripes, only acquired at commit for streamed PUTs per [`ARCHITECTURE.md`](ARCHITECTURE.md) Decision 51).
  2. Buffered `GET`s (`<= 4 MiB`) load or cache immutable `Bytes` before responding; streamed `GET`s (`> 4 MiB`) read from the snapshotted needle offset/staged part files via `PreadBody`. Note: `MvccStore` ([`server/src/mvcc.rs`](server/src/mvcc.rs)) is unit-tested standalone in `edge_cases_test.rs::test_mvcc_isolation_and_negative_control` and tombstoned on deletes, rather than pinning on the `GET` read path.
  3. Online compaction only compacts sealed non-active older volumes when `writers_in_flight() == 0`, syncs copied live needles before removing the old volume (`server/tests/compaction_safety_test.rs`, `server/tests/compaction_inflight_test.rs`). On Windows, if a reader mapping keeps an old `.dat` file open, `remove_file` is skipped safely and replay still converges to the newer copies.

### Edge Case 7: `Expect: 100-continue` Early Gate ([`objects`](features.toml#L930))
- **Scenario:** Clients send `PUT /{bucket}/{key}` with `Expect: 100-continue` and invalid auth, missing bucket, invalid key, failing `If-None-Match: *`, oversized declaration (`> 5 GiB`), or invalid `Content-MD5`.
- **Contract:**
  - All header/auth/bucket/key/precondition/length/Content-MD5 checks run before polling the HTTP body (`server/tests/s3_details_test.rs::expect_100_continue_valid_and_rejected_before_body`).
  - Failed pre-checks return the error response (`400`, `403`, `404`, `411`, `412`) without `HTTP/1.1 100 Continue`; passing requests emit `100 Continue` once body reading begins.
  - Other `Expect` values (such as `200` or empty) are accepted (`server/tests/s3_details_test.rs::other_expect_values_are_not_refused`). There is no `507 InsufficientStorage` disk headroom check.

### Edge Case 8: `aws-chunked` Streaming Payload Framing ([`sigv4-auth`](features.toml#L1014))
- **Scenario:** Clients upload using `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (or `-TRAILER`) / `Content-Encoding: aws-chunked`.
- **Contract:**
  - [`ChunkedDecoder`](server/src/aws_chunked.rs) incrementally strips `<hex-len>[;chunk-signature=<sig>]\r\n<payload>\r\n` frames and verifies each chunk's chained `HMAC-SHA256` signature via `ChunkVerifier` (`403 SignatureDoesNotMatch` on mismatch, `400 MalformedChunkHeader` / `400 IncompleteBody` on bad framing or truncation; trailing headers after the `0`-size chunk are ignored).
  - Decoded payload bytes only are hashed for MD5 `ETag`, `CRC32C`, and `ObjectMeta.size` (`server/tests/chunk_signature_test.rs`, `server/tests/multipart_sigv4_test.rs`).

### Edge Case 9: Multipart Upload Anomalies (`EntityTooSmall`, Out-of-Order, Resent Parts) ([`multipart`](features.toml#L536))
- **Scenario:** Clients upload parts concurrently/out of order, re-upload a `partNumber`, list non-decreasing duplicate parts, or send `< 5 MiB` parts.
- **Contract:**
  1. Parts may arrive in any order and concurrently; each part writes via atomic `.partial-*` rename to `.staging/upload-<hex>/part_{NNNNN}.dat` (`server/tests/staged_mpu_test.rs`, `server/tests/streaming_part_test.rs`). Re-uploading the same `(uploadId, partNumber)` atomically replaces `part_{NNNNN}.dat` once the new upload completes (a failed re-upload preserves the earlier part).
  2. On `CompleteMultipartUpload` (`server/src/multipart.rs:360-436`, `server/tests/multipart_conformance_test.rs`):
     - `PartNumber` order must be **non-decreasing** (`400 InvalidPartOrder` if a part number is less than the preceding entry; consecutive duplicates of the same `PartNumber` keep the last entry so retried parts finish cleanly).
     - Each listed part must exist in the upload and match `<ETag>` (`400 InvalidPart`).
     - Every part except the last must be `>= 5,242,880` bytes (5 MiB) unless `--allow-small-parts` was passed (`400 EntityTooSmall`, tested in `cli_default_rejects_small_parts_and_flag_allows_them`).
     - Only the listed parts become `PartExtent` rows in the `AWNMPU1` manifest needle; unlisted staged files in `.staging/<uploadId>/` are not included in the completed object.

### Edge Case 10: RFC 9110 Byte-Range Corner Cases ([`ranges`](features.toml#L909))
- **Scenario:** `Range: bytes=0-99999999` on a `1,000`-byte object, `Range: bytes=-500`, `Range: bytes=1000-2000`, or range on a `0`-byte object.
- **Contract:**
  1. `start < size` and `end >= size`: clamps `end = size - 1` (`206 Partial Content`, `Content-Range: bytes start-(size-1)/size`).
  2. `bytes=-N` (`N > 0`, `size > 0`): returns the last `min(N, size)` bytes as `206 Partial Content`.
  3. `start >= size`, `start > end`, `N == 0`, multi-range `,`, or any range on a `0`-byte object: returns `416 Range Not Satisfiable` with `Content-Range: bytes */{size}` and `<Code>InvalidRange</Code>` (`server/tests/wire_test.rs::test_rfc9110_range_requests_and_clamping`).

### Edge Case 11: Crash Recovery & Torn Needle Truncation ([`restart`](features.toml#L988), [`volume-format`](features.toml#L1138))
- **Scenario:** Process crashes or loses power while appending a needle to `volume_NNNN.dat`.
- **Contract:**
  1. On startup (`S3ServiceState::try_with_data_dir_limits`, `server/src/s3_api.rs:833-1025`), AwanFS opens `buckets.manifest`, clears and rebuilds `meta/` segments ([`metadata-index`](features.toml#L1130)) by scanning every `volume_NNNN.dat` from `volume_0001` upward (checkpoint boot moved to `#322`; `.idx` journals are written for tooling/future `#38`).
  2. Each needle header's `magic` (`0x41574E33`), `header_crc32c`, and `payload_crc32c` (Castagnoli CRC32C) are verified on scan ([`server/src/volume.rs`](server/src/volume.rs)).
  3. If a torn or invalid needle is encountered at the volume tail on a growing volume, the file is truncated back to the last verified 4096-byte boundary (sparse volumes halt recovery at the unwritten boundary); committed needles before the tear and after `.idx` gaps survive (`server/tests/volume_packing_test.rs`, `server/tests/sparse_mmap_test.rs`, `server/tests/volume_gap_test.rs`).

### Edge Case 12: Volume Rollover & Auto-Compaction ([`compaction`](features.toml#L1045), [`restart`](features.toml#L988))
- **Scenario:** Active volume reaches 32 GiB (`MAX_VOLUME_SPAN`), or deletions/overwrites leave dead needles in sealed volumes.
- **Contract:**
  1. When a write would exceed `MAX_VOLUME_SPAN` (32 GiB), the active volume is sealed and a new `volume_{N+1:04}.dat` opens automatically (`server/tests/volume_rollover_test.rs`). No `507 InsufficientStorage` disk-headroom gate exists (`ARCHITECTURE.md` Decision 13); `/_awanfs/health/ready` returns `503` only while shutting down, when no volume is open, or when configured volume span cannot hold a needle (`server/tests/health_test.rs`). OS `ENOSPC` / `EIO` fails the write with `500 InternalError`.
  2. Unless `--auto-compaction off` is set, background compaction checks every `600s` (`server/src/main.rs:295`, `server/src/s3_api.rs:1383-1404`) and compacts the oldest volume once dead bytes reach `>= 20%` (for a lone active volume, only once it holds `>= 1 GiB`), rate-limited by `--compaction-rate` (`50 MB/s` default, `0` unlimited; `server/tests/auto_compaction_test.rs`, `server/tests/throttle_test.rs`).

---

## 3. S3 XML Error Matrix ([`operation-table`](features.toml#L1153))

All XML error bodies carry `Content-Type: application/xml`, HTTP header `x-amz-request-id: <24-hex-id>`, and the schema emitted by `s3_error_xml` ([`server/src/xml.rs:48-65`](server/src/xml.rs#L48-L65), [`server/tests/request_id_test.rs`](server/tests/request_id_test.rs)):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Resource>snapshots/agent_42.raw</Resource>
  <RequestId>673B4E900000000000000001</RequestId>
</Error>
```

(`HEAD` error responses return the status code with an empty 0-byte body; CORS preflight / bucket-config / object errors follow the HTTP status and `<Code>` values below.)

| HTTP Status | S3 `<Code>` | Trigger Condition |
| :---: | :--- | :--- |
| **`304 Not Modified`** | *(Empty Body)* | `If-None-Match` or `If-Modified-Since` evaluates to not modified on `GET`/`HEAD`. |
| **`400 Bad Request`** | `InvalidBucketName` | Bucket name violates length (`3..=63`) or character/IP/prefix/suffix rules. |
| **`400 Bad Request`** | `InvalidObjectName` | Key is empty, contains NUL / C0 control bytes (`< 0x20`), or has a `..` segment. |
| **`400 Bad Request`** | `InvalidURI` | Object key has invalid percent-encoded UTF-8 or C1/DEL characters (`0x7F`, `0x80..=0x9F`). |
| **`400 Bad Request`** | `KeyTooLongError` | Decoded key length exceeds `1,024` UTF-8 bytes. |
| **`400 Bad Request`** | `InvalidArgument` | Invalid query/header parameter (`partNumber`, `max-keys`, `max-buckets`, `continuation-token`, `encoding-type`, `copy-source`, `x-amz-object-attributes`), or oversized non-policy bucket config. |
| **`400 Bad Request`** | `InvalidRequest` | Incompatible query/header options (e.g., `Range` + `?partNumber`, missing CORS preflight headers, `?versionId` delete marker copy source, unsupported checksum algorithm/type). |
| **`400 Bad Request`** | `InvalidLocationConstraint` | `<LocationConstraint>` in `CreateBucket` does not match server `--region`. |
| **`400 Bad Request`** | `InvalidDigest` | `Content-MD5` header is not valid 16-byte base64. |
| **`400 Bad Request`** | `BadDigest` | Received payload MD5 mismatches `Content-MD5`, or `x-amz-checksum-*` value is malformed or mismatches payload. |
| **`400 Bad Request`** | `XAmzContentSHA256Mismatch` | Decoded payload SHA-256 differs from declared `x-amz-content-sha256` hex digest. |
| **`400 Bad Request`** | `IncompleteBody` | Body shorter/longer than declared `Content-Length` / `x-amz-decoded-content-length`, or truncated `aws-chunked` body. |
| **`400 Bad Request`** | `MalformedChunkHeader` | Invalid `<hex-size>[;chunk-signature=...]\r\n` framing in `aws-chunked` stream. |
| **`400 Bad Request`** | `EntityTooSmall` | Multipart non-final part is smaller than 5 MiB (when `--allow-small-parts` is not set). |
| **`400 Bad Request`** | `EntityTooLarge` | Object body exceeds `5 GiB` (`MAX_OBJECT_BODY`) or XML body exceeds `16 MiB` (`MAX_XML_BODY`). |
| **`400 Bad Request`** | `InvalidPart` | `CompleteMultipartUpload` references a missing part or mismatched `<ETag>`, or `GET`/`HEAD ?partNumber` exceeds uploaded part count. |
| **`400 Bad Request`** | `InvalidPartOrder` | `CompleteMultipartUpload` `<PartNumber>` list decreases. |
| **`400 Bad Request`** | `MalformedXML` | Request XML (`CompleteMultipartUpload`, `DeleteObjects`, `Versioning`, `Lifecycle`, `CORS`, `Tagging`, `CreateBucketConfiguration`) is malformed or fails schema limits. |
| **`400 Bad Request`** | `InvalidTag` | `x-amz-tagging` header or tag key/value exceeds limits or fails tag validation. |
| **`400 Bad Request`** | `MetadataTooLarge` | User metadata (`x-amz-meta-*`) exceeds `2,048` bytes or encoded needle attributes block exceeds `65,535` bytes. |
| **`400 Bad Request`** | `AuthorizationHeaderMalformed` / `AuthorizationQueryParametersError` | Malformed SigV4 `Authorization` header or query parameters (`X-Amz-Algorithm`, `X-Amz-Expires > 604800`, missing fields). |
| **`403 Forbidden`** | `AccessDenied` | IAM policy denies action, expired presigned URL (`now > X-Amz-Date + X-Amz-Expires`), disallowed CORS preflight, or `response-*` query overrides on unauthenticated request. |
| **`403 Forbidden`** | `InvalidAccessKeyId` | SigV4 `Credential` access key is unknown (or credentials missing). |
| **`403 Forbidden`** | `SignatureDoesNotMatch` | SigV4 header, presigned URL, or `aws-chunked` rolling chunk HMAC-SHA256 verification failed. |
| **`403 Forbidden`** | `RequestTimeTooSkewed` | Request `x-amz-date` differs from server clock by `> 900` seconds. |
| **`404 Not Found`** | `NoSuchBucket` | Target (or copy source) bucket does not exist. |
| **`404 Not Found`** | `NoSuchKey` | Target key does not exist or latest version is a delete marker. |
| **`404 Not Found`** | `NoSuchVersion` | Requested `?versionId` does not exist. |
| **`404 Not Found`** | `NoSuchUpload` | Multipart `uploadId` does not exist or was already completed/aborted. |
| **`404 Not Found`** | `NoSuchLifecycleConfiguration` | `GET /{bucket}?lifecycle` called when no lifecycle rules are configured. |
| **`404 Not Found`** | `NoSuchCORSConfiguration` | `GET /{bucket}?cors` called when no CORS configuration is stored. |
| **`404 Not Found`** | `NoSuchTagSet` | `GET /{bucket}?tagging` called when no bucket tags are stored. |
| **`405 Method Not Allowed`** | `MethodNotAllowed` | Unsupported HTTP verb on service/bucket/object, or `GET`/`HEAD`/`tagging`/`attributes` targeting a delete marker without `?versionId`. |
| **`408 Request Timeout`** | `RequestTimeout` | No request body frame received within `--body-idle-timeout` (`20s` default). |
| **`409 Conflict`** | `BucketNotEmpty` | `DELETE /{bucket}` called while live objects remain in the bucket. |
| **`409 Conflict`** | `BucketAlreadyOwnedByYou` | `CreateBucket` called on caller's existing bucket outside `us-east-1`/`default` region. |
| **`409 Conflict`** | `BucketAlreadyExists` | `CreateBucket` called on a bucket owned by another account. |
| **`411 Length Required`** | `MissingContentLength` | `PutObject` missing `Content-Length`, `Transfer-Encoding: chunked`, and `aws-chunked`. |
| **`412 Precondition Failed`** | `PreconditionFailed` | `If-Match`, `If-None-Match: *` (`PUT`), `If-Unmodified-Since`, or `x-amz-copy-source-if-*` condition failed. |
| **`416 Range Not Satisfiable`** | `InvalidRange` | `Range` start `>=` object size, `start > end`, suffix `0`, multi-range, range on 0-byte object, or invalid `x-amz-copy-source-range`. |
| **`500 Internal Server Error`** | `InternalError` | Disk I/O, index/CRC, sync (`AfterRecord`), or volume rollover failure. |
| **`501 Not Implemented`** | `NotImplemented` | Sub-resource or operation is not implemented (`?acl`, `?policy`, `?encryption`, `?object-lock`, `?logging`, etc., `ARCHITECTURE.md` Decision 61). |
| **`503 Service Unavailable`** | `SlowDown` | `--memory-budget` in-flight bodies half exhausted after 100 ms wait, or index reached capacity without spill. |

---

## Not yet

- **Checkpoint boot recovery (`#322`):** Boot currently clears `meta/` segments and rebuilds from `volume_*.dat`; booting from per-partition manifest checkpoints belongs to the partitioned store work ([`ARCHITECTURE.md`](ARCHITECTURE.md) Decisions 59 & 69).
- **Skip gaps via `.idx` during volume scan (`#38`):** `.idx` journals are written, while boot recovery currently scans `.dat` needle headers directly.
- **`503 SlowDown` `Retry-After` guidance (`#356`):** `503 SlowDown` is emitted without a `Retry-After` header today.
- **Large-object background scrubbing (`#155`)** and **Copy-on-write / zero-copy `CopyObject` ref-counting (`#104`)**: Currently `CopyObject` writes a new needle and `> 4 MiB` GET ranges are not checksummed during transfer.
- **Attribute-update needles Stage 2 (`#150`):** Tag/attribute updates and compaction ordering work for `tags`, while object lock/ACL attributes await `#120`/`#122`.
- **Remaining S3 features:** Multipart edge cases ([`multipart`](features.toml#L536), `#111`), conditional `PUT If-Match`/`DELETE` ([`conditional`](features.toml#L470), `#112`), periodic persisted lifecycle sweeps ([`lifecycle`](features.toml#L150), `#118`), `PostObject` ([`post-object`](features.toml#L180), `#119`), Object Lock ([`object-lock`](features.toml#L162), `#120`), SSE ([`encryption`](features.toml#L133), `#121`), Policies/ACLs/Anonymous access ([`policy`](features.toml#L327), [`acl-ownership`](features.toml#L379), `#122`), and Bucket Logging ([`bucket-logging`](features.toml#L88), `#153`).
