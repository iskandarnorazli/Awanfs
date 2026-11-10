# Awan Storage REST & S3 API Specification

> **Wire Protocol: Drop-In S3 REST Compatibility, Dual Host Routing, `aws-chunked` Framing, and Endpoint Contracts**

---

## 1. Scope & Drop-In Compatibility Guarantees

Awan Storage implements the **Amazon S3 REST API (2006-03-01)** specification required for drop-in compatibility with official S3 clients (`aws-cli`, `boto3`, `@aws-sdk/client-s3`, Go `minio-go`, `rclone`, and MinIO `warp`).

### 1.1 Dual Addressing Styles (Path-Style & Virtual-Hosted-Style)
Standard S3 SDKs use either path-style or virtual-hosted-style routing depending on client configuration. The HTTP router inspects the `Host` header on every request:
1. **Path-Style (`http://s3.internal:9000/{bucket}/{key}`):**
   - `Host` matches the configured endpoint domain or IP literal (e.g., `127.0.0.1:9000`, `localhost:9000`, `storage.internal:9000`).
   - First URI segment is extracted as `{bucket}`, remainder as `{key}`.
2. **Virtual-Hosted-Style (`http://{bucket}.s3.internal:9000/{key}`):**
   - `Host` carries a subdomain prefix `<bucket>.<endpoint-domain>`.
   - Subdomain is extracted as `{bucket}`, entire URI path is extracted as `{key}`.

### 1.2 `Expect: 100-continue` Wire Handshake
Clients uploading payloads $> 1\text{ MiB}$ (`curl`, `aws-cli`, `boto3`) send `Expect: 100-continue` and pause payload transmission until the server validates headers:
1. Server validates bucket existence, SigV4 signature, and storage quota headroom (`< 95%` capacity) using only the HTTP request headers.
2. If rejected, server immediately responds with `403 Forbidden`, `404 NoSuchBucket`, or `507 Insufficient Storage` before a single body byte crosses the network.
3. If accepted, server leases an `@arena` slot and immediately writes `HTTP/1.1 100 Continue\r\n\r\n`, unlocking line-rate client payload streaming.

---

## 2. Complete S3 Endpoint Surface

### A. Service & Bucket Operations

| HTTP Verb | Path & Query | S3 Operation | Wire Contract & Status Codes |
| :--- | :--- | :--- | :--- |
| **`GET`** | `/` | `ListBuckets` | Returns `200 OK` with `<ListAllMyBucketsResult>` XML listing all active buckets and creation timestamps. |
| **`PUT`** | `/{bucket}` | `CreateBucket` | Validates bucket name (3–63 chars, lowercase alphanumeric/hyphens, non-IP). Allocates bucket ID and prefix ART root. Returns `200 OK` (idempotent if already owned by caller). |
| **`HEAD`** | `/{bucket}` | `HeadBucket` | Used by `warp`, `boto3`, and `rclone` pre-flight checks. Returns `200 OK` (empty body) if bucket exists, `404 Not Found` otherwise. |
| **`DELETE`**| `/{bucket}` | `DeleteBucket` | If bucket has 0 live objects, reclaims bucket metadata and returns `204 No Content`. If live objects exist, returns `409 Conflict` (`BucketNotEmpty`). |
| **`GET`** | `/{bucket}?location` | `GetBucketLocation` | Returns `200 OK` with `<LocationConstraint>us-east-1</LocationConstraint>` XML (required by AWS SDK region discovery). |
| **`GET`** | `/{bucket}?list-type=2` | `ListObjectsV2` | Lexicographical UTF-8 scan via bucket ART index. Supports `prefix`, `delimiter`, `max-keys` (default 1,000), `start-after`, `continuation-token`, and `fetch-owner`. Returns `200 OK` XML. |
| **`GET`** | `/{bucket}` | `ListObjects` (V1) | Legacy v1 listing (`marker`, `prefix`, `delimiter`, `max-keys`) for older S3 tools. Returns `200 OK` XML. |
| **`POST`** | `/{bucket}?delete` | `DeleteObjects` | Batch multi-object delete (up to 1,000 keys per XML request; required by `warp` cleanup and `aws s3 rb --force`). Evaluates `Content-MD5` header, tombstones all listed keys, and returns `200 OK` `<DeleteResult>` XML. |

### B. Object Operations

| HTTP Verb | Path | S3 Operation | Wire Characteristics & Response Headers |
| :--- | :--- | :--- | :--- |
| **`PUT`** | `/{bucket}/{key}` | `PutObject` | Streams into 4096B-aligned `@arena` slot, writes Needle to volume slab, publishes atomically to `@flat ObjectMeta`. Returns `200 OK` with `ETag: "<32-hex-md5>"`. |
| **`PUT`** | `/{bucket}/{key}` (+ `x-amz-copy-source`) | `CopyObject` | Zero-copy metadata clone pointing to source extent(s) with incremented ref-count. Returns `200 OK` with `<CopyObjectResult>` XML. |
| **`GET`** | `/{bucket}/{key}` | `GetObject` | Epoch-pinned zero-copy read from slab/volume. Supports `Range: bytes=...` and conditional headers. Returns `200 OK` or `206 Partial Content` with `ETag`, `Content-Length`, `Last-Modified`, `Accept-Ranges: bytes`. |
| **`HEAD`** | `/{bucket}/{key}` | `HeadObject` | Sub-microsecond lookup from `@flat ObjectMeta`. Returns exact same headers as `GET` (`Content-Length`, `ETag`, `Content-Type`, `Last-Modified`, `x-amz-meta-*`) with **0 body bytes**. |
| **`DELETE`**| `/{bucket}/{key}` | `DeleteObject` | Sets tombstone bit in `@flat ObjectMeta` and removes key from ART prefix index. Returns `204 No Content` (idempotent even if key did not exist, per S3 spec). |

### C. Multipart Upload Operations (Payloads $> 5\text{ MiB}$)

| HTTP Verb | Path & Query Parameters | S3 Operation | Execution & Validation Contract |
| :--- | :--- | :--- | :--- |
| **`POST`** | `/{bucket}/{key}?uploads` | `CreateMultipartUpload` | Allocates unique 64-bit `UploadId` and staging manifest. Returns `200 OK` with `<InitiateMultipartUploadResult>` XML. |
| **`PUT`** | `/{bucket}/{key}?partNumber={N}&uploadId={ID}` | `UploadPart` | Validates $1 \le N \le 10,000$ (`400 InvalidArgument` otherwise). Writes part needle to volume slab; records part offset, size, and 128-bit MD5 in staging manifest (overwriting any prior upload of the same `partNumber`). Returns `200 OK` with `ETag: "<part-md5>"`. |
| **`POST`** | `/{bucket}/{key}?uploadId={ID}` | `CompleteMultipartUpload` | Parses `<CompleteMultipartUpload>` XML. Verifies parts are in strictly ascending `PartNumber` order (`400 InvalidPartOrder`), each `<ETag>` matches recorded part MD5 (`400 InvalidPart`), and every part except the final part is $\ge 5\text{ MiB}$ (`400 EntityTooSmall`, configurable in test mode). Computes composite ETag `"<md5(raw_part1_md5 || ... || raw_partK_md5)>-<K>"`, populates `@flat PartExtent` rows without copying data, and publishes object. Returns `200 OK` XML. |
| **`DELETE`**| `/{bucket}/{key}?uploadId={ID}` | `AbortMultipartUpload` | Marks all staged part needles as dead and deletes staging manifest. Returns `204 No Content` (`404 NoSuchUpload` if `uploadId` is unknown). |
| **`GET`** | `/{bucket}/{key}?uploadId={ID}` | `ListParts` | Returns `200 OK` `<ListPartsResult>` XML listing uploaded part numbers, sizes, and ETags for resume-capable clients. |
| **`GET`** | `/{bucket}?uploads` | `ListMultipartUploads` | Returns `200 OK` `<ListMultipartUploadsResult>` XML of active in-progress multipart uploads in the bucket. |

### D. Bucket Lifecycle Configuration & Automated Cleanup

| HTTP Verb | Path & Query | S3 Operation | Implementation Details |
| :--- | :--- | :--- | :--- |
| **`PUT`** | `/{bucket}?lifecycle` | `PutBucketLifecycleConfiguration` | Parses `<LifecycleConfiguration>` XML; configures per-prefix object TTL (`Expiration.Days`) and incomplete multipart purge (`AbortIncompleteMultipartUpload.DaysAfterInitiation`). Returns `200 OK`. |
| **`GET`** | `/{bucket}?lifecycle` | `GetBucketLifecycleConfiguration` | Returns `200 OK` with active XML configuration, or `404 NoSuchLifecycleConfiguration`. |
| **`DELETE`**| `/{bucket}?lifecycle` | `DeleteBucketLifecycle` | Disables automated expiration sweeps for the bucket. Returns `204 No Content`. |

---

## 3. AWS Signature Version 4 & Payload Streaming Modes

Every authenticated S3 request is validated against **AWS Signature Version 4 (`AWS4-HMAC-SHA256`)** via either the `Authorization` header or Pre-Signed Query Parameters (`X-Amz-Signature`).

### 3.1 Derived Signing Key Cache
Computing 4 chained HMAC-SHA256 operations per request (`kDate -> kRegion -> kService -> kSigning`) wastes CPU cycles under high IOPS. Each worker caches `(access_key_id, yyyymmdd, region) -> [u8; 32]` in an L1 lookup slot, reducing SigV4 verification to a single HMAC-SHA256 over the `StringToSign`.

### 3.2 Three Payload Integrity Modes (`x-amz-content-sha256`)
S3 clients transmit payloads in three distinct modes, all of which must be supported for drop-in compatibility:
1. **Single-Chunk Pre-Hashed Payload (`x-amz-content-sha256: <64-hex-sha256>`):**
   - Client sends the hex SHA-256 of the body in the header.
   - Server computes hardware SHA-256 on the fly as bytes stream into the `@arena` slot; if the final digest mismatches the header, the write is aborted with `400 Bad Request` (`XAmzContentSHA256Mismatch` / `InvalidDigest`).
2. **Unsigned Payload (`x-amz-content-sha256: UNSIGNED-PAYLOAD`):**
   - Used by `warp`, `aws-cli`, and `boto3` when payload signing is disabled for high throughput.
   - Server verifies the header signature using the literal string `"UNSIGNED-PAYLOAD"` in the Canonical Request and still computes hardware MD5/CRC32C on the payload stream for `ETag` and disk integrity.
3. **Chunked Streaming Payload (`x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`):**
   - Used by `aws-cli` and AWS SDK v3 when streaming files without pre-reading them twice.
   - Header `x-amz-decoded-content-length` specifies the true unframed object size.
   - The wire body is framed as:
     ```http
     10000;chunk-signature=<64-hex-hmac>\r\n
     <65536 raw payload bytes>\r\n
     ...
     0;chunk-signature=<final-64-hex-hmac>\r\n
     \r\n
     ```
   - **Decoder Contract:** The HTTP reactor strips the `<hex-size>;chunk-signature=...\r\n` framing in-place, verifies each chunk's chained HMAC signature against the prior signature seed, and writes **only the decoded raw payload bytes** into the storage slab.

---

## 4. Conditional Headers & Optimistic Concurrency Control

| Header | Operator / Condition | Behavior on Match | Behavior on Mismatch |
| :--- | :--- | :--- | :--- |
| **`If-Match`** | `"{etag}"` | Proceed with `GET`/`PUT`/`DELETE`. | Aborts with **`HTTP 412 PreconditionFailed`**. |
| **`If-None-Match`** | `"*"` | Creates object **only if no active key exists** (atomic mutex lock). | Aborts with **`HTTP 412 PreconditionFailed`**. |
| **`If-None-Match`** | `"{etag}"` | On `GET`/`HEAD`, returns **`HTTP 304 NotModified`** (0-byte body). | Returns `200 OK` with updated payload and `ETag`. |
| **`If-Modified-Since`**| `<HTTP-date>` | Returns `200 OK` with payload if `created_at > timestamp`. | Returns **`HTTP 304 NotModified`**. |
| **`If-Unmodified-Since`**| `<HTTP-date>` | Proceed if `created_at <= timestamp`. | Aborts with **`HTTP 412 PreconditionFailed`**. |

---

## 5. RFC 9110 / S3 Byte-Range Semantics (`Range: bytes=...`)

1. **Explicit Range (`Range: bytes=start-end`):**
   - Requires `0 <= start <= end` and `start < object_size`.
   - If `end >= object_size`, `end` is **clamped to `object_size - 1`** (per RFC 9110 & AWS S3 behavior).
   - Returns `206 Partial Content`, `Content-Length: end - start + 1`, and `Content-Range: bytes start-end/object_size`.
2. **Prefix-Offset Range (`Range: bytes=start-`):**
   - Requires `0 <= start < object_size`. Streams from `start` through `object_size - 1`.
3. **Suffix Range (`Range: bytes=-suffix_len`):**
   - Requires `suffix_len > 0`. Reads the last `min(suffix_len, object_size)` bytes of the object.
4. **Unsatisfiable Range (`start >= object_size`, `suffix_len == 0`, or any range on a `0`-byte object):**
   - Returns **`HTTP 416 Range Not Satisfiable`** with header `Content-Range: bytes */{object_size}` and `<Code>InvalidRange</Code>` XML body.
