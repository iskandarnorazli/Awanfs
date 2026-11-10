# Awan Storage Edge Cases, Failure Semantics & Error Contracts

> **Production Hardening: Network Partitions, MVCC Isolation, Multipart Edge Cases, Torn Write Recovery, and S3 Error Matrix**

---

## 1. Edge Case Operational Matrix (12 Core Scenarios)

| # | Failure / Edge Scenario | Failure Risk | Architectural Contract |
| :-: | :--- | :--- | :--- |
| **1** | Mid-Stream Socket Disconnect | Leased Slab Leak / Torn Object | Lease Watchdog + Uncommitted Needle Drop |
| **2** | Zero-Byte Objects & Folder Keys | Wasted Slabs / Null Pointer DMA | `FLAG_ZERO_BYTE` Slot + Empty MD5 Constant |
| **3** | Thundering Herd on Cold Key | NVMe Queue Saturation / OOM | Single-Flight Read Coalescing Broadcast |
| **4** | Key Traversal (`../`) & Bad UTF-8 | Path Escape / XML Injection | Strict Key Validator (`400`) + Volume Isolation |
| **5** | Hash Collision / Hash-Flooding | Returning Wrong Key's Payload | Keyed XXH3/SipHash + Key String Arena Equality |
| **6** | Concurrent Overwrite During `GET` | Reader Sees Half-Written Object | Epoch-Pinned MVCC Read Isolation |
| **7** | `Expect: 100-continue` Upload | 1–3s Client Stall or Wasted Bandwidth | Pre-Body Header Gate (`100 Continue` or `403`/`507`) |
| **8** | `aws-chunked` Streaming Upload | Chunk Signatures Stored as Payload | Streaming Frame Stripper & Chained HMAC Validator |
| **9** | Multipart Part Anomalies | Out-of-Order / Small / Re-Uploaded Parts | Part Manifest Validation + Superseding Part Reclaim |
| **10**| Byte-Range Corner Cases | `end >= size`, `0B` Object, Suffix `-N` | RFC 9110 Clamping & `416 Content-Range: bytes */sz` |
| **11**| Power Loss Mid-Write (Torn Needle) | Corrupted Volume Index on Reboot | 4096B Superblock + Needle CRC32C Tail Truncation |
| **12**| Out of Disk Space (`ENOSPC`) | Partial Writes / Tombstone Accumulation | 5% Safety Headroom (`507`) + Background Compaction |

---

## 2. Deep Dive: Edge Cases & Operational Contracts

### Edge Case 1: Client Network Disconnect Mid-Upload (`IncompleteBody`)
- **Scenario:** A client starts a 10 GB `PUT`, streams 2.3 GB, and drops the TCP connection (or sends fewer bytes than `Content-Length`).
- **Contract:**
  1. Every active upload stream tracks `bytes_received` vs expected `Content-Length` (or `x-amz-decoded-content-length`) and updates `last_activity_epoch`.
  2. If the socket closes before `bytes_received == expected_length`, the server emits `400 Bad Request` (`IncompleteBody`) if the socket is still writable, releases the `@arena` slot immediately, and marks any written volume space as reclaimable dead space.
  3. If the socket stalls beyond `stream_timeout_ms` (default 15,000 ms), the watchdog resets the connection (`SO_LINGER=0`) and reclaims the slot.
  4. **Atomicity Guarantee:** `@flat ObjectMeta` is updated **only** after the final byte and digest check succeed. Incomplete uploads are never visible to `GET`, `HEAD`, or `ListObjectsV2`.

### Edge Case 2: Zero-Byte Objects & Directory Markers (`Content-Length: 0`)
- **Scenario:** Clients upload `0`-byte objects or directory placeholders ending in `/` (e.g., `PUT /bucket/logs/2026/`).
- **Contract:**
  1. When `Content-Length == 0` (or decoded length is `0`), the engine skips volume slab writing and `@arena` chunk leasing (`vol_offset = 0`, `FLAG_ZERO_BYTE`).
  2. Records `size = 0` and the constant 128-bit MD5 of `""` (`d41d8cd98f00b204e9800998ecf8427e`).
  3. A subsequent `GET` returns `200 OK` with `Content-Length: 0`; a `GET` with any `Range:` header returns `416 Range Not Satisfiable` with `Content-Range: bytes */0`.

### Edge Case 3: Thundering Herd on Popular Checkpoints & Images
- **Scenario:** 1,000 agent sandboxes boot simultaneously and request `GET /sandboxes/ubuntu-rootfs.raw`.
- **Contract (Single-Flight Coalescing):**
  1. Each shard tracks an in-flight read table keyed by `(bucket_id, key_hash)`.
  2. The first request leases the `@arena` buffer and submits the NVMe `O_DIRECT` read.
  3. The 999 concurrent requests attach as subscribers to the same pinned `@arena` buffer slice.
  4. As each chunk completes DMA into RAM, all waiting sockets transmit from the **single shared read-only buffer**. Disk reads: **1 instead of 1,000**.

### Edge Case 4: Key Path Traversal, URI Encoding & XML Escaping
- **Scenario:** A client sends `PUT /bucket/../../etc/passwd`, null bytes `%00`, overlong keys (`> 1,024` bytes), or XML-special characters (`&`, `<`, `>`).
- **Unified Two-Layer Contract:**
  1. **Layer 1 — Strict Input Validation (`400 Bad Request`):**
     - URI percent-encoding is decoded once.
     - Keys that are empty, exceed `1,024` UTF-8 bytes (`KeyTooLongError`), contain null bytes (`\0`) or ASCII control characters (`0x00..0x1F`), or resolve to relative traversal segments (`..` or `/..` as path components) are **rejected immediately at the HTTP router with `400 Bad Request` (`InvalidObjectName` / `InvalidURI`)**.
  2. **Layer 2 — Host Filesystem Isolation (Defense-in-Depth):**
     - Object keys are **never** passed to OS filesystem path APIs (`open()`, `CreateFile()`). All payloads reside inside numbered volume files (`volume_0001.dat`) addressed by `(volume_id, byte_offset)`.
     - When keys containing `&`, `<`, `>`, `'`, or `"` are serialized in `ListObjectsV2` or error XML responses, they are strictly XML-entity escaped (`&amp;`, `&lt;`, etc.).

### Edge Case 5: Hash Collisions & Adversarial Hash-Flooding DoS
- **Scenario:** Two distinct keys produce the same 64-bit hash, or an attacker crafts keys to collide in the metadata table.
- **Contract:**
  1. `key_hash` is computed using **64-bit keyed XXH3 / SipHash-1-3** seeded with a per-cluster random secret generated at volume creation, neutralizing precomputed hash-flood attacks.
  2. Every `@flat ObjectMeta` lookup that matches `key_hash` and `bucket_id` **also compares the full key byte slice** stored in the Key String Arena (`key_slice`).
  3. If `key_hash` matches but the key bytes differ, open-addressing linear/quadratic probing (within the 16-slot SIMD group) advances to the next slot, guaranteeing **zero false-positive key collisions**.

### Edge Case 6: Concurrent Overwrite vs In-Flight Readers (MVCC Epoch Isolation)
- **Scenario:** Client A is streaming a 1 GB `GET /bucket/weights.bin`. Halfway through Client A's download, Client B completes a new `PUT /bucket/weights.bin` (or `DELETE /bucket/weights.bin`).
- **Contract:**
  1. Overwriting a key **never mutates an existing volume extent in-place**. The new `PUT` writes a fresh Needle at the tail of the active volume slab.
  2. When Client A begins `GET`, it acquires a lightweight thread-local **Epoch Pin** (or increments the extent's `ref_count`) and snapshots `(vol_offset, size, etag)` from `@flat ObjectMeta`.
  3. When Client B's `PUT` completes, it atomically updates the `@flat ObjectMeta` row to point to the new Needle and queues the old extent in the **Deferred Reclaim List** for the current epoch.
  4. Client A finishes downloading the complete, consistent old object; the old extent is eligible for compaction only after all readers from that epoch complete.

### Edge Case 7: `Expect: 100-continue` Early Gate
- **Scenario:** `aws-cli` sends `PUT /bucket/10gb.bin` with `Expect: 100-continue` onto a full disk or with an invalid signature.
- **Contract:**
  - Header authentication, bucket lookup, and storage quota checks run **before** sending `100 Continue`.
  - If any check fails, the server returns the final error (`403`, `404`, `507`) with `Connection: close` immediately, saving gigabytes of wasted network transfer.
  - If all checks pass, the server immediately emits `HTTP/1.1 100 Continue\r\n\r\n` so the client never waits for its 1-second fallback timer.

### Edge Case 8: `aws-chunked` Streaming Payload Framing
- **Scenario:** `aws-cli` or AWS SDK v3 uploads a stream using `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and `Content-Encoding: aws-chunked`.
- **Contract:**
  - The server parses `<hex-chunk-len>;chunk-signature=<sig>\r\n<payload>\r\n` frames on the fly.
  - Each chunk's HMAC-SHA256 signature is verified against the rolling signature chain; a bad chunk signature aborts the stream with `403 SignatureDoesNotMatch`.
  - Only the raw decoded payload bytes are hashed for MD5 `ETag` and written to the volume slab, and `ObjectMeta.size` records the decoded byte count (`x-amz-decoded-content-length`).

### Edge Case 9: Multipart Upload Anomalies (`EntityTooSmall`, Out-of-Order, Part Overwrite)
- **Scenario:** Clients upload parts out of order (Part 3, then Part 1, then Part 2), re-upload Part 2 with new content, or send a 100 KB non-final part.
- **Contract:**
  1. Parts may be uploaded in **any chronological order** and concurrently across worker cores.
  2. Re-uploading the same `(uploadId, partNumber)` atomically replaces that part's slot in the staging manifest and marks the previous part needle as dead space.
  3. On `CompleteMultipartUpload`:
     - The XML `<Part>` list must be in strictly ascending `PartNumber` order; otherwise return `400 InvalidPartOrder`.
     - Every `<Part>`'s `<ETag>` must match the latest staged part's MD5; otherwise return `400 InvalidPart`.
     - Every part except the last part in the list must have `part_size >= 5,242,880` bytes (5 MiB) unless `--allow-small-parts` test mode is enabled; otherwise return `400 EntityTooSmall`.
     - Any staged parts not listed in the `CompleteMultipartUpload` XML are discarded and marked dead.

### Edge Case 10: RFC 9110 Byte-Range Corner Cases
- **Scenario:** Clients request `Range: bytes=0-99999999` on a 1,000-byte object, `Range: bytes=-500`, or `Range: bytes=1000-2000`.
- **Contract:**
  1. `Range: bytes=start-end` where `start < size` and `end >= size`: clamp `end = size - 1` and return `206 Partial Content` with `Content-Range: bytes start-(size-1)/size`.
  2. `Range: bytes=-N` (suffix): if `N >= size` (and `size > 0`), return the entire object `[0..size-1]` as `206 Partial Content`.
  3. If `start >= size`, `N == 0`, or `size == 0`: return `416 Range Not Satisfiable` with `Content-Range: bytes */size` and `<Code>InvalidRange</Code>`.

### Edge Case 11: Power Loss Mid-Write & Torn Needle Recovery
- **Scenario:** Host loses power while writing a 64 MB Needle to `volume_0001.dat`.
- **Contract:**
  1. On startup, the recovery scanner reads the last committed offset from `metadata.checkpoint` and scans subsequent records in `.idx` and `volume_XXXX.dat`.
  2. Each Needle header's `magic` (`0x41574E33`), `header_crc32c`, and `payload_crc32c` are verified with hardware SSE4.2/ARMv8 CRC32C.
  3. If a torn or partially written needle is encountered at the volume tail, the volume write pointer is truncated back to the start of that needle (4096-byte aligned), restoring a 100% consistent state in milliseconds.

### Edge Case 12: Out of Disk Space (`ENOSPC`) & Background Compaction
- **Scenario:** Disk utilization crosses 95%, or repeated deletions leave dead holes in sealed volumes.
- **Contract:**
  1. **5% Safety Headroom:** When free physical space drops below 5%, new `PUT` and `UploadPart` requests are rejected immediately with `507 Insufficient Storage` (`InsufficientStorage`), while `GET`, `HEAD`, `DELETE`, and `AbortMultipartUpload` continue unhindered.
  2. **Copy-on-Compact:** Background compaction reclaims dead space from volumes with $> 30\%$ tombstoned/superseded bytes without blocking foreground reads or writes.

---

## 3. Comprehensive S3 XML Error Code Matrix (22 Codes)

All error responses carry `Content-Type: application/xml` and standard S3 XML framing:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Key>snapshots/agent_42.raw</Key>
  <BucketName>sandboxes</BucketName>
  <RequestId>AWAN018B72F9A3</RequestId>
  <HostId>node-01.storage.internal</HostId>
</Error>
```

| HTTP Status | S3 `<Code>` | Trigger Condition |
| :---: | :--- | :--- |
| **`304 Not Modified`** | *(Empty Body)* | `If-None-Match` matches current `ETag` or `If-Modified-Since` >= `Last-Modified`. |
| **`400 Bad Request`** | `InvalidBucketName` | Bucket name violates length (3–63) or character rules. |
| **`400 Bad Request`** | `InvalidObjectName` | Key is empty, contains null/control chars, or attempts `..` traversal. |
| **`400 Bad Request`** | `KeyTooLongError` | Key UTF-8 byte length exceeds 1,024 bytes. |
| **`400 Bad Request`** | `InvalidArgument` | Invalid query parameter (e.g., `partNumber` outside `1..10000`). |
| **`400 Bad Request`** | `InvalidDigest` | `Content-MD5` or `x-amz-content-sha256` does not match payload digest. |
| **`400 Bad Request`** | `IncompleteBody` | Socket closed before `Content-Length` bytes were received. |
| **`400 Bad Request`** | `EntityTooSmall` | Multipart part (other than last part) is smaller than 5 MiB. |
| **`400 Bad Request`** | `EntityTooLarge` | Payload exceeds maximum single-PUT or object size limit. |
| **`400 Bad Request`** | `InvalidPart` | Part number or ETag in `CompleteMultipartUpload` does not match staged part. |
| **`400 Bad Request`** | `InvalidPartOrder` | Parts list in `CompleteMultipartUpload` is not in strictly ascending order. |
| **`400 Bad Request`** | `MalformedXML` | Request XML body (`CompleteMultipartUpload`, `DeleteObjects`, `Lifecycle`) is invalid. |
| **`403 Forbidden`** | `AccessDenied` | Missing credentials on private bucket or unauthorized operation. |
| **`403 Forbidden`** | `SignatureDoesNotMatch`| AWS SigV4 HMAC-SHA256 verification failed on header, query, or chunk. |
| **`403 Forbidden`** | `RequestTimeTooSkewed` | `X-Amz-Date` or `Date` deviates from server clock by $> 900\text{ seconds}$. |
| **`404 Not Found`** | `NoSuchBucket` | Target bucket does not exist. |
| **`404 Not Found`** | `NoSuchKey` | Key not found in `@flat ObjectMeta` or has `FLAG_TOMBSTONE` set. |
| **`404 Not Found`** | `NoSuchUpload` | Specified multipart `uploadId` does not exist or was already completed/aborted. |
| **`404 Not Found`** | `NoSuchLifecycleConfiguration` | `GET /{bucket}?lifecycle` called on a bucket with no lifecycle rules. |
| **`409 Conflict`** | `BucketNotEmpty` | `DELETE /{bucket}` called while bucket still has active (non-tombstoned) objects. |
| **`409 Conflict`** | `BucketAlreadyOwnedByYou` | `CreateBucket` called on an existing bucket in a non-`us-east-1` region constraint. |
| **`411 Length Required`**| `MissingContentLength` | `PUT` or `UploadPart` missing both `Content-Length` and `x-amz-decoded-content-length`. |
| **`412 Precondition Failed`** | `PreconditionFailed` | `If-Match`, `If-None-Match: *`, or `If-Unmodified-Since` condition failed. |
| **`416 Range Not Satisfiable`**| `InvalidRange` | `Range` start $\ge$ object size, or range requested on a 0-byte object. |
| **`503 Slow Down`** | `SlowDown` | Pinned `@arena` slab slots temporarily at 100% watermark under extreme burst. |
| **`507 Insufficient Storage`** | `InsufficientStorage` | Disk free space $< 5\%$ headroom or `LicenseCapacityExceeded`. |
