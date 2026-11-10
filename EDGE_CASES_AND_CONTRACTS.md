# Awan Storage Edge Cases, Failure Semantics & Error Contracts

> **Production Hardening: Network Partitions, Thundering Herds, Empty Slabs, and S3 Error Codes**

---

## 1. Edge Case Resolution Matrix

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              EDGE CASE OPERATIONAL CONTRACTS                           │
├──────────────────────────────┬───────────────────────────────┬─────────────────────────┤
│ Failure / Edge Scenario      │ Failure Risk                  │ Architectural Contract  │
├──────────────────────────────┼───────────────────────────────┼─────────────────────────┤
│ 1. Mid-Stream Disconnect     │ Leased Slab Leak / Orphan Byte│ Lease Watchdog Rollback │
│ 2. Zero-Byte Objects         │ Wasted Slabs / Null Pointer   │ Zero-Slab Metadata Slot │
│ 3. Thundering Herd on Image  │ Disk Saturation / OOM         │ Single-Flight Broadcast │
│ 4. Key Path Traversal (../)  │ Arbitrary File Read / Escape  │ FNV-1a Hash Normalizer  │
│ 5. Clock Drift / Replay      │ Expired Signatures Replayed   │ 900s Skew Guard Window  │
│ 6. Out of Disk Space (ENOSPC)│ Partial Writes Corrupt Slabs  │ Pre-Reserved Headroom   │
└──────────────────────────────┴───────────────────────────────┴─────────────────────────┘
```

---

## 2. Deep Dive: Edge Cases & Operational Semantics

### Edge Case 1: Client Network Disconnect Mid-Upload
- **The Scenario:** A client initiates a 10 GB `PUT` request, writes 2.3 GB into leased `@arena` slots, and abruptly drops the TCP connection (Wi-Fi drop, client killed, network partition).
- **The Failure Risk:** Leased slab slots remain locked forever, causing memory starvation. Partially written NVMe blocks orphan disk space.
- **Contract:**
  1. Each leased slot maintains a `last_activity_epoch` timestamp.
  2. If the socket receives no bytes for `stream_timeout_ms` (default: 15,000 ms), the worker core issues an immediate TCP abort (`RST` packet via `SO_LINGER=0`).
  3. The leased slab slots are unconditionally returned to the core's free pool (`ring_release_slot`).
  4. **Atomicity Guarantee:** No entry is committed to `@flat ObjectMeta`. The object never appears in `ListObjectsV2` or `HEAD` queries.

---

### Edge Case 2: Zero-Byte Objects (`Content-Length: 0`)
- **The Scenario:** S3 tools create "folder" directory markers by uploading an empty key ending in `/` with `Content-Length: 0`.
- **The Failure Risk:** Engines attempting to lease a 64 KB chunk slab waste memory or crash on 0-byte buffer DMA.
- **Contract:**
  - When `Content-Length == 0`:
    1. Skips slab leasing entirely. `offset` is stored as `0`.
    2. Directly records an entry in `@flat ObjectMeta` with `size = 0`.
    3. ETag is assigned the constant MD5 hash of empty input: `"d41d8cd98f00b204e9800998ecf8427e"`.
    4. Memory allocated: **0 bytes of chunk storage**.

---

### Edge Case 3: Thundering Herd on Popular Images & Checkpoints
- **The Scenario:** A compute cluster triggers 1,000 concurrent agent sandboxes simultaneously, each requesting the exact same 500 MB base image (`GET /sandboxes/ubuntu-node20.raw`).
- **The Failure Risk:** 1,000 independent disk reads saturate NVMe controllers, queue depths overflow, and latency spikes from 10 ms to 15 seconds.
- **Contract (Single-Flight Request Coalescing):**
  1. The gateway tracks active in-flight `GET` requests by key hash in a small atomic lookup table.
  2. The first request leases the chunk slab and initiates NVMe DMA.
  3. The subsequent 999 requests subscribe to the **same in-memory slab**.
  4. Once the chunk arrives in RAM, the completion reactor issues parallel kernel socket sends (`IORING_OP_SEND`) across all 1,000 sockets from the **single in-memory buffer**.
  5. Disk reads: **1 (not 1,000)**. NVMe saturation is eliminated.

---

### Edge Case 4: Key Path Traversal & Sanitization
- **The Scenario:** A malicious client attempts an escape attack: `GET /bucket/../../../../etc/shadow` or unicode look-alikes.
- **Contract:**
  1. Keys are treated strictly as **flat binary byte arrays**, not operating system filesystem paths.
  2. The storage engine does not create nested directories on the host filesystem for keys; it stores chunks in preallocated container block files indexed by 64-bit integer hashes (`key_hash`).
  3. The path `../../etc/shadow` is simply hashed into a 64-bit integer. It cannot escape the storage namespace because host filesystem paths are never used as targets.

---

### Edge Case 5: Out of Disk Space (`ENOSPC`) & Over-Quota Hard Stops
- **The Scenario:** The physical NVMe storage pool reaches 99.8% capacity during a high-speed batch upload.
- **Contract (Pre-Reserved Safety Headroom):**
  1. The storage engine reserves **5% of disk capacity** as emergency system headroom.
  2. If free space drops below 5%, incoming `PUT` requests are immediately rejected before accepting body bytes:
     ```http
     HTTP/1.1 507 Insufficient Storage
     Content-Type: application/xml

     <Error>
       <Code>InsufficientStorage</Code>
       <Message>Storage node has exceeded safety headroom allocation.</Message>
     </Error>
     ```
  3. `GET`, `HEAD`, and `DELETE` requests continue operating normally, allowing users or automated lifecycle policies to delete objects and free space.

---

## 3. S3 XML Error Code Specification Matrix

All error responses are emitted in standard Amazon S3 XML format:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Key>snapshots/agent_42.raw</Key>
  <RequestId>AWAN018B72F9A3</RequestId>
  <HostId>node-01.storage.internal</HostId>
</Error>
```

| HTTP Status | S3 Error Code | Cause / Trigger |
| :---: | :--- | :--- |
| **`404 Not Found`** | `NoSuchKey` | Object key not found in `@flat ObjectMeta` or marked with tombstone flag. |
| **`404 Not Found`** | `NoSuchBucket` | Bucket partition ring has not been initialized. |
| **`412 Precondition Failed`** | `PreconditionFailed` | `If-Match` ETag mismatch or `If-None-Match: *` collision. |
| **`304 Not Modified`** | *(Empty Body)* | `If-None-Match` ETag matches (client cache is fresh). |
| **`416 Range Not Satisfiable`**| `InvalidRange` | Requested byte offset exceeds `ObjectMeta.size`. |
| **`403 Forbidden`** | `AccessDenied` | Invalid credentials or authorization policy rejection. |
| **`403 Forbidden`** | `SignatureDoesNotMatch` | Hardware HMAC-SHA256 signature verification failed. |
| **`403 Forbidden`** | `RequestTimeTooSkewed` | `X-Amz-Date` deviates from cluster time by $> 900\text{ seconds}$. |
| **`400 Bad Request`** | `InvalidDigest` | Content-MD5 / SHA-256 header does not match computed chunk hash. |
| **`400 Bad Request`** | `EntityTooLarge` | Payload exceeds maximum allowed single object size (5 TB). |
| **`507 Insufficient Storage`** | `LicenseCapacityExceeded` | Commercial license capacity quota exceeded ([`LICENSING_AND_COMMERCIAL.md`](LICENSING_AND_COMMERCIAL.md)). |
| **`503 Slow Down`** | `SlowDown` | Leased slab pool temporarily exhausted under peak burst. |
