# Awan Storage REST & S3 API Specification

> **Wire Protocol: S3 REST Compatibility, Streaming Framing, and Endpoint Models**

---

## 1. Scope & Compatibility Targets

Awan Storage implements the **Amazon S3 REST API (2006-03-01)** specification subset required for high-throughput blob storage, container registries, and AI checkpoint pipelines. Any standard tool (`aws-cli`, `rclone`, `boto3`, `@aws-sdk/client-s3`) can interact with Awan Storage without modification by pointing `--endpoint-url` to the Awan gateway.

---

## 2. Core S3 Endpoint Surface

### A. Object Operations

| HTTP Verb | Path | Description | Wire Characteristics |
| :--- | :--- | :--- | :--- |
| **`PUT`** | `/{bucket}/{key}` | Upload object | Direct streaming into preallocated `@arena` slot. Zero chunk duplication. |
| **`GET`** | `/{bucket}/{key}` | Download object | Zero-copy kernel handoff to socket. Supports `Range: bytes=start-end`. |
| **`HEAD`** | `/{bucket}/{key}` | Fetch metadata | Microsecond lookup from `@flat ObjectMeta` table (< 1 µs). |
| **`DELETE`**| `/{bucket}/{key}` | Remove object | Single-cycle tombstone bit flip. Compaction runs asynchronously. |

### B. Bucket & Listing Operations

| HTTP Verb | Path | Description | Implementation Details |
| :--- | :--- | :--- | :--- |
| **`GET`** | `/{bucket}?list-type=2` | `ListObjectsV2` | Vectorized prefix scan over `key_hash` array. Up to 1,000 items returned. Supports `prefix`, `delimiter`, `max-keys`, and `continuation-token`. |
| **`PUT`** | `/{bucket}` | Create bucket | Allocates fresh partition ring and metadata columnar slabs. |
| **`DELETE`**| `/{bucket}` | Delete bucket | Reclaims bucket slabs if empty. |

### C. Multipart Upload Operations (For Large Payloads > 5 MB)

| HTTP Verb | Query Parameters | Description |
| :--- | :--- | :--- |
| **`POST`** | `/{bucket}/{key}?uploads` | Initiate multipart upload. Returns `UploadId`. |
| **`PUT`** | `/{bucket}/{key}?partNumber={N}&uploadId={ID}` | Upload individual part into leased slab. |
| **`POST`** | `/{bucket}/{key}?uploadId={ID}` | Complete multipart upload. Links part offsets into single object record. |
| **`DELETE`**| `/{bucket}/{key}?uploadId={ID}` | Abort multipart upload. Releases leased part slabs. |

### D. Bucket Lifecycle Configuration & Automated Cleanup

| HTTP Verb | Query Parameters | Description | Implementation Details |
| :--- | :--- | :--- | :--- |
| **`PUT`** | `/{bucket}?lifecycle` | Set bucket lifecycle rules | Configures object TTL expiration and stale multipart purge windows. |
| **`GET`** | `/{bucket}?lifecycle` | Retrieve lifecycle rules | Returns current XML lifecycle configuration. |
| **`DELETE`**| `/{bucket}?lifecycle` | Remove lifecycle rules | Disables background automated expiration sweeps for the bucket. |

#### Lifecycle Rule Capabilities:
1. **Object Expiration (`Expiration.Days`):** Objects older than $N$ days are automatically marked with tombstone flags during async sweeps and their storage slabs reclaimed.
2. **Abort Incomplete Multipart Uploads (`AbortIncompleteMultipartUpload.DaysAfterInitiation`):** Uncompleted multipart parts older than $N$ days (e.g. 7 days) are automatically purged to prevent abandoned uploads from consuming disk space.

---

## 3. Pre-Signed URLs (Query-Based SigV4 Authentication)

Pre-signed URLs allow untrusted clients (e.g. browser frontends, AI agent sandboxes) to securely download or upload objects directly without embedding AWS API secret keys in the client.

### URL Structure:
```http
https://storage.internal:9443/agent-sandboxes/rootfs-base.tar.gz?
  X-Amz-Algorithm=AWS4-HMAC-SHA256&
  X-Amz-Credential=AWAN_KEY%2F20261110%2Fus-east-1%2Fs3%2Faws4_request&
  X-Amz-Date=20261110T040000Z&
  X-Amz-Expires=3600&
  X-Amz-SignedHeaders=host&
  X-Amz-Signature=8a9f3b...fe01
```

### Verification Pipeline:
1. **Clock Skew Check:** Rejects requests if `X-Amz-Date` deviates from cluster synchronized time by $> 15\text{ minutes}$ (`RequestTimeTooSkewed`).
2. **Expiry Validation:** Current time must be $\le \text{X-Amz-Date} + \text{X-Amz-Expires}$.
3. **Canonical Request Reconstruction:** Reconstructs the canonical query string in-place in memory without allocating string maps.
4. **Hardware HMAC Verification:** Evaluates the HMAC-SHA256 signature against the tenant secret key using hardware intrinsics.

---

## 4. Conditional Headers & Optimistic Concurrency Control

Essential for distributed locks, caching, and preventing concurrent overwrite races across agents:

| Header | Operator / Condition | Behavior on Match | Behavior on Mismatch |
| :--- | :--- | :--- | :--- |
| **`If-Match`** | `"{etag}"` | Proceed with `GET`/`PUT`/`DELETE`. | Aborts with **`HTTP 412 PreconditionFailed`**. |
| **`If-None-Match`** | `"*"` | Creates object **only if it does not already exist**. | Aborts with **`HTTP 412 PreconditionFailed`** (Used for distributed mutex locks). |
| **`If-None-Match`** | `"{etag}"` | Aborts with **`HTTP 304 NotModified`** (Zero-byte body). | Returns updated payload with fresh `ETag`. |
| **`If-Modified-Since`**| `<timestamp>` | Returns `HTTP 200` with payload if object is newer. | Returns **`HTTP 304 NotModified`**. |
| **`If-Unmodified-Since`**| `<timestamp>` | Proceed if object has not changed since timestamp. | Aborts with **`HTTP 412 PreconditionFailed`**. |

---

## 5. Server-Side Object Copy (`x-amz-copy-source`)

Enables atomic renames and cloning of snapshots without transmitting bytes over the network:

```http
PUT /agent-sandboxes/instance-102.snapshot HTTP/1.1
Host: storage.internal:9000
x-amz-copy-source: /agent-sandboxes/base-template.snapshot
```

- **Execution Contract:**
  1. Locates `base-template.snapshot` in the `@flat ObjectMeta` table in $< 1\text{ µs}$.
  2. Creates a new record in `@flat ObjectMeta` pointing to the **same underlying chunk slab offsets**, incrementing the chunk reference count.
  3. Returns `HTTP 200 OK` with `<CopyObjectResult>` in $< 0.1\text{ ms}$ (true zero-copy deduplication).

---

## 6. Byte-Range Requests & Partial Downloads (`Range: bytes=start-end`)

Required for PyTorch SafeTensor checkpoint loading, video seeking, and resume downloads:

- **Single Range Request:** `Range: bytes=1048576-2097151` (Requests the second 1 MB).
  - Returns **`HTTP 206 Partial Content`**.
  - Response Header: `Content-Range: bytes 1048576-2097151/73400320000`.
  - Kernel DMA streams directly from offset `1048576` using `io_uring` fixed buffer slicing with **0 bytes of intermediate userland memory allocation**.
- **Invalid Range:** `Range: bytes=999999999999-` (Past EOF).
  - Returns **`HTTP 416 Range Not Satisfiable`** with `Content-Range: bytes */{total_size}`.

---

## 7. Custom Metadata & Storage Headers

| Header | Storage Target | Description |
| :--- | :--- | :--- |
| `Content-Type` | `ObjectMeta.content_type_idx` | Interned MIME string (e.g. `application/octet-stream`, `image/png`). |
| `Content-Disposition` | `ObjectMeta.disposition_idx` | File attachment directives. |
| `Cache-Control` | `ObjectMeta.cache_control_idx` | Caching directives for reverse proxies and browsers. |
| `x-amz-meta-*` | Secondary Metadata Slab | Key-value pairs stored in a dedicated auxiliary string slab. |

---

## 8. Streaming Framing & Zero-Allocation Parser

```
Request Stream Buffer:
[ "PUT /models/llama-70b.bin HTTP/1.1\r\nHost: s3.internal\r\nContent-Length: 73400320000\r\n..." ]
    │        │                                                │
    ▼        ▼                                                ▼
  Verb:    Path Slice:                                     Length:
  OP_PUT   offset: 5, len: 26                              73,400,320,000 (u64 parsed in-place)
```

- **Zero String Allocations:** Header keys and values remain in-place slices of the request slab.
- **Direct Body Routing:** The payload boundary is calculated directly from `Content-Length` or chunked transfer encoding, and DMA begins streaming directly to disk blocks.
- **Templated XML Responses:** Emitted via precompiled byte slices, reducing XML serialization latency to **under 0.2 ms**.
