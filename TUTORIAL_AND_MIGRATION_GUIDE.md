# Awan Storage (`awan-storage-server`): Drop-In S3 Replacement Tutorial & Migration Guide

`awan-storage-server` is a high-performance, zero-GC, wire-compatible Amazon S3 object storage server built on Awan's 64-byte `@flat ObjectMeta` Struct-of-Arrays (SoA) columnar index and 4096-byte-aligned WORM Needle volume slabs (`O_DIRECT` / `FILE_FLAG_NO_BUFFERING`).

Because `awan-storage-server` speaks the standard AWS S3 REST XML protocol (RFC 9110 byte ranges, Multipart Uploads, AWS Signature Version 4 `AWS4-HMAC-SHA256`, `aws-chunked` streaming, conditional `If-Match` / `If-None-Match: *` mutex headers, and lifecycle expiration rules), **you do not need to rewrite your application logic or install a proprietary SDK**. Any S3 client library or CLI tool can switch to `awan-storage-server` by changing **one configuration line**: `endpoint_url`.

---

## Table of Contents

1. [Quickstart: Starting `awan-storage-server` in 30 Seconds](#1-quickstart-starting-awan-storage-server-in-30-seconds)
2. [How to Replace MinIO / AWS S3 in Your Code (By Language)](#2-how-to-replace-minio--aws-s3-in-your-code-by-language)
   - [Python (`boto3`, `s3fs`, Polars, DuckDB, PyTorch Checkpoints)](#21-python-boto3-s3fs-polars-duckdb-pytorch)
   - [JavaScript / TypeScript / Node.js / Bun (`@aws-sdk/client-s3`)](#22-javascript--typescript--nodejs--bun)
   - [Go (`aws-sdk-go-v2` & `minio-go`)](#23-go-aws-sdk-go-v2--minio-go)
   - [Rust (`aws-sdk-s3` & Apache Arrow `object_store`)](#24-rust-aws-sdk-s3--apache-arrow-object_store)
3. [CLI Tools (`aws s3`, `rclone`, `s5cmd`)](#3-cli-tools-aws-s3-rclone-s5cmd)
4. [Replacing MinIO in Docker Compose & Production (`systemd`)](#4-replacing-minio-in-docker-compose--production-systemd)
5. [Zero-Downtime Migration from MinIO / Ceph / AWS S3](#5-zero-downtime-migration-from-minio--ceph--aws-s3)
6. [Supported S3 Operations & Advanced Features](#6-supported-s3-operations--advanced-features)
7. [Awan Language Syntax Tutorial (Current Idiomatic Syntax)](#7-awan-language-syntax-tutorial-current-idiomatic-syntax)
8. [Compiling Awan & Mixing Native `@flat` Kernels with Rust (C-ABI FFI)](#8-compiling-awan--mixing-native-flat-kernels-with-rust-c-abi-ffi)

---

## 1. Quickstart: Starting `awan-storage-server` in 30 Seconds

### Option A: Run the Prebuilt Release Binary
```bash
# Windows (PowerShell)
.\awan-storage\dist\awan-storage-server-x86_64-windows.exe --port 9000 --data-dir C:\awan-s3-data

# Linux (x86_64 / ARM64)
./awan-storage/dist/awan-storage-server-x86_64-linux-gnu --port 9000 --data-dir /var/lib/awan-s3
```

### Option B: Build from Source (`--release`)
```bash
cargo build --release --manifest-path awan-storage/server/Cargo.toml
./awan-storage/server/target/release/awan-storage-server --port 9000 --data-dir ./s3-data
```

### CLI Flags & Environment Variables

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--port <u16>`, `-p` | `AWAN_S3_PORT` | `9000` | TCP port for the HTTP/1.1 & HTTP/2 S3 wire reactor (`0.0.0.0:<port>`). |
| `--data-dir <path>`, `-d` | `AWAN_S3_DATA_DIR` | System temp dir | Directory where 4096B-aligned `volume_0001.dat` and `volume_0001.idx` WORM Needle slabs are persisted. |

---

## 2. How to Replace MinIO / AWS S3 in Your Code (By Language)

To migrate any existing S3 or MinIO codebase, keep all your `put_object`, `get_object`, `upload_file`, or multipart code identical and point the client's **endpoint** to `http://127.0.0.1:9000` (or your server's hostname).

### 2.1 Python (`boto3`, `s3fs`, Polars, DuckDB, PyTorch)

#### Standard `boto3` Client
```python
import boto3
from botocore.config import Config

# Drop-in replacement: only `endpoint_url` changes from AWS S3 or MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="awanadmin",
    aws_secret_access_key="awansecret",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
)

# 1. Create a bucket
s3.create_bucket(Bucket="ai-checkpoints")

# 2. Upload an object (Standard PUT or automatic Multipart for large files)
s3.put_object(Bucket="ai-checkpoints", Key="models/resnet50.pt", Body=b"...model weights...")

# 3. Byte-Range Partial Read (RFC 9110 — e.g. reading Parquet footer or tensor shard)
resp = s3.get_object(Bucket="ai-checkpoints", Key="models/resnet50.pt", Range="bytes=0-1023")
header_chunk = resp["Body"].read()

# 4. Distributed Atomic Lock using S3 Conditional Put (`IfNoneMatch="*"`)
# Succeeds with 200 OK if lock does not exist; raises 412 PreconditionFailed if held by another worker
s3.put_object(
    Bucket="ai-checkpoints",
    Key="locks/training-job-42.lock",
    Body=b"worker-node-01",
    IfNoneMatch="*",
)
```

#### Polars & Pandas (`s3fs` / `fsspec`)
```python
import polars as pl

storage_options = {
    "aws_endpoint_url": "http://127.0.0.1:9000",
    "aws_access_key_id": "awanadmin",
    "aws_secret_access_key": "awansecret",
    "aws_region": "us-east-1",
    "aws_allow_http": "true",
}

# Polars pushes predicate & byte-range reads directly to awan-storage-server's RFC 9110 Range engine
df = pl.read_parquet("s3://ai-checkpoints/datasets/events.parquet", storage_options=storage_options)
```

#### DuckDB Direct S3 Querying
```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint='127.0.0.1:9000';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET s3_access_key_id='awanadmin';
SET s3_secret_access_key='awansecret';

SELECT count(*), avg(latency_ms)
FROM read_parquet('s3://ai-checkpoints/datasets/*.parquet');
```

---

### 2.2 JavaScript / TypeScript / Node.js / Bun

#### AWS SDK for JavaScript v3 (`@aws-sdk/client-s3`)
```typescript
import {
  S3Client,
  CreateBucketCommand,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";

const s3 = new S3Client({
  endpoint: "http://127.0.0.1:9000",
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "awanadmin",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "awansecret",
  },
  forcePathStyle: true, // Required for IP / localhost endpoints
});

await s3.send(new CreateBucketCommand({ Bucket: "app-assets" }));

await s3.send(
  new PutObjectCommand({
    Bucket: "app-assets",
    Key: "avatars/user_101.png",
    Body: Buffer.from("...binary payload..."),
  })
);

const out = await s3.send(
  new GetObjectCommand({
    Bucket: "app-assets",
    Key: "avatars/user_101.png",
  })
);
const bytes = await out.Body?.transformToByteArray();
```

#### Bun Native `S3Client` (Zero-Dependency)
```typescript
import { S3Client } from "bun";

const client = new S3Client({
  endpoint: "http://127.0.0.1:9000",
  bucket: "app-assets",
  accessKeyId: "awanadmin",
  secretAccessKey: "awansecret",
});

const file = client.file("config/settings.json");
await file.write(JSON.stringify({ theme: "dark", version: 2 }));
const data = await file.json();
```

---

### 2.3 Go (`aws-sdk-go-v2` & `minio-go`)

#### Using `minio-go/v7` (Drop-In Replacement for MinIO Server)
```go
package main

import (
	"bytes"
	"context"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	ctx := context.Background()

	// Point your existing minio-go client directly at awan-storage-server
	client, err := minio.New("127.0.0.1:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("awanadmin", "awansecret", ""),
		Secure: false,
	})
	if err != nil {
		log.Fatal(err)
	}

	_ = client.MakeBucket(ctx, "telemetry", minio.MakeBucketOptions{Region: "us-east-1"})

	payload := []byte("awan-storage needle payload")
	_, err = client.PutObject(
		ctx,
		"telemetry",
		"2026-11-10/batch-001.bin",
		bytes.NewReader(payload),
		int64(len(payload)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

---

### 2.4 Rust (`aws-sdk-s3` & Apache Arrow `object_store`)

```rust
use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = AmazonS3Builder::new()
        .with_endpoint("http://127.0.0.1:9000")
        .with_bucket_name("ai-checkpoints")
        .with_region("us-east-1")
        .with_access_key_id("awanadmin")
        .with_secret_access_key("awansecret")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;

    let path = Path::from("checkpoints/epoch_10.safetensors");
    store.put(&path, Bytes::from_static(b"tensor-data").into()).await?;

    let result = store.get(&path).await?;
    let bytes = result.bytes().await?;
    assert_eq!(bytes.as_ref(), b"tensor-data");
    Ok(())
}
```

---

## 3. CLI Tools (`aws s3`, `rclone`, `s5cmd`)

### AWS CLI (`aws s3` / `aws s3api`)
```bash
export AWS_ACCESS_KEY_ID=awanadmin
export AWS_SECRET_ACCESS_KEY=awansecret
export AWS_DEFAULT_REGION=us-east-1

# Create a bucket
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://my-bucket

# Upload a directory recursively
aws --endpoint-url http://127.0.0.1:9000 s3 sync ./local-dataset s3://my-bucket/dataset

# List objects
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://my-bucket/dataset/
```

### `s5cmd` (High-Throughput Parallel CLI)
```bash
s5cmd --endpoint-url http://127.0.0.1:9000 cp ./checkpoints/*.pt s3://my-bucket/checkpoints/
```

---

## 4. Replacing MinIO in Docker Compose & Production (`systemd`)

### Before vs. After in `docker-compose.yml`

Replace your `minio/minio` service container with `awan-storage-server` on the exact same port (`9000`) and volume mount:

```yaml
services:
  s3-storage:
    # Replaces: image: minio/minio:latest
    build:
      context: ./awan-storage/server
    command: ["awan-storage-server", "--port", "9000", "--data-dir", "/data"]
    ports:
      - "9000:9000"
    environment:
      - AWAN_S3_PORT=9000
      - AWAN_S3_DATA_DIR=/data
    volumes:
      - s3_volume_data:/data
    restart: unless-stopped

volumes:
  s3_volume_data:
```

### Linux Production `systemd` Service (`/etc/systemd/system/awan-storage.service`)
```ini
[Unit]
Description=Awan Storage High-Performance S3 Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=awan
Group=awan
ExecStart=/usr/local/bin/awan-storage-server --port 9000 --data-dir /var/lib/awan-storage
Restart=on-failure
RestartSec=2s
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

---

## 5. Zero-Downtime Migration from MinIO / Ceph / AWS S3

Note that MinIO archived its open-source server binaries on `dl.min.io` (now returning `HTTP 410 Gone`). You can migrate existing buckets from MinIO, Ceph RGW, Garage, or AWS S3 into `awan-storage-server` with `rclone`:

1. **Configure `rclone.conf` with both your legacy S3/MinIO server and `awan-storage`:**
   ```ini
   [legacy-minio]
   type = s3
   provider = Minio
   access_key_id = minioadmin
   secret_access_key = minioadmin
   endpoint = http://10.0.0.10:9000

   [awan-s3]
   type = s3
   provider = Other
   access_key_id = awanadmin
   secret_access_key = awansecret
   endpoint = http://127.0.0.1:9000
   force_path_style = true
   ```

2. **Run initial background bulk sync (while your app is still running):**
   ```bash
   rclone sync legacy-minio:my-production-bucket awan-s3:my-production-bucket --transfers 32 --checkers 64 -P
   ```

3. **Cut over application `S3_ENDPOINT_URL` to `awan-storage-server` and run a final delta pass:**
   ```bash
   rclone sync legacy-minio:my-production-bucket awan-s3:my-production-bucket --checksum
   ```

---

## 6. Supported S3 Operations & Advanced Features

| Category | S3 REST Operations | Awan Architectural Acceleration |
| :--- | :--- | :--- |
| **Bucket Operations** | `CreateBucket` (`PUT /{bucket}`), `DeleteBucket` (`DELETE /{bucket}`), `HeadBucket`, `ListBuckets` (`GET /`), `GetBucketLocation` (`?location`) | Instant $O(1)$ bucket ID resolution up to 65,536 buckets. |
| **Object CRUD** | `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `CopyObject` (`x-amz-copy-source`), `DeleteObjects` (`POST /?delete` batch up to 1,000 keys) | 64B `@flat ObjectMeta` SoA index + 4096B WORM Needle disk slabs with hardware Castagnoli CRC32C verification. `CopyObject` uses zero-copy `AWAN_FLAG_SHARED_REF` pointer cloning. |
| **Listing & Prefix Scan** | `ListObjectsV2` (`GET /{bucket}?list-type=2&prefix=...&start-after=...&max-keys=...`) | Contiguous key arena scan (1,000,000 objects scanned in **1.0 ms**). |
| **RFC 9110 Byte Ranges** | `Range: bytes=start-end`, `bytes=start-`, `bytes=-suffix` (`206 Partial Content`, `416 Range Not Satisfiable`) | Clamped zero-copy slice extraction; cross-part multipart extent stitching. |
| **Multipart Uploads** | `CreateMultipartUpload` (`?uploads`), `UploadPart` (`?partNumber=N&uploadId=ID`), `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads` | Out-of-order concurrent part staging, composite MD5 ETag (`"<md5>-<parts>"`), `EntityTooSmall` & `InvalidPartOrder` validation. |
| **Concurrency & Locks** | `If-None-Match: *` (atomic distributed mutex creation), `If-Match: "<etag>"`, Single-Flight hot-key coalescing, MVCC snapshot reads | Prevents thundering-herd I/O amplification when 1,000+ workers read the same model checkpoint simultaneously; concurrent `GET`s never observe half-written `PUT`s. |
| **Auth & Framing** | AWS Signature V4 (`AWS4-HMAC-SHA256` Header & Pre-Signed URLs), `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (`aws-chunked`) | Thread-local `SigV4KeyCache` caches the 4-step HMAC-SHA256 signing key derivation (`kDate -> kRegion -> kService -> kSigning`). |
| **Lifecycle Management** | `PutBucketLifecycleConfiguration` (`PUT /?lifecycle`), `GetBucketLifecycleConfiguration`, `DeleteBucketLifecycleConfiguration` | Prefix-scoped object expiration and stale multipart upload abort sweeps. |

---

## 7. Awan Language Syntax Tutorial (Current Idiomatic Syntax)

The [`awan-storage/dist/`](dist/) directory ships prebuilt release binaries for both the S3 server (`awan-storage-server-*`) and the **Awan Compiler & Toolchain (`awan-compiler-*`, `awan-aot-*`, `awan-ld-*`)** so you can write, test, and ahead-of-time (AOT) compile `.awan` programs directly.

### 7.1 Variables & Function-Confined Lexical Mutability (`let` vs `var`)
Awan enforces **function-confined lexical mutability**:
- `let`: Immutable binding (cannot be reassigned).
- `var`: Locally mutable variable confined to the current function or block.
- **Function parameters are always immutable (`let`)**: to mutate a parameter value in a loop, copy it into a local `var`.

```awan
fn sum_range(start_in: Int, end_in: Int) -> Int {
    let step = 1;           // immutable constant
    var current = start_in; // mutable local copy
    var total = 0;

    while (current <= end_in) {
        total += current;
        current += step;
    }
    return total;
}
```

### 7.2 Idiomatic Booleans, Built-ins, and Entrypoints
- **Entrypoint**: Always define `fn main() { ... }` and invoke `main();` at the bottom of the file.
- **Negation**: Use `!cond` (never `cond == false`).
- **Preallocated Arrays (No $O(N^2)$ Concatenation in Loops)**: Preallocate contiguous arrays with repeat literals (`[0; n]` or `[0.0; n]`) and mutate by index (`arr[i] = val`).
- **Maps & Built-ins**: Mutate map properties in place (`m["k"] = v`) and use native `keys(map)`, `values(map)`, `len(xs)`, `to_string(x)`, `clock_ms()`, and math built-ins (`sin`, `cos`, `sqrt`, `abs`, `pow`, `floor`, `ceil`, `round`).

```awan
fn main() {
    let n = 1000;
    var scores = [0; 1000]; // O(1) contiguous preallocation
    var i = 0;
    while (i < n) {
        scores[i] = i * 2;
        i += 1;
    }

    var metrics = {"count": n, "last": scores[n - 1]};
    let k = keys(metrics);
    if (!is_empty(k)) {
        println("Keys: " + to_string(len(k)) + ", last=" + to_string(metrics["last"]));
    }
    return 0;
}

main();
```

### 7.3 Columnar Struct-of-Arrays Lowering (`@flat` and `@arena`)
Instead of manually splitting fields into separate primitive arrays, annotate standard `struct` definitions with `@flat` (and `@arena` for arena-backed lifetime slabs). The Awan compiler automatically lowers `Array(ObjectMeta, n)` into cache-line-aligned **Struct-of-Arrays (SoA)** columns for SIMD vectorization and zero-copy FFI handoff:

```awan
// From awan-storage/engine/storage_core.awan
@flat
struct ObjectMeta {
    key_hash: Int,      // 64-bit keyed XXH3 hash of (bucket_id, key_bytes)
    bucket_flags: Int,  // [63:32] bucket_id | [31:16] part_count | [15:0] status flags
    vol_offset: Int,    // [63:48] volume_id | [47:0] 4096B-aligned volume offset
    size: Int,          // Payload size in bytes
    etag_hi: Int,       // Upper 64 bits of 128-bit MD5 ETag
    etag_lo: Int,       // Lower 64 bits of 128-bit MD5 ETag
    created_at: Int,    // Unix timestamp (ms)
    key_slice: Int      // [63:16] Key String Arena offset | [15:0] key byte length
}

fn make_bucket_flags(bucket_id: Int, part_count: Int, flags: Int) -> Int {
    return (bucket_id * 4294967296) + (part_count * 65536) + flags;
}

fn main() {
    let capacity = 100000;
    let t0 = clock_ms();
    var index = Array(ObjectMeta, capacity); // Lowered automatically to 8 contiguous u64 columns

    var i = 0;
    while (i < capacity) {
        index[i] = ObjectMeta {
            key_hash: i * 2654435761,
            bucket_flags: make_bucket_flags(1, 1, 1),
            vol_offset: 4096 + (i * 4096),
            size: 65536,
            etag_hi: 123456789,
            etag_lo: 987654321,
            created_at: 1762732800000,
            key_slice: (i * 64) * 65536 + 32
        };
        i += 1;
    }
    let elapsed = clock_ms() - t0;
    println("Indexed " + to_string(capacity) + " objects in " + to_string(elapsed) + " ms");
    return 0;
}

main();
```

### 7.4 Lazy Fused Functional Pipelines (`|>`)
Awan supports lazy pipeline stages (`filter`, `map`, `take`, `sum`) that fuse into single-pass loops without intermediate array allocations:

```awan
let evens_sum = [1, 2, 3, 4, 5, 6, 7, 8]
    |> filter(x => (x % 2) == 0)
    |> map(x => x * 10)
    |> sum;
// evens_sum == 200
```

### 7.5 Built-In First-Class `test` Blocks (`awan test`)
Write unit tests directly in `.test.awan` files using `test "..." { ... }` and `assert_eq(a, b)`:

```awan
// Run with: ./awan-storage/dist/awan-compiler-x86_64-windows.exe test awan-storage/tests/meta_index.test.awan
test "bucket_flags bitpacking and unpacking" {
    let packed = make_bucket_flags(7, 25, 3);
    assert_eq(get_bucket_id(packed), 7);
    assert_eq(get_part_count(packed), 25);
    assert_eq(get_flags(packed), 3);
}
```

---

## 8. Compiling Awan & Mixing Native `@flat` Kernels with Rust (C-ABI FFI)

### 8.1 Bundled Compiler & Server Binaries (`awan-storage/dist/`)

| Binary Artifact | Target OS / Arch | Role |
| :--- | :--- | :--- |
| `awan-compiler-x86_64-windows.exe` | Windows `x86_64` (`msvc`) | Full Awan Compiler CLI (`check`, `run`, `test`, `build`, `emit-llvm`, `lsp`, `mcp`) |
| `awan-aot-x86_64-windows.exe` | Windows `x86_64` (`msvc`) | Awan AOT Standalone Builder & Sysroot Manager |
| `awan-ld-x86_64-windows.exe` | Windows `x86_64` (`msvc`) | `AwanLink` Native Object Linker |
| `awan-storage-server-x86_64-windows.exe` | Windows `x86_64` (`msvc`) | Drop-in S3-Compatible Storage Server |
| `awan-compiler-x86_64-linux-gnu` | Linux `x86_64` (`glibc`) | Built via `.github/workflows/awan-storage-ci.yml` (`release-matrix-linux`) |
| `awan-compiler-aarch64-linux-gnu` | Linux `ARM64` (`aarch64`) | Built via `.github/workflows/awan-storage-ci.yml` (`release-matrix-linux`) |
| `awan-storage-server-x86_64-linux-gnu` | Linux `x86_64` (`glibc`) | Built via `.github/workflows/awan-storage-ci.yml` (`release-matrix-linux`) |
| `awan-storage-server-x86_64-linux-musl` | Linux `x86_64` (`musl` static) | Built via `.github/workflows/awan-storage-ci.yml` (`release-matrix-linux`) |
| `awan-storage-server-aarch64-linux-gnu` | Linux `ARM64` (`aarch64`) | Built via `.github/workflows/awan-storage-ci.yml` (`release-matrix-linux`) |

To rebuild the entire distribution matrix locally:
```bash
python awan-storage/scripts/build_release_matrix.py
```

### 8.2 Compiling `.awan` Programs (VM, Native Executable, and C-ABI Shared Library)

```bash
# 1. Static type & semantic check
./awan-storage/dist/awan-compiler-x86_64-windows.exe check awan-storage/engine/storage_core.awan

# 2. Run unit test suite (.test.awan)
./awan-storage/dist/awan-compiler-x86_64-windows.exe test awan-storage/tests/meta_index.test.awan

# 3. Execute script directly in the JIT/VM engine
./awan-storage/dist/awan-compiler-x86_64-windows.exe run benchmarks/object_storage_poc/storage_engine.awan

# 4. Compile to a standalone native executable via the LLVM backend
./awan-storage/dist/awan-compiler-x86_64-windows.exe emit-llvm benchmarks/object_storage_poc/storage_engine.awan -o storage_bench.exe --target-cpu x86-64-v3

# 5. Compile an Awan module as a C-ABI shared library (.dll / .so / .dylib) for Rust FFI
./awan-storage/dist/awan-compiler-x86_64-windows.exe build awan-storage/engine/storage_core.awan --release --shared -o awan-storage/dist/
```

### 8.3 How Awan and Rust Mix Across the Zero-Copy C-ABI Boundary
`awan-storage` uses the **Substrate / Engine Hybrid Pattern**:
- **Rust (`awan-storage/server/`)** handles async network I/O (`hyper` / `tokio`), thread-per-core affinity pinning, and `O_DIRECT` disk descriptors.
- **Awan (`awan-storage/engine/storage_core.awan` & `include/awan_storage.h`)** defines the 64-byte cache-line-aligned `@flat ObjectMeta` Struct-of-Arrays (SoA) layout and C-ABI contract.

#### Step 1: Shared C Header (`awan-storage/server/include/awan_storage.h`)
```c
// 64-byte cache-line-aligned metadata record (matches @flat struct ObjectMeta)
typedef struct {
    uint64_t key_hash;      // 64-bit keyed XXH3 of (bucket_id, key)
    uint64_t bucket_flags;  // [63:32] bucket_id | [31:16] part_count | [15:0] flags
    uint64_t vol_offset;    // [63:48] volume_id | [47:0] 4KB-aligned offset
    uint64_t size;          // Decoded payload byte length
    uint64_t etag_hi;       // Upper 64 bits of 128-bit MD5 digest
    uint64_t etag_lo;       // Lower 64 bits of 128-bit MD5 digest
    uint64_t created_at;    // Unix epoch timestamp (ms)
    uint64_t key_slice;     // [63:16] Key String Arena offset | [15:0] key_len
} AwanObjectMeta;
```

#### Step 2: Rust `#[repr(C)]` & Panic-Safe `extern "C"` Bridge (`awan-storage/server/src/ffi_bridge.rs`)
In Rust, `AwanObjectMeta` is marked `#[repr(C)]` with a compile-time size assertion (`assert!(std::mem::size_of::<AwanObjectMeta>() == 64)`), and every `#[no_mangle] pub extern "C" fn` wraps its body in `std::panic::catch_unwind` so panics never unwind across the FFI boundary (`AWAN_ERR_PANIC = -999`):

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct AwanObjectMeta {
    pub key_hash: u64,
    pub bucket_flags: u64,
    pub vol_offset: u64,
    pub size: u64,
    pub etag_hi: u64,
    pub etag_lo: u64,
    pub created_at: u64,
    pub key_slice: u64,
}

const _: () = assert!(std::mem::size_of::<AwanObjectMeta>() == 64);

#[no_mangle]
pub extern "C" fn awan_storage_index_put(
    engine: *mut std::ffi::c_void,
    bucket_id: u32,
    key_ptr: *const u8,
    key_len: u16,
    vol_offset: u64,
    size: u64,
    etag_hi: u64,
    etag_lo: u64,
    part_count: u16,
    flags: u16,
) -> i32 {
    std::panic::catch_unwind(|| {
        if engine.is_null() || (key_ptr.is_null() && key_len > 0) {
            return AWAN_ERR_INVALID_ARG;
        }
        let eng = unsafe { &mut *(engine as *mut AwanStorageEngine) };
        let key_slice = unsafe { std::slice::from_raw_parts(key_ptr, key_len as usize) };
        match eng.put_object(bucket_id, key_slice, vol_offset, size, etag_hi, etag_lo, part_count, flags) {
            Ok(()) => AWAN_OK,
            Err(code) => code,
        }
    })
    .unwrap_or(AWAN_ERR_PANIC)
}
```

#### Step 3: Linking or Loading in Your Own Rust Project
You can link Awan's compiled C-ABI library either:
1. **Statically / via `build.rs`** (`println!("cargo:rustc-link-lib=dylib=awan_storage");`), or
2. **Dynamically at runtime via `libloading`** (the same mechanism `awan-core` uses for `compiler_lib.dll` / `libcompiler_lib.so`):
```rust
use libloading::{Library, Symbol};

unsafe {
    let lib = Library::new("awan-storage/dist/storage_core.dll")?;
    let init_fn: Symbol<unsafe extern "C" fn(u64, u64, u64) -> *mut std::ffi::c_void> =
        lib.get(b"awan_storage_init")?;
    let engine_ptr = init_fn(100_000, 4_194_304, 65536);
}
```

