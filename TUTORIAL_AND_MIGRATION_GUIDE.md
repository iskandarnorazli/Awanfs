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
