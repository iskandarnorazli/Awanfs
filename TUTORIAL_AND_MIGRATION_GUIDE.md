# S3 Storage Server (`awanfs`): Tutorial & Migration Guide

`awanfs` is a standalone, single-node S3-compatible object storage daemon in Rust ([`server/src/main.rs`](server/src/main.rs)). It stores payloads in 4096-byte sector-aligned format-3 WORM needle volumes (growing on write up to `32 GiB`, rolled over automatically), indexes hot rows in a 64-byte columnar `ObjectMeta` table with leveled [`SpillIndex`](server/src/spill.rs) disk segments (`meta/`), and stages multipart upload parts in `.staging/` files.

You can switch supported S3 tools (`aws` CLI, `boto3`, and S3 clients speaking the implemented subset in [`API_SPECIFICATION.md`](API_SPECIFICATION.md)) by pointing `endpoint_url` at `http://127.0.0.1:9000` (path-style by default) and supplying matching SigV4 credentials. Features outside that subset return `501 NotImplemented` or are tracked in [Not yet](#not-yet) and [`features.toml`](features.toml).

---

## Table of Contents

1. [Quickstart: Starting `awanfs` & Verifying with AWS CLI](#1-quickstart-starting-awanfs--verifying-with-aws-cli)
   - [CLI Flags & Environment Variables](#cli-flags--environment-variables)
2. [Using S3 SDKs (`boto3`, JS v3, Go v2, Rust)](#2-using-s3-sdks-boto3-js-v3-go-v2-rust)
3. [CLI Tools (`aws s3`, `s5cmd`, `rclone`)](#3-cli-tools-aws-s3-s5cmd-rclone)
4. [Docker Compose & Production (`systemd`)](#4-docker-compose--production-systemd)
5. [Migrating from MinIO / Ceph / AWS S3](#5-migrating-from-minio--ceph--aws-s3)
6. [Supported Operations Summary](#6-supported-operations-summary)
7. [Architecture Summary (`ObjectMeta`, Volumes, Staged Multipart, Memory Budget)](#7-architecture-summary-objectmeta-volumes-staged-multipart-memory-budget)
8. [Building from Source, Test Suite & Benchmark Tooling](#8-building-from-source-test-suite--benchmark-tooling)
9. [Not yet](#not-yet)

---

## 1. Quickstart: Starting `awanfs` & Verifying with AWS CLI

Build the release binary from the repository root:

```bash
cargo build --locked --release --manifest-path server/Cargo.toml
```

(Prebuilt archives for Windows `x86_64`, Linux `x86_64` glibc/musl, and macOS `aarch64` are built by [`.github/workflows/release.yml`](.github/workflows/release.yml) on releases/dispatch; the `dist/` directory in git tracks only `dist/SHA256SUMS.txt` unless [`scripts/build_release_matrix.py`](scripts/build_release_matrix.py) is run locally on Windows.)

Start `awanfs` with static root keys (`AWANFS_ACCESS_KEY` and `AWANFS_SECRET_KEY`). Running `awanfs` without credentials, `--config`, or `--allow-anonymous` exits with status code `2` ([`sigv4-auth`](features.toml#L1014), [`server/tests/auth_test.rs`](server/tests/auth_test.rs)):

```bash
AWANFS_ACCESS_KEY=s3admin AWANFS_SECRET_KEY=s3secret \
  ./server/target/release/awanfs --port 9000 --data-dir ./s3-data
```

On Windows PowerShell:

```powershell
$env:AWANFS_ACCESS_KEY="s3admin"; $env:AWANFS_SECRET_KEY="s3secret"
.\server\target\release\awanfs.exe --port 9000 --data-dir .\s3-data
```

In a second terminal, check [`health`](features.toml#L1060) readiness and run end-to-end [`buckets`](features.toml#L847) and [`objects`](features.toml#L930) operations with `aws` (verified continuously by `j_quickstart` in [`scripts/verify_drive.sh`](scripts/verify_drive.sh)):

```bash
curl -sf http://127.0.0.1:9000/_awanfs/health/ready
export AWS_ACCESS_KEY_ID=s3admin
export AWS_SECRET_ACCESS_KEY=s3secret
export AWS_DEFAULT_REGION=us-east-1

echo "hello awanfs" > hello.txt
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://quickstart-bkt
aws --endpoint-url http://127.0.0.1:9000 s3 cp hello.txt s3://quickstart-bkt/hello.txt
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://quickstart-bkt/
aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://quickstart-bkt/hello.txt hello.out
cmp -s hello.txt hello.out
aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://quickstart-bkt/hello.txt
aws --endpoint-url http://127.0.0.1:9000 s3 rb s3://quickstart-bkt
```

### CLI Flags & Environment Variables

All CLI options accepted by `awanfs` are defined in [`server/src/main.rs`](server/src/main.rs):

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-p, --port <PORT>` | `S3_PORT` / `AWAN_S3_PORT` | `9000` | Listen port for S3 HTTP/HTTPS traffic. |
| `--listen <ADDR>` | `AWANFS_LISTEN` | `127.0.0.1` | Bind IP address (loopback by default per `ARCHITECTURE.md` Decision 2). |
| `-d, --data-dir <DIR>` | `S3_DATA_DIR` / `AWAN_S3_DATA_DIR` | `<temp_dir>/s3_vol_<pid>_<id>` (`S3ServiceState::new()`) | Volume/manifest/segment directory (growing format-3 `volume_NNNN.dat` + `.idx`, `buckets.manifest`, `meta/`, `.staging/`). |
| `--durability <MODE>` | `AWANFS_DURABILITY` | `group` | Sync durability mode: `group` (default shared sync before 200), `fast` (~1s background flush), or `strict` (per-write sync) ([`restart`](features.toml#L988)). |
| `--cert <PATH>` | `AWANFS_TLS_CERT` | *(none)* | TLS certificate PEM path (must be paired with `--key`; disables plain HTTP unless `--http-port` is set) ([`tls`](features.toml#L1078)). |
| `--key <PATH>` | `AWANFS_TLS_KEY` | *(none)* | TLS private key PEM path (reloaded every 60s when files change and on `SIGHUP`). |
| `--http-port <PORT>` | *(none)* | *(none)* | Optional plain HTTP listener port when TLS (`--cert`/`--key`) is active. |
| `--compaction-rate <MB/S>` | `AWANFS_COMPACTION_RATE_MB` | `50` | Max background compaction copy throughput in MB/s (`0` = unlimited) ([`compaction`](features.toml#L1045)). |
| `--auto-compaction <on\|off>` | `AWANFS_AUTO_COMPACTION` | `on` | Background compaction pass (every `600s` on oldest volume with `>= 20%` dead space and `>= 1 GiB` for a lone volume). |
| `--otlp-endpoint <URL>` | `OTEL_EXPORTER_OTLP_ENDPOINT` | *(none)* | OpenTelemetry OTLP/HTTP trace export URL (`traceparent` header propagation) ([`otlp-tracing`](features.toml#L1102)). |
| `--access-log <PATH\|->` | `AWANFS_ACCESS_LOG` | *(off)* | Write S3 access log lines to a file or `-` for stdout (bounded 4,096-line queue; may drop under saturation) ([`access-log`](features.toml#L1086)). |
| `--config <PATH>` | `AWANFS_CONFIG` | *(none)* | TOML config for IAM users/policies ([`iam`](features.toml#L1029)) and per-bucket `zstd` compression (`[[buckets]]`) ([`compression`](features.toml#L1037)). |
| `--memory-budget <SIZE>` | `AWANFS_MEMORY_BUDGET` | `25%` of RAM/cgroup (`128 MiB..2 GiB`; `2 GiB` fallback) | Total heap cap split evenly between cache (`PayloadCache` + `<= 64 MiB` spill cache) and in-flight bodies ([`memory-budget`](features.toml#L1111)). |
| `--body-idle-timeout <DUR>` | *(none)* | `20s` | Idle deadline between consecutive request body frames (accepts `s`, `ms`, or seconds). Returns `408 RequestTimeout`. |
| `--region <NAME>` | `AWANFS_REGION` | `us-east-1` | Server region checked by `CreateBucket` `<LocationConstraint>` and returned by `?location`. |
| `--allow-anonymous` | `AWANFS_ALLOW_ANONYMOUS=1` | `false` | Permit unsigned requests. Refused with exit code `2` if `--config` defines any IAM users. |
| `--allow-small-parts` | *(none)* | `false` | Allow non-final multipart parts `< 5 MiB` (benchmark/test option). |
| `--lifecycle-day-seconds <N>` | *(none)* | `86400` | Seconds per day for internal timing/multipart expiry (test-use only; prints a warning if not `86400`). |
| `-h, --help` | *(none)* | — | Print CLI help and exit `0`. |

Additional environment variables read by the server:
- **Credentials:** `AWANFS_ACCESS_KEY` and `AWANFS_SECRET_KEY` (root SigV4 credential pair).
- **Request log:** `AWANFS_LOG=off` disables the per-request JSON line written asynchronously to stderr ([`request-log`](features.toml#L1094)).
- **Volume/I/O tuning:** `AWAN_SPARSE_VOLUME=1` (opt into legacy 32 GiB sparse volume files instead of growing volumes), `AWAN_VOLUME_GROWTH_STEP` (default `256 MiB`), `AWAN_READ_CHUNK` (default `1 MiB`), `AWAN_READ_AHEAD` (1..8, default `1`), `AWAN_DIRECT_READ_MIN` (off by default), `AWAN_DIRECT_IO=1`, and `MALLOC_MMAP_THRESHOLD_` (on Linux glibc, server pins `M_MMAP_THRESHOLD` to `128 KiB` unless overridden).

*(Note: `--sizes`, `--concurrent`, and `--duration` are arguments to [`benchmarks/run_warp_benchmark.py`](benchmarks/run_warp_benchmark.py), not `awanfs`.)*

---

## 2. Using S3 SDKs (`boto3`, JS v3, Go v2, Rust)

Start `awanfs` with matching credentials (`AWANFS_ACCESS_KEY=s3admin AWANFS_SECRET_KEY=s3secret`) before running any of the examples below. Note: among client SDKs, **`boto3`** (via [`server/tests/test_boto3_compat.py`](server/tests/test_boto3_compat.py) and `ceph/s3-tests`), **`aws` CLI**, **`reqwest`**, and **`warp put`** are continuously tested in CI; examples for JS/Go/Rust/Polars/DuckDB are standard S3 path-style configurations not run in CI.

### 2.1 Python (`boto3`, `s3fs`, Polars, DuckDB)

#### Standard `boto3` Client (Tested in CI)
```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="s3admin",
    aws_secret_access_key="s3secret",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
)

s3.create_bucket(Bucket="ai-checkpoints")
s3.put_object(Bucket="ai-checkpoints", Key="models/resnet50.pt", Body=b"...model weights v1...")
s3.put_object(Bucket="ai-checkpoints", Key="models/resnet50.pt", Body=b"...model weights v2...")

resp = s3.get_object(Bucket="ai-checkpoints", Key="models/resnet50.pt", Range="bytes=0-1023")
header_chunk = resp["Body"].read()

s3.delete_object(Bucket="ai-checkpoints", Key="models/resnet50.pt")
s3.delete_objects(
    Bucket="ai-checkpoints",
    Delete={"Objects": [{"Key": "k1"}, {"Key": "k2"}], "Quiet": False},
)

# Distributed atomic lock using If-None-Match: *
s3.put_object(
    Bucket="ai-checkpoints",
    Key="locks/training-job-42.lock",
    Body=b"worker-node-01",
    IfNoneMatch="*",
)
```

#### Polars & DuckDB (Standard S3 Path-Style Configuration; Not Run in CI)
```python
import polars as pl

storage_options = {
    "aws_endpoint_url": "http://127.0.0.1:9000",
    "aws_access_key_id": "s3admin",
    "aws_secret_access_key": "s3secret",
    "aws_region": "us-east-1",
    "aws_allow_http": "true",
}
df = pl.read_parquet("s3://ai-checkpoints/datasets/events.parquet", storage_options=storage_options)
```

```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint='127.0.0.1:9000';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET s3_access_key_id='s3admin';
SET s3_secret_access_key='s3secret';

SELECT count(*) FROM read_parquet('s3://ai-checkpoints/datasets/*.parquet');
```

### 2.2 JavaScript / TypeScript (`@aws-sdk/client-s3`; Not Run in CI)
```typescript
import { S3Client, CreateBucketCommand, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({
  endpoint: "http://127.0.0.1:9000",
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "s3admin",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "s3secret",
  },
  forcePathStyle: true,
});

await s3.send(new CreateBucketCommand({ Bucket: "app-assets" }));
await s3.send(new PutObjectCommand({ Bucket: "app-assets", Key: "logo.png", Body: Buffer.from("...") }));
const getResp = await s3.send(new GetObjectCommand({ Bucket: "app-assets", Key: "logo.png", Range: "bytes=0-511" }));
await s3.send(new DeleteObjectCommand({ Bucket: "app-assets", Key: "logo.png" }));
```

### 2.3 Go (`aws-sdk-go-v2`; Not Run in CI)
```go
cfg, _ := config.LoadDefaultConfig(context.TODO(),
	config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("s3admin", "s3secret", "")),
	config.WithRegion("us-east-1"),
)
client := s3.NewFromConfig(cfg, func(o *s3.Options) {
	o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
	o.UsePathStyle = true
})
_, _ = client.CreateBucket(context.TODO(), &s3.CreateBucketInput{Bucket: aws.String("data")})
_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
	Bucket: aws.String("data"),
	Key:    aws.String("log.txt"),
	Body:   strings.NewReader("hello world"),
})
```

### 2.4 Rust (`aws-sdk-s3`; Not Run in CI)
```rust
let config = Builder::new()
    .endpoint_url("http://127.0.0.1:9000")
    .region(Region::new("us-east-1"))
    .credentials_provider(Credentials::new("s3admin", "s3secret", None, None, "static"))
    .force_path_style(true)
    .build();
let client = Client::from_conf(config);
client.create_bucket().bucket("rust-bucket").send().await?;
client.put_object().bucket("rust-bucket").key("records.json").body(b"{}".to_vec().into()).send().await?;
```

---

## 3. CLI Tools (`aws s3`, `s5cmd`, `rclone`)

### AWS CLI (`aws s3` / `aws s3api`, Tested in CI)
```bash
export AWS_ACCESS_KEY_ID=s3admin
export AWS_SECRET_ACCESS_KEY=s3secret
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://my-bucket
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./dataset.tar.gz s3://my-bucket/
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://my-bucket/
aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://my-bucket/dataset.tar.gz
```

### `s5cmd` (Standard S3 Endpoint Override; Not Run in CI)
```bash
export AWS_ACCESS_KEY_ID="s3admin"
export AWS_SECRET_ACCESS_KEY="s3secret"

s5cmd --endpoint-url http://127.0.0.1:9000 cp ./local-folder/ s3://my-bucket/
```

---

## 4. Docker Compose & Production (`systemd`)

### Docker / Docker Compose (Matches [`Dockerfile`](Dockerfile) & [`.github/workflows/docker.yml`](.github/workflows/docker.yml))

`Dockerfile` compiles `awanfs`, sets `ENV AWANFS_LISTEN=0.0.0.0`, and invokes `ENTRYPOINT ["/usr/local/bin/awanfs", "--port", "9000", "--data-dir", "/data"]`:

```yaml
services:
  awanfs:
    build: .
    container_name: awanfs
    ports:
      - "9000:9000"
    environment:
      - AWANFS_ACCESS_KEY=s3admin
      - AWANFS_SECRET_KEY=s3secret
    volumes:
      - awanfs_data:/data
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://127.0.0.1:9000/_awanfs/health/ready || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 5
    restart: unless-stopped

volumes:
  awanfs_data:
```

(Pass `--build` on first run or pull a tagged image if published by `.github/workflows/docker.yml`. Note: `Dockerfile` hardcodes `--port 9000 --data-dir /data` via `ENTRYPOINT`, so `S3_PORT` / `S3_DATA_DIR` are overridden unless `--entrypoint` is replaced.)

### Linux Production `systemd` Service ([`deploy/systemd/awanfs.service`](deploy/systemd/awanfs.service))

Install `awanfs` to `/usr/local/bin/awanfs`, copy [`deploy/systemd/awanfs.service`](deploy/systemd/awanfs.service) to `/etc/systemd/system/awanfs.service`, and create `/etc/awanfs/awanfs.env` (`chmod 0600`) with:

```ini
AWANFS_ACCESS_KEY=s3admin
AWANFS_SECRET_KEY=s3secret
# AWANFS_LISTEN=0.0.0.0
# AWANFS_DURABILITY=group
```

[`deploy/systemd/awanfs.service`](deploy/systemd/awanfs.service) runs with `DynamicUser=yes`, `StateDirectory=awanfs`, `ExecStart=/usr/local/bin/awanfs --port 9000 --data-dir /var/lib/awanfs`, `TimeoutStopSec=45`, and `LimitNOFILE=65536`.

---

## 5. Migrating from MinIO / Ceph / AWS S3

### 5.1 Wire Compatibility & Converter-Free Migration
- AwanFS's columnar `ObjectMeta` index, `meta/` segments, and format-3 WORM needle volumes are internal storage formats; data enters and leaves over HTTP/HTTPS S3 XML/REST.
- Unimplemented S3 sub-resources (such as `?policy`, `?acl`, `?encryption`, `?object-lock`, `?logging`) return `501 NotImplemented` (`ARCHITECTURE.md` Decision 61), and error responses follow `<Code><Message><Resource><RequestId>` (`server/src/xml.rs:48-65`).

### 5.2 Updates, Deletes & Auto-Compaction
1. **Overwrites (`PutObject`):** Appends a fresh needle to the active volume under a striped key lock (acquired only at commit for `> 4 MiB` streamed PUTs) and swaps the index row atomically.
2. **Single-Object Deletes (`DeleteObject`):** Appends a tombstone needle (or delete marker when versioned) and updates the index (`204 No Content`, idempotent if missing when unversioned).
3. **Multi-Object Batch Deletes (`DeleteObjects`):** `POST /{bucket}?delete` deletes `1..=1000` keys per `<Delete>` XML request and returns `<DeleteResult>` XML (`Content-MD5` is not checked on `DeleteObjects`).
4. **Background Compaction:** Runs every `600s` (unless `--auto-compaction off`) when dead space in the oldest volume is `>= 20%` (and a lone active volume is `>= 1 GiB`), rate-limited to `50 MB/s` default (`--compaction-rate`).

### 5.3 Hardware Checksums & Software Fallback
- `StreamingDigest` ([`server/src/digest.rs`](server/src/digest.rs)) uses `is_x86_feature_detected!("sse4.2") || is_x86_feature_detected!("avx2")` or `aarch64` CRC detection with a bit-identical pure-Rust software fallback (`StreamingDigest::new_software_fallback()`, 256-entry `0x82F63B78` CRC32C table), verified across 9 buffer sizes in [`server/tests/streaming_digest_test.rs`](server/tests/streaming_digest_test.rs).

### 5.4 Live Migration Example with `rclone` (Not Run in CI)
1. Configure `rclone.conf` (using server credentials matching `AWANFS_ACCESS_KEY`/`AWANFS_SECRET_KEY`):
   ```ini
   [new-awanfs]
   type = s3
   provider = Other
   access_key_id = s3admin
   secret_access_key = s3secret
   endpoint = http://127.0.0.1:9000
   force_path_style = true
   ```
2. Run bulk transfer and final delta sync:
   ```bash
   rclone sync legacy-minio:my-production-bucket new-awanfs:my-production-bucket --transfers 32 --checkers 64 -P
   rclone sync legacy-minio:my-production-bucket new-awanfs:my-production-bucket --checksum
   ```

---

## 6. Supported Operations Summary

| Feature ([`features.toml`](features.toml)) | Implemented Surface Today |
| :--- | :--- |
| [`buckets`](features.toml#L847) | `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListBuckets` (`prefix`, `max-buckets`, `continuation-token`), `GetBucketLocation`. |
| [`objects`](features.toml#L930), [`object-metadata`](features.toml#L599) | `PutObject` (`<= 5 GiB`, streaming `> 4 MiB`), `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects` (`1..=1000` keys), `x-amz-meta-*` (`<= 2048 B`), standard HTTP metadata, `response-*` query overrides. |
| [`copy`](features.toml#L663) | `CopyObject` (`x-amz-copy-source`, `?versionId`, `COPY`/`REPLACE` metadata & tags directives, `x-amz-copy-source-if-*`; copies payload into a new needle). |
| [`listing`](features.toml#L743) | `ListObjectsV2` (`?list-type=2`), `ListObjects` (V1), `prefix`, `delimiter`, `max-keys`, `start-after`, `marker`, `continuation-token`, `encoding-type=url`, `fetch-owner`. |
| [`ranges`](features.toml#L909) | Single-span `Range: bytes=start-end`, `bytes=start-`, `bytes=-suffix` (`206` clamped / `416 InvalidRange`). `<= 4 MiB` objects/parts may use cache/mmap; `> 4 MiB` objects & multipart objects stream via `PreadBody`. |
| [`multipart`](features.toml#L536) | `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload` (`--allow-small-parts` optional), `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads`, `GET`/`HEAD ?partNumber=N`. |
| [`conditional`](features.toml#L470) | `If-None-Match: *` on `PutObject`; full RFC 7232 preconditions on `GetObject`, `HeadObject`, `CopyObject`, `UploadPartCopy` (`PUT If-Match` / `DELETE` conditionals `#112`). |
| [`versioning`](features.toml#L692) | `PutBucketVersioning`, `GetBucketVersioning`, `ListObjectVersions`, `?versionId` on `Get`/`Head`/`Delete`/`Copy`/`Tagging`/`Attributes`, null versions for suspended buckets. |
| [`tagging`](features.toml#L269) | Bucket & object `Put/Get/Delete*Tagging`, `PutObject`/`CreateMultipartUpload` `x-amz-tagging`, `x-amz-tagging-count`. |
| [`cors`](features.toml#L191) | `Put/Get/DeleteBucketCors`, `OPTIONS` preflight, actual CORS response headers. |
| [`checksums`](features.toml#L220), [`object-attributes-api`](features.toml#L251) | `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256` on PUT/Multipart, `x-amz-checksum-mode: ENABLED`, `GetObjectAttributes` (`?attributes`). |
| [`sigv4-auth`](features.toml#L1014), [`anonymous-and-presigned`](features.toml#L631), [`iam`](features.toml#L1029) | `AWS4-HMAC-SHA256` header & presigned query URLs, `aws-chunked` chunk signature verification, `--allow-anonymous`, TOML `--config` IAM users & identity policies (`SigV4KeyCache` shared `RwLock<HashMap>`). |
| [`compression`](features.toml#L1037), [`compaction`](features.toml#L1045), [`restart`](features.toml#L988), [`memory-budget`](features.toml#L1111) | Per-bucket `zstd` level 1 in 64 KiB blocks, `group`/`fast`/`strict` durability, automatic 32 GiB rollover, `>= 20%` dead-space auto-compaction, `--memory-budget` cache/body enforcement (`503 SlowDown`). |
| [`health`](features.toml#L1060), [`metrics`](features.toml#L1069), [`tls`](features.toml#L1078), [`request-log`](features.toml#L1094), [`access-log`](features.toml#L1086), [`otlp-tracing`](features.toml#L1102) | `/_awanfs/health/live`, `/_awanfs/health/ready`, `/_awanfs/metrics`, `--cert`/`--key`/`--http-port`, stderr JSON request log, `--access-log`, `--otlp-endpoint`. |

---

## 7. Architecture Summary (`ObjectMeta`, Volumes, Staged Multipart, Memory Budget)

- **Metadata index ([`metadata-index`](features.toml#L1130)):** [`ObjectMeta`](server/src/meta_index.rs#L15-L26) is `64` bytes per in-memory row (`key_hash`, `bucket_flags`, `vol_offset`, `size`, `etag_hi`, `etag_lo`, `created_at`, `key_slice`), backed by an append-only `key_arena`, `HashMap<(u32, u64), Vec<usize>>` collision chains, per-bucket `BTreeMap` key index, and leveled [`SpillIndex`](server/src/spill.rs) segments under `meta/` for keys past the in-memory limit (plus key/index structures in heap).
- **Growing volumes ([`volume-format`](features.toml#L1138)):** Format-3 needle files (`volume_NNNN.dat` + `.idx`) grow as written up to `32 GiB` (`AWAN_SPARSE_VOLUME=1` opts into 32 GiB sparse pre-allocation). On restart, the store replays `buckets.manifest` and scans `volume_*.dat` sequentially, truncating torn tails back to a 4096-byte boundary.
- **Staged multipart ([`multipart`](features.toml#L536)):** `UploadPart` writes `.staging/upload-<hex>/part_NNNNN.dat` files; `CompleteMultipartUpload` keeps part payloads in `.staging/` and records an `AWNMPU1` manifest needle in the active volume.
- **Read paths & memory budget ([`memory-budget`](features.toml#L1111)):** `PayloadCache` and `SingleFlight` serve buffered reads `<= 4 MiB`, bounded by `--memory-budget` (`total/2` for cache minus `min(cache/4, 64 MiB)` spill cache; `2 GiB` fallback when RAM/cgroup detection is unavailable). Single objects and multipart objects `> 4 MiB` stream via positioned page-cache reads (`PreadBody`).

---

## 8. Building from Source, Test Suite & Benchmark Tooling

### 8.1 Multi-OS Release Builds
- CI release workflow ([`.github/workflows/release.yml`](.github/workflows/release.yml)) builds binaries for `x86_64-pc-windows-msvc`, `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, and `aarch64-apple-darwin`.
- On Windows, run [`scripts/build_release_matrix.py`](scripts/build_release_matrix.py) to compile `dist/awanfs-x86_64-windows.exe` + `dist/SHA256SUMS.txt` and `cargo check` the three Linux targets:
  ```bash
  python scripts/build_release_matrix.py
  ```

### 8.2 Running the Test & Verification Suites
Run all Rust unit and integration tests (98 test files across `server/tests/`):

```bash
cargo test --locked --no-fail-fast --manifest-path server/Cargo.toml
```

Representative suites in [`server/tests/`](server/tests/):
- [`sparse_mmap_test.rs`](server/tests/sparse_mmap_test.rs): 32 concurrent workers on legacy sparse mode (`AWAN_SPARSE_VOLUME=1` opt-in), boundary recovery, and trailing garbage detection.
- [`staged_mpu_test.rs`](server/tests/staged_mpu_test.rs) & [`multipart_conformance_test.rs`](server/tests/multipart_conformance_test.rs): 16 parallel part upload workers, abort cleanup, XML/ETag/`--allow-small-parts` validation.
- [`streaming_digest_test.rs`](server/tests/streaming_digest_test.rs): SIMD vs `StreamingDigest::new_software_fallback()` across 9 buffer sizes, streaming PUT digest, and `BadDigest` tamper check.
- [`multipart_zero_copy_test.rs`](server/tests/multipart_zero_copy_test.rs) & [`multipart_get_reads_test.rs`](server/tests/multipart_get_reads_test.rs): single/multi-part ranges (`<= 4 MiB` and `> 4 MiB` positioned reads).
- [`edge_cases_test.rs`](server/tests/edge_cases_test.rs), [`body_timeout_test.rs`](server/tests/body_timeout_test.rs), & [`memory_admit_test.rs`](server/tests/memory_admit_test.rs): path traversal, 0-byte objects, `SingleFlight`, `If-None-Match: *` contention, `408 RequestTimeout`, and `503 SlowDown`.
- [`meta_index_test.rs`](server/tests/meta_index_test.rs) & [`tiered_meta_test.rs`](server/tests/tiered_meta_test.rs): `ObjectMeta` layout, collision check, prefix scan, C-ABI FFI, `1,000,000`-object benchmark test, and leveled segment recovery.
- [`lifecycle_test.rs`](server/tests/lifecycle_test.rs): XML roundtrip + direct `run_lifecycle_sweep` unit tests (`#118` covers wiring periodic persisted sweeps).
- [`multipart_sigv4_test.rs`](server/tests/multipart_sigv4_test.rs), [`auth_test.rs`](server/tests/auth_test.rs), & [`chunk_signature_test.rs`](server/tests/chunk_signature_test.rs): SigV4, presigned URLs, skew checks, and `aws-chunked` verification.
- [`property_tests.rs`](server/tests/property_tests.rs) & [`volume_packing_test.rs`](server/tests/volume_packing_test.rs) & [`wire_test.rs`](server/tests/wire_test.rs): 500-case proptests, 4096B needle alignment/recovery, virtual-host vs path routing, and range clamping.
- End-to-end AWS CLI drive script: `bash scripts/verify_drive.sh`.

### 8.3 Running the Benchmark Harness
[`benchmarks/run_warp_benchmark.py`](benchmarks/run_warp_benchmark.py) drives `AwanFS`, MinIO, and RustFS comparisons (`--sizes`, `--concurrent`, `--duration`, `--runs`, `--rss-ceiling-mib`, `--output` are flags of that benchmark script, not `awanfs`):

```bash
python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 3s --output measurements/s3_warp_minio_vs_awan.md
```

---

## 9. Not yet

- **Multipart edge cases ([`multipart`](features.toml#L536), `#111`):** Remaining `ceph/s3-tests` multipart conformance checks.
- **Conditional `PUT If-Match` / `DELETE` ([`conditional`](features.toml#L470), `#112`):** Full preconditions beyond `PutObject` `If-None-Match: *` and `GET`/`HEAD`/`Copy`.
- **Automated lifecycle expiration & persistence ([`lifecycle`](features.toml#L150), `#118`):** Currently lifecycle rules live in memory and `run_lifecycle_sweep` is not registered on the background `Scheduler`.
- **POST Object ([`post-object`](features.toml#L180), `#119`)**, **Object Lock ([`object-lock`](features.toml#L162), `#120`)**, **Server-Side Encryption ([`encryption`](features.toml#L133), `#121`)**, **Bucket Policies / ACLs / Public Access Block ([`policy`](features.toml#L327), [`acl-ownership`](features.toml#L379), `#122`)**, **Bucket Logging ([`bucket-logging`](features.toml#L88), `#153`)**.
- **Large-object background scrub (`#155`)**, **Checkpoint boot recovery (`#322`)**, **`503 SlowDown` `Retry-After` guidance (`#356`)**, and **Multi-node replication / erasure coding ([`erasure-coding`](features.toml#L1161), `#52`, `#27`)**.
