# Awan Storage S3 Hybrid PoC — Autonomous Execution Runbook (Flash Edition)

> **Self-Contained Execution Guide for Completing Milestone #10 ([#462](https://github.com/iskandarnorazli/awan-lang/issues/462)–[#471](https://github.com/iskandarnorazli/awan-lang/issues/471)) Start-to-Finish**  
> **Target Executor:** Gemini 3.8 Flash running under `/iskandar-mode`  
> **Task State ID:** `awan-storage-poc-build` (`.agents/state/awan-storage-poc-build/`)

---

## 1. Golden Rules & Non-Negotiable Guardrails (Read First)

1. **NEVER Touch `core/`, `compiler_src/`, or Root `libs/`:**
   - All code for this PoC lives strictly under `awan-storage/`, `benchmarks/s3_warp/`, and `docs/measurements/`.
   - **What to do if you hit a bug in Awan (`core/` or `compiler_src/`):**
     1. **Do NOT edit `core/` or `compiler_src/`.**
     2. File a GitHub issue immediately (`gh issue create --title "bug: <summary>" --milestone "Bugs & Test Hardening" --label "bug,finding" --body "<minimal reproducer + command + output>"`).
     3. **Work around the bug locally inside `awan-storage/`** (see Section 3 Decision Table for pre-approved workarounds), cite the filed issue number `#NNN` in a code comment and in your commit message, and **keep going**.
2. **Evidence Before Claims:**
   - Every test, gate, and benchmark claim must include the exact command run and its output.
   - Every code issue requires a **Negative Control** (temporarily break the check or inject the fault flag, run the test to prove it fails, then restore the fix).
3. **Commit Attribution & Hygiene:**
   - Always commit with:
     ```powershell
     git -c user.name="Iskandar Norazli" -c user.email="111472977+iskandarnorazli@users.noreply.github.com" commit -m "..."
     ```
   - Never add `Co-Authored-By` or AI attribution lines.
   - Never force-push or push directly to `main`. Work on a dedicated branch (e.g., `feature/awan-storage-s3-poc`) or in an isolated worktree `.tmp/wt-s3-poc` if another process is using the root tree.
4. **Stop When All 10 Issues & Release Binaries Are Complete:**
   - Update each issue (`gh issue edit <n> --remove-label wip:antigravity --add-label needs-review` + gate evidence comment). Do **not** close the issues (`AGENTS.md`: reviewer/owner closes after audit).
   - Once Wave 1 $\to$ Wave 5 are complete and verified, update task state `awan-storage-poc-build` and stop to notify the owner.

---

## 2. Source-of-Truth Reference Documents

Before starting each wave, read only the specific sections listed below:

| Document | What It Specifies | When to Read |
| :--- | :--- | :--- |
| [`HYBRID_POC_SPECIFICATION.md`](HYBRID_POC_SPECIFICATION.md) | Master PoC blueprint, `awan_storage.h` 64-byte C-ABI struct, error codes, issue dependency graph | Before Wave 1 (`#462`, `#463`) |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | 64-byte `@flat ObjectMeta` layout, Key String Arena, ART prefix index, `@flat PartExtent` table, 4096B WORM Needle layout, `.idx` crash recovery | Before `#463` and `#470` |
| [`API_SPECIFICATION.md`](API_SPECIFICATION.md) | Path-style vs virtual-host routing, Bucket/Object/Multipart/Lifecycle endpoints, `Expect: 100-continue`, 3 SigV4 modes (`<hex>`, `UNSIGNED-PAYLOAD`, `aws-chunked`), RFC 9110 Range rules | Before `#462`, `#467`, `#468` |
| [`EDGE_CASES_AND_CONTRACTS.md`](EDGE_CASES_AND_CONTRACTS.md) | 12 edge-case operational contracts (disconnects, `0B` objects, single-flight coalescing, path traversal, MVCC overwrite isolation, etc.) & 22 S3 XML error codes | Before `#469` |
| [`COMPETITIVE_LANDSCAPE.md`](COMPETITIVE_LANDSCAPE.md) | MinIO `warp` benchmark parameters and competitive baseline context | Before `#465`, `#466`, `#471` |

---

## 3. Autonomous Decision Table (How to Decide Without Asking the Owner)

Whenever you encounter an implementation choice or environment limitation, use these pre-approved decisions instead of blocking:

| Situation / Question | Pre-Approved Autonomous Decision |
| :--- | :--- |
| **1. How should the Rust server crate be structured?** | Create a standalone Cargo package at `awan-storage/server/Cargo.toml` (not inside `core/Cargo.toml` workspace) so it compiles in `< 5s` independently without touching `core/`. Provide both a library target (`src/lib.rs`) for fast integration tests and a binary target (`src/main.rs` -> `awan-storage-server`). |
| **2. Which Rust crates should `awan-storage/server/Cargo.toml` use?** | Keep dependencies minimal, fast-compiling, and 100% pure Rust (so cross-compilation to Linux/Windows x86_64 & ARM64 works without C toolchain headaches): `tokio` (full), `hyper` / `http-body-util` / `hyper-util` (or `axum`), `bytes`, `md-5`, `sha2`, `hmac`, `crc32c`, `xxhash-rust` (with `xxh3`), `core_affinity`, `quick-xml` (or zero-alloc template formatting), and `proptest` + `reqwest` under `[dev-dependencies]`. |
| **3. What if compiling `.awan` directly into a C staticlib/cdylib hits a compiler limitation in `core/`?** | 1. Write and test the pure Awan reference implementation in `awan-storage/engine/storage_core.awan` and verify it via `awan test` (`awan-storage/tests/*.test.awan`).<br>2. If `core` cannot emit a linkable C staticlib for a specific construct, file a `bug` issue under `Bugs & Test Hardening` and implement the exact same 64-byte SoA columnar arrays (`Vec<u64>` per column: `key_hashes`, `bucket_flags`, `vol_offsets`, `sizes`, `etags_hi`, `etags_lo`, `created_ats`, `key_slices`) inside `awan-storage/server/src/ffi_bridge.rs` exposing the exact `extern "C"` functions from `awan_storage.h`. Both the `.awan` oracle and the `extern "C"` bridge must pass identical test vectors. |
| **4. What if `O_DIRECT` / `FILE_FLAG_NO_BUFFERING` fails on a tmpfs / CI filesystem that doesn't support Direct I/O?** | Implement 4096-byte aligned buffer allocation (`std::alloc::Layout::from_size_align(len, 4096)`) unconditionally. Attempt to open the volume file with `O_DIRECT` (`#[cfg(target_os = "linux")] libc::O_DIRECT`) or `FILE_FLAG_NO_BUFFERING` (`#[cfg(target_os = "windows")] 0x20000000`); if the OS filesystem returns `EINVAL` (common on `tmpfs` or certain container mounts), automatically fall back to standard aligned positional I/O and log a warning so tests never fail on CI filesystems. |
| **5. What if `minio`, `warp`, `aws-cli`, or `boto3` is not pre-installed on the machine during Waves 3–4?** | Check with `where.exe minio` / `which minio`, `where.exe warp`, `python -c "import boto3"`, `where.exe aws`.<br>- If `boto3` is missing: install in a local venv or use a zero-dependency Python S3 SigV4 client in `awan-storage/server/tests/s3_client_helper.py` (`urllib.request` + `hmac` + `hashlib`).<br>- If `minio` or `warp` binary is not installed locally: check `.tmp/bin/`; if absent, download the official standalone `minio.exe` and `warp.exe` binaries into `.tmp/bin/` (using `Invoke-WebRequest` / `curl -L` from `https://dl.min.io/server/minio/release/windows-amd64/minio.exe` and GitHub releases for `minio/warp`), verify they run, and execute the live twin fuzzing (`#465`) and `warp` benchmark (`#466`). |
| **6. What if port `9000` is already in use during tests?** | Allow `awan-storage-server` to accept `--port <u16>` (and bind port `0` in integration tests to let the OS assign an ephemeral free port, returning `local_addr().port()`). Use `:9000` / `:9001` by default for CLI scripts, with `--port` override. |
| **7. How should cross-compilation for the target OS/Arch binary matrix work on Windows?** | Build native `x86_64-pc-windows-msvc` (`cargo build --release`). For Linux (`x86_64-unknown-linux-gnu` / `x86_64-unknown-linux-musl` / `aarch64-unknown-linux-gnu`), use `cargo check --target <triple>` (and `cargo zigbuild --release --target <triple>` or `cargo build --release --target <triple>` if linker/zig is available, or Docker `rust:alpine` if Docker Desktop is running). Record exact built artifacts and SHA-256 hashes in `awan-storage/dist/SHA256SUMS.txt`. |

---

## 4. Step-by-Step Execution Waves (Start to Finish)

Execute the waves in this exact order. Before starting each issue `#N`:
1. `gh issue edit <N> --add-label wip:antigravity`
2. Write the tests first and observe them fail.
3. Implement the code under `awan-storage/`.
4. Run the issue's Gate commands and its Negative Control.
5. Commit with evidence, comment on `#N`, and run `gh issue edit <N> --remove-label wip:antigravity --add-label needs-review`.

---

### Wave 1: Core Substrate & Columnar Index (`#463` $\to$ `#462`)

#### Step 1.1: Issue [#463](https://github.com/iskandarnorazli/awan-lang/issues/463) — Awan Native `@flat ObjectMeta` Index & C-ABI FFI Bridge
- **Reference Docs:** [`ARCHITECTURE.md`](ARCHITECTURE.md) Section 2, [`HYBRID_POC_SPECIFICATION.md`](HYBRID_POC_SPECIFICATION.md) Section 3.
- **Files to Create:**
  - `awan-storage/engine/storage_core.awan`
  - `awan-storage/server/Cargo.toml`
  - `awan-storage/server/include/awan_storage.h`
  - `awan-storage/server/src/lib.rs`
  - `awan-storage/server/src/ffi_bridge.rs`
  - `awan-storage/server/tests/ffi_bridge_test.rs`
- **Key Implementation Requirements:**
  - `AwanObjectMeta` is `#[repr(C)]` and exactly 64 bytes (`std::mem::size_of::<AwanObjectMeta>() == 64`).
  - Columnar SoA storage for `key_hash`, `bucket_flags`, `vol_offset`, `size`, `etag_hi`, `etag_lo`, `created_at`, `key_slice`.
  - Keyed 64-bit hash (`xxh3` / `SipHash`) of `(bucket_id, key_bytes)` + open-addressing probe that compares full key bytes in the Key String Arena (`key_slice`) to guarantee zero false-positive collisions.
  - Per-bucket lexicographical key index (`BTreeMap<Vec<u8>, usize>` or Radix Tree mapping UTF-8 key to SoA row index) so `awan_storage_index_scan` returns keys in strict UTF-8 lexicographical order with `prefix` and `start_after`.
  - Wrap all `extern "C"` functions in `std::panic::catch_unwind`, returning `AWAN_ERR_PANIC` (`-999`) on panic.
- **Gate Command:**
  ```powershell
  cargo test --manifest-path awan-storage/server/Cargo.toml --test ffi_bridge_test
  ```

#### Step 1.2: Issue [#462](https://github.com/iskandarnorazli/awan-lang/issues/462) — Rust HTTP/1.1 & HTTP/2 Daemon & S3 REST Wire Reactor
- **Reference Docs:** [`API_SPECIFICATION.md`](API_SPECIFICATION.md) Sections 1, 2.A, 2.B, 5.
- **Files to Create/Update:**
  - `awan-storage/server/src/main.rs`
  - `awan-storage/server/src/reactor.rs`
  - `awan-storage/server/src/s3_api.rs`
  - `awan-storage/server/src/xml.rs`
  - `awan-storage/server/tests/wire_test.rs`
- **Key Implementation Requirements:**
  - Parse both path-style (`/{bucket}/{key}`) and virtual-hosted-style (`Host: {bucket}.s3.localhost:9000` with URI `/{key}`).
  - Implement Service & Bucket ops: `GET /` (`ListBuckets`), `PUT /{bucket}` (`CreateBucket`), `HEAD /{bucket}` (`HeadBucket`), `DELETE /{bucket}` (`DeleteBucket` -> `204` or `409 BucketNotEmpty`), `GET /{bucket}?location` (`GetBucketLocation`), `GET /{bucket}?list-type=2` (`ListObjectsV2` with `prefix`, `delimiter`, `max-keys`, `start-after`, `continuation-token`), and `POST /{bucket}?delete` (`DeleteObjects` batch delete).
  - Implement Object ops: `PUT /{bucket}/{key}`, `GET /{bucket}/{key}`, `HEAD /{bucket}/{key}` (0 body bytes, full headers), `DELETE /{bucket}/{key}` (`204 No Content`).
  - Handle `Expect: 100-continue` before reading body stream.
  - Implement RFC 9110 `Range: bytes=...` (`bytes=start-end` with `end >= size` clamped to `size - 1`, `bytes=start-`, `bytes=-suffix`, and `416 Range Not Satisfiable` with `Content-Range: bytes */size`).
- **Gate Command:**
  ```powershell
  cargo test --manifest-path awan-storage/server/Cargo.toml --test wire_test
  ```

---

### Wave 2: Drop-In S3 Protocol, Lifecycle, Hardening & WORM Needle Volumes (`#467`, `#468`, `#469`, `#470`)

#### Step 2.1: Issue [#467](https://github.com/iskandarnorazli/awan-lang/issues/467) — Multipart Upload Engine & AWS Signature V4 Authentication
- **Reference Docs:** [`API_SPECIFICATION.md`](API_SPECIFICATION.md) Sections 2.C & 3, [`ARCHITECTURE.md`](ARCHITECTURE.md) Section 2.C.
- **Files to Create:**
  - `awan-storage/server/src/multipart.rs`
  - `awan-storage/server/src/sigv4.rs`
  - `awan-storage/server/src/aws_chunked.rs`
  - `awan-storage/server/tests/multipart_sigv4_test.rs`
- **Key Implementation Requirements:**
  - Multipart endpoints: `POST /{bucket}/{key}?uploads`, `PUT /{bucket}/{key}?partNumber=N&uploadId=ID`, `POST /{bucket}/{key}?uploadId=ID`, `DELETE /{bucket}/{key}?uploadId=ID`, `GET /{bucket}/{key}?uploadId=ID` (`ListParts`), `GET /{bucket}?uploads` (`ListMultipartUploads`).
  - Validate `1 <= partNumber <= 10,000`, ascending part order (`400 InvalidPartOrder`), part ETag match (`400 InvalidPart`), and `>= 5 MiB` non-final part size (`400 EntityTooSmall`, bypassable when server config `allow_small_parts: true` for unit tests).
  - Compute composite ETag `"<hex_md5(raw_16b_md5_part1 || ... || raw_16b_md5_partK)>-<K>"` and store extents in `@flat PartExtent` without copying payload bytes.
  - Support all 3 SigV4 payload modes: `<64-hex-sha256>`, `UNSIGNED-PAYLOAD`, and `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (`aws-chunked` frame stripping + rolling chunk HMAC validation), plus Pre-Signed URLs (`X-Amz-Signature`, `X-Amz-Expires`, `RequestTimeTooSkewed` $> 900\text{s}$) and derived `kSigning` key caching.
- **Gate Command:**
  ```powershell
  cargo test --manifest-path awan-storage/server/Cargo.toml --test multipart_sigv4_test
  ```

#### Step 2.2: Issue [#468](https://github.com/iskandarnorazli/awan-lang/issues/468) — Bucket Lifecycle Management & Automated Expiration Engine
- **Reference Docs:** [`API_SPECIFICATION.md`](API_SPECIFICATION.md) Section 2.D.
- **Files to Create:**
  - `awan-storage/server/src/lifecycle.rs`
  - `awan-storage/server/tests/lifecycle_test.rs`
- **Key Implementation Requirements:**
  - `PUT /{bucket}?lifecycle`, `GET /{bucket}?lifecycle` (`404 NoSuchLifecycleConfiguration` if unset), `DELETE /{bucket}?lifecycle`.
  - Parse `<Rule>`, `<ID>`, `<Status>Enabled</Status>`, `<Filter><Prefix>`, `<Expiration><Days>`, and `<AbortIncompleteMultipartUpload><DaysAfterInitiation>`.
  - Provide `run_lifecycle_sweep(now_epoch_ms)` (invoked periodically by background task and directly callable in tests with simulated clock) that tombstones expired objects matching the rule prefix and aborts expired multipart uploads.
- **Gate Command:**
  ```powershell
  cargo test --manifest-path awan-storage/server/Cargo.toml --test lifecycle_test
  ```

#### Step 2.3: Issue [#469](https://github.com/iskandarnorazli/awan-lang/issues/469) — 12 Edge Cases, Path Defenses, Single-Flight & 22 S3 XML Errors
- **Reference Docs:** [`EDGE_CASES_AND_CONTRACTS.md`](EDGE_CASES_AND_CONTRACTS.md) Sections 1–3, [`API_SPECIFICATION.md`](API_SPECIFICATION.md) Sections 4–5.
- **Files to Create:**
  - `awan-storage/server/src/error.rs`
  - `awan-storage/server/src/coalesce.rs`
  - `awan-storage/server/src/mvcc.rs`
  - `awan-storage/server/tests/edge_cases_test.rs`
- **Key Implementation Requirements:**
  - Incomplete upload rollback (`400 IncompleteBody` when bytes received $<$ `Content-Length`).
  - Zero-byte object & `folder/` key handling (`FLAG_ZERO_BYTE`, empty MD5 `d41d8cd98f00b204e9800998ecf8427e`).
  - Single-flight request coalescing (`coalesce.rs`) so $N$ concurrent `GET`s on the same cold key share a single disk read (`disk_read_count == 1`).
  - Key validation: reject empty keys, `> 1024` UTF-8 bytes (`KeyTooLongError`), null/control chars, and relative traversal segments (`..`) with `400 Bad Request` (`InvalidObjectName`). Escape XML entities (`&amp;`, `&lt;`, `&gt;`, `&quot;`, `&apos;`) in all XML outputs.
  - Conditional headers: `If-Match`, `If-None-Match` (`"*"` for atomic mutex locks and `"{etag}"` for `304 Not Modified`), `If-Modified-Since`, `If-Unmodified-Since`.
  - Server-side `PUT` with `x-amz-copy-source` (`CopyObject` zero-copy extent reference clone).
  - Epoch-pinned MVCC read isolation (`mvcc.rs`) so an in-flight `GET` reader continues reading its pinned extent even if a concurrent `PUT` overwrites or `DELETE` tombstones the key.
- **Gate Command:**
  ```powershell
  cargo test --manifest-path awan-storage/server/Cargo.toml --test edge_cases_test
  ```

#### Step 2.4: Issue [#470](https://github.com/iskandarnorazli/awan-lang/issues/470) — Direct I/O (`O_DIRECT`) & WORM Needle Volume Engine
- **Reference Docs:** [`ARCHITECTURE.md`](ARCHITECTURE.md) Section 3.
- **Files to Create:**
  - `awan-storage/server/src/volume.rs`
  - `awan-storage/server/src/direct_io.rs`
  - `awan-storage/server/src/compaction.rs`
  - `awan-storage/server/tests/volume_packing_test.rs`
- **Key Implementation Requirements:**
  - 4096-byte aligned buffer allocator (`direct_io.rs`) and platform Direct I/O flags (`O_DIRECT` on Linux, `FILE_FLAG_NO_BUFFERING` on Windows, with automatic fallback if the host filesystem rejects Direct I/O).
  - WORM Needle framing (`magic = 0x41574E33`, `header_crc32c`, `payload_crc32c`, metadata fields, key bytes, payload bytes, 4096-byte tail padding) inside `volume_0001.dat` plus `.idx` journal.
  - On read: verify `payload_crc32c` with hardware SSE4.2/ARMv8 CRC32C (`crc32c` crate) to detect silent bitrot.
  - On startup: scan `.idx` and volume tail; if a torn/partial needle is found at the end of the volume, truncate back to the last valid 4096-byte boundary and recover all committed keys.
  - Copy-on-Compact (`compaction.rs`): compact sealed volumes exceeding $30\%$ dead space into a new volume, atomically update `@flat ObjectMeta` offsets once MVCC readers drain, and remove the old volume file.
- **Gate Command:**
  ```powershell
  cargo test --manifest-path awan-storage/server/Cargo.toml --test volume_packing_test
  ```

---

### Wave 3: Dual Unit Suites, Twin MinIO Differential Oracle & Conformance (`#464`, `#465`, `#471`)

#### Step 3.1: Issue [#464](https://github.com/iskandarnorazli/awan-lang/issues/464) — Dual Awan Unit Tests & Client Compatibility Suite
- **Files to Create:**
  - `awan-storage/tests/meta_index.test.awan`
  - `awan-storage/tests/slab_alloc.test.awan`
  - `awan-storage/server/tests/test_boto3_compat.py`
  - `awan-storage/server/tests/test_aws_cli.py`
- **Gate Commands:**
  ```powershell
  cargo run --manifest-path core/Cargo.toml --bin awan -- test awan-storage/tests/meta_index.test.awan
  cargo run --manifest-path core/Cargo.toml --bin awan -- test awan-storage/tests/slab_alloc.test.awan
  python awan-storage/server/tests/test_boto3_compat.py
  ```

#### Step 3.2: Issue [#465](https://github.com/iskandarnorazli/awan-lang/issues/465) — Twin MinIO Differential Testing & Fuzzing Oracle
- **Files to Create:**
  - `awan-storage/tests/differential_minio_fuzz.py`
  - `awan-storage/tests/fuzz_generators.py`
  - `docs/measurements/differential_fuzz_s3_oracle.md`
- **Key Implementation Requirements:**
  - Automatically locate or download standalone `minio` into `.tmp/bin/minio(.exe)`.
  - Spawn `minio server` on a free local port and `awan-storage-server` on a second free local port.
  - Execute 10,000 randomized operations (`CreateBucket`, `HeadBucket`, `PutObject`, `CopyObject`, `GetObject`, `GetObject(Range)`, `HeadObject`, `MultipartUpload`, `ListObjectsV2`, `DeleteObject`, `DeleteObjects`, conditional headers, invalid ranges, missing keys).
  - Assert 100% parity on status code, `ETag`, `Content-Length`, `Content-Range`, XML `<Error><Code>`, and SHA-256 payload digest.
  - Support `--inject-fault range-status-200` flag for the negative control check.
  - Write `docs/measurements/differential_fuzz_s3_oracle.md` with the exact command on line 1.
- **Gate Command:**
  ```powershell
  python awan-storage/tests/differential_minio_fuzz.py --count 10000 --seed 20261110
  ```

#### Step 3.3: Issue [#471](https://github.com/iskandarnorazli/awan-lang/issues/471) — Stateful Property-Based Testing & S3 Conformance Gauntlet
- **Files to Create:**
  - `awan-storage/server/tests/property_tests.rs`
  - `awan-storage/tests/run_s3_conformance.py`
  - `docs/measurements/s3_conformance_report.md`
- **Gate Commands:**
  ```powershell
  cargo test --manifest-path awan-storage/server/Cargo.toml --test property_tests
  python awan-storage/tests/run_s3_conformance.py --output docs/measurements/s3_conformance_report.md
  ```

---

### Wave 4: Head-to-Head MinIO `warp` Benchmark (`#466`)

#### Step 4.1: Issue [#466](https://github.com/iskandarnorazli/awan-lang/issues/466) — Live `warp` Benchmark & Performance Claims Ledger
- **Files to Create/Update:**
  - `benchmarks/s3_warp/run_warp_benchmark.py`
  - `benchmarks/s3_warp/test_run_warp_benchmark.py`
  - `docs/measurements/s3_warp_minio_vs_awan.md`
  - `docs/PERF_CLAIMS_LEDGER.md` (Section 3 row for `awan-storage`)
- **Key Implementation Requirements:**
  - Automatically locate or download standalone `minio` and `warp` into `.tmp/bin/`.
  - Pin server process to cores `0..3` and `warp` client to disjoint cores `4..7` (using `SetProcessAffinityMask` / `psutil` / `start /affinity` on Windows, `taskset` on Linux).
  - Run `warp put`, `warp get`, and `warp mixed` across `64KiB` and `1MiB` payloads with 64 concurrent clients.
  - Write `docs/measurements/s3_warp_minio_vs_awan.md` with the exact command on line 1, hardware specs, and full throughput/latency/RSS tables.
  - Update the `awan-storage` row in `docs/PERF_CLAIMS_LEDGER.md` Section 3 with the measured `warp` numbers.
- **Gate Commands:**
  ```powershell
  python -m unittest benchmarks/s3_warp/test_run_warp_benchmark.py
  python benchmarks/s3_warp/run_warp_benchmark.py --sizes 64KiB,1MiB --concurrent 64 --duration 30s --output docs/measurements/s3_warp_minio_vs_awan.md
  ```

---

### Wave 5: Multi-OS & Multi-Arch Release Binary Matrix Packaging

Once Waves 1–4 pass all gates, package the finished deliverables using `awan-storage/scripts/build_release_matrix.py`:

| Target Triple | OS / Architecture | Output Binary Artifact | Verification |
| :--- | :--- | :--- | :--- |
| `x86_64-pc-windows-msvc` | Windows x86_64 | `awan-storage/dist/awan-storage-server-x86_64-windows.exe` | Native release build + smoke test + SHA-256 |
| `x86_64-unknown-linux-gnu` | Linux x86_64 (glibc) | `awan-storage/dist/awan-storage-server-x86_64-linux-gnu` | Cross-compile (`cargo zigbuild` / Docker / `cargo check --target`) + SHA-256 |
| `x86_64-unknown-linux-musl`| Linux x86_64 (static) | `awan-storage/dist/awan-storage-server-x86_64-linux-musl`| Static musl build / check + SHA-256 |
| `aarch64-unknown-linux-gnu`| Linux ARM64 (Graviton)| `awan-storage/dist/awan-storage-server-aarch64-linux-gnu`| ARM64 build / check + SHA-256 |

- Generate `awan-storage/dist/SHA256SUMS.txt` and `docs/measurements/s3_release_binaries_matrix.md` (with producing command on line 1).
- Run the final pre-push ratchet suite before stopping:
  ```powershell
  cargo test --manifest-path core/Cargo.toml --test doc_links_test --test complexity_budget_test --test no_conflict_markers_test
  ```
