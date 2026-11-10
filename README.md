# Awan Storage Platform (`awan-storage`)

> **Next-Generation S3-Compatible Storage Engine & SDK**  
> Designed for microsecond-latency compute, AI agent sandboxes, and high-throughput checkpointing.  
> Zero garbage collection pauses. Zero per-request dynamic heap allocations. 100% mechanical sympathy.

---

## 1. Executive Summary

Modern cloud storage APIs (AWS S3, Azure Blob, Google Cloud Storage) were engineered for petabyte durability over public networks, not for microsecond compute workloads. They impose heavy multi-tenant proxy layers, TLS renegotiation overhead, and dynamic allocation taxes in client SDKs that cap single-core throughput at ~2 GB/s and introduce 10–30 ms latency floors.

Meanwhile, emerging compute paradigms—such as **AI agent sandboxes (BoxCompute, E2B, Modal)**, **GPU model checkpointing**, and **in-memory simulation engines**—require restoring 500 MB to 10 GB images in single-digit milliseconds.

**Awan Storage** is a high-performance, S3-compatible, distributed object storage engine and SDK written in Awan. By unifying:
1. **Contiguous `@arena` slabs** (preallocated memory rings, zero `malloc`/`free` churn on hot paths),
2. **`@flat` Struct-of-Arrays (SoA)** metadata indexing (1,000,000 objects indexed in 40 MB, scanned in 1.0 ms), and
3. **Kernel completion I/O (`io_uring` / Windows IOCP)** with zero-copy socket handoffs,

Awan Storage delivers **19.4 GB/s throughput per CPU core** and **sub-microsecond hot reads**, outpacing MinIO by 10x and standard S3 SDKs by up to 78x.

---

## 2. Key Performance Benchmarks (Empirical Evidence)

Measured on `LAPTOP-6RBIQUM5` (AMD Ryzen 7 5800H, Windows 11) using [`benchmarks/storage_engine.awan`](benchmarks/storage_engine.awan):

| Operation | Awan Native (`@flat` / `@arena`) | Node.js / Go S3 Baseline | Advantage / Speedup |
| :--- | :---: | :---: | :---: |
| **1,000,000 Metadata Ingestion** | **3.0 ms** | 234.2 ms | **78.1x faster** |
| **1,000,000 Metadata Prefix/Range Scan** | **1.0 ms** | 48.9 ms | **48.9x faster** |
| **10,000 Chunk Streaming (640 MB Payload)** | **33.0 ms** | 338.3 ms (Dynamic alloc) | **10.3x faster** |
| **Metadata RAM per 1,000,000 Objects** | **40.00 MB** | 113.96 MB (V8 Heap) | **2.85x less RAM (0 GC)** |
| **Hot Read Latency (Slab Hit)** | **< 1 µs** | 15 – 35 µs | **15x – 35x lower latency** |

---

## 3. Platform Document Index

This specification is organized across dedicated modular documents:

1. [**`ARCHITECTURE.md`**](ARCHITECTURE.md): Core storage engine architecture, memory model, zero-copy buffer handoff, and kernel I/O substrate.
2. [**`API_SPECIFICATION.md`**](API_SPECIFICATION.md): Wire-level S3-compatible REST API, streaming chunk framing, and multipart upload state machine.
3. [**`SDK_SPECIFICATION.md`**](SDK_SPECIFICATION.md): High-level client SDK specifications across Awan, TypeScript/Node.js, and C-ABI.
4. [**`DISTRIBUTED_TOPOLOGY.md`**](DISTRIBUTED_TOPOLOGY.md): Multi-node cluster replication, $N=3, W=2, R=2$ quorum consensus, consistent hash ring, and $8+4$ erasure coding.
5. [**`LICENSING_AND_COMMERCIAL.md`**](LICENSING_AND_COMMERCIAL.md): Enterprise licensing model, cryptographic hardware-bound license verification, capacity/node metering, and pricing tiers.
6. [**`CONFLICT_RESOLUTION_MATRIX.md`**](CONFLICT_RESOLUTION_MATRIX.md): Detailed conflict resolutions across scoped slab lifetimes, value semantics, VM vs native parity, and FFI panic boundaries.
7. [**`ROADMAP_AND_DEPENDENCIES.md`**](ROADMAP_AND_DEPENDENCIES.md): Phased execution roadmap, prerequisite issue gates (#53, #202, #339, #447, #451, #454), and B2B pilot milestones.
8. [**`IO_URING_IOCP_SPEC.md`**](IO_URING_IOCP_SPEC.md): Technical specification for the thread-per-core completion I/O reactor (Linux `io_uring` + Windows IOCP/RIO).
9. [**`CRYPTO_INTRINSICS_SPEC.md`**](CRYPTO_INTRINSICS_SPEC.md): Hardware-accelerated Intel SHA-NI, ARMv8 Crypto, and Castagnoli CRC32C intrinsics.
10. [**`ERASURE_CODING_SPEC.md`**](ERASURE_CODING_SPEC.md): $8+4$ Cauchy Reed-Solomon matrix formulation and SIMD vector look-up table (LUT) reconstruction.
11. [**`EDGE_CASES_AND_CONTRACTS.md`**](EDGE_CASES_AND_CONTRACTS.md): Comprehensive edge case handling: mid-stream disconnects, zero-byte folder markers, thundering herd coalescing, path traversal defenses, and S3 XML error code matrix.
12. [**`USE_CASES_AND_WORKLOADS.md`**](USE_CASES_AND_WORKLOADS.md): Industrial workload blueprints: sub-15ms AI agent sandbox restoration, PyTorch model checkpoints, distributed mutex locking, and video stream ingestion.
13. [**`COMPETITIVE_LANDSCAPE.md`**](COMPETITIVE_LANDSCAPE.md): S3 alternatives matrix: Open-source Phase 2 (MinIO, Ceph RGW, SeaweedFS, Garage) and hyperscale cloud Phase 3 (AWS S3 Standard & S3 Express One Zone, Azure Premium Blob, Google Cloud Storage, Cloudflare R2).
14. [**`HYBRID_POC_SPECIFICATION.md`**](HYBRID_POC_SPECIFICATION.md): Phase 2 Fast-Track PoC: Rust substrate (Hyper/Tokio, thread pinning, disk slab) + Awan compiled native `@flat` engine, MinIO differential fuzzing oracle, and live `warp` benchmark protocol.
15. [**`AUTONOMOUS_EXECUTION_RUNBOOK.md`**](AUTONOMOUS_EXECUTION_RUNBOOK.md): Self-contained start-to-finish execution playbook, decision table, gate commands, and multi-OS/multi-arch release binary matrix for Milestone #10 ([#462](https://github.com/iskandarnorazli/awan-lang/issues/462)–[#471](https://github.com/iskandarnorazli/awan-lang/issues/471)).
16. [**`TUTORIAL_AND_MIGRATION_GUIDE.md`**](TUTORIAL_AND_MIGRATION_GUIDE.md): Step-by-step drop-in S3 replacement tutorial across Python (`boto3`, Polars, DuckDB), TypeScript (`@aws-sdk/client-s3`, Bun), Go (`minio-go`), Rust (`object_store`), CLI (`aws s3`, `rclone`, `s5cmd`), Docker Compose, `systemd`, and zero-downtime MinIO migration.

