# Competitive Benchmark Landscape & Alternatives (`awan-storage`)

**Version**: 1.0.0-SPEC  
**Module**: S3-Compatible Storage Multi-Stack Competitive Matrix  
**Parent Plan**: [`README.md`](README.md)  

---

## 1. The S3-Compatible Storage Landscape

`awan-storage` is evaluated across two distinct competitive horizons:
1. **Phase 2 (Pre-Commercial)**: Open-source self-hosted S3 storage engines deployed on bare metal and private VPCs (MinIO, RustFS, Ceph RGW, SeaweedFS, Garage).
2. **Phase 3 (Commercialization)**: Public hyperscale cloud object storage services (AWS S3, Azure Blob, Google Cloud Storage, Cloudflare R2).

---

## 2. Phase 2: Open-Source S3 Engines Matrix (Pre-Commercial Benchmarks)

| Engine | Implementation Language | Storage Architecture | Protocol | Strengths | Architectural Vulnerabilities |
|---|---|---|---|---|---|
| **MinIO** | Go + AVX-512 Assembly | Single/Multi-Drive Erasure Coding ($8+4$, $12+4$) | S3 REST API | High-throughput on large files; direct AVX-512 assembly for Reed-Solomon and HighwayHash. | **Go Runtime GC churn**: struggles on millions of small metadata objects ($<64\text{ KB}$); high memory footprint per 1M objects (114 MB vs Awan 40 MB); complex AGPLv3 commercial licensing. |
| **RustFS** | Rust | Multi-volume direct file backend | S3 REST API | Pure Rust implementation, zero GC pauses, low idle memory footprint. | Single-handle I/O bottlenecks on Windows, slower small-object PUT latency, entity size restrictions. |
| **Ceph RADOS Gateway (Ceph RGW)** | C++ | Distributed Object Storage Daemon (OSD) cluster | S3 REST / Swift | Extreme petabyte-scale durability, enterprise multi-tenancy, POSIX/block/object unified. | **Heavy operational footprint**: complex multi-node daemon setup; high latency floors (15–40 ms) due to multi-layer RADOS object indexing; poor performance on small objects. |
| **SeaweedFS** | Go | Facebook Haystack-style volume files + Master metadata | S3 REST / Filer | Exceptional small-object IOPS ($O(1)$ disk seeks via sequential volume append). | S3 compatibility layer is secondary to internal volume protocol; high memory usage for master metadata when scaling to billions of keys; Go GC pauses. |
| **Garage** | Rust | Lightweight geo-distributed CRDTs + local sled/sqlite index | S3 REST API | Zero garbage collection; tiny binary footprint; designed for edge clusters and low-resource nodes. | Limited throughput on high-throughput NVMe RAID arrays; primarily targeted at low-power geo-replication rather than 100 Gbps compute clusters. |
| **Zenko / CloudServer** | Scality (Node.js / C++) | Multi-cloud abstraction gateway over backends | S3 REST API | Unified control plane across multi-cloud object stores; metadata search integration. | Node.js V8 event-loop bottlenecks; high latency variance under massive concurrent multi-part uploads. |

---

## 3. Phase 3: Hyperscale Cloud Storage Matrix (Commercialization Benchmarks)

Once `awan-storage` enters its commercial GA tier, it benchmarks against public hyperscale cloud providers:

| Cloud Service | Typical Read Latency (p50) | Ingress / Egress Pricing | API Request Pricing | Target Workloads & Vulnerabilities |
|---|---|---|---|---|
| **Amazon S3 Standard** | 15 – 35 ms | Free Ingress; **$0.09 / GB** Egress | $0.005 / 1,000 PUTs | High petabyte durability, but high latency floor; prohibitive egress bandwidth bills for AI/analytics workloads. |
| **Amazon S3 Express One Zone** | 2 – 8 ms | Free Ingress; $0.09 / GB Egress | **$0.0025 / 1,000 PUTs (50% cheaper)** | High-performance single-AZ tier for AI training; storage base price is **7x higher** ($0.16/GB/mo vs $0.023/GB/mo). |
| **Azure Blob Storage (Premium)** | 3 – 10 ms | Free Ingress; $0.087 / GB Egress | $0.005 / 1,000 PUTs | Solid-state drive (SSD) backed performance for Azure HPC and machine learning workloads. |
| **Google Cloud Storage (GCS)** | 15 – 30 ms | Free Ingress; $0.12 / GB Egress | $0.005 / 1,000 PUTs | Dual-region 15-minute RPO turbo replication; high egress costs. |
| **Cloudflare R2** | 25 – 45 ms | **$0.00 Egress (Free)** | $4.50 / 1M Class A operations | Global edge caching with zero egress fees, but higher latency floors than local bare-metal NVMe. |

---

## 4. Awan Storage (`awan-storage`) Architectural Positioning

`awan-storage` delivers **local bare-metal NVMe speeds with standard S3 protocol compatibility**:
1. **Hot Read Latency**: Sub-microsecond reads from pinned `@arena` memory slabs and zero-copy mmap extent slices vs 15–35 ms on AWS S3 Standard and 2–8 ms on S3 Express One Zone.
2. **Metadata Density**: 1,000,000 object records indexed in **40 MB of RAM** and scanned in **1.0 ms**, outperforming MinIO by 2.85x in memory density.
3. **Chunk Streaming Bandwidth**: Sustained wire-rate NVMe streaming using 32 GiB sparse virtual volumes and staged multipart part files, outpacing MinIO by up to 41x on small objects and 5.8x on 1 MiB writes.
4. **Zero Egress Tax**: Deployed directly alongside high-density AI agent sandboxes (BoxCompute/KotakSakti), eliminating cloud egress fees entirely.

---

## 5. Phase 2 Benchmark Suite & Methodology

The Phase 2 head-to-head benchmark matrix executes using the standard HTTP/1.1 persistent keep-alive harness (`benchmarks/s3_warp/run_warp_benchmark.py`):

```
[ Load Generator: run_warp_benchmark.py (Multi-Threaded HTTP/1.1 S3 Client) ]
                     │ (Dedicated Client Cores 4..7)
                     ▼ (Loopback / TCP Keep-Alive)
[ Target Server (Server Cores 0..3): s3-server vs MinIO vs RustFS ]
```

### Benchmark Scenarios:
1. **Small-Object Saturation (64 KiB Objects)**: 64 concurrent client workers measuring metadata indexing, HTTP framing, and needle write latency.
2. **Medium-Object Saturation (1 MiB Objects)**: 32 concurrent client workers measuring sustained PUT and wire-rate GET streaming.
3. **Mixed Read/Write Concurrency**: 80% GET, 20% PUT under 32 and 64 concurrent workers.
4. **Concurrency Scaling Sweep**: Evaluating queueing stability across C in {8, 32, 64, 128} workers.
5. **Large-Object Multi-Part Streaming (1 GiB – 10 GiB Objects)**: Parallel 64 MiB part streaming and parallel RFC 9110 Range GET reads.

---

## 6. Which S3 Metrics Matter in Practice (Concurrency vs IOPS vs MiB/s vs Tail Latency)

When infrastructure teams evaluate S3-compatible engines (`warp`, `s5cmd`, `rclone`, PyTorch DataLoader, DuckDB/Polars), they look at four distinct axes depending on object size:

| Metric | What It Measures | When Engineers Care Most | How `s3-server` Performs (Measured Live in [`s3_warp_minio_vs_awan.md`](measurements/s3_warp_minio_vs_awan.md)) |
| :--- | :--- | :--- | :--- |
| **Operations/sec (IOPS)** | Metadata lookup, HTTP framing, SigV4 auth, and small-needle disk append rate | **Small Objects (`<= 64 KiB`)** & **Byte-Range Reads (`206 Partial Content`)** (e.g., Parquet footer/column headers, Safetensors metadata, JSON logs) | **5,590 PUT ops/s** (**41.1x faster** vs MinIO 136 ops/s), **5,425 GET ops/s** (**1.50x faster** vs MinIO 3,610 ops/s), **5,907 Range GET ops/s** (**1.64x faster** vs MinIO 3,612 ops/s) at 64-client concurrency |
| **Throughput (`MiB/s` / `GiB/s`)** | Sustained NVMe + socket DMA bandwidth | **Large Objects (`1 MiB` – `10 GiB`)** & **Multipart Uploads (`16 x 64 MiB` parts)** (e.g., LLM checkpoints, container snapshots, Parquet blobs) | **737.3 MiB/s** (1 MiB PUT, **5.84x faster** vs MinIO 126.3 MiB/s), **2,165.6 MiB/s** (1 MiB GET, **1.65x faster** vs MinIO 1,307.7 MiB/s), **1,734.5 MiB/s** (1 MiB Mixed 80/20, **3.61x faster** vs MinIO 481.0 MiB/s), **1,412.7 MiB/s** (10 GiB Volume Parallel GET vs MinIO 1,149.2 MiB/s) |
| **Concurrency Scaling & P99 / P99.9 Tail Latency** | Queueing stability under 8–128 parallel client connections (whether `SingleFlight`, sparse mmap, and staged MPU prevent latency spikes) | **Multi-tenant AI/Analytics Clusters** where 64+ workers read/write simultaneously; Go-based stores suffer GC tail spikes (`>100 ms`) and file-per-object stores suffer directory lock contention | At `C=128`: Throughput is **324.1 MiB/s** (**8.57x faster** vs MinIO 37.8 MiB/s, **8.23x faster** vs RustFS 39.4 MiB/s); P99 tail latency is **60.03 ms** (**20.6x lower** vs MinIO 1,239.43 ms, **16.0x lower** vs RustFS 958.97 ms) |
| **Peak RSS Memory & Uptime Stability** | Server resident memory under sustained load | **Edge / Sidecar / Co-located Compute Nodes** where the storage daemon cannot steal GBs of RAM from GPU/AI workers | **100.6 MB** (64 KiB workload) and **245.6 MB** (1 MiB workload) vs MinIO **936.4 MB** (Go GC Heap); bounded 16 MiB slab cache prevents memory growth |
