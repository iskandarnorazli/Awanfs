# Object Storage PoC Benchmark Report (Phase 1)
> Workload: 1,000,000 Object Metadata Indexing & Range Scanning + 10,000 Chunks (640 MB Payload)

| Benchmark Metric | Awan Native (@flat SoA) | Node.js Baseline (AoS / Heap) | Speedup / Advantage |
| :--- | :---: | :---: | :---: |
| **1M Metadata Ingestion** | **3.0 ms** | 234.2 ms | **78.1x faster** |
| **1M Metadata Range Scan** | **1.0 ms** | 48.9 ms | **48.9x faster** |
| **10k Chunk Streaming (Dynamic Allocation)** | **33.0 ms** (Prealloc slab) | 338.3 ms (Buffer.alloc/chunk) | **10.3x faster** |
| **10k Chunk Streaming (Preallocated)** | **33.0 ms** | 37.2 ms | **Parity (~1.0x)** |
| **Metadata Memory Footprint** | **40.00 MB** (Contiguous SoA) | 113.96 MB (V8 Heap Objects) | **2.85x less RAM (0 GC churn)** |
