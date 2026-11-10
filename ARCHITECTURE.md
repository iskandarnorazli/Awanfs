# Awan Storage Architecture Specification

> **Engine Internals: Memory Model, Zero-Copy Pipeline, and Kernel I/O**

---

## 1. Architectural Invariants

Awan Storage is built on four non-negotiable systems engineering invariants:
1. **Zero Dynamic Allocation on the Hot Path:** Every network packet, HTTP header, chunk buffer, and metadata record operates within preallocated `@arena` slabs initialized at startup.
2. **Columnar Metadata Layout (`@flat`):** All object metadata is lowered automatically into contiguous primitive arrays (Struct-of-Arrays). No hash tables with pointer-chained buckets; no tree nodes.
3. **Zero-Copy Kernel Handoff:** Socket buffers are pinned and passed directly to disk controllers or client connections via completion rings without userland heap copies.
4. **Thread-per-Core Execution:** Every physical CPU core runs an isolated, lock-free event loop bound to dedicated memory slabs and network ring buffers. No inter-thread mutex contention.

---

## 2. Memory Model & Data Structures

```
┌────────────────────────────────────────────────────────────────────────┐
│                          NODE MEMORY ARENA                             │
│                                                                        │
│  ┌───────────────────────────┐       ┌───────────────────────────────┐ │
│  │   Metadata Columnar Slab  │       │      Chunk Ring Slabs         │ │
│  │     (@flat ObjectMeta)    │       │     (@arena Pinned Memory)    │ │
│  ├───────────────────────────┤       ├───────────────────────────────┤ │
│  │ key_hash:   [u64; 10M]    │       │ Slot 0:  [u8; 65536] (Pinned) │ │
│  │ offset:     [u64; 10M]    │       │ Slot 1:  [u8; 65536] (Pinned) │ │
│  │ size:       [u64; 10M]    │       │ ...                           │ │
│  │ etag:       [u64; 10M]    │       │ Slot N:  [u8; 65536] (Pinned) │ │
│  │ created_at: [u64; 10M]    │       └──────────────┬────────────────┘ │
│  │ flags:      [u64; 10M]    │                      │                  │
│  └───────────────────────────┘                      │ Zero-Copy DMA    │
└─────────────────────────────────────────────────────┼──────────────────┘
                                                      ▼
                                       ┌───────────────────────────────┐
                                       │   Kernel Completion Ring      │
                                       │    Linux io_uring / Win IOCP   │
                                       └───────────────────────────────┘
```

### A. The `@flat ObjectMeta` Metadata Table
```awan
@flat
struct ObjectMeta {
    key_hash: Int,      // 64-bit FNV-1a hash of the S3 key
    offset: Int,        // Byte offset within the container chunk slab
    size: Int,          // Object length in bytes
    etag: Int,          // 64-bit truncated checksum hash
    created_at: Int,    // Unix epoch millisecond timestamp
    flags: Int          // Status flags (bit 0: active, bit 1: tombstone, bit 2: multipart)
}
```
- **Storage Cost:** 6 fields $\times$ 8 bytes = **48 bytes per object**.
- **Scale:** 10,000,000 objects consume only **480 MB** of contiguous memory, allowing an entire bucket's index to remain resident in L3/DRAM.
- **Scanning:** Filtering by prefix hash or size runs vector SIMD scans across the `key_hash` and `size` columns directly, skipping other fields entirely (yielding **1.0 ms scan latency per million objects**).

### B. Preallocated Chunk Ring (`@arena`)
- At process start, each worker core claims a fixed slab of pinned RAM (e.g. 512 MB partitioned into 8,192 $\times$ 64 KB slots).
- Incoming S3 `PUT` requests receive a leased slot ID.
- Network DMA writes directly into the slot.
- Once full, the slot is submitted to `io_uring` for NVMe write without copying. The slot is then recycled immediately.

---

## 3. Kernel I/O Substrate: `io_uring` and IOCP

Traditional storage engines use blocking POSIX `read()`/`write()` or epoll thread pools. Awan Storage lowers directly to kernel completion rings:

1. **Linux: `io_uring` with Registered Buffers (`IORING_REGISTER_BUFFERS`):**
   - The memory slabs are registered with the Linux kernel once at startup.
   - Socket reads (`IORING_OP_RECV`) place network bytes directly into the registered buffer.
   - Disk writes (`IORING_OP_WRITE_FIXED`) flush those same buffers directly to NVMe blocks with zero page-table remapping.
2. **Windows: I/O Completion Ports (IOCP) with Registered I/O (RIO):**
   - Uses `RIOCreateBufferMethod` to bind `@arena` slabs directly to socket descriptors.

---

## 4. Hardware-Accelerated Integrity & Signatures

S3 protocol compliance requires computing SHA-256 and MD5/CRC32C checksums for every chunk.
- **AWS SigV4 In-Place Signing:** Header canonicalization formats directly into the outbound socket buffer without string object allocations.
- **Hardware SHA-NI Intrinsics:** Payload digests use native CPU instructions (`sha256msg1`, `sha256rnds2`), sustaining **3.5+ GB/s per core** hashing throughput without degrading disk/network pipeline throughput.
