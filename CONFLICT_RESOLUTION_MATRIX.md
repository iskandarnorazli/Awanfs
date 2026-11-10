# Awan Storage Conflict Resolution & Design Invariant Matrix

> **Deep Systems Analysis: Memory Lifecycles, Value Semantics, Engine Parity, and Cross-Platform Boundaries**

---

## 1. Overview

Before writing production runtime code for `awan-storage`, we must identify and resolve all fundamental architectural, memory-model, and compiler invariants that could introduce race conditions, undefined behavior, or roadmap conflicts.

This document details the **5 critical conflict zones**, their failure modes, and their formal resolutions.

---

## 2. Conflict Matrix

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              CONFLICT RESOLUTION MATRIX                                │
├──────────────────────────────┬───────────────────────────────┬─────────────────────────┤
│ Conflict Domain              │ Root Tension                  │ Formal Resolution       │
├──────────────────────────────┼───────────────────────────────┼─────────────────────────┤
│ 1. Slab Memory vs Async I/O  │ Scoped @arena vs Kernel DMA   │ Leased Kernel Slabs     │
│ 2. Value Semantics vs In-Place│ Immutability vs Zero-Copy     │ Provably Unshared VSSA  │
│ 3. Hardware SIMD vs VM Parity│ LLVM Intrinsics vs Pure VM    │ Coupled Parity Oracle   │
│ 4. Rust FFI vs Runtime Panic │ Unwinding across C/JIT Stack  │ catch_unwind Error Code │
│ 5. Linux io_uring vs Windows │ Dual-platform parity (Rule 10)│ Unified Ring Reactor    │
└──────────────────────────────┴───────────────────────────────┴─────────────────────────┘
```

---

## 3. Deep Dive: Conflicts & Resolutions

### Conflict 1: `@arena` Scoped Lifecycles vs Kernel DMA Ring Buffers
- **The Tension:** In Awan, `@arena` memory slabs are conventionally scoped to the enclosing function or task lifecycle. However, asynchronous I/O (`io_uring` / IOCP) is non-blocking: a network socket read or disk write is submitted to the kernel, and the kernel may write to that buffer hundreds of milliseconds later. If the Awan stack frame unwinds or reclaims the `@arena` before the kernel emits a Completion Queue Entry (CQE), memory corruption occurs.
- **The Failure Mode:** Kernel DMA overwrites reclaimed memory $\to$ silent data corruption or segmentation fault.
- **Formal Resolution (Leased Kernel Slabs):**
  - Chunk slabs used for kernel I/O are managed by a dedicated **Pinned Slab Leaser** in the storage daemon runtime.
  - Slabs are registered with the OS kernel once (`IORING_REGISTER_BUFFERS` on Linux, `RIOCreateBufferMethod` on Windows).
  - An Awan task leases a slab slot by ID (`slot_id = ring_lease_slot()`).
  - The slot cannot be reused or freed until the completion reactor dequeues the kernel CQE and explicitly releases the lease (`ring_release_slot(slot_id)`).

---

### Conflict 2: Value Semantics & CoW (`MAKE_MUT`) vs High-Throughput In-Place Mutation
- **The Tension:** Awan enforces strict value semantics (Decisions N13/N14). By default, container index assignments (`arr[i] = v`) pass through the VSSA frontend's `vssa_lower_mut_place`, emitting copy-on-write checks (`VSSA_OP_MAKE_MUT`) to prevent aliasing.
- **The Failure Mode:** When streaming 10,000 chunks of 64 KB (640 MB), checking `MAKE_MUT` on every packet iteration adds 13,000,000 C runtime calls, causing severe register spilling and killing throughput (as diagnosed in [Issue #436](../docs/measurements/pipeline_plain_lists_ir_2026-11-08.md)).
- **Formal Resolution (Provably Unshared Analysis):**
  - Leverage the whole-program unshared container analysis implemented in `vssa_builder.awan`:
    - When a chunk ring buffer is allocated locally with a fresh literal or leased from the runtime slab, and never escapes to global references, `vssa_is_provably_unshared` returns `true`.
  - The compiler lowers writes directly to raw memory pointer stores (`vals[idx] = val`), bypassing `MAKE_MUT` and alloca reload/stores entirely.

---

### Conflict 3: Hardware-Accelerated Intrinsics vs "One Oracle for Every Engine" (Rule 2)
- **The Tension:** Rule 2 of `AGENTS.md` mandates that **the VM is the reference oracle for the language**. Every language feature must produce bit-identical results on both the VM and Native backends, verified by differential parity probes (`core/tests/feature_parity_test.rs`). If the Native backend lowers `sha256` to hardware CPU instructions (Intel SHA-NI), how does the VM comply?
- **The Failure Mode:** If Native and VM implement different hashing semantics, differential parity tests fail, or VM mocks produce different S3 ETags than production native binaries.
- **Formal Resolution (Coupled Reference Implementation):**
  - The VM executes the pure-Awan standard implementation in `libs/awan-crypto/sha256.awan` (FIPS PUB 180-4 compliant).
  - The Native compiler lowers the exact same built-in signature (`crypto_sha256_update`, `crypto_crc32c`) to hardware intrinsics in `llvm_bridge.rs`.
  - Both implementations are validated against the standard NIST test vectors in `feature_parity_test.rs`. The VM serves as the oracle for logical correctness; Native accelerates the execution.

---

### Conflict 4: Rust Runtime Safety & Panics across FFI Boundaries
- **The Tension:** Integrating `rustls` (for TLS 1.3) or low-level C libraries into Awan's runtime requires FFI transitions between the Awan C runtime (`awan_rt.c`), LLVM JIT code, and Rust.
- **The Failure Mode:** If an unexpected error or panic occurs in Rust code (e.g. malformed TLS handshake), unwinding across the `extern "C"` boundary is undefined behavior in Rust and immediately aborts the process (`SIGABRT`).
- **Formal Resolution (Catch Unwind & Error Code Protocol):**
  - Every Rust FFI entry point is wrapped in `std::panic::catch_unwind`.
  - Panics are intercepted and translated into structured, negative C integer error codes:
    ```rust
    #[no_mangle]
    pub extern "C" fn __awan_tls_read(ctx: *mut TlsContext, out: *mut u8, len: usize) -> i64 {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Safe Rust TLS read logic...
        }));
        match result {
            Ok(Ok(bytes_read)) => bytes_read as i64,
            Ok(Err(e)) => -1, // TLS protocol error
            Err(_) => -999,   // Intercepted panic
        }
    }
    ```
  - Awan runtime converts negative return codes into standard S3 error responses (`HTTP 500 InternalServerError`).

---

### Conflict 5: Cross-Platform Parity (Linux `io_uring` vs Windows IOCP/RIO)
- **The Tension:** Rule 10 of `AGENTS.md` establishes that **Windows and Linux are both first-class platforms**. `io_uring` is exclusive to Linux kernel 5.1+; Windows does not have `io_uring` and uses I/O Completion Ports (IOCP) or Registered I/O (RIO).
- **The Failure Mode:** Writing Linux-only `io_uring` code leaves Windows unsupported, breaking CI and violating Core Invariant 10.
- **Formal Resolution (Unified Completion Ring Interface):**
  - Implement a common completion-based abstraction in `core/src/runtime/reactor.rs`:
    ```
    sys_ring_init(queue_depth) -> RingHandle
    sys_ring_register_buffers(ring, ptr, size) -> BufferHandle
    sys_ring_prep_recv(ring, socket, buffer_id, offset, len)
    sys_ring_prep_write(ring, file_fd, buffer_id, offset, len)
    sys_ring_submit_and_wait(ring, min_complete, timeout_ms) -> [CompletionEvent]
    ```
  - Backends are cleanly isolated behind conditional compilation (`#[cfg(target_os = "linux")]` uses `io_uring`, `#[cfg(target_os = "windows")]` uses IOCP). Both platforms run the exact same Awan storage engine source.
