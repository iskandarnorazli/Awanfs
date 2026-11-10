# Awan Storage Kernel I/O Reactor Specification (`io_uring` & IOCP)

> **Substrate Internals: Thread-per-Core Architecture, Zero-Syscall Ring Polling, and Dual-Platform Parity**

---

## 1. Thread-per-Core Reactor Model

Traditional multi-threaded servers use a shared work queue (e.g. Tokio / Go runtime) where worker threads compete for tasks. Under 50,000+ connections, thread synchronization, mutex contention, and CPU cache-line bouncing degrade throughput.

Awan Storage implements a strict **Thread-per-Core (Shared-Nothing) Architecture**:
- Each physical CPU core runs an isolated, single-threaded reactor instance.
- Each core is pinned to its hardware thread via CPU affinity (`pthread_setaffinity_np` on Linux, `SetThreadAffinityMask` on Windows).
- Each core owns a dedicated completion ring and preallocated `@arena` slab pool.
- Sockets are partitioned across cores using Linux `SO_REUSEPORT` or Windows `SO_REUSE_UNICASTPORT`. No memory or lock is ever shared between worker threads.

---

## 2. Linux Engine: `io_uring` with Registered Fixed Buffers

```
┌────────────────────────────────────────────────────────────────────────┐
│                        LINUX io_uring SUBSYSTEM                        │
│                                                                        │
│  Userland Worker Core                Linux Kernel Space                │
│  ┌────────────────────────┐         ┌───────────────────────────────┐  │
│  │  Submission Queue (SQ) ├────────►│  Kernel SQ Poller Thread      │  │
│  │   (Ring Buffer in RAM) │         │   (IORING_SETUP_SQPOLL)       │  │
│  └────────────────────────┘         └──────────────┬────────────────┘  │
│                                                    │ Zero Syscall DMA  │
│  ┌────────────────────────┐         ┌──────────────▼────────────────┐  │
│  │  Completion Queue (CQ) │◄────────┤  Direct NVMe / NIC DMA        │  │
│  │   (Ring Buffer in RAM) │         │   (IORING_REGISTER_BUFFERS)   │  │
│  └────────────────────────┘         └───────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### Key Linux Flags & Optimizations:
1. **`IORING_SETUP_SQPOLL` (Zero-Syscall Operation):**
   - A dedicated kernel thread continuously polls the Submission Queue (SQ).
   - The userland reactor writes SQEs directly to shared memory without issuing the `io_uring_enter` syscall. Network and disk operations execute with zero context switches.
2. **`IORING_REGISTER_BUFFERS`:**
   - Pre-registers the `@arena` memory slab with the kernel at startup.
   - The kernel pins the virtual memory pages once, eliminating per-I/O page table walks and allowing direct hardware DMA from network cards and NVMe controllers.
3. **`IORING_REGISTER_FILES`:**
   - Pre-registers storage file descriptors, bypassing file table lookup locks in the VFS.

---

## 3. Windows Engine: IOCP with Registered I/O (RIO)

On Windows, `awan-storage` utilizes the **Registered I/O (RIO)** extensions of Windows Sockets (Winsock) combined with I/O Completion Ports:

1. **`RIORegisterBuffer`:**
   - Similar to Linux fixed buffers, pins the Awan `@arena` memory slab into physical RAM.
2. **`RIOCreateCompletionQueue` & `RIOCreateRequestQueue`:**
   - Creates lock-free submission and completion queues directly bound to the network interface.
3. **`GetQueuedCompletionStatusEx` (Batch Dequeuing):**
   - Dequeues up to 64 completed requests in a single call, amortizing thread wakeup overhead.

---

## 4. Unified C-ABI & Runtime Built-in Interface

To ensure Core Invariant 10 (Dual-Platform Parity), both platforms implement an identical C ABI surface in `core/src/runtime/reactor.rs`:

```c
// Handle types
typedef struct awan_ring awan_ring_t;
typedef uint32_t slab_slot_t;

// Lifecycle
awan_ring_t* awan_ring_init(uint32_t queue_depth, uint32_t slot_count, uint32_t slot_size);
void awan_ring_free(awan_ring_t* ring);

// Memory Slab Leasing
slab_slot_t  awan_ring_lease_slot(awan_ring_t* ring);
void*        awan_ring_get_slot_ptr(awan_ring_t* ring, slab_slot_t slot);
void         awan_ring_release_slot(awan_ring_t* ring, slab_slot_t slot);

// Async Operations Submission (Non-Blocking)
void awan_ring_prep_recv(awan_ring_t* ring, intptr_t socket, slab_slot_t slot, uint32_t len);
void awan_ring_prep_send(awan_ring_t* ring, intptr_t socket, slab_slot_t slot, uint32_t len);
void awan_ring_prep_read(awan_ring_t* ring, intptr_t fd, slab_slot_t slot, uint64_t offset, uint32_t len);
void awan_ring_prep_write(awan_ring_t* ring, intptr_t fd, slab_slot_t slot, uint64_t offset, uint32_t len);

// Polling & Completion
uint32_t awan_ring_poll_completions(awan_ring_t* ring, uint32_t max_events, uint32_t timeout_us);
```

---

## 5. Awan Language Built-in Bindings (`catalog.rs`)

Exposed to Awan storage services as high-level asynchronous primitives:

| Built-in | Signature | Description |
| :--- | :--- | :--- |
| `sys_ring_create` | `(int, int, int) -> ring` | Initialize reactor with depth, slots, slot size. |
| `sys_ring_lease` | `(ring) -> int` | Lease an `@arena` slot index for an I/O operation. |
| `sys_ring_submit` | `(ring) -> int` | Submit queued entries to kernel. |
| `sys_ring_wait` | `(ring, int) -> list` | Wait for completion events (returns list of `[slot_id, bytes, err]`). |
| `sys_ring_release` | `(ring, int) -> nil` | Return leased slot to the available pool. |
