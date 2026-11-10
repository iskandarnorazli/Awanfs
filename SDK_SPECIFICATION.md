# Awan Storage Client SDK Specification

> **Client Bindings: Native Awan SDK, TypeScript/Node.js, and High-Performance C-ABI**

---

## 1. SDK Philosophy: Zero Overhead at the Client Boundary

Official AWS SDKs in Go, Java, and JavaScript allocate tens of thousands of intermediate heap objects per second, bottlenecking high-throughput workloads. The Awan Storage Client SDKs are designed to:
1. Provide a drop-in S3-compatible client interface.
2. Allow passing raw preallocated memory buffers (`@arena`, `Float32Array`, `Buffer`, or raw C pointers) directly into requests without cloning.
3. Automatically pool connections with persistent HTTP/1.1 and HTTP/2 keep-alive.

---

## 2. Native Awan SDK (`std/s3` / `libs/awan-storage`)

Designed for applications written in Awan (data pipelines, AI training scripts, web microservices):

```awan
import "storage.awan"

fn main() {
    let client = s3_client_new({
        "endpoint": "http://10.0.0.1:9000",
        "region": "us-east-1",
        "access_key": "AWAN_ACCESS_KEY",
        "secret_key": "AWAN_SECRET_KEY"
    });

    // 1. Streaming Upload from @arena slab
    var data_slab = [0; 65536]; // Preallocated buffer
    // populate data_slab...
    
    let put_res = s3_put(client, "agent-sandboxes", "snapshot-42.bin", data_slab);
    println("Upload status: " + to_string(put_res["status"]));

    // 2. High-Speed Object Retrieval
    let get_res = s3_get(client, "agent-sandboxes", "snapshot-42.bin");
    let payload = get_res["body"]; // Zero-copy view of the network buffer
    println("Received bytes: " + to_string(len(payload)));

    // 3. Fast Prefix Listing
    let list_res = s3_list_objects(client, "agent-sandboxes", "snapshot-");
    let count = list_res["key_count"];
    println("Matching sandboxes: " + to_string(count));
}
```

---

## 3. TypeScript / JavaScript SDK (`@awan/storage`)

Designed for Node.js, Bun, and browser-based agent sandboxes:

```typescript
import { AwanStorageClient } from "@awan/storage";

const client = new AwanStorageClient({
  endpoint: "http://storage.internal:9000",
  credentials: {
    accessKeyId: process.env.AWAN_KEY!,
    secretAccessKey: process.env.AWAN_SECRET!
  }
});

// Upload using preallocated Buffer or SharedArrayBuffer (Zero Copy)
const buffer = Buffer.allocUnsafe(1024 * 1024); // 1 MB
await client.putObject({
  bucket: "agent-checkpoints",
  key: "vm-mem-checkpoint.raw",
  body: buffer
});

// Stream directly into response or memory
const response = await client.getObject({
  bucket: "agent-checkpoints",
  key: "vm-mem-checkpoint.raw"
});
```

---

## 4. High-Performance C-ABI (`libawan_storage.so` / `.dll`)

For systems integration (C, C++, Rust, Zig, Firecracker microVM hypervisors, Python FFI):

```c
#include "awan_storage.h"

// Initialize client with connection pool
awan_storage_t* client = awan_storage_init("http://10.0.0.1:9000", "KEY", "SECRET");

// Upload contiguous memory buffer directly to S3 endpoint (Zero Copy)
const void* memory_ptr = get_vm_memory_base();
uint64_t memory_size = 536870912; // 512 MB

int status = awan_storage_put_raw(
    client, 
    "microvm-snapshots", 
    "snap_instance_102.raw", 
    memory_ptr, 
    memory_size
);

awan_storage_free(client);
```
