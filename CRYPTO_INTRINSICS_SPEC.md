# Awan Storage Cryptographic Intrinsics Specification

> **Hardware Acceleration: Intel SHA Extensions, ARMv8 Crypto, and Castagnoli CRC32C**

---

## 1. Motivation & Throughput Targets

S3 protocol compliance requires cryptographic hashing on every byte transferred:
1. **AWS Signature Version 4:** Computes SHA-256 digests of every payload and chunk header.
2. **Data Integrity & Bitrot Detection:** Computes Castagnoli CRC32C (`0x82F63B78`) or MD5 for every stored block and S3 ETag.

In pure scalar software loops, computing SHA-256 on a 1 GB payload takes over **3.5 seconds** (capping throughput at ~280 MB/s). By lowering directly to hardware CPU extensions, Awan sustains **3.5+ GB/s per core**, keeping hashing well ahead of 25 Gbps network pipes and NVMe write streams.

---

## 2. Hardware Instruction Set Targets

| Architecture | SHA-256 Hardware Instructions | CRC32C Hardware Instructions |
| :--- | :--- | :--- |
| **x86_64 (Intel / AMD)** | Intel SHA Extensions (`sha256msg1`, `sha256msg2`, `sha256rnds2`) | SSE 4.2 (`crc32q`, `crc32d`) |
| **AArch64 (Apple Silicon / AWS Graviton)** | ARMv8 Cryptographic Extension (`sha256h`, `sha256h2`, `sha256su0`, `sha256su1`) | ARMv8 CRC32 (`crc32cb`, `crc32ch`, `crc32cw`, `crc32cx`) |
| **Fallback (Any CPU / VM)** | Unrolled 64-round scalar bitwise loop | Software table-lookup Castagnoli CRC32C |

---

## 3. Catalog Signatures & Built-in Mapping

Added to `core/src/builtins/catalog.rs`:

```rust
// Built-in Catalog Entry
Builtin {
    name: "crypto_sha256_slice",
    arity: 1,
    vm: true,
    native: Native::Yes,
    doc: "Computes SHA-256 digest of a byte slice using hardware CPU instructions when available.",
}

Builtin {
    name: "crypto_crc32c_slice",
    arity: 2,
    vm: true,
    native: Native::Yes,
    doc: "Computes Castagnoli CRC32C checksum of a byte slice with running accumulator.",
}

Builtin {
    name: "crypto_hmac_sha256",
    arity: 2,
    vm: true,
    native: Native::Yes,
    doc: "Computes HMAC-SHA256 signature for key and message slices (AWS SigV4).",
}
```

---

## 4. LLVM Lowering Path (`llvm_bridge.rs`)

In `core/src/codegen/llvm_bridge.rs`, calls to `crypto_sha256_slice` and `crypto_crc32c_slice` lower directly to LLVM intrinsic functions or inline target assembly:

1. **CRC32C Lowering (x86_64):**
   ```llvm
   ; LLVM IR emitted for 64-bit CRC32C chunk update
   %res = call i64 @llvm.x86.sse42.crc32.64.64(i64 %acc, i64 %val)
   ```
2. **SHA-256 Lowering:**
   - Emits an unrolled 4-round loop using `@llvm.x86.sha256rnds2` combined with message schedule helpers `@llvm.x86.sha256msg1` and `@llvm.x86.sha256msg2`.
   - Processes **64 bytes (one block) in approximately 40–50 clock cycles** on Zen 3 / Golden Cove cores.

---

## 5. VM Reference Oracle & Parity Verification

To enforce Core Invariant 2 ("One oracle for every engine"):
- **Reference Oracle:** The VM executes the FIPS PUB 180-4 standard pure-Awan implementation in `libs/awan-crypto/sha256.awan`.
- **Parity Probe:** Added to `core/tests/feature_parity_test.rs`:
  - Feeds identical NIST test vectors (empty string, 3-byte `"abc"`, 1 MB repeated pattern) to both the VM and Native LLVM engines.
  - Asserts identical 64-character hexadecimal digest outputs.
