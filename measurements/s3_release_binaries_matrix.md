`python awan-storage/scripts/build_release_matrix.py`

# S3 Server Standalone Release Binary Matrix Report

- **Distribution Directory:** `awan-storage/dist/` (100% self-contained compiled Rust binary; zero external dependencies)
- **Integrity Manifest:** `awan-storage/dist/SHA256SUMS.txt`
- **Host Environment:** Windows x86_64 (`x86_64-pc-windows-msvc`)
- **Release Profile:** Optimized (`--release`), Pure Rust S3 Server (`s3-server`).

## 1. Multi-OS & Multi-Architecture Build & Verification Matrix

| Target Artifact | Rust Target Triple | Verification on Host | Binary Size | SHA-256 Fingerprint |
| :--- | :--- | :--- | :--- | :--- |
| `s3-server-x86_64-windows.exe` | `x86_64-pc-windows-msvc` | **Compiled (`cargo build --release`) + Smoke-Tested** | 2237.5 KiB | `55809eb1c35fa58e...` |
| `s3-server-x86_64-linux-gnu` | `x86_64-unknown-linux-gnu` | **`cargo check --target x86_64-unknown-linux-gnu` PASS** | Built in Linux CI | Built in Linux CI |
| `s3-server-x86_64-linux-musl` | `x86_64-unknown-linux-musl` | **`cargo check --target x86_64-unknown-linux-musl` PASS** | Built in Linux CI | Built in Linux CI |
| `s3-server-aarch64-linux-gnu` | `aarch64-unknown-linux-gnu` | **`cargo check --target aarch64-unknown-linux-gnu` PASS** | Built in Linux CI | Built in Linux CI |

## 2. Release Binary Smoke Test & Checksum Verification
- S3 Server binary tested: `awan-storage/dist/s3-server-x86_64-windows.exe --port 9999`
- Full `SHA256SUMS.txt` entry:
  `55809eb1c35fa58e9e7877d8fe91b030a72fe611f60420a548dc0faf39ddeb29  s3-server-x86_64-windows.exe`
