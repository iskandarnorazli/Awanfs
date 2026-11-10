`python awan-storage/scripts/build_release_matrix.py`

# S3 Server & Awan Compiler Release Binary Matrix Report (Wave 5)

- **Distribution Directory:** `awan-storage/dist/` (100% self-contained compiled binaries; zero `compiler_src/` source files shipped)
- **Integrity Manifest:** `awan-storage/dist/SHA256SUMS.txt`
- **Host Environment:** Windows x86_64 (`x86_64-pc-windows-msvc`)
- **Release Profile:** Optimized (`--release`), Bundled Awan Compiler (`awan`), Compiled VSSA Frontend (`awan-vssa-frontend`), AOT SDK (`awan-aot`), Linker (`awan_ld`), and S3 Server (`awan-storage-server`).

## 1. Multi-OS & Multi-Architecture Build & Verification Matrix

| Target Artifact | Rust Target Triple | Verification on Windows Host | Binary Size | SHA-256 Fingerprint |
| :--- | :--- | :--- | :--- | :--- |
| `awan-storage-server-x86_64-windows.exe` | `x86_64-pc-windows-msvc` | **Compiled (`cargo build --release`) + Smoke-Tested** | 2236.5 KiB | `e13d35a749e20759...` |
| `awan-compiler-x86_64-windows.exe` | `x86_64-pc-windows-msvc` | **Compiled (`cargo build --release --bin awan`) + Verified on `test`, `run` & `emit-llvm`** | 23307.5 KiB | `add891a1b01ea371...` |
| `awan-vssa-frontend-x86_64-windows.exe` | `x86_64-pc-windows-msvc` | **Compiled Native VSSA Frontend (`.awan` -> `.vssa.json`, zero `compiler_src/` needed)** | 1383.0 KiB | `fd4dd146b5314df2...` |
| `awan-aot-x86_64-windows.exe` | `x86_64-pc-windows-msvc` | **Compiled (`cargo build --release --bin awan-aot`)** | 5726.5 KiB | `3664c23c3e72c5d1...` |
| `awan-ld-x86_64-windows.exe` | `x86_64-pc-windows-msvc` | **Compiled (`AwanLink` Native Linker `--release`)** | 5216.0 KiB | `f67be21df72717d6...` |
| `awan-storage-server-x86_64-linux-gnu` | `x86_64-unknown-linux-gnu` | **`cargo check --target x86_64-unknown-linux-gnu` PASS** (linked in Linux CI `.github/workflows/awan-storage-ci.yml`) | built in Linux CI | built in Linux CI |
| `awan-storage-server-x86_64-linux-musl` | `x86_64-unknown-linux-musl` | **`cargo check --target x86_64-unknown-linux-musl` PASS** (linked in Linux CI) | built in Linux CI | built in Linux CI |
| `awan-storage-server-aarch64-linux-gnu` | `aarch64-unknown-linux-gnu` | **`cargo check --target aarch64-unknown-linux-gnu` PASS** (linked in Linux CI) | built in Linux CI | built in Linux CI |
| `awan-compiler-x86_64-linux-gnu` | `x86_64-unknown-linux-gnu` | **Built in Linux CI (`cargo build --release --bin awan --target x86_64-unknown-linux-gnu`)** | built in Linux CI | built in Linux CI |
| `awan-compiler-aarch64-linux-gnu` | `aarch64-unknown-linux-gnu` | **Built in Linux CI (`cargo build --release --bin awan --target aarch64-unknown-linux-gnu`)** | built in Linux CI | built in Linux CI |

## 2. Release Binary Smoke Test & Checksum Verification
- S3 Server binary tested: `awan-storage/dist/awan-storage-server-x86_64-windows.exe --port 9999`
- Awan Compiler binary tested standalone: `awan-storage/dist/awan-compiler-x86_64-windows.exe test awan-storage/tests/meta_index.test.awan` (3/3 passed) and `emit-llvm awan-storage/benchmarks/storage_engine.awan -o storage_bench.exe -r` (1,000,000 objects ingested in 17 ms, scanned in 3 ms)
- Full `SHA256SUMS.txt` entries:
  `e13d35a749e2075921502e93cdeadfc5ac1254fde8c822e463d264335022c65b  awan-storage-server-x86_64-windows.exe`
  `add891a1b01ea371c5ab96ea1a7c2f7e28f8b17e8ef1f7230e4790c4446a796e  awan-compiler-x86_64-windows.exe`
  `fd4dd146b5314df29ea55efc313deaa6ea5c37d4eefed0add968b5053d7a12aa  awan-vssa-frontend-x86_64-windows.exe`
  `3664c23c3e72c5d1a6b4e0474b3e045e9f6667a6cab083b959a2743fd6bc1258  awan-aot-x86_64-windows.exe`
  `f67be21df72717d6dd8c5312c47cfdbf42c147cda153cf3804b48598c678d41d  awan-ld-x86_64-windows.exe`
