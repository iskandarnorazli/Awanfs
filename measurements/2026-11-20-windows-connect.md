Command: `python -u benchmarks/large_volume_probe.py --server-bin server/target/release/s3-server --bare-bin server/target/release/examples/bare_listeners --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --connect-only --connect-rounds 3 --report connect_probe.md` (`.github/workflows/probe-sync.yml`, run 44682249037, commit 1b9f30e)

# Windows loopback connect cost: bare std and tokio listeners against s3-server and MinIO (#320)

Run [44682249037](https://github.com/iskandarnorazli/Awanfs/actions/runs/44682249037) on draft PR #338 (commit `1b9f30e`).
Jobs: `probe (windows-latest)` [136108145565](https://github.com/iskandarnorazli/Awanfs/actions/runs/44682249037/job/136108145565), `probe (ubuntu-latest)` [136108145219](https://github.com/iskandarnorazli/Awanfs/actions/runs/44682249037/job/136108145219).

Settings:
- Client: `http.client.HTTPConnection("127.0.0.1", port, timeout=600.0).connect()` followed by a 64 KiB `GET /probe/connect-probe` on the same open socket (`benchmarks/large_volume_probe.py::measure_connect`).
- Listeners on `127.0.0.1`:
  - `bare std`: `server/examples/bare_listeners.rs --mode std` (`std::net::TcpListener`, `std::thread::spawn`, no Tokio).
  - `bare tokio`: `server/examples/bare_listeners.rs --mode tokio` (`tokio::net::TcpListener`, `tokio::runtime::Builder::new_multi_thread().enable_all()`, `tokio::spawn`, no `hyper` or `s3-server` reactor).
  - `s3-server`: unmodified `server/src/reactor.rs` + `server/src/main.rs` (`--allow-anonymous --auto-compaction off`).
  - `MinIO`: `MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z` (`go1.27.4` on `windows/amd64` and `linux/amd64`).
- 300 fresh connections per round, 3 rounds per platform in alternating order (`round 1`: `bare std -> bare tokio -> s3-server -> MinIO`; `round 2`: `MinIO -> s3-server -> bare tokio -> bare std`; `round 3`: `bare tokio -> s3-server -> MinIO -> bare std`).

## Combined summary (`windows-latest` and `ubuntu-latest`)

| platform | listener | connect P50 ms, median (min..max) | connect P99 ms, median (min..max) | open-conn GET P50 ms, median (min..max) |
|---|---|---|---|---|
| `windows-latest` | `bare std` | `15.39` (`15.39..15.43`) | `21.35` (`21.23..24.58`) | `0.25` (`0.25..0.28`) |
| `windows-latest` | `bare tokio` | `15.43` (`15.41..15.45`) | `21.24` (`21.15..21.34`) | `0.24` (`0.23..0.24`) |
| `windows-latest` | `s3-server` | `15.39` (`15.36..15.39`) | `21.28` (`21.28..23.18`) | `0.37` (`0.36..0.37`) |
| `windows-latest` | `MinIO` | `0.34` (`0.33..0.50`) | `25.76` (`25.22..25.92`) | `0.90` (`0.90..1.01`) |
| `ubuntu-latest` | `bare std` | `0.05` (`0.04..0.05`) | `0.08` (`0.07..0.08`) | `0.11` (`0.11..0.11`) |
| `ubuntu-latest` | `bare tokio` | `0.04` (`0.04..0.06`) | `0.07` (`0.07..0.16`) | `0.09` (`0.09..0.11`) |
| `ubuntu-latest` | `s3-server` | `0.05` (`0.05..0.06`) | `0.08` (`0.08..0.08`) | `0.14` (`0.14..0.17`) |
| `ubuntu-latest` | `MinIO` | `0.06` (`0.06..0.06`) | `0.08` (`0.08..0.09`) | `0.50` (`0.50..0.51`) |

## Per-round detail

### Windows (`windows-latest`, `runnervmm7ud1`, `Windows-2025Server-10.0.26100-SP0`, 4 CPUs, data under `D:\a\_temp`)

| listener | round 1 (connect P50, P99; GET P50 ms) | round 2 (connect P50, P99; GET P50 ms) | round 3 (connect P50, P99; GET P50 ms) |
|---|---|---|---|
| `bare std` | `15.43, 24.58; 0.25` | `15.39, 21.23; 0.25` | `15.39, 21.35; 0.28` |
| `bare tokio` | `15.41, 21.24; 0.23` | `15.45, 21.15; 0.24` | `15.43, 21.34; 0.24` |
| `s3-server` | `15.36, 21.28; 0.37` | `15.39, 21.28; 0.37` | `15.39, 23.18; 0.36` |
| `MinIO` | `0.33, 25.76; 0.90` | `0.34, 25.22; 0.90` | `0.50, 25.92; 1.01` |

Ratios (`windows-latest`):
- `bare tokio / bare std` connect P50 ratio: `1.00x`
- `s3-server / bare tokio` connect P50 ratio: `1.00x`
- `s3-server / MinIO` connect P50 ratio: `45.88x`

### Linux (`ubuntu-latest`, `runnervmccnms`, `Linux-7.0.0-1019-azure-x86_64-with-glibc2.43`, 4 CPUs, data under `/home/runner/work/_temp`)

| listener | round 1 (connect P50, P99; GET P50 ms) | round 2 (connect P50, P99; GET P50 ms) | round 3 (connect P50, P99; GET P50 ms) |
|---|---|---|---|
| `bare std` | `0.04, 0.07; 0.11` | `0.05, 0.08; 0.11` | `0.05, 0.08; 0.11` |
| `bare tokio` | `0.04, 0.07; 0.09` | `0.06, 0.16; 0.11` | `0.04, 0.07; 0.09` |
| `s3-server` | `0.05, 0.08; 0.14` | `0.06, 0.08; 0.17` | `0.05, 0.08; 0.14` |
| `MinIO` | `0.06, 0.08; 0.51` | `0.06, 0.09; 0.50` | `0.06, 0.08; 0.50` |

Ratios (`ubuntu-latest`):
- `bare tokio / bare std` connect P50 ratio: `0.90x`
- `s3-server / bare tokio` connect P50 ratio: `1.08x`
- `s3-server / MinIO` connect P50 ratio: `0.78x`

## What it says

- **The cost does not come from AwanFS's reactor.** On `windows-latest`, `s3-server` (`15.39 ms` P50, `15.36..15.39 ms`), `bare tokio` (`15.43 ms` P50, `15.41..15.45 ms`), and `bare std` (`15.39 ms` P50, `15.39..15.43 ms`) all measure `1.00x` of each other at P50 and `21.24–21.35 ms` at P99 across all three rounds. `server/src/reactor.rs` (`hyper`, `AutoConnBuilder`, `TokioTimer`, `GracefulShutdown`, `set_nodelay`) adds zero measurable connect overhead relative to a bare `std::net::TcpListener`.
- **The cost does not come from Tokio relative to `std`.** A bare `std::net::TcpListener` with no Tokio runtime initialized hits the exact same `15.39 ms` P50 (`bare tokio / bare std = 1.00x`) on `windows-latest`, and `0.05 ms` vs `0.04 ms` on `ubuntu-latest`.
- **The difference sits between all three Rust `TcpListener` processes (`std`, `tokio`, `s3-server`) and Go's MinIO (`net`) on Windows under the same Python `HTTPConnection("127.0.0.1", port, timeout=600.0).connect()` client.** MinIO connects in `0.34 ms` at P50 (`0.33..0.50 ms`), while still hitting the ~15–25 ms Windows tick at P99 (`25.76 ms`, `25.22..25.92 ms`), whereas all three Rust `TcpListener` binaries hit the ~15.4 ms Windows timer tick at P50 (`>50%` of connects). Meanwhile, once the socket is open, 64 KiB GET P50 is faster on all three Rust listeners (`0.24–0.37 ms`) than on MinIO (`0.90 ms`).
- **Scope (`#320`).** No `server/src/` change was made in this issue; whether to leave `reactor.rs` alone, adjust the benchmark client (`#248` Option A), or pursue a separate Windows `AcceptEx` / socket change is for a separate follow-up issue.
