gh workflow run bench-large.yml --ref perf/155-positioned-reads -f sizes=16MiB,64MiB,256MiB -f runs=3 -f server_env="<setting>"

# Large GETs with positioned reads: the reader's settings (#155, step two)

`benchmarks/large_object_check.py` through `.github/workflows/bench-large.yml`, GitHub
`ubuntu-latest`, 4 CPUs, AwanFS and MinIO on the same runner, 3 runs per server, medians, all at
commit `4baf15f` (pooled read buffers). Each value is AwanFS against MinIO ("x" is AwanFS better
by); higher is better. Reads go through the page cache (`AWAN_DIRECT_READ_MIN` unset).

| setting (`server_env`) | run | first GET | repeat GET, 4 clients | AwanFS peak RSS / heap, MiB |
|---|---|---|---|---|
| defaults: 1 MiB chunks, read-ahead 1 | 43736773027 | 2.05-2.35x | 1.21-1.38x | 370.5 / 28.0 |
| defaults (the PR's own run) | 43736778838 | 2.19-2.26x | 1.24-1.39x | 370.6 / 28.1 |
| `AWAN_READ_CHUNK=1MiB AWAN_READ_AHEAD=4` | 43736778306 | 2.07-2.26x | 1.06-1.22x | 370.3 / 36.1 |
| `AWAN_READ_CHUNK=4MiB AWAN_READ_AHEAD=1` | 43736775662 | 1.70-1.99x | 0.95-1.00x | 418.6 / 78.1 |
| `AWAN_READ_CHUNK=4MiB AWAN_READ_AHEAD=4` | 43736781008 | 1.65-1.78x | 0.88-0.97x | 418.5 / 122.1 |

Ranges over the three sizes are 16, 64 and 256 MiB objects.

For comparison, same workflow:

| code | run | first GET | repeat GET, 4 clients | AwanFS peak RSS, MiB |
|---|---|---|---|---|
| main, through the mapping | 43722345701 | 1.76-1.96x | 1.30-1.46x | 1464.7 |
| positioned reads, a buffer allocated per chunk, direct | 43732366908 | 0.95-0.98x | 0.54-0.58x | 354.5 |
| positioned reads, a buffer allocated per chunk, cached | 43732369148 | 0.80-0.87x | 0.46-0.51x | 354.5 |

With the defaults, run 43736773027, every GET row is at or above MinIO's. 1 MiB range P50:
1.50-1.99x. GET of a multipart object: 1.10-1.27x. 4 KiB GET P99 during 4 large PUTs:
2.16-2.57x. MinIO's peak RSS: 365.3 MiB.

What changed the result: reusing read buffers (a pool of 16) instead of allocating and zeroing
one per chunk. Larger chunks and deeper read-ahead made it worse, and held more heap.
