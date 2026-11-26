`gh workflow run bench-boot.yml --ref <ref> -f objects=2000 -f size=4194304` on `try/412-boot-before` (458af68, before #398) run 45756943590, and `main` (fb990a3) run 45756947053; GitHub-hosted `ubuntu-latest` and `windows-latest`, 4 logical cores.

# Boot of 2,000 objects of 4 MiB (#412)

Each run fills one volume with 2,000 objects of 4 MiB (8,468,568,136 bytes on disk), then boots it.
`boot_cold` is Linux only: the page cache is dropped first (`echo 3 > /proc/sys/vm/drop_caches`).
`boot` follows with the volume in the page cache. Numbers are the `secs` fields of the JSON lines
the job printed.

| Boot | 458af68 (headers only) | main (every payload's CRC, #398) | main / before |
|---|---|---|---|
| Linux, cold | 5.022 s | 20.234 s | 4.0x |
| Linux, warm | 0.354 s | 1.031 s | 2.9x |
| Windows, warm | 0.489 s | 1.826 s | 3.7x |

`scan` (`open_secs`, the volume open alone): Linux 0.144 s before, 0.912 s on main; Windows 0.241 s
before, 1.448 s on main.

Bytes read at boot: not measured. `boot_bench` prints disk read calls for `scan` (2,000 on both)
but no byte count, and the work counters do not count the bytes `verify_needle_integrity` reads.
Inferred from the cold boot: 8.47 GB in 20.2 s is about 420 MB/s, the order of a hosted runner's
disk bandwidth, so the cold boot on main reads the payloads, not just the headers.

What it means: boot time on main grows with the bytes stored. On this runner's disk, 1 TB of large
objects would take about 40 minutes to boot cold (inferred from the rate above, not measured).

Not done in this file: the fix's row. #412's first stop condition holds (a needle the journal
records can be torn, `server/tests/boot_verify_journal_test.rs`), so the fix waits for the
owner's decision on #412.
