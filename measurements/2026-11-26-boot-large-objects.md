`gh workflow run bench-boot.yml --ref <ref> -f objects=2000 -f size=4194304` on `try/412-boot-before` (458af68, before #398) run 45756943590, and `main` (fb990a3) run 45756947053, and the fix (`issue/412-boot-verify-tail` at ca6be2b) run 45765662395; GitHub-hosted `ubuntu-latest` and `windows-latest`, 4 logical cores.

# Boot of 2,000 objects of 4 MiB (#412)

Each run fills one volume with 2,000 objects of 4 MiB (8,468,568,136 bytes on disk), then boots it.
`boot_cold` is Linux only: the page cache is dropped first (`echo 3 > /proc/sys/vm/drop_caches`).
`boot` follows with the volume in the page cache. Numbers are the `secs` fields of the JSON lines
the job printed.

| Boot | 458af68 (headers only) | main (every payload's CRC, #398) | fix (#412, option B) |
|---|---|---|---|
| Linux, cold | 5.022 s | 20.234 s | 6.328 s |
| Linux, warm | 0.354 s | 1.031 s | 0.109 s |
| Windows, warm | 0.489 s | 1.826 s | 0.480 s |

`scan` (`open_secs`, the volume open alone): Linux 0.144 s before, 0.912 s on main, 0.012 s with
the fix; Windows 0.241 s before, 1.448 s on main, 0.036 s with the fix.

Payload bytes checked at open (`open_payload_verified_bytes`, added by the fix): 0 of 8,388,608,000
on both platforms with the fix, since every needle is journaled. Not printed before the fix; on
main the open checks all 2,000 payloads (`verify_needle_integrity` has no skip there).

Bytes read at boot: not measured as a byte count. Inferred from the cold boot: 8.47 GB in 20.2 s on
main is about 420 MB/s, the order of a hosted runner's disk bandwidth, so main reads the payloads.
With the fix the cold boot (6.3 s) is near the headers-only boot before #398 (5.0 s).

What it means: boot time on main grows with the bytes stored; on this runner's disk, 1 TB of large
objects would take about 40 minutes to boot cold (inferred from the rate above, not measured).
With the fix, boot reads headers and the payloads of needles the journal does not record. The fix
is the owner's option B on #412: a record is written only after the volume sync that covers its
needle, so a recorded needle cannot be torn (`server/tests/boot_verify_journal_test.rs`).
