//! #207, decision 59: the metadata index at N keys, redb as AwanFS uses it (`spill::SpillIndex`)
//! against a minimal segment prototype. A measurement, not server code.
//!
//!   meta_index_bench build --store redb|seg --dir DIR --keys N
//!   meta_index_bench query --store redb|seg --dir DIR --keys N [--gets 1000000] [--scans 1000]
//!
//! Each run prints one JSON line. `build` inserts N keys in a scrambled order, as PUTs arrive.
//! `query` opens what `build` left (its time is the open time), gets present and absent keys,
//! and scans prefixes. Read system calls (`reads_per_get`) are counted by the OS for both
//! stores, and peak RSS is the process's own. `answers` is a hash of every answer, so the
//! workflow can check that both stores answered the same.
//!
//! The prototype: a memtable of up to 1M rows, as the server's in-memory index holds today,
//! flushed to an immutable sorted segment. A segment is 4 KiB blocks of (key, 64-byte row),
//! then a footer of fence keys (the first key of each block) and a blocked bloom filter, both
//! loaded into heap at open. A get checks the filter, finds the block by its fence, and reads
//! that one block with a positioned read, never a mapping (decision 58). A manifest names the
//! live segments, so open reads footers and nothing else. There is no compaction.

use s3_server::meta_index::ObjectMeta;
use s3_server::spill::SpillIndex;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

const BUCKET: u32 = 1;
const MEMTABLE_ROWS: usize = 1_000_000;
const BLOCK: usize = 4096;
const BLOOM_BITS_PER_KEY: u64 = 10;
const BLOOM_PROBES: u32 = 7;
const MAGIC: u64 = 0x4157_4E53_4547_3031; // "AWNSEG01"
const PERMUTE: u64 = 1_000_003;

/// A positioned read through the page cache: no shared cursor, no mapping.
fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short read"));
            }
            done += n;
        }
        Ok(())
    }
}

fn h64(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
}

/// The i-th key: S3-like, 25 to 75 bytes, 500 keys under each "logs/hNNN/dNN/" prefix at 10M.
fn key(i: u64) -> Vec<u8> {
    const HEX: &[u8] = b"0123456789abcdef0123456789abcdef0123456789abcdef";
    let tail = (h64(&i.to_le_bytes()) % 48) as usize;
    let mut k = format!("logs/h{:03}/d{:02}/{:010}-", i % 200, (i / 200) % 100, i).into_bytes();
    k.extend_from_slice(&HEX[..tail]);
    k
}

fn absent_key(i: u64) -> Vec<u8> {
    format!("logs/h{:03}/d{:02}/absent-{:010}", i % 200, (i / 200) % 100, i).into_bytes()
}

fn row(i: u64) -> ObjectMeta {
    ObjectMeta {
        key_hash: h64(&key(i)),
        bucket_flags: 1,
        vol_offset: i * 4096,
        size: i,
        etag_hi: i ^ 0x5555,
        etag_lo: !i,
        created_at: 1_700_000_000_000_000_000 + i,
        key_slice: 0,
    }
}

fn encode(m: &ObjectMeta) -> [u8; 64] {
    let mut b = [0u8; 64];
    for (j, v) in [m.key_hash, m.bucket_flags, m.vol_offset, m.size, m.etag_hi, m.etag_lo, m.created_at, m.key_slice].iter().enumerate() {
        b[j * 8..j * 8 + 8].copy_from_slice(&v.to_le_bytes());
    }
    b
}

fn decode(b: &[u8]) -> ObjectMeta {
    let f = |j: usize| u64::from_le_bytes(b[j * 8..j * 8 + 8].try_into().unwrap());
    ObjectMeta {
        key_hash: f(0),
        bucket_flags: f(1),
        vol_offset: f(2),
        size: f(3),
        etag_hi: f(4),
        etag_lo: f(5),
        created_at: f(6),
        key_slice: f(7),
    }
}

trait Store {
    fn put(&mut self, key: &[u8], row: &ObjectMeta);
    fn get(&mut self, key: &[u8]) -> Option<ObjectMeta>;
    fn scan(&mut self, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, ObjectMeta)>;
    fn finish(&mut self);
}

struct Redb(SpillIndex);

impl Store for Redb {
    fn put(&mut self, key: &[u8], row: &ObjectMeta) {
        self.0.put(BUCKET, key, row).expect("redb put");
    }
    fn get(&mut self, key: &[u8]) -> Option<ObjectMeta> {
        self.0.get(BUCKET, key).expect("redb get")
    }
    fn scan(&mut self, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, ObjectMeta)> {
        self.0.scan_prefix(BUCKET, prefix, None, limit).expect("redb scan")
    }
    fn finish(&mut self) {}
}

/// A blocked bloom filter: each key sets `BLOOM_PROBES` bits in one 512-bit block.
struct Bloom {
    blocks: Vec<[u64; 8]>,
}

impl Bloom {
    fn with_keys(n: usize) -> Bloom {
        let nblocks = ((n as u64 * BLOOM_BITS_PER_KEY).div_ceil(512)).max(1) as usize;
        Bloom { blocks: vec![[0u64; 8]; nblocks] }
    }
    fn probes(&self, key: &[u8]) -> (usize, [u32; BLOOM_PROBES as usize]) {
        let h = h64(key);
        let block = (h % self.blocks.len() as u64) as usize;
        let (mut a, b) = ((h >> 32) as u32, (h as u32) | 1);
        let mut bits = [0u32; BLOOM_PROBES as usize];
        for bit in bits.iter_mut() {
            *bit = a % 512;
            a = a.wrapping_add(b);
        }
        (block, bits)
    }
    fn insert(&mut self, key: &[u8]) {
        let (block, bits) = self.probes(key);
        for bit in bits {
            self.blocks[block][(bit / 64) as usize] |= 1 << (bit % 64);
        }
    }
    fn may_contain(&self, key: &[u8]) -> bool {
        let (block, bits) = self.probes(key);
        bits.iter().all(|&bit| self.blocks[block][(bit / 64) as usize] & (1 << (bit % 64)) != 0)
    }
}

struct Segment {
    file: File,
    fences: Vec<Vec<u8>>,
    bloom: Bloom,
}

impl Segment {
    fn write(path: &Path, rows: &BTreeMap<Vec<u8>, [u8; 64]>) -> Segment {
        let mut out = Vec::with_capacity(rows.len() * 110);
        let mut fences = Vec::new();
        let mut bloom = Bloom::with_keys(rows.len());
        let mut block: Vec<u8> = Vec::with_capacity(BLOCK);
        let mut count = 0u16;
        let flush = |out: &mut Vec<u8>, block: &mut Vec<u8>, count: &mut u16| {
            let mut b = vec![0u8; BLOCK];
            b[0..2].copy_from_slice(&count.to_le_bytes());
            b[2..2 + block.len()].copy_from_slice(block);
            out.extend_from_slice(&b);
            block.clear();
            *count = 0;
        };
        for (k, r) in rows {
            if 2 + block.len() + 2 + k.len() + 64 > BLOCK {
                flush(&mut out, &mut block, &mut count);
            }
            if count == 0 {
                fences.push(k.clone());
            }
            block.extend_from_slice(&(k.len() as u16).to_le_bytes());
            block.extend_from_slice(k);
            block.extend_from_slice(r);
            count += 1;
            bloom.insert(k);
        }
        if count > 0 {
            flush(&mut out, &mut block, &mut count);
        }
        let data_len = out.len() as u64;
        let fence_at = out.len();
        for f in &fences {
            out.extend_from_slice(&(f.len() as u16).to_le_bytes());
            out.extend_from_slice(f);
        }
        let fence_len = (out.len() - fence_at) as u64;
        for b in &bloom.blocks {
            for w in b {
                out.extend_from_slice(&w.to_le_bytes());
            }
        }
        let trailer_crc = crc32c::crc32c(&out[fence_at..]);
        out.extend_from_slice(&data_len.to_le_bytes());
        out.extend_from_slice(&fence_len.to_le_bytes());
        out.extend_from_slice(&(bloom.blocks.len() as u64).to_le_bytes());
        out.extend_from_slice(&(fences.len() as u64).to_le_bytes());
        out.extend_from_slice(&trailer_crc.to_le_bytes());
        out.extend_from_slice(&[0u8; 4]);
        out.extend_from_slice(&MAGIC.to_le_bytes());
        let mut f = File::create(path).unwrap();
        f.write_all(&out).unwrap();
        f.sync_all().unwrap();
        Segment::open(path)
    }

    fn open(path: &Path) -> Segment {
        let file = OpenOptions::new().read(true).open(path).unwrap();
        let len = file.metadata().unwrap().len();
        let mut t = [0u8; 48];
        read_exact_at(&file, len - 48, &mut t).unwrap();
        let u = |j: usize| u64::from_le_bytes(t[j * 8..j * 8 + 8].try_into().unwrap());
        assert_eq!(u(5), MAGIC, "{} is not a segment", path.display());
        let (data_len, fence_len, bloom_blocks, nfences) = (u(0), u(1), u(2) as usize, u(3) as usize);
        let footer_len = (fence_len + bloom_blocks as u64 * 64) as usize;
        let mut footer = vec![0u8; footer_len];
        read_exact_at(&file, data_len, &mut footer).unwrap();
        assert_eq!(crc32c::crc32c(&footer), u32::from_le_bytes(t[32..36].try_into().unwrap()), "{}: footer CRC", path.display());
        let mut fences = Vec::with_capacity(nfences);
        let mut p = 0usize;
        for _ in 0..nfences {
            let n = u16::from_le_bytes([footer[p], footer[p + 1]]) as usize;
            fences.push(footer[p + 2..p + 2 + n].to_vec());
            p += 2 + n;
        }
        let blocks = footer[fence_len as usize..]
            .chunks_exact(64)
            .map(|c| {
                let mut b = [0u64; 8];
                for (j, w) in b.iter_mut().enumerate() {
                    *w = u64::from_le_bytes(c[j * 8..j * 8 + 8].try_into().unwrap());
                }
                b
            })
            .collect();
        Segment {
            file,
            fences,
            bloom: Bloom { blocks },
        }
    }

    fn read_block(&self, i: usize) -> Vec<(Vec<u8>, [u8; 64])> {
        let mut b = vec![0u8; BLOCK];
        read_exact_at(&self.file, (i * BLOCK) as u64, &mut b).unwrap();
        let n = u16::from_le_bytes([b[0], b[1]]) as usize;
        let mut out = Vec::with_capacity(n);
        let mut p = 2;
        for _ in 0..n {
            let kl = u16::from_le_bytes([b[p], b[p + 1]]) as usize;
            let k = b[p + 2..p + 2 + kl].to_vec();
            let mut r = [0u8; 64];
            r.copy_from_slice(&b[p + 2 + kl..p + 2 + kl + 64]);
            out.push((k, r));
            p += 2 + kl + 64;
        }
        out
    }

    /// The block that would hold `key`: the last whose fence is at or below it.
    fn block_for(&self, key: &[u8]) -> Option<usize> {
        match self.fences.binary_search_by(|f| f.as_slice().cmp(key)) {
            Ok(i) => Some(i),
            Err(0) => None,
            Err(i) => Some(i - 1),
        }
    }
}

struct Seg {
    dir: PathBuf,
    mem: BTreeMap<Vec<u8>, [u8; 64]>,
    segments: Vec<Segment>,
}

impl Seg {
    fn open(dir: &Path) -> Seg {
        let mut segments = Vec::new();
        if let Ok(m) = std::fs::read_to_string(dir.join("MANIFEST")) {
            for name in m.lines().filter(|l| !l.is_empty()) {
                segments.push(Segment::open(&dir.join(name)));
            }
        }
        Seg {
            dir: dir.to_path_buf(),
            mem: BTreeMap::new(),
            segments,
        }
    }

    fn flush(&mut self) {
        if self.mem.is_empty() {
            return;
        }
        let name = format!("seg_{:04}.dat", self.segments.len());
        let seg = Segment::write(&self.dir.join(&name), &self.mem);
        self.segments.push(seg);
        self.mem.clear();
        // The manifest is switched by a rename, so it always names complete segments.
        let names: Vec<String> = (0..self.segments.len()).map(|i| format!("seg_{:04}.dat", i)).collect();
        let tmp = self.dir.join("MANIFEST.tmp");
        std::fs::write(&tmp, names.join("\n")).unwrap();
        std::fs::rename(&tmp, self.dir.join("MANIFEST")).unwrap();
    }
}

impl Store for Seg {
    fn put(&mut self, key: &[u8], row: &ObjectMeta) {
        self.mem.insert(key.to_vec(), encode(row));
        if self.mem.len() >= MEMTABLE_ROWS {
            self.flush();
        }
    }
    fn get(&mut self, key: &[u8]) -> Option<ObjectMeta> {
        if let Some(r) = self.mem.get(key) {
            return Some(decode(r));
        }
        for s in self.segments.iter().rev() {
            if !s.bloom.may_contain(key) {
                continue;
            }
            if let Some(i) = s.block_for(key) {
                if let Some((_, r)) = s.read_block(i).into_iter().find(|(k, _)| k == key) {
                    return Some(decode(&r));
                }
            }
        }
        None
    }
    fn scan(&mut self, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, ObjectMeta)> {
        let mut found: BTreeMap<Vec<u8>, [u8; 64]> = BTreeMap::new();
        for s in &self.segments {
            let mut i = s.block_for(prefix).unwrap_or(0);
            let mut taken = 0;
            'blocks: while i < s.fences.len() {
                for (k, r) in s.read_block(i) {
                    if k.as_slice() < prefix {
                        continue;
                    }
                    if !k.starts_with(prefix) || taken >= limit {
                        break 'blocks;
                    }
                    found.insert(k, r);
                    taken += 1;
                }
                i += 1;
            }
        }
        for (k, r) in self.mem.range(prefix.to_vec()..) {
            if !k.starts_with(prefix) {
                break;
            }
            found.insert(k.clone(), *r);
        }
        found.into_iter().take(limit).map(|(k, r)| (k, decode(&r))).collect()
    }
    fn finish(&mut self) {
        self.flush();
    }
}

/// Read system calls this process has made, counted by the OS.
fn read_calls() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let io = std::fs::read_to_string("/proc/self/io").ok()?;
        io.lines().find_map(|l| l.strip_prefix("syscr: ")).and_then(|v| v.trim().parse().ok())
    }
    #[cfg(windows)]
    {
        #[repr(C)]
        struct IoCounters {
            read_ops: u64,
            write_ops: u64,
            other_ops: u64,
            read_bytes: u64,
            write_bytes: u64,
            other_bytes: u64,
        }
        extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn GetProcessIoCounters(process: *mut std::ffi::c_void, counters: *mut IoCounters) -> i32;
        }
        let mut c = IoCounters {
            read_ops: 0,
            write_ops: 0,
            other_ops: 0,
            read_bytes: 0,
            write_bytes: 0,
            other_bytes: 0,
        };
        // SAFETY: a valid pseudo-handle and a correctly sized, writable struct.
        let ok = unsafe { GetProcessIoCounters(GetCurrentProcess(), &mut c) };
        (ok != 0).then_some(c.read_ops)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        None
    }
}

/// The process's peak RSS in MiB, from the OS.
fn peak_rss_mib() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/status").ok()?;
        let kb: f64 = s.lines().find_map(|l| l.strip_prefix("VmHWM:")).and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())?;
        Some(kb / 1024.0)
    }
    #[cfg(windows)]
    {
        #[repr(C)]
        struct Counters {
            cb: u32,
            page_fault_count: u32,
            peak_working_set: usize,
            working_set: usize,
            quota_peak_paged: usize,
            quota_paged: usize,
            quota_peak_nonpaged: usize,
            quota_nonpaged: usize,
            pagefile: usize,
            peak_pagefile: usize,
        }
        extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn K32GetProcessMemoryInfo(process: *mut std::ffi::c_void, counters: *mut Counters, cb: u32) -> i32;
        }
        let mut c = Counters {
            cb: std::mem::size_of::<Counters>() as u32,
            page_fault_count: 0,
            peak_working_set: 0,
            working_set: 0,
            quota_peak_paged: 0,
            quota_paged: 0,
            quota_peak_nonpaged: 0,
            quota_nonpaged: 0,
            pagefile: 0,
            peak_pagefile: 0,
        };
        // SAFETY: a valid pseudo-handle and a struct of the size passed.
        let ok = unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut c, c.cb) };
        (ok != 0).then_some(c.peak_working_set as f64 / 1048576.0)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        None
    }
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|rd| rd.flatten().filter_map(|e| e.metadata().ok()).filter(|m| m.is_file()).map(|m| m.len()).sum())
        .unwrap_or(0)
}

fn json_num(v: Option<f64>) -> String {
    v.map(|x| format!("{:.3}", x)).unwrap_or_else(|| "null".into())
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * p) as usize] as f64 / 1000.0
}

fn open_store(kind: &str, dir: &Path) -> Box<dyn Store> {
    match kind {
        "redb" => Box::new(Redb(SpillIndex::open(&dir.join("metadata.db")).expect("open redb"))),
        "seg" => Box::new(Seg::open(dir)),
        other => panic!("--store redb|seg, not {}", other),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str, default: &str| -> String { args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned().unwrap_or_else(|| default.to_string()) };
    let phase = args.get(1).cloned().unwrap_or_default();
    let kind = arg("--store", "seg");
    let dir = PathBuf::from(arg("--dir", "meta_bench"));
    let keys: u64 = arg("--keys", "1000000").parse().unwrap();
    let gets: u64 = arg("--gets", "1000000").parse().unwrap();
    let scans: u64 = arg("--scans", "1000").parse().unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    assert!(keys % PERMUTE != 0, "--keys must not be a multiple of {}", PERMUTE);

    match phase.as_str() {
        "build" => {
            let t = Instant::now();
            let mut store = open_store(&kind, &dir);
            for n in 0..keys {
                let i = (n * PERMUTE) % keys;
                store.put(&key(i), &row(i));
            }
            store.finish();
            let secs = t.elapsed().as_secs_f64();
            println!(
                "{{\"store\":\"{}\",\"phase\":\"build\",\"keys\":{},\"secs\":{:.3},\"puts_per_s\":{:.0},\"bytes_on_disk\":{},\"peak_rss_mib\":{}}}",
                kind,
                keys,
                secs,
                keys as f64 / secs,
                dir_bytes(&dir),
                json_num(peak_rss_mib())
            );
        }
        "query" => {
            let t = Instant::now();
            let mut store = open_store(&kind, &dir);
            let open_secs = t.elapsed().as_secs_f64();
            let mut answers: u64 = 0;
            let mut wrong = 0u64;
            let mut lat = Vec::with_capacity(gets as usize);
            let r0 = read_calls();
            for n in 0..gets {
                let i = h64(&n.to_le_bytes()) % keys;
                let t = Instant::now();
                let got = store.get(&key(i));
                lat.push(t.elapsed().as_nanos() as u64);
                if got != Some(row(i)) {
                    wrong += 1;
                }
                answers = answers.wrapping_mul(31).wrapping_add(got.map_or(0, |m| m.vol_offset));
            }
            let r1 = read_calls();
            lat.sort_unstable();
            let mut alat = Vec::with_capacity(gets as usize);
            for n in 0..gets {
                let t = Instant::now();
                let got = store.get(&absent_key(n));
                alat.push(t.elapsed().as_nanos() as u64);
                if got.is_some() {
                    wrong += 1;
                }
            }
            let r2 = read_calls();
            alat.sort_unstable();
            let t = Instant::now();
            let mut scanned = 0u64;
            for n in 0..scans {
                let prefix = format!("logs/h{:03}/d{:02}/", n % 200, (n / 200) % 100).into_bytes();
                let items = store.scan(&prefix, 1000);
                if items.windows(2).any(|w| w[0].0 >= w[1].0) || items.iter().any(|(k, _)| !k.starts_with(&prefix)) {
                    wrong += 1;
                }
                scanned += items.len() as u64;
                for (k, m) in &items {
                    answers = answers.wrapping_mul(31).wrapping_add(h64(k) ^ m.vol_offset);
                }
            }
            let scan_secs = t.elapsed().as_secs_f64();
            let per = |a: Option<u64>, b: Option<u64>| match (a, b) {
                (Some(a), Some(b)) => Some((b - a) as f64 / gets as f64),
                _ => None,
            };
            println!(
                concat!(
                    "{{\"store\":\"{}\",\"phase\":\"query\",\"keys\":{},\"open_secs\":{:.3},",
                    "\"get_p50_us\":{:.2},\"get_p99_us\":{:.2},\"reads_per_get\":{},",
                    "\"absent_p50_us\":{:.2},\"absent_p99_us\":{:.2},\"reads_per_absent_get\":{},",
                    "\"scans\":{},\"scanned_keys\":{},\"scan_ms_each\":{:.3},",
                    "\"wrong\":{},\"answers\":\"{:016x}\",\"peak_rss_mib\":{}}}"
                ),
                kind,
                keys,
                open_secs,
                percentile(&lat, 0.5),
                percentile(&lat, 0.99),
                json_num(per(r0, r1)),
                percentile(&alat, 0.5),
                percentile(&alat, 0.99),
                json_num(per(r1, r2)),
                scans,
                scanned,
                scan_secs * 1000.0 / scans.max(1) as f64,
                wrong,
                answers,
                json_num(peak_rss_mib())
            );
            assert_eq!(wrong, 0, "{} gave {} wrong answers", kind, wrong);
        }
        _ => panic!("first argument: build or query"),
    }
}
