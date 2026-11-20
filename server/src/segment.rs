//! Metadata segments (#26 stage 2, decision 59): an immutable, sorted run of index entries in
//! one file, read with positioned reads, never a mapping (decision 58). The byte layout is in
//! `FORMAT.md` ("Metadata segment"); this file follows it and nothing else.
//!
//! `SegmentWriter` writes a segment as a stream, block by block (stage 5's compaction), so a
//! segment is never held in memory whole.
//!
//! The server's spill index (`spill.rs`) keeps its rows in segments since #26 stage 6.

use crate::meta_index::ObjectMeta;
use crate::vfile::{FileKind, VolFile, VolFs};
use crate::work::WorkCounters;
use std::path::Path;
use std::sync::Arc;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

pub const BLOCK: usize = 4096;
pub const SEGMENT_MAGIC: u32 = 0x4753_5741; // "AWSG"
pub const TRAILER_MAGIC: u32 = 0x4653_5741; // "AWSF"
pub const SEGMENT_FORMAT: u16 = 1;
/// A segment's largest size (#26).
pub const MAX_SEGMENT_BYTES: u64 = 64 << 20;
/// S3's longest object key.
pub const MAX_KEY_BYTES: usize = 1024;
const BLOCK_PAYLOAD: usize = BLOCK - 4;
const ROW_BYTES: usize = 64;
const BLOOM_BLOCK_BITS: u64 = 512;
const BLOOM_BITS_PER_KEY: u64 = 10;
const BLOOM_PROBES: u32 = 7;
const BLOOM_SEED: u64 = 0x5345_474D_454E_5453; // "SEGMENTS"
const KIND_ROW: u8 = 1;
const KIND_TOMBSTONE: u8 = 2;

#[derive(Debug)]
pub enum SegmentError {
    Io(std::io::Error),
    /// The file does not hold a segment as `FORMAT.md` describes: a magic, a CRC or a length
    /// is wrong. Never treated as "not found".
    Corrupt(String),
    /// Written by a newer AwanFS.
    NewerFormat(u16),
    /// The entries given to the writer are not in strictly increasing sort-key order.
    Unsorted,
    KeyTooLong(usize),
    TooLarge(u64),
}

impl From<std::io::Error> for SegmentError {
    fn from(e: std::io::Error) -> Self {
        SegmentError::Io(e)
    }
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// `bucket id (big-endian) || key`: byte order is (bucket, key) order.
pub fn sort_key(bucket_id: u32, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len());
    k.extend_from_slice(&bucket_id.to_be_bytes());
    k.extend_from_slice(key);
    k
}

/// What a segment says about one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Entry {
    Row(ObjectMeta),
    /// The key is deleted: it hides every older segment's entry for it.
    Tombstone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegEntry {
    pub key: Vec<u8>,
    pub entry: Entry,
}

fn encode_row(m: &ObjectMeta, out: &mut Vec<u8>) {
    for v in [m.key_hash, m.bucket_flags, m.vol_offset, m.size, m.etag_hi, m.etag_lo, m.created_at, 0u64] {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn decode_row(b: &[u8]) -> ObjectMeta {
    let u = |i: usize| u64::from_le_bytes(b[i * 8..i * 8 + 8].try_into().unwrap());
    ObjectMeta {
        key_hash: u(0),
        bucket_flags: u(1),
        vol_offset: u(2),
        size: u(3),
        etag_hi: u(4),
        etag_lo: u(5),
        created_at: u(6),
        key_slice: 0,
    }
}

fn entry_len(e: &SegEntry) -> usize {
    3 + e.key.len() + if matches!(e.entry, Entry::Row(_)) { ROW_BYTES } else { 0 }
}

/// A blocked Bloom filter: a key sets `BLOOM_PROBES` bits, all in one 512-bit block, so a
/// lookup touches one cache line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bloom {
    blocks: Vec<[u64; 8]>,
}

/// The bits a key sets: its block and `BLOOM_PROBES` bit positions in it (FORMAT.md names this).
pub fn bloom_probes(key: &[u8], nblocks: usize) -> (usize, [u32; BLOOM_PROBES as usize]) {
    let h = xxh3_64(key);
    let block = (h % nblocks.max(1) as u64) as usize;
    // The bits come from a second, independent hash, so keys that share a block do not share
    // their bit pattern.
    let g = xxh3_64_with_seed(key, BLOOM_SEED);
    let mut bits = [0u32; BLOOM_PROBES as usize];
    let (h1, h2) = (g >> 32, (g & 0xFFFF_FFFF) | 1);
    for (i, b) in bits.iter_mut().enumerate() {
        *b = ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % BLOOM_BLOCK_BITS) as u32;
    }
    (block, bits)
}

impl Bloom {
    fn with_keys(n: usize) -> Self {
        Bloom { blocks: vec![[0u64; 8]; bloom_blocks_for(n as u64) as usize] }
    }
    fn insert(&mut self, key: &[u8]) {
        let (block, bits) = bloom_probes(key, self.blocks.len());
        for bit in bits {
            self.blocks[block][(bit / 64) as usize] |= 1u64 << (bit % 64);
        }
    }
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (block, bits) = bloom_probes(key, self.blocks.len());
        bits.iter().all(|&bit| self.blocks[block][(bit / 64) as usize] & (1u64 << (bit % 64)) != 0)
    }
}

/// Builds the bytes of a segment holding `entries`, which must be in strictly increasing
/// sort-key order. Deterministic: the same entries give the same bytes.
pub fn encode_segment(seq: u64, created_at_ns: u64, entries: &[SegEntry]) -> Result<Vec<u8>, SegmentError> {
    for e in entries {
        if e.key.len() > 4 + MAX_KEY_BYTES {
            return Err(SegmentError::KeyTooLong(e.key.len()));
        }
    }
    if entries.windows(2).any(|w| w[0].key >= w[1].key) {
        return Err(SegmentError::Unsorted);
    }

    let mut out = vec![0u8; BLOCK];
    let mut fences: Vec<&[u8]> = Vec::new();
    let mut bloom = Bloom::with_keys(entries.len());
    let mut block: Vec<u8> = Vec::with_capacity(BLOCK);
    let mut count: u16 = 0;
    let flush = |out: &mut Vec<u8>, block: &mut Vec<u8>, count: &mut u16| {
        out.extend_from_slice(&data_block(*count, block));
        block.clear();
        *count = 0;
    };
    for e in entries {
        if 2 + block.len() + entry_len(e) > BLOCK_PAYLOAD {
            flush(&mut out, &mut block, &mut count);
        }
        if count == 0 {
            fences.push(&e.key);
        }
        encode_entry(e, &mut block);
        count += 1;
        bloom.insert(&e.key);
    }
    if count > 0 {
        flush(&mut out, &mut block, &mut count);
    }
    let data_blocks = (out.len() / BLOCK - 1) as u64;
    let (header, tail) = header_and_tail(seq, created_at_ns, entries.len() as u64, data_blocks, &fences, &bloom);
    out[0..BLOCK].copy_from_slice(&header);
    out.extend_from_slice(&tail);
    if out.len() as u64 > MAX_SEGMENT_BYTES {
        return Err(SegmentError::TooLarge(out.len() as u64));
    }
    Ok(out)
}

fn encode_entry(e: &SegEntry, block: &mut Vec<u8>) {
    match e.entry {
        Entry::Row(m) => {
            block.push(KIND_ROW);
            block.extend_from_slice(&(e.key.len() as u16).to_le_bytes());
            block.extend_from_slice(&e.key);
            encode_row(&m, block);
        }
        Entry::Tombstone => {
            block.push(KIND_TOMBSTONE);
            block.extend_from_slice(&(e.key.len() as u16).to_le_bytes());
            block.extend_from_slice(&e.key);
        }
    }
}

/// One data block's bytes: the count, the packed entries, zeros, the CRC.
fn data_block(count: u16, packed: &[u8]) -> Vec<u8> {
    let mut b = vec![0u8; BLOCK];
    b[0..2].copy_from_slice(&count.to_le_bytes());
    b[2..2 + packed.len()].copy_from_slice(packed);
    let crc = crc32c::crc32c(&b[..BLOCK_PAYLOAD]);
    b[BLOCK_PAYLOAD..].copy_from_slice(&crc.to_le_bytes());
    b
}

fn bloom_blocks_for(entries: u64) -> u64 {
    (entries * BLOOM_BITS_PER_KEY).div_ceil(BLOOM_BLOCK_BITS).max(1)
}

/// The header block, and the index blocks and trailer that follow the data blocks.
fn header_and_tail<K: AsRef<[u8]>>(seq: u64, created_at_ns: u64, entries: u64, data_blocks: u64, fences: &[K], bloom: &Bloom) -> (Vec<u8>, Vec<u8>) {
    let index_block = 1 + data_blocks;
    let mut out = Vec::new();
    for f in fences {
        let f = f.as_ref();
        out.extend_from_slice(&(f.len() as u16).to_le_bytes());
        out.extend_from_slice(f);
    }
    let fence_len = out.len() as u64;
    for b in &bloom.blocks {
        for w in b {
            out.extend_from_slice(&w.to_le_bytes());
        }
    }
    let index_crc = crc32c::crc32c(&out);
    let bloom_offset = fence_len;
    while out.len() % BLOCK != 0 {
        out.push(0);
    }

    let mut t = vec![0u8; BLOCK];
    t[0..4].copy_from_slice(&TRAILER_MAGIC.to_le_bytes());
    t[4..12].copy_from_slice(&index_block.to_le_bytes());
    t[12..20].copy_from_slice(&fence_len.to_le_bytes());
    t[20..28].copy_from_slice(&(fences.len() as u64).to_le_bytes());
    t[28..36].copy_from_slice(&bloom_offset.to_le_bytes());
    t[36..44].copy_from_slice(&(bloom.blocks.len() as u64).to_le_bytes());
    t[44..48].copy_from_slice(&BLOOM_PROBES.to_le_bytes());
    t[48..52].copy_from_slice(&index_crc.to_le_bytes());
    let tcrc = crc32c::crc32c(&t[0..52]);
    t[52..56].copy_from_slice(&tcrc.to_le_bytes());
    out.extend_from_slice(&t);

    let mut h = vec![0u8; BLOCK];
    h[0..4].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    h[4..6].copy_from_slice(&SEGMENT_FORMAT.to_le_bytes());
    h[8..16].copy_from_slice(&seq.to_le_bytes());
    h[16..24].copy_from_slice(&created_at_ns.to_le_bytes());
    h[24..32].copy_from_slice(&entries.to_le_bytes());
    h[32..40].copy_from_slice(&data_blocks.to_le_bytes());
    let hcrc = crc32c::crc32c(&h[0..40]);
    h[40..44].copy_from_slice(&hcrc.to_le_bytes());
    (h, out)
}

/// Writes a segment as a stream: each data block goes to the file when it is full, so memory
/// holds one block, the fences and nothing else. `finish` reads the data blocks back to build
/// the Bloom filter (its size depends on the final entry count), writes the index, the trailer
/// and the header, and syncs. The bytes equal `encode_segment`'s for the same entries.
pub struct SegmentWriter<F: VolFile> {
    file: Arc<F>,
    seq: u64,
    created_at_ns: u64,
    max_bytes: u64,
    block: Vec<u8>,
    count: u16,
    data_blocks: u64,
    entries: u64,
    fences: Vec<Vec<u8>>,
    fence_len: u64,
    last: Option<Vec<u8>>,
}

impl<F: VolFile> SegmentWriter<F> {
    /// Starts the segment at `path`, at most `max_bytes` long (at most `MAX_SEGMENT_BYTES`, at
    /// least four blocks: one entry of the longest key always fits).
    pub fn create<Fs: VolFs<File = F>>(fs: &Fs, path: &Path, seq: u64, created_at_ns: u64, max_bytes: u64) -> Result<Self, SegmentError> {
        let max_bytes = max_bytes.clamp(4 * BLOCK as u64, MAX_SEGMENT_BYTES);
        let file = fs.open(path, FileKind::Segment, true)?;
        file.set_len(0)?;
        Ok(SegmentWriter { file, seq, created_at_ns, max_bytes, block: Vec::with_capacity(BLOCK), count: 0, data_blocks: 0, entries: 0, fences: Vec::new(), fence_len: 0, last: None })
    }

    fn len_with(data_blocks: u64, fence_len: u64, entries: u64) -> u64 {
        let index_blocks = (fence_len + 64 * bloom_blocks_for(entries)).div_ceil(BLOCK as u64);
        (2 + data_blocks + index_blocks) * BLOCK as u64
    }

    /// Whether `e` can be added without the segment growing past its limit.
    pub fn fits(&self, e: &SegEntry) -> bool {
        let fence = 2 + e.key.len() as u64;
        let (data, fences) = if self.count == 0 {
            (self.data_blocks + 1, self.fence_len + fence)
        } else if 2 + self.block.len() + entry_len(e) > BLOCK_PAYLOAD {
            (self.data_blocks + 2, self.fence_len + fence)
        } else {
            (self.data_blocks + 1, self.fence_len)
        };
        Self::len_with(data, fences, self.entries + 1) <= self.max_bytes
    }

    pub fn entries(&self) -> u64 {
        self.entries
    }

    fn write_block(&mut self) -> Result<(), SegmentError> {
        let b = data_block(self.count, &self.block);
        self.file.write_at((1 + self.data_blocks) * BLOCK as u64, &b)?;
        self.data_blocks += 1;
        self.block.clear();
        self.count = 0;
        Ok(())
    }

    /// Adds an entry after every entry added so far (strictly increasing sort keys).
    pub fn push(&mut self, e: &SegEntry) -> Result<(), SegmentError> {
        if e.key.len() > 4 + MAX_KEY_BYTES {
            return Err(SegmentError::KeyTooLong(e.key.len()));
        }
        if self.last.as_ref().is_some_and(|l| *l >= e.key) {
            return Err(SegmentError::Unsorted);
        }
        if !self.fits(e) {
            return Err(SegmentError::TooLarge(self.max_bytes));
        }
        if self.count > 0 && 2 + self.block.len() + entry_len(e) > BLOCK_PAYLOAD {
            self.write_block()?;
        }
        if self.count == 0 {
            self.fence_len += 2 + e.key.len() as u64;
            self.fences.push(e.key.clone());
        }
        encode_entry(e, &mut self.block);
        self.count += 1;
        self.entries += 1;
        self.last = Some(e.key.clone());
        Ok(())
    }

    /// Writes the rest and syncs. Returns the segment's length in bytes.
    pub fn finish(mut self) -> Result<u64, SegmentError> {
        if self.count > 0 {
            self.write_block()?;
        }
        let mut bloom = Bloom::with_keys(self.entries as usize);
        let mut b = vec![0u8; BLOCK];
        for i in 0..self.data_blocks {
            self.file.read_at((1 + i) * BLOCK as u64, &mut b)?;
            for e in parse_block(&b, i)? {
                bloom.insert(&e.key);
            }
        }
        let (header, tail) = header_and_tail(self.seq, self.created_at_ns, self.entries, self.data_blocks, &self.fences, &bloom);
        let tail_at = (1 + self.data_blocks) * BLOCK as u64;
        self.file.write_at(tail_at, &tail)?;
        self.file.write_at(0, &header)?;
        self.file.sync_data()?;
        Ok(tail_at + tail.len() as u64)
    }
}

/// Writes a segment to `path` in `fs` and syncs it. The caller names it in the manifest only
/// after this returns (stage 4).
pub fn write_segment<Fs: VolFs>(fs: &Fs, path: &Path, seq: u64, created_at_ns: u64, entries: &[SegEntry]) -> Result<(), SegmentError> {
    let bytes = encode_segment(seq, created_at_ns, entries)?;
    let f = fs.open(path, FileKind::Segment, true)?;
    f.set_len(0)?;
    f.write_at(0, &bytes)?;
    f.sync_data()?;
    Ok(())
}

/// An open segment: its header facts, fences and Bloom filter in memory; rows on disk.
pub struct Segment<F: VolFile> {
    file: Arc<F>,
    pub seq: u64,
    pub created_at_ns: u64,
    pub entry_count: u64,
    data_blocks: u64,
    len_bytes: u64,
    fences: Fences,
    /// The last entry's sort key (empty when the segment has no entries).
    last_key: Vec<u8>,
    bloom: Bloom,
    /// Counts each block read, when the segment belongs to a server's index (stage 3).
    work: Option<Arc<WorkCounters>>,
}

/// The first key of each data block, in one buffer: one heap allocation per segment, not one per
/// block (#26 stage 6: at 10M keys the per-block vectors were about 16 MB of the build's peak RSS).
struct Fences {
    keys: Vec<u8>,
    /// `keys[ends[i-1]..ends[i]]` is block `i`'s first key.
    ends: Vec<u32>,
}

impl Fences {
    fn with_capacity(n: usize, bytes: usize) -> Self {
        Fences { keys: Vec::with_capacity(bytes), ends: Vec::with_capacity(n) }
    }

    fn push(&mut self, key: &[u8]) {
        self.keys.extend_from_slice(key);
        self.ends.push(self.keys.len() as u32);
    }

    fn len(&self) -> usize {
        self.ends.len()
    }

    fn get(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.keys[start..self.ends[i] as usize]
    }

    /// The number of blocks whose first key is at or before `key`.
    fn at_or_before(&self, key: &[u8]) -> usize {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.get(mid) <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

fn corrupt(what: impl Into<String>) -> SegmentError {
    SegmentError::Corrupt(what.into())
}

/// The entries of data block `i`, after its CRC check.
fn parse_block(b: &[u8], i: u64) -> Result<Vec<SegEntry>, SegmentError> {
    if crc32c::crc32c(&b[..BLOCK_PAYLOAD]) != u32::from_le_bytes(b[BLOCK_PAYLOAD..BLOCK].try_into().unwrap()) {
        return Err(corrupt(format!("data block {} CRC", i)));
    }
    let n = u16::from_le_bytes(b[0..2].try_into().unwrap());
    let mut out = Vec::with_capacity(n as usize);
    let mut p = 2usize;
    for _ in 0..n {
        if p + 3 > BLOCK_PAYLOAD {
            return Err(corrupt(format!("data block {} entry", i)));
        }
        let kind = b[p];
        let k = u16::from_le_bytes(b[p + 1..p + 3].try_into().unwrap()) as usize;
        let key_end = p + 3 + k;
        let (entry, end) = match kind {
            KIND_ROW if key_end + ROW_BYTES <= BLOCK_PAYLOAD => (Entry::Row(decode_row(&b[key_end..key_end + ROW_BYTES])), key_end + ROW_BYTES),
            KIND_TOMBSTONE if key_end <= BLOCK_PAYLOAD => (Entry::Tombstone, key_end),
            _ => return Err(corrupt(format!("data block {} entry", i))),
        };
        out.push(SegEntry { key: b[p + 3..key_end].to_vec(), entry });
        p = end;
    }
    if n == 0 {
        return Err(corrupt(format!("data block {} is empty", i)));
    }
    Ok(out)
}

impl<F: VolFile> Segment<F> {
    /// Opens and checks a segment: header, trailer and index region, each against its CRC.
    pub fn open<Fs: VolFs<File = F>>(fs: &Fs, path: &Path) -> Result<Self, SegmentError> {
        let file = fs.open(path, FileKind::Segment, false)?;
        let len = file.len()?;
        if len < 2 * BLOCK as u64 || len % BLOCK as u64 != 0 || len > MAX_SEGMENT_BYTES {
            return Err(corrupt(format!("length {} is not a whole segment", len)));
        }
        let mut h = vec![0u8; BLOCK];
        file.read_at(0, &mut h)?;
        let u64at = |b: &[u8], i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        let u32at = |b: &[u8], i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        if u32at(&h, 0) != SEGMENT_MAGIC {
            return Err(corrupt("header magic"));
        }
        if crc32c::crc32c(&h[0..40]) != u32at(&h, 40) {
            return Err(corrupt("header CRC"));
        }
        let format = u16::from_le_bytes(h[4..6].try_into().unwrap());
        if format > SEGMENT_FORMAT {
            return Err(SegmentError::NewerFormat(format));
        }
        let (seq, created_at_ns, entry_count, data_blocks) = (u64at(&h, 8), u64at(&h, 16), u64at(&h, 24), u64at(&h, 32));

        let mut t = vec![0u8; BLOCK];
        file.read_at(len - BLOCK as u64, &mut t)?;
        if u32at(&t, 0) != TRAILER_MAGIC {
            return Err(corrupt("trailer magic"));
        }
        if crc32c::crc32c(&t[0..52]) != u32at(&t, 52) {
            return Err(corrupt("trailer CRC"));
        }
        let (index_block, fence_len, nfences, bloom_offset, bloom_blocks) =
            (u64at(&t, 4), u64at(&t, 12), u64at(&t, 20), u64at(&t, 28), u64at(&t, 36));
        let probes = u32at(&t, 44);
        let index_start = index_block.checked_mul(BLOCK as u64).ok_or_else(|| corrupt("index offset"))?;
        let index_len = bloom_blocks.checked_mul(64).and_then(|b| b.checked_add(fence_len)).ok_or_else(|| corrupt("index length"))?;
        if index_block != 1 + data_blocks || nfences != data_blocks || bloom_offset != fence_len || probes != BLOOM_PROBES || bloom_blocks == 0 {
            return Err(corrupt("trailer fields disagree with the header"));
        }
        if index_start + index_len > len - BLOCK as u64 {
            return Err(corrupt("index region past the trailer"));
        }
        let mut idx = vec![0u8; index_len as usize];
        file.read_at(index_start, &mut idx)?;
        if crc32c::crc32c(&idx) != u32at(&t, 48) {
            return Err(corrupt("index CRC"));
        }
        let mut fences = Fences::with_capacity(nfences as usize, fence_len as usize);
        let mut p = 0usize;
        for _ in 0..nfences {
            if p + 2 > fence_len as usize {
                return Err(corrupt("fence region"));
            }
            let l = u16::from_le_bytes(idx[p..p + 2].try_into().unwrap()) as usize;
            if p + 2 + l > fence_len as usize {
                return Err(corrupt("fence region"));
            }
            fences.push(&idx[p + 2..p + 2 + l]);
            p += 2 + l;
        }
        if p != fence_len as usize {
            return Err(corrupt("fence region length"));
        }
        let mut blocks = Vec::with_capacity(bloom_blocks as usize);
        for i in 0..bloom_blocks as usize {
            let mut b = [0u64; 8];
            for (w, slot) in b.iter_mut().enumerate() {
                let at = fence_len as usize + i * 64 + w * 8;
                *slot = u64::from_le_bytes(idx[at..at + 8].try_into().unwrap());
            }
            blocks.push(b);
        }
        // The last key bounds the segment's range (a level's lookups and compaction use it).
        let mut last_key = Vec::new();
        if data_blocks > 0 {
            let mut b = vec![0u8; BLOCK];
            file.read_at(data_blocks * BLOCK as u64, &mut b)?;
            let es = parse_block(&b, data_blocks - 1)?;
            match es.last() {
                Some(e) if e.key.as_slice() >= fences.get(fences.len() - 1) => last_key = e.key.clone(),
                _ => return Err(corrupt("last data block")),
            }
        }
        Ok(Segment { file, seq, created_at_ns, entry_count, data_blocks, len_bytes: len, fences, last_key, bloom: Bloom { blocks }, work: None })
    }

    /// The first and last sort keys, or `None` for a segment with no entries.
    pub fn key_range(&self) -> Option<(&[u8], &[u8])> {
        (self.data_blocks > 0).then(|| (self.fences.get(0), self.last_key.as_slice()))
    }

    /// Whether `key` lies between the first and last keys (no read).
    pub fn spans(&self, key: &[u8]) -> bool {
        self.key_range().is_some_and(|(lo, hi)| lo <= key && key <= hi)
    }

    /// The file's length.
    pub fn len_bytes(&self) -> u64 {
        self.len_bytes
    }

    /// Counts this segment's block reads in `work` from now on.
    pub fn with_work(mut self, work: Arc<WorkCounters>) -> Self {
        self.work = Some(work);
        self
    }

    fn read_block(&self, i: u64) -> Result<Vec<SegEntry>, SegmentError> {
        let mut b = vec![0u8; BLOCK];
        self.file.read_at((1 + i) * BLOCK as u64, &mut b)?;
        if let Some(w) = &self.work {
            w.disk_read();
        }
        parse_block(&b, i)
    }

    /// The entry for `key`, if this segment has one. A Bloom filter miss costs no read; a hit
    /// costs one block read. A damaged block is an error, never `None`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>, SegmentError> {
        if self.data_blocks == 0 || !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let i = self.fences.at_or_before(key);
        if i == 0 {
            return Ok(None);
        }
        let block = self.read_block(i as u64 - 1)?;
        Ok(block.into_iter().find(|e| e.key == key).map(|e| e.entry))
    }

    /// A cursor over the entries with a key at or after `start`, reading each block only when
    /// the scan reaches it.
    pub fn scan_from(&self, start: &[u8]) -> Cursor<'_, F> {
        let first = self.fences.at_or_before(start).saturating_sub(1) as u64;
        Cursor { seg: self, block: first, buf: Vec::new(), pos: 0, start: start.to_vec(), loaded: false }
    }

    /// Every entry, in sort-key order (compaction and tests).
    pub fn entries(&self) -> Result<Vec<SegEntry>, SegmentError> {
        let mut out = Vec::with_capacity(self.entry_count as usize);
        for i in 0..self.data_blocks {
            out.extend(self.read_block(i)?);
        }
        if out.len() as u64 != self.entry_count {
            return Err(corrupt("entry count"));
        }
        Ok(out)
    }
}

/// Walks one segment's entries in sort-key order from a start key (`Segment::scan_from`).
pub struct Cursor<'a, F: VolFile> {
    seg: &'a Segment<F>,
    block: u64,
    buf: Vec<SegEntry>,
    pos: usize,
    start: Vec<u8>,
    loaded: bool,
}

impl<F: VolFile> Cursor<'_, F> {
    /// The next entry at or after the start key, or `None` at the end of the segment.
    pub fn next_entry(&mut self) -> Result<Option<SegEntry>, SegmentError> {
        loop {
            if !self.loaded {
                if self.block >= self.seg.data_blocks {
                    return Ok(None);
                }
                self.buf = self.seg.read_block(self.block)?;
                self.pos = 0;
                self.loaded = true;
            }
            while self.pos < self.buf.len() {
                let e = &mut self.buf[self.pos];
                self.pos += 1;
                if e.key.as_slice() >= self.start.as_slice() {
                    // Moved out, not cloned: the cursor never reads an entry twice.
                    return Ok(Some(SegEntry { key: std::mem::take(&mut e.key), entry: e.entry }));
                }
            }
            self.block += 1;
            self.loaded = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{SimFs, SimOp, SimRng};
    use std::collections::BTreeMap;

    fn path() -> &'static Path {
        Path::new("meta/seg_0000000000000001.seg")
    }

    fn row(i: u64) -> ObjectMeta {
        ObjectMeta { key_hash: i, bucket_flags: i << 3, vol_offset: i * 4096, size: i * 7 + 1, etag_hi: !i, etag_lo: i ^ 0xABCD, created_at: 1_700_000_000_000 + i, key_slice: 0 }
    }

    /// `n` entries over three buckets, keys of 1 to 200 bytes, about 1 in 10 a tombstone.
    fn entries(seed: u64, n: usize) -> Vec<SegEntry> {
        let mut rng = SimRng::new(seed);
        let mut m = BTreeMap::new();
        while m.len() < n {
            let bucket = rng.below(3) as u32 + 1;
            let len = rng.below(200) as usize + 1;
            let key: Vec<u8> = (0..len).map(|_| b"abcdefghij/0123456789-_. "[rng.below(25) as usize]).collect();
            let i = m.len() as u64;
            let entry = if rng.below(10) == 0 { Entry::Tombstone } else { Entry::Row(row(i)) };
            m.insert(sort_key(bucket, &key), entry);
        }
        m.into_iter().map(|(key, entry)| SegEntry { key, entry }).collect()
    }

    fn written(es: &[SegEntry]) -> SimFs {
        let fs = SimFs::new();
        write_segment(&fs, path(), 1, 42, es).unwrap();
        fs
    }

    /// The packed fences answer as a vector of keys searched with `partition_point` would,
    /// for keys before, between, on and after the fences, the empty key included.
    #[test]
    fn packed_fences_find_the_same_block_as_a_vector_of_keys() {
        let mut rng = SimRng::new(26);
        for round in 0..200 {
            let mut keys: Vec<Vec<u8>> = (0..rng.below(40) + 1).map(|_| (0..rng.below(6)).map(|_| b"ab\xff"[rng.below(3) as usize]).collect()).collect();
            keys.sort();
            keys.dedup();
            let mut f = Fences::with_capacity(keys.len(), 0);
            for k in &keys {
                f.push(k);
            }
            assert_eq!(f.len(), keys.len());
            for (i, k) in keys.iter().enumerate() {
                assert_eq!(f.get(i), k.as_slice());
            }
            for _ in 0..50 {
                let probe: Vec<u8> = (0..rng.below(7)).map(|_| b"ab\xff"[rng.below(3) as usize]).collect();
                assert_eq!(f.at_or_before(&probe), keys.partition_point(|k| k.as_slice() <= probe.as_slice()), "round {} probe {:?}", round, probe);
            }
        }
    }

    #[test]
    fn every_entry_reads_back_and_absent_keys_are_none() {
        let es = entries(1, 5000);
        let fs = written(&es);
        let s = Segment::open(&fs, path()).unwrap();
        assert_eq!((s.seq, s.created_at_ns, s.entry_count), (1, 42, 5000));
        for e in &es {
            assert_eq!(s.get(&e.key).unwrap(), Some(e.entry), "key {:?}", e.key);
        }
        for e in entries(2, 2000) {
            if es.binary_search_by(|x| x.key.cmp(&e.key)).is_err() {
                assert_eq!(s.get(&e.key).unwrap(), None);
            }
        }
        assert_eq!(s.entries().unwrap(), es, "a full scan gives back the input in order");
    }

    #[test]
    fn the_streaming_writer_writes_the_same_bytes_and_splits_at_its_limit() {
        for (seed, n) in [(11u64, 0usize), (12, 1), (13, 700), (14, 5000)] {
            let es = entries(seed, n);
            let fs = SimFs::new();
            let mut w = SegmentWriter::create(&fs, path(), 1, 42, MAX_SEGMENT_BYTES).unwrap();
            for e in &es {
                w.push(e).unwrap();
            }
            let len = w.finish().unwrap();
            let want = encode_segment(1, 42, &es).unwrap();
            assert_eq!(len, want.len() as u64, "seed {}", seed);
            assert_eq!(fs.file(path()).unwrap().contents(), want, "seed {}: the bytes of encode_segment", seed);
            if n > 0 {
                let s = Segment::open(&fs, path()).unwrap();
                assert_eq!(s.key_range(), Some((es[0].key.as_slice(), es[n - 1].key.as_slice())));
            }
        }
        // A small limit: the writer refuses the entry that would not fit, and what it wrote opens.
        let es = entries(15, 3000);
        let fs = SimFs::new();
        let limit = 16 * BLOCK as u64;
        let mut w = SegmentWriter::create(&fs, path(), 1, 0, limit).unwrap();
        let mut taken = 0;
        for e in &es {
            if !w.fits(e) {
                assert!(matches!(w.push(e), Err(SegmentError::TooLarge(_))));
                break;
            }
            w.push(e).unwrap();
            taken += 1;
        }
        assert!(taken > 0 && taken < es.len());
        let len = w.finish().unwrap();
        assert!(len <= limit, "{} bytes, limit {}", len, limit);
        assert_eq!(Segment::open(&fs, path()).unwrap().entries().unwrap(), es[..taken].to_vec());
        let mut w = SegmentWriter::create(&fs, path(), 1, 0, MAX_SEGMENT_BYTES).unwrap();
        w.push(&es[1]).unwrap();
        assert!(matches!(w.push(&es[0]), Err(SegmentError::Unsorted)));
    }

    #[test]
    fn the_same_entries_give_the_same_bytes() {
        let es = entries(3, 1000);
        assert_eq!(encode_segment(1, 42, &es).unwrap(), encode_segment(1, 42, &es).unwrap());
    }

    #[test]
    fn a_lookup_reads_one_block_and_a_bloom_miss_reads_none() {
        let es = entries(4, 3000);
        let fs = written(&es);
        let s = Segment::open(&fs, path()).unwrap();
        let f = fs.file(path()).unwrap();
        let before = f.calls(SimOp::Read);
        for e in es.iter().take(100) {
            s.get(&e.key).unwrap();
        }
        assert_eq!(f.calls(SimOp::Read) - before, 100, "one read per present key");
        let absent: Vec<Vec<u8>> = (0..10_000u32).map(|i| sort_key(9, format!("absent-{}", i).as_bytes())).collect();
        let before = f.calls(SimOp::Read);
        for k in &absent {
            assert_eq!(s.get(k).unwrap(), None);
        }
        let reads = f.calls(SimOp::Read) - before;
        assert!(reads < 300, "the Bloom filter answers most absent keys without a read: {} of 10000", reads);
    }

    #[test]
    fn keys_of_the_longest_length_fit() {
        let es: Vec<SegEntry> = (0..50u32)
            .map(|i| SegEntry { key: sort_key(1, format!("{:04}{}", i, "k".repeat(MAX_KEY_BYTES - 4)).as_bytes()), entry: Entry::Row(row(i as u64)) })
            .collect();
        let s = Segment::open(&written(&es), path()).unwrap();
        for e in &es {
            assert_eq!(s.get(&e.key).unwrap(), Some(e.entry));
        }
    }

    #[test]
    fn an_empty_segment_opens_and_finds_nothing() {
        let fs = written(&[]);
        let s = Segment::open(&fs, path()).unwrap();
        assert_eq!(s.entry_count, 0);
        assert_eq!(s.get(&sort_key(1, b"x")).unwrap(), None);
        assert!(s.entries().unwrap().is_empty());
    }

    #[test]
    fn the_writer_refuses_unsorted_duplicate_and_overlong_keys() {
        let a = SegEntry { key: sort_key(1, b"b"), entry: Entry::Tombstone };
        let b = SegEntry { key: sort_key(1, b"a"), entry: Entry::Tombstone };
        assert!(matches!(encode_segment(1, 0, &[a.clone(), b]), Err(SegmentError::Unsorted)));
        assert!(matches!(encode_segment(1, 0, &[a.clone(), a]), Err(SegmentError::Unsorted)));
        let long = SegEntry { key: sort_key(1, &vec![b'k'; MAX_KEY_BYTES + 1]), entry: Entry::Tombstone };
        assert!(matches!(encode_segment(1, 0, &[long]), Err(SegmentError::KeyTooLong(_))));
    }

    /// Flips one byte at `at` in the stored segment.
    fn damaged(es: &[SegEntry], at: u64) -> SimFs {
        let fs = written(es);
        let f = fs.file(path()).unwrap();
        let mut b = [0u8; 1];
        f.read_at(at, &mut b).unwrap();
        f.write_at(at, &[b[0] ^ 0x5A]).unwrap();
        fs
    }

    #[test]
    fn damage_is_an_error_never_a_wrong_or_missing_entry() {
        let es = entries(5, 2000);
        let len = encode_segment(1, 42, &es).unwrap().len() as u64;
        // Header, trailer, index region: open refuses.
        for at in [3, 20, len - BLOCK as u64 + 2, len - BLOCK as u64 + 50] {
            assert!(matches!(Segment::open(&damaged(&es, at), path()), Err(SegmentError::Corrupt(_))), "byte {}", at);
        }
        let s = Segment::open(&written(&es), path()).unwrap();
        let index_at = (1 + s.data_blocks) * BLOCK as u64 + 5;
        assert!(matches!(Segment::open(&damaged(&es, index_at), path()), Err(SegmentError::Corrupt(_))));
        // A data block: the lookups that read it are errors; the others still answer.
        let fs = damaged(&es, BLOCK as u64 + 100);
        let s = Segment::open(&fs, path()).unwrap();
        let first_block_key = &es[0].key;
        assert!(matches!(s.get(first_block_key), Err(SegmentError::Corrupt(_))), "a damaged block is not 'not found'");
        assert!(s.entries().is_err());
        assert_eq!(s.get(&es.last().unwrap().key).unwrap(), Some(es.last().unwrap().entry));
        // A truncated file.
        let fs = written(&es);
        fs.file(path()).unwrap().set_len(len - 1).unwrap();
        assert!(Segment::open(&fs, path()).is_err());
    }

    #[test]
    fn the_bloom_filter_has_no_false_negatives_and_few_false_positives() {
        let es = entries(6, 20_000);
        let mut bloom = Bloom::with_keys(es.len());
        for e in &es {
            bloom.insert(&e.key);
        }
        assert!(es.iter().all(|e| bloom.may_contain(&e.key)));
        let fp = (0..100_000u32).filter(|i| bloom.may_contain(&sort_key(77, format!("no-{}", i).as_bytes()))).count();
        assert!(fp < 2_500, "false positives: {} of 100000", fp);
    }
}
