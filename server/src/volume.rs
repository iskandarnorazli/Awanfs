//! awan-storage/server/src/volume.rs
//! 32 GiB WORM Needle Volume Slab Storage Engine and Crash Recovery Journal (#470)

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use crate::clock::{Clock, SystemClock};
use bytes::Bytes;
use memmap2::Mmap;
use parking_lot::{Mutex, RwLock};
use xxhash_rust::xxh3::xxh3_64;

pub use crate::direct_io::SECTOR_SIZE;
use crate::direct_io::AlignedBuffer;
use crate::vfile::{DiskFs, FileKind, VolFile, VolFs};
use crate::work::WorkCounters;

pub const NEEDLE_MAGIC: u32 = 0x41574E33; // "AWN3"
pub const SUPERBLOCK_MAGIC: u32 = 0x53555042; // "SUPB"

/// On-disk format of volumes, journals and manifests (#50, owner decision 20), stored in the
/// superblock at bytes 20..24. Volumes written before the field existed hold 0 there and are
/// format 1. A newer version is refused, an older one read as it is; nothing is upgraded in
/// place. Layout: FORMAT.md.
pub const FORMAT_VERSION: u32 = 3;

/// The format a superblock records (0 = written before the field, i.e. format 1).
pub fn superblock_format(super_buf: &[u8]) -> u32 {
    match u32::from_le_bytes(super_buf[20..24].try_into().unwrap()) {
        0 => 1,
        v => v,
    }
}
pub const MAX_VOLUME_SPAN: u64 = 32 * 1024 * 1024 * 1024; // 32 GiB WORM Needle Volume (#470)

/// Object locations in the index are global: the volume number in the bits above
/// `VOLUME_SHIFT`, the offset inside the volume below. Volume 1's global offsets equal its
/// local ones. 2^40 bytes per slot is far more than `MAX_VOLUME_SPAN` (#37).
pub const VOLUME_SHIFT: u32 = 40;

pub fn global_offset(volume_id: u32, offset: u64) -> u64 {
    ((volume_id.saturating_sub(1) as u64) << VOLUME_SHIFT) | offset
}

/// (volume id, offset inside the volume) of a global offset.
pub fn split_offset(global: u64) -> (u32, u64) {
    ((global >> VOLUME_SHIFT) as u32 + 1, global & ((1u64 << VOLUME_SHIFT) - 1))
}

#[derive(Debug)]
pub enum VolumeError {
    Io(io::Error),
    InvalidMagic { expected: u32, actual: u32 },
    HeaderCrcMismatch { expected: u32, actual: u32 },
    CrcMismatch { expected: u32, actual: u32 },
    InvalidSuperblock,
    TornNeedleAtOffset(u64),
    CorruptIndex,
    /// The needle would end past the volume span; nothing was written.
    VolumeFull { offset: u64, len: u64 },
    /// Written by a newer AwanFS; this one reads formats up to `supported` (#50).
    UnsupportedFormat { found: u32, supported: u32 },
    /// The needle is whole in the volume, and a later step failed: its journal append (#184).
    /// Recovery will replay the record, so the caller indexes it as it would on success
    /// (owner decision 4B, 2026-11-16) and still answers an error.
    AfterRecord { loc: NeedleLocation, flags: u16, cause: String },
}

impl std::fmt::Display for VolumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolumeError::UnsupportedFormat { found, supported } => write!(
                f,
                "on-disk format {} was written by a newer AwanFS; this version reads formats up to {}",
                found, supported
            ),
            other => write!(f, "{:?}", other),
        }
    }
}

impl From<io::Error> for VolumeError {
    fn from(err: io::Error) -> Self {
        VolumeError::Io(err)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeedleLocation {
    pub volume_id: u32,
    pub offset: u64,
    pub total_len: u64,
    pub payload_size: u64,
    pub payload_crc32c: u32,
    /// Write sequence number; `VolumeManager::make_durable(seq)` waits until it is on disk.
    pub seq: u64,
}

/// Size of the pieces a streamed needle is written in.
pub const STREAM_CHUNK: usize = 1 << 20;

/// Sequential aligned read batch size for `VolumeManager::scan_needles` and `open_with` (#324, Decision 7A).
const SCAN_BATCH_BYTES: usize = 1 << 20;

/// Reusable 1 MiB sector-aligned read window for sequential volume scans.
struct AlignedScanWindow<'a, F: VolFile> {
    file: &'a F,
    file_len: u64,
    work: Option<&'a WorkCounters>,
    buf: AlignedBuffer,
    buf_start: u64,
    buf_len: usize,
}

impl<'a, F: VolFile> AlignedScanWindow<'a, F> {
    fn new(file: &'a F, file_len: u64, work: Option<&'a WorkCounters>) -> Self {
        Self {
            file,
            file_len,
            work,
            buf: AlignedBuffer::new(SCAN_BATCH_BYTES),
            buf_start: 0,
            buf_len: 0,
        }
    }

    fn count_read(&self) {
        if let Some(work) = self.work {
            work.disk_read();
        }
    }

    /// Returns at least `need_bytes` sector-aligned bytes starting at `offset`, refilling up to
    /// `SCAN_BATCH_BYTES` clamped to whole sectors inside `file_len`.
    fn read_at(&mut self, offset: u64, need_bytes: usize) -> Option<&[u8]> {
        debug_assert_eq!(offset % SECTOR_SIZE as u64, 0);
        debug_assert!(need_bytes > 0 && need_bytes % SECTOR_SIZE == 0);
        if need_bytes > self.buf.len() {
            self.buf = AlignedBuffer::new(need_bytes);
            self.buf_len = 0;
        }
        let end = offset.checked_add(need_bytes as u64)?;
        if end > self.file_len {
            return None;
        }
        if offset >= self.buf_start && end <= self.buf_start + self.buf_len as u64 {
            let start = (offset - self.buf_start) as usize;
            return Some(&self.buf[start..start + need_bytes]);
        }

        let avail_bytes = ((self.file_len - offset) / SECTOR_SIZE as u64) * SECTOR_SIZE as u64;
        if avail_bytes < need_bytes as u64 {
            return None;
        }
        let to_read = (self.buf.len() as u64).min(avail_bytes) as usize;
        self.buf_len = 0;
        self.count_read();
        if self.file.read_aligned_at(offset, &mut self.buf[..to_read]).is_err() {
            if to_read > need_bytes {
                self.count_read();
                if self.file.read_aligned_at(offset, &mut self.buf[..need_bytes]).is_err() {
                    return None;
                }
                self.buf_start = offset;
                self.buf_len = need_bytes;
                return Some(&self.buf[..need_bytes]);
            }
            return None;
        }
        self.buf_start = offset;
        self.buf_len = to_read;
        Some(&self.buf[..need_bytes])
    }
}

/// How far a growing volume's file is extended at a time (#178): `AWAN_VOLUME_GROWTH_STEP` in
/// bytes, for measurements, or 256 MiB.
pub const DEFAULT_GROWTH_STEP: u64 = 256 * 1024 * 1024;

fn growth_step_from_env() -> u64 {
    std::env::var("AWAN_VOLUME_GROWTH_STEP")
        .ok()
        .and_then(|v| crate::memory::parse_size(&v))
        .filter(|&n| n >= SECTOR_SIZE as u64)
        .map(|n| n / SECTOR_SIZE as u64 * SECTOR_SIZE as u64)
        .unwrap_or(DEFAULT_GROWTH_STEP)
}

/// A needle being written as its payload arrives; see `VolumeManager::begin_needle`.
pub struct NeedleWriter<Fs: VolFs = DiskFs> {
    vm: Arc<VolumeManager<Fs>>,
    bucket_id: u16,
    key: String,
    flags: u16,
    offset: u64,
    total_len: u64,
    payload_size: u64,
    attrs_len: u16,
    /// Where the payload starts in the needle image: after the header, key and attributes.
    data_start: u64,
    written: u64,
    crc: u32,
    /// Sector 0 of the needle: header, key, start of the payload. Written last.
    first: AlignedBuffer,
    /// The piece of the needle image starting at `chunk_start`.
    chunk: AlignedBuffer,
    chunk_start: u64,
    chunk_fill: usize,
    /// Creation time for the header; now when unset (a version id, #31).
    created_at: Option<u64>,
    /// Where the writer is: `crate::fsm::needle_step` decides every move (STATE_MACHINES.md).
    state: crate::fsm::NeedleState,
    /// Tracks the streaming buffers held in memory while this writer is open.
    _inflight: crate::work::InflightGuard,
}

fn refused(r: crate::fsm::Refusal, what: &str) -> VolumeError {
    let kind = if r.status == 400 { std::io::ErrorKind::InvalidInput } else { std::io::ErrorKind::Other };
    VolumeError::Io(std::io::Error::new(kind, what.to_string()))
}

impl<Fs: VolFs> NeedleWriter<Fs> {
    /// The needle's creation time (header bytes 36..44): a version's id (#31).
    pub fn set_created_at(&mut self, ns: u64) {
        self.created_at = Some(ns);
    }

    pub fn volume(&self) -> &Arc<VolumeManager<Fs>> {
        &self.vm
    }

    /// The header flags. The header is written last, so a streamed PUT sets them when it
    /// commits, from the bucket's versioning status at that moment (#154).
    pub fn set_flags(&mut self, flags: u16) {
        // A writer started with an attributes block keeps saying so (#150).
        let attrs = if self.attrs_len > 0 { crate::meta_index::FLAG_ATTRS } else { 0 };
        self.flags = flags | attrs;
    }

    /// Where the needle starts inside its volume: reserved when the writer was made.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn written(&self) -> u64 {
        self.written
    }

    pub fn write(&mut self, data: &[u8]) -> Result<(), VolumeError> {
        self.state = match crate::fsm::needle_step(self.state, crate::fsm::NeedleEvent::Write(data.len() as u64)) {
            Ok(next) => next,
            Err(r) => {
                // A refused write ends the needle: nothing after it may commit.
                self.state = crate::fsm::NeedleState::Failed;
                return Err(refused(r, r.why));
            }
        };
        self.crc = crc32c::crc32c_append(self.crc, data);
        let pos = self.data_start + self.written;
        self.written += data.len() as u64;
        self.place(pos, data)
    }

    /// Puts `data` at `pos` of the needle image: sector 0 is kept for the end, later bytes
    /// go through the chunk buffer, which must have reached `pos`.
    fn place(&mut self, mut pos: u64, mut data: &[u8]) -> Result<(), VolumeError> {
        // Bytes that land in sector 0.
        if pos < SECTOR_SIZE as u64 {
            let n = ((SECTOR_SIZE as u64 - pos) as usize).min(data.len());
            self.first[pos as usize..pos as usize + n].copy_from_slice(&data[..n]);
            data = &data[n..];
            pos += n as u64;
        }
        while !data.is_empty() {
            debug_assert_eq!(pos, self.chunk_start + self.chunk_fill as u64);
            let n = (STREAM_CHUNK - self.chunk_fill).min(data.len());
            self.chunk[self.chunk_fill..self.chunk_fill + n].copy_from_slice(&data[..n]);
            self.chunk_fill += n;
            data = &data[n..];
            pos += n as u64;
            if self.chunk_fill == STREAM_CHUNK {
                if let Err(e) = self.flush_chunk() {
                    self.state = crate::fsm::NeedleState::Failed;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> Result<(), VolumeError> {
        let end = (self.chunk_start + self.chunk_fill as u64).div_ceil(SECTOR_SIZE as u64) * SECTOR_SIZE as u64;
        let len = (end - self.chunk_start) as usize;
        if len == 0 {
            return Ok(());
        }
        self.chunk[self.chunk_fill..len].fill(0);
        self.vm.file.write_aligned_at(self.offset + self.chunk_start, &self.chunk[..len])?;
        self.vm.work.disk_write(len as u64);
        self.chunk_start += len as u64;
        self.chunk_fill = 0;
        Ok(())
    }

    /// Writes what is left and then the header; the needle is complete and journaled.
    pub fn finish(mut self, etag_hi: u64, etag_lo: u64) -> Result<NeedleLocation, VolumeError> {
        // Checked before the header is written; the header itself is the move to Committed.
        crate::fsm::needle_step(self.state, crate::fsm::NeedleEvent::Finish).map_err(|r| {
            if r == crate::fsm::BODY_TOO_SHORT {
                VolumeError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, r.why))
            } else {
                refused(r, r.why)
            }
        })?;
        self.flush_chunk()?;
        let now = self.created_at.unwrap_or_else(|| self.vm.clock.now_ns());
        let key_bytes = self.key.as_bytes();
        let mut header = NeedleHeader {
            magic: NEEDLE_MAGIC,
            header_crc32c: 0,
            payload_crc32c: self.crc,
            flags: self.flags,
            bucket_id: self.bucket_id,
            key_len: key_bytes.len() as u16,
            attrs_len: self.attrs_len,
            key_hash: xxh3_64(key_bytes),
            payload_size: self.payload_size,
            created_at_ns: now,
            etag_hi,
            etag_lo,
            reserved2: 0,
        };
        let mut hdr_bytes = [0u8; 64];
        header.encode(&mut hdr_bytes);
        header.header_crc32c = crc32c::crc32c(&hdr_bytes[8..64]);
        header.encode(&mut hdr_bytes);
        self.first[0..64].copy_from_slice(&hdr_bytes);
        self.first[64..64 + key_bytes.len()].copy_from_slice(key_bytes);
        self.vm.file.write_aligned_at(self.offset, &self.first)?;
        self.vm.work.disk_write(SECTOR_SIZE as u64);
        self.vm.work.payload_copy(self.payload_size);
        let journaled = self.vm.append_index(header.key_hash, self.offset, self.total_len, self.payload_size, self.flags, self.crc);
        // The header is on disk: the record is whole, journaled or not, so it is no gap.
        self.vm.gaps.lock().remove(&self.offset);
        let seq = self.vm.written_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let loc = NeedleLocation {
            volume_id: self.vm.volume_id,
            offset: self.offset,
            total_len: self.total_len,
            payload_size: self.payload_size,
            payload_crc32c: self.crc,
            seq,
        };
        match journaled {
            Ok(()) => Ok(loc),
            Err(e) => Err(VolumeError::AfterRecord { loc, flags: self.flags, cause: format!("{:?}", e) }),
        }
    }
}

/// When an acknowledged write reaches stable storage (#29, owner decision 2026-11-12).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// The default. Writers wait for a shared sync: one sync of the volume and one of the
    /// journal cover every write that finished while the previous sync ran.
    Group,
    /// Acknowledge after the write; `flush_if_dirty` (a background thread, once a second)
    /// syncs later. A power cut loses about the last second.
    Fast,
    /// Sync after each write before answering.
    Strict,
}

impl Durability {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "group" => Some(Durability::Group),
            "fast" => Some(Durability::Fast),
            "strict" => Some(Durability::Strict),
            _ => None,
        }
    }

    fn code(self) -> u8 {
        match self {
            Durability::Group => 0,
            Durability::Fast => 1,
            Durability::Strict => 2,
        }
    }

    fn from_code(c: u8) -> Self {
        match c {
            1 => Durability::Fast,
            2 => Durability::Strict,
            _ => Durability::Group,
        }
    }
}

#[derive(Default)]
struct SyncState {
    /// Every write with a sequence number up to this one is on stable storage.
    synced: u64,
    /// A writer is running the shared sync.
    syncing: bool,
}

/// A needle found by `scan_needles`.
#[derive(Debug, Clone)]
pub struct ScannedNeedle {
    pub bucket_id: u16,
    pub key: String,
    /// Local offset in the volume.
    pub offset: u64,
    pub payload_size: u64,
    pub etag_hi: u64,
    pub etag_lo: u64,
    pub flags: u16,
    pub payload_crc32c: u32,
    pub created_at_ns: u64,
}

/// 64-byte Header of a Needle on disk.
#[derive(Debug, Clone)]
pub struct NeedleHeader {
    pub magic: u32,
    pub header_crc32c: u32,
    pub payload_crc32c: u32,
    pub flags: u16,
    pub bucket_id: u16,
    pub key_len: u16,
    /// Length of the attributes block between the key and the payload (format 3, #150).
    /// 0 for none: such a needle is byte for byte a format 2 needle.
    pub attrs_len: u16,
    pub key_hash: u64,
    pub payload_size: u64,
    pub created_at_ns: u64,
    pub etag_hi: u64,
    pub etag_lo: u64,
    pub reserved2: u32,
}

impl NeedleHeader {
    /// Where the attributes block starts, from the needle's start.
    pub fn attrs_start(&self) -> usize {
        64 + self.key_len as usize
    }

    /// Where the payload starts, from the needle's start.
    pub fn payload_start(&self) -> usize {
        64 + self.key_len as usize + self.attrs_len as usize
    }

    /// The needle's length on disk, padded to a sector.
    pub fn total_len(&self) -> u64 {
        ((self.payload_start() + self.payload_size as usize).div_ceil(SECTOR_SIZE) * SECTOR_SIZE) as u64
    }

    pub fn encode(&self, buf: &mut [u8; 64]) {
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.header_crc32c.to_le_bytes());
        buf[8..12].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        buf[12..14].copy_from_slice(&self.flags.to_le_bytes());
        buf[14..16].copy_from_slice(&self.bucket_id.to_le_bytes());
        buf[16..18].copy_from_slice(&self.key_len.to_le_bytes());
        buf[18..20].copy_from_slice(&self.attrs_len.to_le_bytes());
        buf[20..28].copy_from_slice(&self.key_hash.to_le_bytes());
        buf[28..36].copy_from_slice(&self.payload_size.to_le_bytes());
        buf[36..44].copy_from_slice(&self.created_at_ns.to_le_bytes());
        buf[44..52].copy_from_slice(&self.etag_hi.to_le_bytes());
        buf[52..60].copy_from_slice(&self.etag_lo.to_le_bytes());
        buf[60..64].copy_from_slice(&self.reserved2.to_le_bytes());
    }

    pub fn decode(buf: &[u8; 64]) -> Result<Self, VolumeError> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != NEEDLE_MAGIC {
            return Err(VolumeError::InvalidMagic {
                expected: NEEDLE_MAGIC,
                actual: magic,
            });
        }

        let header_crc32c = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let computed_header_crc = crc32c::crc32c(&buf[8..64]);
        if header_crc32c != computed_header_crc {
            return Err(VolumeError::HeaderCrcMismatch {
                expected: header_crc32c,
                actual: computed_header_crc,
            });
        }

        Ok(Self {
            magic,
            header_crc32c,
            payload_crc32c: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            flags: u16::from_le_bytes(buf[12..14].try_into().unwrap()),
            bucket_id: u16::from_le_bytes(buf[14..16].try_into().unwrap()),
            key_len: u16::from_le_bytes(buf[16..18].try_into().unwrap()),
            attrs_len: u16::from_le_bytes(buf[18..20].try_into().unwrap()),
            key_hash: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
            payload_size: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
            created_at_ns: u64::from_le_bytes(buf[36..44].try_into().unwrap()),
            etag_hi: u64::from_le_bytes(buf[44..52].try_into().unwrap()),
            etag_lo: u64::from_le_bytes(buf[52..60].try_into().unwrap()),
            reserved2: u32::from_le_bytes(buf[60..64].try_into().unwrap()),
        })
    }
}

pub struct MmapOwner(pub Arc<Mmap>);

impl AsRef<[u8]> for MmapOwner {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

/// Bytes per `.idx` journal record (key hash, volume id, offset, total length, payload size,
/// flags, payload CRC).
const IDX_RECORD: usize = 42;
/// Streaming buffer size for `.idx` journal scans (~688 KiB, an exact multiple of `IDX_RECORD`).
const IDX_SCAN_CHUNK: usize = IDX_RECORD * 16_384;

/// Streams the `.idx` journal in `IDX_SCAN_CHUNK` pieces and returns the highest indexed needle
/// offset (`>= SECTOR_SIZE`), or `None` when the journal has no valid offset.
fn scan_journal_max_offset<F: VolFile>(journal: &F, journal_len: u64) -> Result<Option<u64>, VolumeError> {
    let valid_len = journal_len - (journal_len % IDX_RECORD as u64);
    if valid_len == 0 {
        return Ok(None);
    }
    let mut buf = vec![0u8; IDX_SCAN_CHUNK.min(valid_len as usize)];
    let mut pos = 0u64;
    let mut max_off: Option<u64> = None;
    while pos < valid_len {
        let n = buf.len().min((valid_len - pos) as usize);
        journal.read_at(pos, &mut buf[..n])?;
        for r in buf[..n].chunks_exact(IDX_RECORD) {
            let o = u64::from_le_bytes(r[12..20].try_into().unwrap());
            if o >= SECTOR_SIZE as u64 {
                max_off = Some(max_off.map_or(o, |m| m.max(o)));
            }
        }
        pos += n as u64;
    }
    Ok(max_off)
}

/// Streams the `.idx` journal in `IDX_SCAN_CHUNK` pieces and returns the smallest indexed needle
/// offset strictly greater than `after` (`>= SECTOR_SIZE`), so recovery can jump over a gap
/// without retaining every offset in memory.
fn next_indexed_in_journal<F: VolFile>(journal: &F, journal_len: u64, after: u64) -> Result<Option<u64>, VolumeError> {
    let valid_len = journal_len - (journal_len % IDX_RECORD as u64);
    if valid_len == 0 {
        return Ok(None);
    }
    let mut buf = vec![0u8; IDX_SCAN_CHUNK.min(valid_len as usize)];
    let mut pos = 0u64;
    let mut best: Option<u64> = None;
    while pos < valid_len {
        let n = buf.len().min((valid_len - pos) as usize);
        journal.read_at(pos, &mut buf[..n])?;
        for r in buf[..n].chunks_exact(IDX_RECORD) {
            let o = u64::from_le_bytes(r[12..20].try_into().unwrap());
            if o >= SECTOR_SIZE as u64 && o > after {
                best = Some(best.map_or(o, |b| b.min(o)));
            }
        }
        pos += n as u64;
    }
    Ok(best)
}

/// Appends the pending journal records in order. A write that fails is cut back off the file,
/// so a partly written record never shifts the records after it, and it and the ones after
/// it stay pending.
fn write_pending<F: VolFile>(journal: &F, end: &mut u64, pending: &mut Vec<[u8; IDX_RECORD]>) -> Result<(), VolumeError> {
    let mut written = 0;
    let mut result = Ok(());
    for rec in pending.iter() {
        let start = *end;
        if let Err(e) = journal.write_at(start, rec) {
            let _ = journal.set_len(start);
            result = Err(VolumeError::Io(e));
            break;
        }
        *end = start + IDX_RECORD as u64;
        written += 1;
    }
    pending.drain(..written);
    result
}

fn verify_needle_integrity<F: VolFile>(
    window: &mut AlignedScanWindow<'_, F>,
    offset: u64,
    hdr: &NeedleHeader,
) -> bool {
    let key_end = 64usize + hdr.key_len as usize;
    let attrs_len = hdr.attrs_len as usize;
    let p_start = key_end + attrs_len;
    let Some(p_end) = (p_start as u64).checked_add(hdr.payload_size) else {
        return false;
    };
    let total_len = p_end.div_ceil(SECTOR_SIZE as u64).saturating_mul(SECTOR_SIZE as u64);
    if total_len != hdr.total_len() || p_end > total_len {
        return false;
    }
    let meta_sectors = p_start.div_ceil(SECTOR_SIZE) * SECTOR_SIZE;
    let mut crc = 0u32;
    let mut pos = 0u64;
    while pos < total_len {
        let remain = ((total_len - pos) as usize).min(SCAN_BATCH_BYTES);
        let Some(chunk) = window.read_at(offset + pos, remain) else {
            return false;
        };
        if pos == 0 {
            if remain < meta_sectors {
                return false;
            }
            if xxh3_64(&chunk[64..key_end]) != hdr.key_hash {
                return false;
            }
            if attrs_len > 0 && crate::attrs::Attrs::decode(&chunk[key_end..p_start]).is_err() {
                return false;
            }
        }
        let lo = (p_start as u64).max(pos);
        let hi = p_end.min(pos + remain as u64);
        if lo < hi {
            crc = crc32c::crc32c_append(crc, &chunk[(lo - pos) as usize..(hi - pos) as usize]);
        }
        pos += remain as u64;
    }
    crc == hdr.payload_crc32c
}
/// One write counted in `VolumeManager::writers_in_flight` (#167).
struct InFlight<'a>(&'a std::sync::atomic::AtomicUsize);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<Fs: VolFs> Drop for NeedleWriter<Fs> {
    /// The streamed write has ended: finished (its header is written) or abandoned.
    fn drop(&mut self) {
        self.vm.writers_in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct VolumeManager<Fs: VolFs = DiskFs> {
    dir: PathBuf,
    volume_id: u32,
    file: Arc<Fs::File>,
    /// The `.idx` journal, and where the next record goes.
    journal: Arc<Fs::File>,
    journal_end: Mutex<u64>,
    current_offset: AtomicU64,
    mmap: RwLock<Option<Arc<Mmap>>>,
    remap_count: AtomicU64,
    is_sparse: bool,
    /// The file's length as this process set it: a growing volume is extended in steps of
    /// `growth_step` ahead of its writes, so the read mapping, which covers the whole file,
    /// is redone once per step and not once per new read (#178). The written end is
    /// `current_offset`; what lies between is zeros, which recovery stops at and the next
    /// open truncates. A sparse volume is mapped once at its full span and never grows.
    allocated: AtomicU64,
    growth_step: AtomicU64,
    grow_lock: parking_lot::Mutex<()>,
    max_span: AtomicU64,
    work: Arc<WorkCounters>,
    clock: Arc<dyn Clock>,
    fail_next_write: std::sync::atomic::AtomicBool,
    /// Tests: the next journal append fails, after the needle is on disk (#184).
    fail_next_index: std::sync::atomic::AtomicBool,
    fail_next_sync: std::sync::atomic::AtomicBool,
    /// Journal records whose append failed, written before the next one (#184).
    pending_journal: Mutex<Vec<[u8; IDX_RECORD]>>,
    /// Writes that have reserved an offset here and not yet written their header (#167). A
    /// streamed PUT counts from `begin_needle` until its `NeedleWriter` is dropped.
    writers_in_flight: std::sync::atomic::AtomicUsize,
    /// Set by compaction before it lists the needles: no new reservation lands here after it.
    sealed: std::sync::atomic::AtomicBool,
    /// Reservations made in this process that may never get a header: a streamed needle from
    /// `begin_needle` until `finish` indexes it, and a `write_needle_at` that failed after it
    /// reserved. `scan_needles` steps over them; `indexed` covers only what the journal held
    /// at open, so without this a scan stopped at such a gap and compaction did not list the
    /// needles after it (#167). Offset to length; one entry per streamed PUT in flight.
    gaps: parking_lot::Mutex<std::collections::BTreeMap<u64, u64>>,
    /// Gaps discovered at open time (`gap_offset -> gap_len`) from the `.idx` journal.
    open_gaps: BTreeMap<u64, u64>,
    /// Highest offset in the `.idx` journal at open time, and the journal byte length then.
    max_indexed_at_open: Option<u64>,
    journal_len_at_open: u64,
    /// Format version in the superblock: what the needles of this volume may use (FORMAT.md).
    format: u32,
    durability: std::sync::atomic::AtomicU8,
    /// Sequence number of the last write whose needle and journal record are written.
    written_seq: AtomicU64,
    sync_state: std::sync::Mutex<SyncState>,
    sync_done: std::sync::Condvar,
}

impl VolumeManager<DiskFs> {
    pub fn open(dir: &Path, volume_id: u32) -> Result<Self, VolumeError> {
        let sparse = std::env::var("AWAN_SPARSE_VOLUME").ok().as_deref() == Some("1");
        Self::open_mode(dir, volume_id, sparse)
    }

    pub fn open_sparse(dir: &Path, volume_id: u32) -> Result<Self, VolumeError> {
        Self::open_mode(dir, volume_id, true)
    }

    pub fn open_mode(dir: &Path, volume_id: u32, sparse: bool) -> Result<Self, VolumeError> {
        Self::open_mode_with_clock(dir, volume_id, sparse, Arc::new(SystemClock))
    }

    pub fn open_mode_with_clock(dir: &Path, volume_id: u32, sparse: bool, clock: Arc<dyn Clock>) -> Result<Self, VolumeError> {
        Self::open_with(&DiskFs, dir, volume_id, sparse, clock)
    }
}

impl<Fs: VolFs> VolumeManager<Fs> {
    /// Opens a volume whose files live in `fs`: the disk in production, the simulation's
    /// files in tests (#213, decision 67).
    pub fn open_with(fs: &Fs, dir: &Path, volume_id: u32, mut sparse: bool, clock: Arc<dyn Clock>) -> Result<Self, VolumeError> {
        let dat_path = dir.join(format!("volume_{:04}.dat", volume_id));
        let idx_path = dir.join(format!("volume_{:04}.idx", volume_id));

        let file_exists = fs.exists(&dat_path);
        let aligned_file = fs.open(&dat_path, FileKind::Data, true)?;

        let journal = fs.open(&idx_path, FileKind::Journal, true)?;
        // Truncate any torn trailing bytes so subsequent appends stay IDX_RECORD-aligned.
        let raw_journal_len = journal.len()?;
        let journal_len = (raw_journal_len / IDX_RECORD as u64) * IDX_RECORD as u64;
        if raw_journal_len != journal_len {
            journal.set_len(journal_len)?;
            journal.sync_all()?;
        }
        let max_indexed = scan_journal_max_offset(&*journal, journal_len)?;
        let mut open_gaps: BTreeMap<u64, u64> = BTreeMap::new();

        let mut file_len = aligned_file.len()?;
        if file_len >= MAX_VOLUME_SPAN {
            sparse = true;
        }

        if !file_exists {
            // Write 4096B Superblock
            let mut super_buf = AlignedBuffer::new(SECTOR_SIZE);
            super_buf[0..4].copy_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
            super_buf[4..8].copy_from_slice(&volume_id.to_le_bytes());
            let now = clock.now_ns();
            super_buf[8..16].copy_from_slice(&now.to_le_bytes());
            let crc = crc32c::crc32c(&super_buf[0..16]);
            super_buf[16..20].copy_from_slice(&crc.to_le_bytes());
            super_buf[20..24].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
            aligned_file.write_aligned_at(0, &super_buf)?;

            if sparse {
                let _ = aligned_file.make_sparse();
                aligned_file.set_len(MAX_VOLUME_SPAN)?;
                file_len = MAX_VOLUME_SPAN;
            } else {
                file_len = SECTOR_SIZE as u64;
            }
        } else if sparse && file_len < MAX_VOLUME_SPAN {
            let _ = aligned_file.make_sparse();
            aligned_file.set_len(MAX_VOLUME_SPAN)?;
            file_len = MAX_VOLUME_SPAN;
        }

        // Run Crash Recovery Scan
        let mut scan_offset = SECTOR_SIZE as u64;
        let mut format = FORMAT_VERSION;

        if file_len < SECTOR_SIZE as u64 {
            // Corrupt superblock or empty file, rewrite superblock
            let mut super_buf = AlignedBuffer::new(SECTOR_SIZE);
            super_buf[0..4].copy_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
            super_buf[4..8].copy_from_slice(&volume_id.to_le_bytes());
            super_buf[20..24].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
            aligned_file.write_aligned_at(0, &super_buf)?;
            scan_offset = SECTOR_SIZE as u64;
        } else {
            // Verify Superblock
            let mut super_buf = AlignedBuffer::new(SECTOR_SIZE);
            aligned_file.read_aligned_at(0, &mut super_buf)?;
            let magic = u32::from_le_bytes(super_buf[0..4].try_into().unwrap());
            if magic != SUPERBLOCK_MAGIC {
                return Err(VolumeError::InvalidSuperblock);
            }
            let found = superblock_format(&super_buf);
            if found > FORMAT_VERSION {
                return Err(VolumeError::UnsupportedFormat { found, supported: FORMAT_VERSION });
            }
            format = found;

            let jump_in_journal = |at: u64, open_gaps: &mut BTreeMap<u64, u64>| -> Result<Option<u64>, VolumeError> {
                if !max_indexed.is_some_and(|m| at < m) {
                    return Ok(None);
                }
                let next = next_indexed_in_journal(&*journal, journal_len, at)?;
                if let Some(n) = next {
                    open_gaps.insert(at, n - at);
                }
                Ok(next)
            };

            // Scan valid needles
            let mut last_valid_end = SECTOR_SIZE as u64;
            let mut window = AlignedScanWindow::new(&*aligned_file, file_len, None);
            while let Some(sector) = window.read_at(scan_offset, SECTOR_SIZE) {
                let mut header_bytes = [0u8; 64];
                header_bytes.copy_from_slice(&sector[0..64]);

                let magic = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
                let Some(hdr) = (magic == NEEDLE_MAGIC)
                    .then(|| NeedleHeader::decode(&header_bytes).ok())
                    .flatten()
                    .filter(|hdr| {
                        let total_len = hdr.total_len();
                        scan_offset.checked_add(total_len).is_some_and(|end| end <= file_len)
                            && verify_needle_integrity(&mut window, scan_offset, hdr)
                    })
                else {
                    // Unwritten or torn needle: step over it when the journal knows a later needle.
                    match jump_in_journal(scan_offset, &mut open_gaps)? {
                        Some(next) => {
                            scan_offset = next;
                            continue;
                        }
                        None => break,
                    }
                };

                let total_len = hdr.total_len();
                last_valid_end = scan_offset + total_len;
                scan_offset += total_len;
            }
            scan_offset = last_valid_end;

            if sparse {
                if file_len > MAX_VOLUME_SPAN {
                    aligned_file.set_len(MAX_VOLUME_SPAN)?;
                    aligned_file.sync_all()?;
                }
            } else if file_len != scan_offset {
                // Truncate torn tail bytes if any
                aligned_file.set_len(scan_offset)?;
                aligned_file.sync_all()?;
            }
        }

        let initial_mmap = if sparse {
            aligned_file.map()?.map(Arc::new)
        } else {
            None
        };

        open_gaps.retain(|&off, _| off < scan_offset);

        let allocated = if sparse { MAX_VOLUME_SPAN } else { aligned_file.len()? };
        Ok(Self {
            dir: dir.to_path_buf(),
            volume_id,
            file: aligned_file,
            journal,
            journal_end: Mutex::new(journal_len),
            durability: std::sync::atomic::AtomicU8::new(Durability::Group.code()),
            written_seq: AtomicU64::new(0),
            sync_state: std::sync::Mutex::new(SyncState::default()),
            sync_done: std::sync::Condvar::new(),
            current_offset: AtomicU64::new(scan_offset),
            mmap: RwLock::new(initial_mmap),
            remap_count: AtomicU64::new(0),
            is_sparse: sparse,
            allocated: AtomicU64::new(allocated),
            growth_step: AtomicU64::new(growth_step_from_env()),
            grow_lock: parking_lot::Mutex::new(()),
            max_span: AtomicU64::new(MAX_VOLUME_SPAN),
            work: Arc::new(WorkCounters::default()),
            fail_next_write: std::sync::atomic::AtomicBool::new(false),
            fail_next_index: std::sync::atomic::AtomicBool::new(false),
            fail_next_sync: std::sync::atomic::AtomicBool::new(false),
            pending_journal: Mutex::new(Vec::new()),
            writers_in_flight: std::sync::atomic::AtomicUsize::new(0),
            sealed: std::sync::atomic::AtomicBool::new(false),
            gaps: parking_lot::Mutex::new(std::collections::BTreeMap::new()),
            open_gaps,
            max_indexed_at_open: max_indexed,
            journal_len_at_open: journal_len,
            format,
            clock,
        })
    }

    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The volume's format version (1 or 2). Compressed needles (#51) need 2.
    pub fn format(&self) -> u32 {
        self.format
    }

    /// The first `n` payload bytes of the needle at `local`, from its first sector.
    pub fn payload_prefix(&self, local: u64, key_len: usize, n: usize) -> Result<Vec<u8>, VolumeError> {
        let mut buf = AlignedBuffer::new(SECTOR_SIZE);
        self.work.disk_read();
        self.file.read_aligned_at(local, &mut buf)?;
        let mut hdr_bytes = [0u8; 64];
        hdr_bytes.copy_from_slice(&buf[0..64]);
        let header = NeedleHeader::decode(&hdr_bytes)?;
        let start = header.payload_start();
        if header.key_len as usize != key_len || start + n > SECTOR_SIZE {
            return Err(VolumeError::CorruptIndex);
        }
        Ok(buf[start..start + n].to_vec())
    }

    /// Fault injection for tests: the next `write_needle` reserves its offset and then fails,
    /// as a disk error would, leaving a gap in the volume.
    pub fn fail_next_write(&self) {
        self.fail_next_write.store(true, Ordering::SeqCst);
    }

    /// Tests: makes the next journal append fail after its needle was written, which leaves
    /// a complete record in the volume that the write reports as failed (#184).
    pub fn fail_next_index(&self) {
        self.fail_next_index.store(true, Ordering::SeqCst);
    }

    /// Tests: makes the next sync of the volume fail, as a disk error would, after the needles
    /// it covers were written (#184).
    pub fn fail_next_sync(&self) {
        self.fail_next_sync.store(true, Ordering::SeqCst);
    }

    /// Largest byte offset a needle may reach in this volume (default `MAX_VOLUME_SPAN`).
    /// Smaller spans are for tests and small deployments.
    pub fn set_max_span(&self, span: u64) {
        self.max_span.store(span, Ordering::SeqCst);
    }

    pub fn max_span(&self) -> u64 {
        self.max_span.load(Ordering::SeqCst)
    }

    /// How far ahead of its writes a growing volume's file is extended (#178).
    pub fn set_growth_step(&self, step: u64) {
        self.growth_step.store(step.max(SECTOR_SIZE as u64) / SECTOR_SIZE as u64 * SECTOR_SIZE as u64, Ordering::SeqCst);
    }

    /// The file's length: the written end plus at most one growth step of zeros.
    pub fn allocated_len(&self) -> u64 {
        self.allocated.load(Ordering::SeqCst)
    }

    /// Makes the file at least `end` bytes long (#178). Up to a quarter of the growth step
    /// (64 MiB by default) a volume grows by its writes, as before: remapping a small volume
    /// is cheap, and a small deployment pays no disk for space it has not written. Past that
    /// the file is extended ahead, by a quarter of its length, at least 1 MiB and at most the
    /// step, so the mapping is redone once per extension. Not past the span, and never to
    /// exactly `MAX_VOLUME_SPAN`, which is how a sparse volume is recognised. The extension
    /// is a plain length change, not a sparse file, so a flush costs what it costs for a file
    /// that grows by writing (decision 52).
    fn ensure_allocated(&self, end: u64) -> Result<(), VolumeError> {
        let step = self.growth_step.load(Ordering::SeqCst);
        if self.is_sparse || end <= step / 4 || end <= self.allocated.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _g = self.grow_lock.lock();
        // Below the threshold the file grew by its writes; it is at least that long now.
        let current = self.allocated.load(Ordering::SeqCst).max(step / 4);
        if end <= self.allocated.load(Ordering::SeqCst) {
            return Ok(());
        }
        let grow = (current / 4).clamp(step.min(1 << 20), step);
        let limit = self.max_span().min(MAX_VOLUME_SPAN - SECTOR_SIZE as u64);
        let target = end.max(current + grow).div_ceil(SECTOR_SIZE as u64).saturating_mul(SECTOR_SIZE as u64).min(limit).max(end);
        self.file.set_len(target)?;
        self.allocated.store(target, Ordering::SeqCst);
        Ok(())
    }

    pub fn id(&self) -> u32 {
        self.volume_id
    }

    /// Counts this volume's disk writes, reads and payload copies into `work`.
    pub fn durability(&self) -> Durability {
        Durability::from_code(self.durability.load(Ordering::Relaxed))
    }

    pub fn set_durability(&self, mode: Durability) {
        self.durability.store(mode.code(), Ordering::Relaxed);
    }

    /// Syncs the volume, then the journal (two counted syncs), and marks every write that had
    /// finished before the sync started as durable.
    fn sync_files(&self) -> Result<(), VolumeError> {
        if self.fail_next_sync.swap(false, Ordering::SeqCst) {
            return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, "injected sync failure")));
        }
        let target = self.written_seq.load(Ordering::SeqCst);
        self.flush_pending_journal()?;
        self.file.sync_data()?;
        self.work.disk_sync();
        self.journal.sync_data()?;
        self.work.disk_sync();
        let mut st = self.sync_state.lock().unwrap_or_else(|e| e.into_inner());
        st.synced = st.synced.max(target);
        Ok(())
    }

    /// Appends one record to the `.idx` journal. A record that cannot be written stays pending
    /// and is written before the next one, or by the next sync (#184): recovery steps over a
    /// gap only through the journal, so a needle whose journal append failed would otherwise
    /// be lost at a restart when it sits after a gap. Scanning the gap for a header instead
    /// was rejected: a gap can hold a client's own bytes (an abandoned streamed PUT), and a
    /// header CRC is not a secret, so a planted header would come back as a real object.
    fn append_index(&self, key_hash: u64, offset: u64, total_len: u64, payload_size: u64, flags: u16, crc: u32) -> Result<(), VolumeError> {
        let mut rec = [0u8; IDX_RECORD];
        rec[0..8].copy_from_slice(&key_hash.to_le_bytes());
        rec[8..12].copy_from_slice(&self.volume_id.to_le_bytes());
        rec[12..20].copy_from_slice(&offset.to_le_bytes());
        rec[20..28].copy_from_slice(&total_len.to_le_bytes());
        rec[28..36].copy_from_slice(&payload_size.to_le_bytes());
        rec[36..38].copy_from_slice(&flags.to_le_bytes());
        rec[38..42].copy_from_slice(&crc.to_le_bytes());
        let mut end = self.journal_end.lock();
        let mut pending = self.pending_journal.lock();
        pending.push(rec);
        if self.fail_next_index.swap(false, Ordering::SeqCst) {
            return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, "injected index failure")));
        }
        write_pending(&*self.journal, &mut end, &mut pending)
    }

    /// Writes journal records left pending by a failed append (#184).
    fn flush_pending_journal(&self) -> Result<(), VolumeError> {
        let mut end = self.journal_end.lock();
        let mut pending = self.pending_journal.lock();
        write_pending(&*self.journal, &mut end, &mut pending)
    }

    /// Tests: journal records not yet written.
    pub fn pending_journal_len(&self) -> usize {
        self.pending_journal.lock().len()
    }

    /// Syncs the volume and its journal now, whatever the durability mode.
    pub fn sync_now(&self) -> Result<(), VolumeError> {
        self.sync_files()
    }

    /// Returns once the write numbered `seq` is as durable as the mode promises: on disk for
    /// `Group` and `Strict`, immediately for `Fast`. Blocking; call it off the async executor.
    pub fn make_durable(&self, seq: u64) -> Result<(), VolumeError> {
        match self.durability() {
            Durability::Fast => Ok(()),
            Durability::Strict => self.sync_files(),
            Durability::Group => {
                let mut st = self.sync_state.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    if st.synced >= seq {
                        return Ok(());
                    }
                    if !st.syncing {
                        break;
                    }
                    st = self.sync_done.wait(st).unwrap_or_else(|e| e.into_inner());
                }
                st.syncing = true;
                drop(st);
                let r = self.sync_files();
                let mut st = self.sync_state.lock().unwrap_or_else(|e| e.into_inner());
                st.syncing = false;
                drop(st);
                self.sync_done.notify_all();
                r
            }
        }
    }

    /// Syncs when a write is not yet durable. Returns whether it synced. The `Fast` mode's
    /// background flusher calls this; it is harmless in the other modes.
    pub fn flush_if_dirty(&self) -> Result<bool, VolumeError> {
        let synced = self.sync_state.lock().unwrap_or_else(|e| e.into_inner()).synced;
        if self.written_seq.load(Ordering::SeqCst) <= synced {
            return Ok(false);
        }
        self.sync_files()?;
        Ok(true)
    }

    /// Starts a thread that calls `flush_if_dirty` every `every` while the volume is alive.
    pub fn start_flusher(self: &Arc<Self>, every: std::time::Duration) {
        let weak = Arc::downgrade(self);
        std::thread::spawn(move || loop {
            std::thread::sleep(every);
            match weak.upgrade() {
                Some(vm) => {
                    if vm.durability() == Durability::Fast {
                        let _ = vm.flush_if_dirty();
                    }
                }
                None => return,
            }
        });
    }

    pub fn attach_work_counters(&mut self, work: Arc<WorkCounters>) {
        self.work = work;
    }

    pub fn work_counters(&self) -> Arc<WorkCounters> {
        Arc::clone(&self.work)
    }

    pub fn file(&self) -> Arc<Fs::File> {
        Arc::clone(&self.file)
    }

    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::SeqCst)
    }

    pub fn remap_count(&self) -> u64 {
        self.remap_count.load(Ordering::Relaxed)
    }

    pub fn is_sparse(&self) -> bool {
        self.is_sparse
    }

    pub fn get_or_remap(&self, required_end: u64) -> Result<Arc<Mmap>, VolumeError> {
        {
            let guard = self.mmap.read();
            if let Some(m) = &*guard {
                if m.len() as u64 >= required_end {
                    return Ok(Arc::clone(m));
                }
            }
        }
        let mut guard = self.mmap.write();
        if let Some(m) = &*guard {
            if m.len() as u64 >= required_end {
                return Ok(Arc::clone(m));
            }
        }
        let file_len = self.file.len()?;
        if file_len < required_end && file_len == 0 {
            return Err(VolumeError::TornNeedleAtOffset(required_end));
        }
        self.remap_count.fetch_add(1, Ordering::Relaxed);
        let mmap = self.file.map()?.ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Unsupported, "this volume's files have no read mapping"))
        })?;
        let arc_m = Arc::new(mmap);
        *guard = Some(Arc::clone(&arc_m));
        Ok(arc_m)
    }

    pub fn write_needle(
        &self,
        bucket_id: u16,
        key: &str,
        payload: &[u8],
        flags: u16,
        etag_hi: u64,
        etag_lo: u64,
    ) -> Result<NeedleLocation, VolumeError> {
        self.write_needle_at(bucket_id, key, payload, flags, etag_hi, etag_lo, None)
    }

    /// The flags of a needle with this block, or `VolumeFull` (move to a newer volume) when
    /// this volume is older than format 3, which an older AwanFS would misread.
    fn attrs_flags(&self, attrs: &[u8], flags: u16) -> Result<u16, VolumeError> {
        if attrs.is_empty() {
            return Ok(flags);
        }
        if attrs.len() > crate::attrs::MAX_BLOCK {
            return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, "attributes block over 65535 bytes")));
        }
        if self.format < 3 {
            return Err(VolumeError::VolumeFull { offset: self.current_offset.load(Ordering::SeqCst), len: 0 });
        }
        Ok(flags | crate::meta_index::FLAG_ATTRS)
    }

    /// The attributes block of the needle at `offset` (empty for none), read from the
    /// mapping and checked (`crate::attrs::Attrs::decode`). HEAD and GET read it only for
    /// rows flagged `FLAG_ATTRS` (#150).
    pub fn read_attrs(&self, offset: u64) -> Result<crate::attrs::Attrs, VolumeError> {
        let mmap = self.get_or_remap(offset + SECTOR_SIZE as u64)?;
        if offset as usize + 64 > mmap.len() {
            return Err(VolumeError::TornNeedleAtOffset(offset));
        }
        let mut hdr_bytes = [0u8; 64];
        hdr_bytes.copy_from_slice(&mmap[offset as usize..offset as usize + 64]);
        let header = NeedleHeader::decode(&hdr_bytes)?;
        if header.attrs_len == 0 {
            return Ok(crate::attrs::Attrs::new());
        }
        let start = offset as usize + header.attrs_start();
        let end = start + header.attrs_len as usize;
        let mmap = if end > mmap.len() { self.get_or_remap(end as u64)? } else { mmap };
        if end > mmap.len() {
            return Err(VolumeError::TornNeedleAtOffset(offset));
        }
        crate::attrs::Attrs::decode(&mmap[start..end]).map_err(|_| VolumeError::CorruptIndex)
    }

    /// `write_needle` with a given creation time: a version id, kept by compaction (#31).
    #[allow(clippy::too_many_arguments)]
    pub fn write_needle_at(
        &self,
        bucket_id: u16,
        key: &str,
        payload: &[u8],
        flags: u16,
        etag_hi: u64,
        etag_lo: u64,
        created_at: Option<u64>,
    ) -> Result<NeedleLocation, VolumeError> {
        self.write_needle_with_attrs(bucket_id, key, &[], payload, flags, etag_hi, etag_lo, created_at)
    }

    /// `write_needle_at` with an attributes block (`crate::attrs`, format 3, #150). A needle
    /// with a block is flagged `FLAG_ATTRS` and goes only into a format 3 volume.
    #[allow(clippy::too_many_arguments)]
    pub fn write_needle_with_attrs(
        &self,
        bucket_id: u16,
        key: &str,
        attrs: &[u8],
        payload: &[u8],
        flags: u16,
        etag_hi: u64,
        etag_lo: u64,
        created_at: Option<u64>,
    ) -> Result<NeedleLocation, VolumeError> {
        let flags = self.attrs_flags(attrs, flags)?;
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u16;
        let payload_size = payload.len() as u64;
        let key_hash = xxh3_64(key_bytes);
        let payload_crc32c = crc32c::crc32c(payload);

        let p_start = 64 + key_bytes.len() + attrs.len();
        let raw_len = p_start + payload.len();
        let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;

        let mut buf = AlignedBuffer::new(total_len as usize);

        let now = created_at.unwrap_or_else(|| self.clock.now_ns());
        let mut header = NeedleHeader {
            magic: NEEDLE_MAGIC,
            header_crc32c: 0,
            payload_crc32c,
            flags,
            bucket_id,
            key_len,
            attrs_len: attrs.len() as u16,
            key_hash,
            payload_size,
            created_at_ns: now,
            etag_hi,
            etag_lo,
            reserved2: 0,
        };

        let mut hdr_bytes = [0u8; 64];
        header.encode(&mut hdr_bytes);

        // Compute CRC32C over offset 8..64
        let header_crc = crc32c::crc32c(&hdr_bytes[8..64]);
        header.header_crc32c = header_crc;
        header.encode(&mut hdr_bytes);

        buf[0..64].copy_from_slice(&hdr_bytes);
        buf[64..64 + key_bytes.len()].copy_from_slice(key_bytes);
        buf[64 + key_bytes.len()..p_start].copy_from_slice(attrs);
        buf[p_start..raw_len].copy_from_slice(payload);
        self.work.payload_copy(payload.len() as u64);
        if total_len as usize > raw_len {
            buf[raw_len..total_len as usize].fill(0);
        }

        // Counted from before the reservation to the end of this function (#167).
        let _in_flight = self.enter_write(total_len)?;
        // A WORM volume never reuses space: a needle that would end past the span is refused,
        // and the reserved range stays unused, so later writes are refused too.
        let offset = self.current_offset.fetch_add(total_len, Ordering::SeqCst);
        if offset + total_len > self.max_span() {
            return Err(VolumeError::VolumeFull { offset, len: total_len });
        }
        let written = if self.fail_next_write.swap(false, Ordering::SeqCst) {
            Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, "injected write failure")))
        } else {
            // Write payload tail sectors before sector 0 (NeedleHeader) so a crash never leaves
            // a valid header pointing at unwritten tail sectors (matching NeedleWriter::finish).
            self.ensure_allocated(offset + total_len).and_then(|()| {
                if total_len > SECTOR_SIZE as u64 {
                    self.file.write_aligned_at(offset + SECTOR_SIZE as u64, &buf[SECTOR_SIZE..])?;
                    self.file.write_aligned_at(offset, &buf[..SECTOR_SIZE])?;
                } else {
                    self.file.write_aligned_at(offset, &buf)?;
                }
                Ok(())
            })
        };
        if let Err(e) = written {
            self.gaps.lock().insert(offset, total_len);
            return Err(e);
        }
        self.work.disk_write(total_len);

        let journaled = self.append_index(key_hash, offset, total_len, payload_size, flags, payload_crc32c);
        let seq = self.written_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let loc = NeedleLocation {
            volume_id: self.volume_id,
            offset,
            total_len,
            payload_size,
            payload_crc32c,
            seq,
        };
        match journaled {
            Ok(()) => Ok(loc),
            Err(e) => Err(VolumeError::AfterRecord { loc, flags, cause: format!("{:?}", e) }),
        }
    }

    /// Starts a needle whose payload arrives in pieces (#36). The space is reserved now; the
    /// payload is written in `STREAM_CHUNK` pieces as it arrives, and the first sector, which
    /// holds the header with the payload CRC, is written last by `NeedleWriter::finish`.
    /// A writer dropped before `finish` leaves a gap that recovery steps over (#38).
    pub fn begin_needle(self: &Arc<Self>, bucket_id: u16, key: &str, payload_size: u64, flags: u16) -> Result<NeedleWriter<Fs>, VolumeError> {
        self.begin_needle_with_attrs(bucket_id, key, &[], payload_size, flags)
    }

    /// `begin_needle` with an attributes block, written before the payload (#150).
    pub fn begin_needle_with_attrs(self: &Arc<Self>, bucket_id: u16, key: &str, attrs: &[u8], payload_size: u64, flags: u16) -> Result<NeedleWriter<Fs>, VolumeError> {
        let flags = self.attrs_flags(attrs, flags)?;
        let key_len = key.len();
        let data_start = 64 + key_len as u64 + attrs.len() as u64;
        let total_len = (data_start + payload_size).div_ceil(SECTOR_SIZE as u64) * SECTOR_SIZE as u64;
        // Counted until the writer is dropped, after its header is written or it is abandoned
        // (#167). `forget` hands the count to the writer below.
        let in_flight = self.enter_write(total_len)?;
        let offset = self.current_offset.fetch_add(total_len, Ordering::SeqCst);
        if offset + total_len > self.max_span() {
            return Err(VolumeError::VolumeFull { offset, len: total_len });
        }
        // A gap until `finish` indexes it, whatever happens to the writer.
        self.gaps.lock().insert(offset, total_len);
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, "injected write failure")));
        }
        // The whole needle at once: its chunks then never extend the file (#178).
        self.ensure_allocated(offset + total_len)?;
        let mut first = AlignedBuffer::new(SECTOR_SIZE);
        first.fill(0);
        let mut writer = NeedleWriter {
            vm: Arc::clone(self),
            bucket_id,
            key: key.to_string(),
            flags,
            offset,
            total_len,
            payload_size,
            attrs_len: attrs.len() as u16,
            data_start,
            written: 0,
            crc: 0,
            first,
            chunk: AlignedBuffer::new(STREAM_CHUNK),
            chunk_start: SECTOR_SIZE as u64,
            chunk_fill: 0,
            created_at: None,
            state: crate::fsm::NeedleState::Receiving { declared: payload_size, written: 0 },
            _inflight: self.work.hold_inflight((STREAM_CHUNK + SECTOR_SIZE) as u64),
        };
        // The writer now owns the count: its `Drop` gives it back.
        std::mem::forget(in_flight);
        // The block goes where it belongs in the image: into sector 0 when it fits, the rest
        // through the chunk buffer like payload bytes.
        writer.place(64 + key_len as u64, attrs)?;
        Ok(writer)
    }

    /// Counts a write that is about to reserve an offset, or refuses it with `VolumeFull`
    /// when the volume is sealed, so the caller moves to the active volume. The count goes up
    /// before `sealed` is read, and `seal` sets `sealed` before compaction reads the count
    /// (both SeqCst): either the write sees the seal, or compaction sees the write.
    fn enter_write(&self, len: u64) -> Result<InFlight<'_>, VolumeError> {
        self.writers_in_flight.fetch_add(1, Ordering::SeqCst);
        let guard = InFlight(&self.writers_in_flight);
        if self.sealed.load(Ordering::SeqCst) {
            return Err(VolumeError::VolumeFull { offset: self.current_offset.load(Ordering::SeqCst), len });
        }
        Ok(guard)
    }

    /// No reservation lands in this volume after this returns (#167). Compaction seals the
    /// volume it is about to list; writers still holding it are sent to the active volume.
    pub fn seal(&self) {
        self.sealed.store(true, Ordering::SeqCst);
    }

    /// Writes that reserved an offset here and have not written their header yet.
    pub fn writers_in_flight(&self) -> usize {
        self.writers_in_flight.load(Ordering::SeqCst)
    }

    /// Reads a needle payload as a zero-copy Bytes slice directly from the memory map.
    pub fn read_needle_bytes(
        &self,
        _volume_id: u32,
        offset: u64,
        expected_payload_crc: u32,
    ) -> Result<Bytes, VolumeError> {
        let payload = self.needle_payload_unverified(offset)?;
        let actual_crc = crc32c::crc32c(&payload);
        self.work.payload_verify(payload.len() as u64);
        if actual_crc != expected_payload_crc {
            return Err(VolumeError::CrcMismatch {
                expected: expected_payload_crc,
                actual: actual_crc,
            });
        }
        Ok(payload)
    }

    /// Reads a needle payload from the mapping and verifies it against the `payload_crc32c`
    /// stored in its 64-byte CRC-protected header (#323, Decision 7A).
    pub fn read_needle_bytes_verified(&self, bucket_id: u32, offset: u64) -> Result<Bytes, VolumeError> {
        let (header, payload) = self.needle_header_and_payload_unverified(offset)?;
        if header.bucket_id != bucket_id as u16 {
            return Err(VolumeError::CorruptIndex);
        }
        let actual_crc = crc32c::crc32c(&payload);
        self.work.payload_verify(payload.len() as u64);
        if actual_crc != header.payload_crc32c {
            return Err(VolumeError::CrcMismatch {
                expected: header.payload_crc32c,
                actual: actual_crc,
            });
        }
        Ok(payload)
    }

    /// Where the payload of the needle at `local` starts in the file, and its length, from
    /// one aligned read of its header sector: no mapping (#155).
    pub fn payload_extent(&self, local: u64) -> Result<(u64, u64), VolumeError> {
        let mut buf = AlignedBuffer::new(SECTOR_SIZE);
        self.read_aligned(local, &mut buf)?;
        let mut hdr_bytes = [0u8; 64];
        hdr_bytes.copy_from_slice(&buf[0..64]);
        let header = NeedleHeader::decode(&hdr_bytes)?;
        Ok((local + header.payload_start() as u64, header.payload_size))
    }

    /// Like `payload_extent`, also returning the header's `payload_crc32c` and checking
    /// `bucket_id` from the same aligned header-sector read (#323, Decision 7A).
    pub fn payload_extent_with_crc(&self, bucket_id: u32, local: u64) -> Result<(u64, u64, u32), VolumeError> {
        let mut buf = AlignedBuffer::new(SECTOR_SIZE);
        self.read_aligned(local, &mut buf)?;
        let mut hdr_bytes = [0u8; 64];
        hdr_bytes.copy_from_slice(&buf[0..64]);
        let header = NeedleHeader::decode(&hdr_bytes)?;
        if header.bucket_id != bucket_id as u16 {
            return Err(VolumeError::CorruptIndex);
        }
        Ok((local + header.payload_start() as u64, header.payload_size, header.payload_crc32c))
    }

    /// A positioned read of `buf.len()` bytes at `offset` through the page cache: any offset
    /// and length. Repeat reads of a hot object come from memory, and the pages do not count
    /// in the RSS as mapped pages do (#155).
    pub fn read_cached(&self, offset: u64, buf: &mut [u8]) -> Result<(), VolumeError> {
        self.work.disk_read();
        self.file.read_at(offset, buf)?;
        Ok(())
    }

    /// A positioned read of `buf.len()` bytes at `offset`, both sector aligned, with direct
    /// I/O: the bytes do not stay in the page cache, so they do not add to the RSS (#155).
    /// Concurrent reads do not share a file cursor.
    pub fn read_aligned(&self, offset: u64, buf: &mut [u8]) -> Result<(), VolumeError> {
        self.work.disk_read();
        self.file.read_aligned_at(offset, buf)?;
        Ok(())
    }

    /// Calls `f` with the payload of the needle at `offset`, in pieces of at most
    /// `STREAM_CHUNK`, read with aligned reads into one buffer: not through the mapping, so
    /// copying a large needle does not fault it into the RSS (#185). The payload CRC is
    /// computed as it is read; on a mismatch the error comes after the last piece, so the
    /// caller must not commit what `f` built before this returns `Ok`.
    pub fn copy_payload<F>(&self, offset: u64, expected_payload_crc: u32, mut f: F) -> Result<u64, VolumeError>
    where
        F: FnMut(&[u8]) -> Result<(), VolumeError>,
    {
        let mut buf = AlignedBuffer::new(STREAM_CHUNK);
        self.work.disk_read();
        self.file.read_aligned_at(offset, &mut buf[..SECTOR_SIZE])?;
        let mut hdr_bytes = [0u8; 64];
        hdr_bytes.copy_from_slice(&buf[0..64]);
        let header = NeedleHeader::decode(&hdr_bytes)?;
        // After the key and the attributes block (#150).
        let p_start = header.payload_start() as u64;
        let p_end = p_start + header.payload_size;
        let total_len = header.total_len();
        let mut crc = 0u32;
        let mut pos = 0u64;
        while pos < total_len {
            let n = (STREAM_CHUNK as u64).min(total_len - pos) as usize;
            if pos > 0 {
                self.work.disk_read();
                self.file.read_aligned_at(offset + pos, &mut buf[..n])?;
            } else if n > SECTOR_SIZE {
                self.work.disk_read();
                self.file.read_aligned_at(offset, &mut buf[..n])?;
            }
            let lo = p_start.max(pos);
            let hi = p_end.min(pos + n as u64);
            if lo < hi {
                let piece = &buf[(lo - pos) as usize..(hi - pos) as usize];
                crc = crc32c::crc32c_append(crc, piece);
                f(piece)?;
            }
            pos += n as u64;
        }
        self.work.payload_verify(header.payload_size);
        if crc != expected_payload_crc {
            return Err(VolumeError::CrcMismatch { expected: expected_payload_crc, actual: crc });
        }
        Ok(header.payload_size)
    }

    /// Decodes the CRC-protected 64-byte needle header and returns it alongside an unverified
    /// slice of the volume mapping covering the payload (#323).
    pub fn needle_header_and_payload_unverified(&self, offset: u64) -> Result<(NeedleHeader, Bytes), VolumeError> {
        let mmap = self.get_or_remap(offset + SECTOR_SIZE as u64)?;
        if (offset as usize + 64) > mmap.len() {
            return Err(VolumeError::TornNeedleAtOffset(offset));
        }

        let mut hdr_bytes = [0u8; 64];
        hdr_bytes.copy_from_slice(&mmap[offset as usize..offset as usize + 64]);
        let header = NeedleHeader::decode(&hdr_bytes)?;
        let total_len = header.total_len();

        let mmap = if (offset + total_len) as usize > mmap.len() {
            self.get_or_remap(offset + total_len)?
        } else {
            mmap
        };

        let p_start = offset as usize + header.payload_start();
        let p_end = p_start + header.payload_size as usize;
        if p_end > mmap.len() {
            return Err(VolumeError::TornNeedleAtOffset(offset));
        }

        let full_bytes = Bytes::from_owner(MmapOwner(mmap));
        Ok((header, full_bytes.slice(p_start..p_end)))
    }

    /// A needle's payload as a slice of the mapping, with its header checked and its bytes
    /// not: nothing of the payload is read. For objects above 4 MiB, which are checksummed
    /// while they are sent, and for ranges of them, which are not checksummed (#155,
    /// decision 46). Everything else reads through `read_needle_bytes`.
    pub fn needle_payload_unverified(&self, offset: u64) -> Result<Bytes, VolumeError> {
        self.needle_header_and_payload_unverified(offset).map(|(_, payload)| payload)
    }

    /// Whether `p` points into this volume's current mapping. A payload the write path never
    /// read back is not in it (#126): writes go through `AlignedFile` with direct I/O, so
    /// reading a payload through the mapping faults it in from disk and adds it to the RSS.
    pub fn maps(&self, p: *const u8) -> bool {
        self.mmap.read().as_ref().is_some_and(|m| {
            let base = m.as_ptr() as usize;
            (base..base + m.len()).contains(&(p as usize))
        })
    }

    pub fn read_needle(
        &self,
        volume_id: u32,
        offset: u64,
        expected_payload_crc: u32,
    ) -> Result<Vec<u8>, VolumeError> {
        let bytes = self.read_needle_bytes(volume_id, offset, expected_payload_crc)?;
        self.work.payload_copy(bytes.len() as u64);
        Ok(bytes.to_vec())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn offset(&self) -> u64 {
        self.current_offset.load(Ordering::SeqCst)
    }

    /// Scans the volume sequentially from superblock offset to EOF, validating headers
    /// and invoking `visitor(bucket_id, key, offset, size, etag_hi, etag_lo, flags, payload_crc32c)`.
    pub fn recover_needles<F>(&self, mut visitor: F) -> Result<(), VolumeError>
    where
        F: FnMut(u16, String, u64, u64, u64, u64, u16, u32),
    {
        self.scan_needles(|n| visitor(n.bucket_id, n.key, n.offset, n.payload_size, n.etag_hi, n.etag_lo, n.flags, n.payload_crc32c))
    }

    /// `recover_needles` with every header field the replay needs, creation time included.
    pub fn scan_needles<F>(&self, mut visitor: F) -> Result<(), VolumeError>
    where
        F: FnMut(ScannedNeedle),
    {
        match self.try_scan_needles(|n| {
            visitor(n);
            Ok::<(), std::convert::Infallible>(())
        })? {
            Ok(()) => Ok(()),
            Err(never) => match never {},
        }
    }

    fn jump_gap_after(&self, scan_offset: u64) -> Option<u64> {
        if let Some(len) = self.gaps.lock().get(&scan_offset).copied() {
            return Some(scan_offset + len);
        }
        if let Some(len) = self.open_gaps.get(&scan_offset).copied() {
            return Some(scan_offset + len);
        }
        let j_end = *self.journal_end.lock();
        if self.max_indexed_at_open.is_some_and(|m| scan_offset < m) || j_end > self.journal_len_at_open {
            if let Ok(Some(next)) = next_indexed_in_journal(&*self.journal, j_end, scan_offset) {
                return Some(next);
            }
        }
        None
    }

    /// Streams needles in exact offset order directly to `visitor`, propagating any visitor error
    /// without buffering the volume's records in memory (#323).
    pub fn try_scan_needles<E, F>(&self, mut visitor: F) -> Result<Result<(), E>, VolumeError>
    where
        F: FnMut(ScannedNeedle) -> Result<(), E>,
    {
        // The written end, not the file's length: a growing volume holds up to one growth
        // step of zeros past it (#178).
        let file_len = self.file.len()?.min(self.current_offset.load(Ordering::SeqCst));
        let mut scan_offset = SECTOR_SIZE as u64;
        let mut window = AlignedScanWindow::new(&*self.file, file_len, Some(&self.work));

        while let Some(sector) = window.read_at(scan_offset, SECTOR_SIZE) {
            if let Some(len) = self
                .gaps
                .lock()
                .get(&scan_offset)
                .copied()
                .or_else(|| self.open_gaps.get(&scan_offset).copied())
            {
                if len > 0 {
                    scan_offset += len;
                    continue;
                }
            }
            let mut header_bytes = [0u8; 64];
            header_bytes.copy_from_slice(&sector[0..64]);

            let hdr = match NeedleHeader::decode(&header_bytes) {
                Ok(h) => h,
                Err(_) => match self.jump_gap_after(scan_offset) {
                    Some(next) => {
                        scan_offset = next;
                        continue;
                    }
                    None => break,
                },
            };

            let total_len = hdr.total_len();

            if scan_offset.checked_add(total_len).is_none_or(|end| end > file_len) {
                match self.jump_gap_after(scan_offset) {
                    Some(next) => {
                        scan_offset = next;
                        continue;
                    }
                    None => break,
                }
            }

            let key_end = 64 + hdr.key_len as usize;
            let key_str = if key_end <= SECTOR_SIZE {
                String::from_utf8_lossy(&sector[64..key_end]).to_string()
            } else {
                let need_bytes = key_end.div_ceil(SECTOR_SIZE) * SECTOR_SIZE;
                let slice = match window.read_at(scan_offset, need_bytes) {
                    Some(s) => s,
                    None => break,
                };
                String::from_utf8_lossy(&slice[64..key_end]).to_string()
            };

            if let Err(e) = visitor(ScannedNeedle {
                bucket_id: hdr.bucket_id,
                key: key_str,
                offset: scan_offset,
                payload_size: hdr.payload_size,
                etag_hi: hdr.etag_hi,
                etag_lo: hdr.etag_lo,
                flags: hdr.flags,
                payload_crc32c: hdr.payload_crc32c,
                created_at_ns: hdr.created_at_ns,
            }) {
                return Ok(Err(e));
            }

            scan_offset += total_len;
        }

        Ok(Ok(()))
    }
}
