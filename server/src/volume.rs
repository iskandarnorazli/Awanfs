//! awan-storage/server/src/volume.rs
//! 32 GiB WORM Needle Volume Slab Storage Engine and Crash Recovery Journal (#470)

use std::fs::{File, OpenOptions};
use std::collections::BTreeSet;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use memmap2::Mmap;
use parking_lot::{Mutex, RwLock};
use xxhash_rust::xxh3::xxh3_64;

pub use crate::direct_io::SECTOR_SIZE;
use crate::direct_io::{AlignedBuffer, AlignedFile};
use crate::work::WorkCounters;

pub const NEEDLE_MAGIC: u32 = 0x41574E33; // "AWN3"
pub const SUPERBLOCK_MAGIC: u32 = 0x53555042; // "SUPB"

/// On-disk format of volumes, journals and manifests (#50, owner decision 20), stored in the
/// superblock at bytes 20..24. Volumes written before the field existed hold 0 there and are
/// format 1. A newer version is refused, an older one read as it is; nothing is upgraded in
/// place. Layout: FORMAT.md.
pub const FORMAT_VERSION: u32 = 2;

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

/// A needle being written as its payload arrives; see `VolumeManager::begin_needle`.
pub struct NeedleWriter {
    vm: Arc<VolumeManager>,
    bucket_id: u16,
    key: String,
    flags: u16,
    offset: u64,
    total_len: u64,
    payload_size: u64,
    written: u64,
    crc: u32,
    /// Sector 0 of the needle: header, key, start of the payload. Written last.
    first: AlignedBuffer,
    /// The piece of the needle image starting at `chunk_start`.
    chunk: AlignedBuffer,
    chunk_start: u64,
    chunk_fill: usize,
}

impl NeedleWriter {
    pub fn volume(&self) -> &Arc<VolumeManager> {
        &self.vm
    }

    pub fn written(&self) -> u64 {
        self.written
    }

    pub fn write(&mut self, mut data: &[u8]) -> Result<(), VolumeError> {
        if self.written + data.len() as u64 > self.payload_size {
            return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, "more bytes than the declared length")));
        }
        self.crc = crc32c::crc32c_append(self.crc, data);
        let mut pos = 64 + self.key.len() as u64 + self.written;
        self.written += data.len() as u64;
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
                self.flush_chunk()?;
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
        if self.written != self.payload_size {
            return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "fewer bytes than the declared length")));
        }
        self.flush_chunk()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        let key_bytes = self.key.as_bytes();
        let mut header = NeedleHeader {
            magic: NEEDLE_MAGIC,
            header_crc32c: 0,
            payload_crc32c: self.crc,
            flags: self.flags,
            bucket_id: self.bucket_id,
            key_len: key_bytes.len() as u16,
            reserved: 0,
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
        self.vm.append_index(header.key_hash, self.offset, self.total_len, self.payload_size, self.flags, self.crc)?;
        let seq = self.vm.written_seq.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(NeedleLocation {
            volume_id: self.vm.volume_id,
            offset: self.offset,
            total_len: self.total_len,
            payload_size: self.payload_size,
            payload_crc32c: self.crc,
            seq,
        })
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

/// 64-byte Header of a Needle on disk.
#[derive(Debug, Clone)]
pub struct NeedleHeader {
    pub magic: u32,
    pub header_crc32c: u32,
    pub payload_crc32c: u32,
    pub flags: u16,
    pub bucket_id: u16,
    pub key_len: u16,
    pub reserved: u16,
    pub key_hash: u64,
    pub payload_size: u64,
    pub created_at_ns: u64,
    pub etag_hi: u64,
    pub etag_lo: u64,
    pub reserved2: u32,
}

impl NeedleHeader {
    pub fn encode(&self, buf: &mut [u8; 64]) {
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.header_crc32c.to_le_bytes());
        buf[8..12].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        buf[12..14].copy_from_slice(&self.flags.to_le_bytes());
        buf[14..16].copy_from_slice(&self.bucket_id.to_le_bytes());
        buf[16..18].copy_from_slice(&self.key_len.to_le_bytes());
        buf[18..20].copy_from_slice(&self.reserved.to_le_bytes());
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
            reserved: u16::from_le_bytes(buf[18..20].try_into().unwrap()),
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

/// The next needle offset the journal knows about after `after`: recovery jumps there over a
/// gap left by a failed or interrupted write (#38).
fn next_indexed(indexed: &BTreeSet<u64>, after: u64) -> Option<u64> {
    indexed.range(after + 1..).next().copied()
}

pub struct VolumeManager {
    dir: PathBuf,
    volume_id: u32,
    file: Arc<AlignedFile>,
    idx_file: Mutex<File>,
    current_offset: AtomicU64,
    mmap: RwLock<Option<Arc<Mmap>>>,
    remap_count: AtomicU64,
    is_sparse: bool,
    max_span: AtomicU64,
    work: Arc<WorkCounters>,
    fail_next_write: std::sync::atomic::AtomicBool,
    /// Needle offsets from the `.idx` journal at open time.
    indexed: BTreeSet<u64>,
    /// Format version in the superblock: what the needles of this volume may use (FORMAT.md).
    format: u32,
    /// A second handle on the journal, so a sync does not hold the append lock.
    idx_sync: File,
    durability: std::sync::atomic::AtomicU8,
    /// Sequence number of the last write whose needle and journal record are written.
    written_seq: AtomicU64,
    sync_state: std::sync::Mutex<SyncState>,
    sync_done: std::sync::Condvar,
}

impl VolumeManager {
    pub fn open(dir: &Path, volume_id: u32) -> Result<Self, VolumeError> {
        let sparse = std::env::var("AWAN_SPARSE_VOLUME").ok().as_deref() == Some("1");
        Self::open_mode(dir, volume_id, sparse)
    }

    pub fn open_sparse(dir: &Path, volume_id: u32) -> Result<Self, VolumeError> {
        Self::open_mode(dir, volume_id, true)
    }

    pub fn open_mode(dir: &Path, volume_id: u32, mut sparse: bool) -> Result<Self, VolumeError> {
        let dat_path = dir.join(format!("volume_{:04}.dat", volume_id));
        let idx_path = dir.join(format!("volume_{:04}.idx", volume_id));

        let file_exists = dat_path.exists();
        let aligned_file = Arc::new(AlignedFile::open_direct(&dat_path, false)?);

        let mut idx_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&idx_path)?;
        // Read the journal, then append after it: a reopen must not overwrite earlier records.
        let mut idx_bytes = Vec::new();
        idx_file.read_to_end(&mut idx_bytes)?;
        idx_file.seek(SeekFrom::End(0))?;
        let indexed: BTreeSet<u64> = idx_bytes
            .chunks_exact(IDX_RECORD)
            .map(|r| u64::from_le_bytes(r[12..20].try_into().unwrap()))
            .filter(|&o| o >= SECTOR_SIZE as u64)
            .collect();

        let mut file_len = aligned_file.metadata()?.len();
        if file_len >= MAX_VOLUME_SPAN {
            sparse = true;
        }

        if !file_exists {
            // Write 4096B Superblock
            let mut super_buf = AlignedBuffer::new(SECTOR_SIZE);
            super_buf[0..4].copy_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
            super_buf[4..8].copy_from_slice(&volume_id.to_le_bytes());
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
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

            // Scan valid needles
            let mut read_buf = AlignedBuffer::new(SECTOR_SIZE);
            while scan_offset + (SECTOR_SIZE as u64) <= file_len {
                if aligned_file.read_aligned_at(scan_offset, &mut read_buf).is_err() {
                    break;
                }

                let mut header_bytes = [0u8; 64];
                header_bytes.copy_from_slice(&read_buf[0..64]);

                let magic = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
                if magic != NEEDLE_MAGIC {
                    // Unwritten sector: the end of the volume, or a gap the journal can step over.
                    match next_indexed(&indexed, scan_offset) {
                        Some(next) => {
                            scan_offset = next;
                            continue;
                        }
                        None => break,
                    }
                }

                match NeedleHeader::decode(&header_bytes) {
                    Ok(hdr) => {
                        let raw_len = 64 + hdr.key_len as usize + hdr.payload_size as usize;
                        let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;
                        if scan_offset + total_len > file_len {
                            // Incomplete needle written during crash
                            match next_indexed(&indexed, scan_offset) {
                                Some(next) => {
                                    scan_offset = next;
                                    continue;
                                }
                                None => break,
                            }
                        }
                        scan_offset += total_len;
                    }
                    Err(_) => {
                        // Torn needle: step over it when the journal knows a later needle.
                        match next_indexed(&indexed, scan_offset) {
                            Some(next) => {
                                scan_offset = next;
                                continue;
                            }
                            None => break,
                        }
                    }
                }
            }

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
            Some(Arc::new(unsafe { Mmap::map(aligned_file.file()) }?))
        } else {
            None
        };

        let idx_sync = idx_file.try_clone()?;
        Ok(Self {
            dir: dir.to_path_buf(),
            volume_id,
            file: aligned_file,
            idx_file: Mutex::new(idx_file),
            idx_sync,
            durability: std::sync::atomic::AtomicU8::new(Durability::Group.code()),
            written_seq: AtomicU64::new(0),
            sync_state: std::sync::Mutex::new(SyncState::default()),
            sync_done: std::sync::Condvar::new(),
            current_offset: AtomicU64::new(scan_offset),
            mmap: RwLock::new(initial_mmap),
            remap_count: AtomicU64::new(0),
            is_sparse: sparse,
            max_span: AtomicU64::new(MAX_VOLUME_SPAN),
            work: Arc::new(WorkCounters::default()),
            fail_next_write: std::sync::atomic::AtomicBool::new(false),
            indexed,
            format,
        })
    }

    /// The volume's format version (1 or 2). Compressed needles (#51) need 2.
    pub fn format(&self) -> u32 {
        self.format
    }

    /// The first `n` payload bytes of the needle at `local`, from its first sector.
    pub fn payload_prefix(&self, local: u64, key_len: usize, n: usize) -> Result<Vec<u8>, VolumeError> {
        let start = 64 + key_len;
        if start + n > SECTOR_SIZE {
            return Err(VolumeError::CorruptIndex);
        }
        let mut buf = AlignedBuffer::new(SECTOR_SIZE);
        self.work.disk_read();
        self.file.read_aligned_at(local, &mut buf)?;
        Ok(buf[start..start + n].to_vec())
    }

    /// Fault injection for tests: the next `write_needle` reserves its offset and then fails,
    /// as a disk error would, leaving a gap in the volume.
    pub fn fail_next_write(&self) {
        self.fail_next_write.store(true, Ordering::SeqCst);
    }

    /// Largest byte offset a needle may reach in this volume (default `MAX_VOLUME_SPAN`).
    /// Smaller spans are for tests and small deployments.
    pub fn set_max_span(&self, span: u64) {
        self.max_span.store(span, Ordering::SeqCst);
    }

    pub fn max_span(&self) -> u64 {
        self.max_span.load(Ordering::SeqCst)
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
        let target = self.written_seq.load(Ordering::SeqCst);
        self.file.sync_data()?;
        self.work.disk_sync();
        self.idx_sync.sync_data()?;
        self.work.disk_sync();
        let mut st = self.sync_state.lock().unwrap_or_else(|e| e.into_inner());
        st.synced = st.synced.max(target);
        Ok(())
    }

    /// Appends one record to the `.idx` journal.
    fn append_index(&self, key_hash: u64, offset: u64, total_len: u64, payload_size: u64, flags: u16, crc: u32) -> Result<(), VolumeError> {
        let mut rec = [0u8; IDX_RECORD];
        rec[0..8].copy_from_slice(&key_hash.to_le_bytes());
        rec[8..12].copy_from_slice(&self.volume_id.to_le_bytes());
        rec[12..20].copy_from_slice(&offset.to_le_bytes());
        rec[20..28].copy_from_slice(&total_len.to_le_bytes());
        rec[28..36].copy_from_slice(&payload_size.to_le_bytes());
        rec[36..38].copy_from_slice(&flags.to_le_bytes());
        rec[38..42].copy_from_slice(&crc.to_le_bytes());
        self.idx_file.lock().write_all(&rec)?;
        Ok(())
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

    pub fn file(&self) -> Arc<AlignedFile> {
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
        let file_len = self.file.metadata()?.len();
        if file_len < required_end && file_len == 0 {
            return Err(VolumeError::TornNeedleAtOffset(required_end));
        }
        self.remap_count.fetch_add(1, Ordering::Relaxed);
        let mmap = unsafe { Mmap::map(self.file.file()) }?;
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
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u16;
        let payload_size = payload.len() as u64;
        let key_hash = xxh3_64(key_bytes);
        let payload_crc32c = crc32c::crc32c(payload);

        let raw_len = 64 + key_bytes.len() + payload.len();
        let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;

        let mut buf = AlignedBuffer::new(total_len as usize);

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        let mut header = NeedleHeader {
            magic: NEEDLE_MAGIC,
            header_crc32c: 0,
            payload_crc32c,
            flags,
            bucket_id,
            key_len,
            reserved: 0,
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
        buf[64 + key_bytes.len()..raw_len].copy_from_slice(payload);
        self.work.payload_copy(payload.len() as u64);
        if total_len as usize > raw_len {
            buf[raw_len..total_len as usize].fill(0);
        }

        // A WORM volume never reuses space: a needle that would end past the span is refused,
        // and the reserved range stays unused, so later writes are refused too.
        let offset = self.current_offset.fetch_add(total_len, Ordering::SeqCst);
        if offset + total_len > self.max_span() {
            return Err(VolumeError::VolumeFull { offset, len: total_len });
        }
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, "injected write failure")));
        }

        // Lock-free concurrent positional write directly to disjoint disk offset
        self.file.write_aligned_at(offset, &buf)?;
        self.work.disk_write(total_len);

        self.append_index(key_hash, offset, total_len, payload_size, flags, payload_crc32c)?;
        let seq = self.written_seq.fetch_add(1, Ordering::SeqCst) + 1;

        Ok(NeedleLocation {
            volume_id: self.volume_id,
            offset,
            total_len,
            payload_size,
            payload_crc32c,
            seq,
        })
    }

    /// Starts a needle whose payload arrives in pieces (#36). The space is reserved now; the
    /// payload is written in `STREAM_CHUNK` pieces as it arrives, and the first sector, which
    /// holds the header with the payload CRC, is written last by `NeedleWriter::finish`.
    /// A writer dropped before `finish` leaves a gap that recovery steps over (#38).
    pub fn begin_needle(self: &Arc<Self>, bucket_id: u16, key: &str, payload_size: u64, flags: u16) -> Result<NeedleWriter, VolumeError> {
        let key_len = key.len();
        let total_len = (64 + key_len as u64 + payload_size).div_ceil(SECTOR_SIZE as u64) * SECTOR_SIZE as u64;
        let offset = self.current_offset.fetch_add(total_len, Ordering::SeqCst);
        if offset + total_len > self.max_span() {
            return Err(VolumeError::VolumeFull { offset, len: total_len });
        }
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, "injected write failure")));
        }
        let mut first = AlignedBuffer::new(SECTOR_SIZE);
        first.fill(0);
        Ok(NeedleWriter {
            vm: Arc::clone(self),
            bucket_id,
            key: key.to_string(),
            flags,
            offset,
            total_len,
            payload_size,
            written: 0,
            crc: 0,
            first,
            chunk: AlignedBuffer::new(STREAM_CHUNK),
            chunk_start: SECTOR_SIZE as u64,
            chunk_fill: 0,
        })
    }

    /// Reads a needle payload as a zero-copy Bytes slice directly from the memory map.
    pub fn read_needle_bytes(
        &self,
        _volume_id: u32,
        offset: u64,
        expected_payload_crc: u32,
    ) -> Result<Bytes, VolumeError> {
        let mmap = self.get_or_remap(offset + SECTOR_SIZE as u64)?;
        if (offset as usize + 64) > mmap.len() {
            return Err(VolumeError::TornNeedleAtOffset(offset));
        }

        let mut hdr_bytes = [0u8; 64];
        hdr_bytes.copy_from_slice(&mmap[offset as usize..offset as usize + 64]);
        let header = NeedleHeader::decode(&hdr_bytes)?;

        let raw_len = 64 + header.key_len as usize + header.payload_size as usize;
        let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;

        let mmap = if (offset + total_len) as usize > mmap.len() {
            self.get_or_remap(offset + total_len)?
        } else {
            mmap
        };

        let p_start = offset as usize + 64 + header.key_len as usize;
        let p_end = p_start + header.payload_size as usize;
        if p_end > mmap.len() {
            return Err(VolumeError::TornNeedleAtOffset(offset));
        }

        let payload_slice = &mmap[p_start..p_end];
        let actual_crc = crc32c::crc32c(payload_slice);
        if actual_crc != expected_payload_crc {
            return Err(VolumeError::CrcMismatch {
                expected: expected_payload_crc,
                actual: actual_crc,
            });
        }

        let full_bytes = Bytes::from_owner(MmapOwner(mmap));
        Ok(full_bytes.slice(p_start..p_end))
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
        let file_len = self.file.metadata()?.len();
        let mut scan_offset = SECTOR_SIZE as u64;
        let mut read_buf = AlignedBuffer::new(SECTOR_SIZE);

        while scan_offset + (SECTOR_SIZE as u64) <= file_len {
            self.work.disk_read();
            if self.file.read_aligned_at(scan_offset, &mut read_buf).is_err() {
                break;
            }

            let mut header_bytes = [0u8; 64];
            header_bytes.copy_from_slice(&read_buf[0..64]);

            let hdr = match NeedleHeader::decode(&header_bytes) {
                Ok(h) => h,
                Err(_) => match next_indexed(&self.indexed, scan_offset) {
                    Some(next) => {
                        scan_offset = next;
                        continue;
                    }
                    None => break,
                },
            };

            let raw_len = 64 + hdr.key_len as usize + hdr.payload_size as usize;
            let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;

            if scan_offset + total_len > file_len {
                match next_indexed(&self.indexed, scan_offset) {
                    Some(next) => {
                        scan_offset = next;
                        continue;
                    }
                    None => break,
                }
            }

            let key_str = if (64 + hdr.key_len as usize) <= SECTOR_SIZE {
                let k_bytes = &read_buf[64..64 + hdr.key_len as usize];
                String::from_utf8_lossy(k_bytes).to_string()
            } else {
                let mut full_buf = AlignedBuffer::new(total_len as usize);
                self.work.disk_read();
                if self.file.read_aligned_at(scan_offset, &mut full_buf).is_err() {
                    break;
                }
                let k_bytes = &full_buf[64..64 + hdr.key_len as usize];
                String::from_utf8_lossy(k_bytes).to_string()
            };

            visitor(
                hdr.bucket_id,
                key_str,
                scan_offset,
                hdr.payload_size,
                hdr.etag_hi,
                hdr.etag_lo,
                hdr.flags,
                hdr.payload_crc32c,
            );

            scan_offset += total_len;
        }

        Ok(())
    }
}
