//! awan-storage/server/src/volume.rs
//! 32 GiB WORM Needle Volume Slab Storage Engine and Crash Recovery Journal (#470)

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
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

pub const NEEDLE_MAGIC: u32 = 0x41574E33; // "AWN3"
pub const SUPERBLOCK_MAGIC: u32 = 0x53555042; // "SUPB"
pub const MAX_VOLUME_SPAN: u64 = 32 * 1024 * 1024 * 1024; // 32 GiB WORM Needle Volume (#470)

#[derive(Debug)]
pub enum VolumeError {
    Io(io::Error),
    InvalidMagic { expected: u32, actual: u32 },
    HeaderCrcMismatch { expected: u32, actual: u32 },
    CrcMismatch { expected: u32, actual: u32 },
    InvalidSuperblock,
    TornNeedleAtOffset(u64),
    CorruptIndex,
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

pub struct VolumeManager {
    dir: PathBuf,
    volume_id: u32,
    file: Arc<AlignedFile>,
    idx_file: Mutex<File>,
    current_offset: AtomicU64,
    mmap: RwLock<Option<Arc<Mmap>>>,
    remap_count: AtomicU64,
    is_sparse: bool,
    max_span: u64,
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

        let idx_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&idx_path)?;

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

        if file_len < SECTOR_SIZE as u64 {
            // Corrupt superblock or empty file, rewrite superblock
            let mut super_buf = AlignedBuffer::new(SECTOR_SIZE);
            super_buf[0..4].copy_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
            super_buf[4..8].copy_from_slice(&volume_id.to_le_bytes());
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
                    // Sparse unwritten sector or invalid magic encountered
                    break;
                }

                match NeedleHeader::decode(&header_bytes) {
                    Ok(hdr) => {
                        let raw_len = 64 + hdr.key_len as usize + hdr.payload_size as usize;
                        let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;
                        if scan_offset + total_len > file_len {
                            // Incomplete needle written during crash
                            break;
                        }
                        scan_offset += total_len;
                    }
                    Err(_) => {
                        // Torn needle encountered
                        break;
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

        Ok(Self {
            dir: dir.to_path_buf(),
            volume_id,
            file: aligned_file,
            idx_file: Mutex::new(idx_file),
            current_offset: AtomicU64::new(scan_offset),
            mmap: RwLock::new(initial_mmap),
            remap_count: AtomicU64::new(0),
            is_sparse: sparse,
            max_span: MAX_VOLUME_SPAN,
        })
    }

    /// Largest byte offset a needle may reach in this volume (default `MAX_VOLUME_SPAN`).
    /// Smaller spans are for tests and small deployments.
    pub fn set_max_span(&mut self, span: u64) {
        self.max_span = span;
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
        if total_len as usize > raw_len {
            buf[raw_len..total_len as usize].fill(0);
        }

        let raw_off = self.current_offset.fetch_add(total_len, Ordering::SeqCst);
        let offset = if raw_off < self.max_span {
            raw_off
        } else {
            let wrapped = (raw_off - SECTOR_SIZE as u64) % (self.max_span - SECTOR_SIZE as u64);
            SECTOR_SIZE as u64 + ((wrapped / SECTOR_SIZE as u64) * SECTOR_SIZE as u64)
        };

        // Lock-free concurrent positional write directly to disjoint disk offset
        self.file.write_aligned_at(offset, &buf)?;

        // Append 46-byte record to .idx under dedicated lock
        if raw_off < self.max_span {
            let mut idx_entry = [0u8; 46];
            idx_entry[0..8].copy_from_slice(&key_hash.to_le_bytes());
            idx_entry[8..12].copy_from_slice(&self.volume_id.to_le_bytes());
            idx_entry[12..20].copy_from_slice(&offset.to_le_bytes());
            idx_entry[20..28].copy_from_slice(&total_len.to_le_bytes());
            idx_entry[28..36].copy_from_slice(&payload_size.to_le_bytes());
            idx_entry[36..38].copy_from_slice(&flags.to_le_bytes());
            idx_entry[38..42].copy_from_slice(&payload_crc32c.to_le_bytes());
            let mut idx = self.idx_file.lock();
            idx.write_all(&idx_entry[..42])?;
        }

        Ok(NeedleLocation {
            volume_id: self.volume_id,
            offset,
            total_len,
            payload_size,
            payload_crc32c,
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
        self.read_needle_bytes(volume_id, offset, expected_payload_crc)
            .map(|b| b.to_vec())
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
            if self.file.read_aligned_at(scan_offset, &mut read_buf).is_err() {
                break;
            }

            let mut header_bytes = [0u8; 64];
            header_bytes.copy_from_slice(&read_buf[0..64]);

            let hdr = match NeedleHeader::decode(&header_bytes) {
                Ok(h) => h,
                Err(_) => break,
            };

            let raw_len = 64 + hdr.key_len as usize + hdr.payload_size as usize;
            let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;

            if scan_offset + total_len > file_len {
                break;
            }

            let key_str = if (64 + hdr.key_len as usize) <= SECTOR_SIZE {
                let k_bytes = &read_buf[64..64 + hdr.key_len as usize];
                String::from_utf8_lossy(k_bytes).to_string()
            } else {
                let mut full_buf = AlignedBuffer::new(total_len as usize);
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
