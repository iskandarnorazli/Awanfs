//! awan-storage/server/src/volume.rs
//! 32 GiB WORM Needle Volume Slab Storage Engine and Crash Recovery Journal (#470)

use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

pub use crate::direct_io::SECTOR_SIZE;
use crate::direct_io::{AlignedBuffer, AlignedFile};

pub const NEEDLE_MAGIC: u32 = 0x41574E33; // "AWN3"
pub const SUPERBLOCK_MAGIC: u32 = 0x53555042; // "SUPB"

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

pub struct VolumeManager {
    dir: PathBuf,
    volume_id: u32,
    file: Mutex<AlignedFile>,
    idx_file: Mutex<File>,
    current_offset: AtomicU64,
}

impl VolumeManager {
    pub fn open(dir: &Path, volume_id: u32) -> Result<Self, VolumeError> {
        let dat_path = dir.join(format!("volume_{:04}.dat", volume_id));
        let idx_path = dir.join(format!("volume_{:04}.idx", volume_id));

        let file_exists = dat_path.exists();
        let mut aligned_file = AlignedFile::open_direct(&dat_path, false)?;

        let idx_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&idx_path)?;

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
        }

        // Run Crash Recovery Scan
        let file_len = aligned_file.metadata()?.len();
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

            // Truncate torn tail bytes if any
            if file_len != scan_offset {
                aligned_file.set_len(scan_offset)?;
                aligned_file.sync_all()?;
            }
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            volume_id,
            file: Mutex::new(aligned_file),
            idx_file: Mutex::new(idx_file),
            current_offset: AtomicU64::new(scan_offset),
        })
    }

    pub fn write_needle(
        &self,
        bucket_id: u16,
        key: &str,
        payload: &[u8],
        flags: u16,
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
            etag_hi: 0,
            etag_lo: 0,
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
        buf[64 + key_bytes.len()..64 + key_bytes.len() + payload.len()].copy_from_slice(payload);

        let mut file_guard = self.file.lock();
        let offset = self.current_offset.load(Ordering::SeqCst);

        file_guard.write_aligned_at(offset, &buf)?;

        // Append to .idx
        {
            let mut idx = self.idx_file.lock();
            idx.seek(SeekFrom::End(0))?;
            idx.write_all(&key_hash.to_le_bytes())?;
            idx.write_all(&self.volume_id.to_le_bytes())?;
            idx.write_all(&offset.to_le_bytes())?;
            idx.write_all(&total_len.to_le_bytes())?;
            idx.write_all(&payload_size.to_le_bytes())?;
            idx.write_all(&flags.to_le_bytes())?;
            idx.write_all(&payload_crc32c.to_le_bytes())?;
            idx.sync_data()?;
        }

        self.current_offset.store(offset + total_len, Ordering::SeqCst);

        Ok(NeedleLocation {
            volume_id: self.volume_id,
            offset,
            total_len,
            payload_size,
            payload_crc32c,
        })
    }

    pub fn read_needle(
        &self,
        _volume_id: u32,
        offset: u64,
        expected_payload_crc: u32,
    ) -> Result<Vec<u8>, VolumeError> {
        let mut file_guard = self.file.lock();
        let mut first_sector = AlignedBuffer::new(SECTOR_SIZE);
        file_guard.read_aligned_at(offset, &mut first_sector)?;

        let mut hdr_bytes = [0u8; 64];
        hdr_bytes.copy_from_slice(&first_sector[0..64]);
        let header = NeedleHeader::decode(&hdr_bytes)?;

        let raw_len = 64 + header.key_len as usize + header.payload_size as usize;
        let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;

        let payload = if total_len <= SECTOR_SIZE as u64 {
            let p_start = 64 + header.key_len as usize;
            let p_end = p_start + header.payload_size as usize;
            first_sector[p_start..p_end].to_vec()
        } else {
            let mut full_buf = AlignedBuffer::new(total_len as usize);
            file_guard.read_aligned_at(offset, &mut full_buf)?;
            let p_start = 64 + header.key_len as usize;
            let p_end = p_start + header.payload_size as usize;
            full_buf[p_start..p_end].to_vec()
        };

        let actual_crc = crc32c::crc32c(&payload);
        if actual_crc != expected_payload_crc {
            return Err(VolumeError::CrcMismatch {
                expected: expected_payload_crc,
                actual: actual_crc,
            });
        }

        Ok(payload)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn offset(&self) -> u64 {
        self.current_offset.load(Ordering::SeqCst)
    }
}
