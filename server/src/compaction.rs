//! awan-storage/server/src/compaction.rs
//! Background Copy-on-Compact Volume Garbage Collector (#470)

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use crate::direct_io::{AlignedBuffer, AlignedFile, SECTOR_SIZE};
use crate::volume::{NeedleHeader, NeedleLocation, VolumeError, VolumeManager};

#[derive(Debug, Default)]
pub struct CompactionReport {
    pub live_needles_copied: usize,
    pub dead_needles_reclaimed: usize,
    pub new_locations: HashMap<String, NeedleLocation>,
}

pub fn compact_volume(
    dir: &Path,
    source_volume_id: u32,
    target_volume_id: u32,
    live_keys: &HashSet<String>,
) -> Result<CompactionReport, VolumeError> {
    let source_dat = dir.join(format!("volume_{:04}.dat", source_volume_id));
    let source_idx = dir.join(format!("volume_{:04}.idx", source_volume_id));

    let mut src_file = AlignedFile::open_direct(&source_dat, true)?;
    let target_mgr = VolumeManager::open(dir, target_volume_id)?;

    let file_len = src_file.metadata()?.len();
    let mut scan_offset = SECTOR_SIZE as u64;

    let mut report = CompactionReport::default();
    let mut read_buf = AlignedBuffer::new(SECTOR_SIZE);

    while scan_offset + (SECTOR_SIZE as u64) <= file_len {
        src_file.read_aligned_at(scan_offset, &mut read_buf)?;

        let mut header_bytes = [0u8; 64];
        header_bytes.copy_from_slice(&read_buf[0..64]);

        let hdr = match NeedleHeader::decode(&header_bytes) {
            Ok(h) => h,
            Err(_) => break, // Reached end of valid needles
        };

        let raw_len = 64 + hdr.key_len as usize + hdr.payload_size as usize;
        let total_len = (((raw_len + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE) as u64;

        if scan_offset + total_len > file_len {
            break;
        }

        // Read full needle if multi-sector
        let (key_str, payload) = if total_len <= SECTOR_SIZE as u64 {
            let k_bytes = &read_buf[64..64 + hdr.key_len as usize];
            let key = String::from_utf8_lossy(k_bytes).to_string();
            let p_start = 64 + hdr.key_len as usize;
            let payload = read_buf[p_start..p_start + hdr.payload_size as usize].to_vec();
            (key, payload)
        } else {
            let mut full_buf = AlignedBuffer::new(total_len as usize);
            src_file.read_aligned_at(scan_offset, &mut full_buf)?;
            let k_bytes = &full_buf[64..64 + hdr.key_len as usize];
            let key = String::from_utf8_lossy(k_bytes).to_string();
            let p_start = 64 + hdr.key_len as usize;
            let payload = full_buf[p_start..p_start + hdr.payload_size as usize].to_vec();
            (key, payload)
        };

        if live_keys.contains(&key_str) {
            let new_loc = target_mgr.write_needle(hdr.bucket_id, &key_str, &payload, hdr.flags)?;
            report.new_locations.insert(key_str, new_loc);
            report.live_needles_copied += 1;
        } else {
            report.dead_needles_reclaimed += 1;
        }

        scan_offset += total_len;
    }

    // Drop source file handle before unlinking
    drop(src_file);

    // Unlink old volume files
    let _ = fs::remove_file(&source_dat);
    let _ = fs::remove_file(&source_idx);

    Ok(report)
}
