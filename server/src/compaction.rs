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
    /// The compacted volume's data file was deleted.
    pub volume_removed: bool,
    /// Nothing was done: a write was still landing in the oldest volume (#167). The volume is
    /// sealed, so the next pass finds it quiet once that write ends.
    pub busy: bool,
}

pub fn compact_volume(
    dir: &Path,
    source_volume_id: u32,
    target_volume_id: u32,
    live_keys: &HashSet<String>,
) -> Result<CompactionReport, VolumeError> {
    let source_dat = dir.join(format!("volume_{:04}.dat", source_volume_id));
    let source_idx = dir.join(format!("volume_{:04}.idx", source_volume_id));

    let src_file = AlignedFile::open_direct(&source_dat, true)?;
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

        let total_len = hdr.total_len();

        if scan_offset + total_len > file_len {
            break;
        }

        // Read full needle if multi-sector
        // The attributes block is copied as it is: unknown tags included (#150).
        let (a_start, p_start) = (hdr.attrs_start(), hdr.payload_start());
        let (key_str, attrs, payload) = if total_len <= SECTOR_SIZE as u64 {
            let k_bytes = &read_buf[64..a_start];
            let key = String::from_utf8_lossy(k_bytes).to_string();
            (key, read_buf[a_start..p_start].to_vec(), read_buf[p_start..p_start + hdr.payload_size as usize].to_vec())
        } else {
            let mut full_buf = AlignedBuffer::new(total_len as usize);
            src_file.read_aligned_at(scan_offset, &mut full_buf)?;
            let k_bytes = &full_buf[64..a_start];
            let key = String::from_utf8_lossy(k_bytes).to_string();
            (key, full_buf[a_start..p_start].to_vec(), full_buf[p_start..p_start + hdr.payload_size as usize].to_vec())
        };

        if live_keys.contains(&key_str) {
            let new_loc = target_mgr.write_needle_with_attrs(
                hdr.bucket_id,
                &key_str,
                &attrs,
                &payload,
                hdr.flags,
                hdr.etag_hi,
                hdr.etag_lo,
                Some(hdr.created_at_ns),
            )?;
            report.new_locations.insert(key_str, new_loc);
            report.live_needles_copied += 1;
        } else {
            report.dead_needles_reclaimed += 1;
        }

        scan_offset += total_len;
    }

    // The copies must be on disk before the source goes.
    target_mgr.sync_now()?;
    // Drop source file handle before unlinking
    drop(src_file);

    // Unlink old volume files
    let _ = fs::remove_file(&source_dat);
    let _ = fs::remove_file(&source_idx);

    Ok(report)
}
