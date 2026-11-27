//! awan-storage/server/src/compaction.rs
//! Background Copy-on-Compact Volume Garbage Collector (#470)

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use crate::volume::{NeedleLocation, VolumeError, VolumeManager};

#[derive(Debug, Default)]
pub struct CompactionReport {
    pub live_needles_copied: usize,
    /// Subset of `live_needles_copied` relocated via copy-on-write cloning (`FICLONERANGE`).
    pub live_needles_cloned: usize,
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

    let src_mgr = VolumeManager::open(dir, source_volume_id)?;
    src_mgr.sync_now()?;
    let target_mgr = VolumeManager::open(dir, target_volume_id)?;

    let mut scanned = Vec::new();
    src_mgr.scan_needles(|n| scanned.push(n))?;

    let mut report = CompactionReport::default();
    for n in scanned {
        if live_keys.contains(&n.key) {
            let (new_loc, cloned) = target_mgr.copy_needle_from(&src_mgr, n.offset, &n.header)?;
            report.new_locations.insert(n.key, new_loc);
            report.live_needles_copied += 1;
            if cloned {
                report.live_needles_cloned += 1;
            }
        } else {
            report.dead_needles_reclaimed += 1;
        }
    }

    // The copies must be on disk before the source goes.
    target_mgr.sync_now()?;
    // Drop source handle before unlinking.
    drop(src_mgr);

    // Unlink old volume files
    report.volume_removed = fs::remove_file(&source_dat).is_ok();
    let _ = fs::remove_file(&source_idx);

    Ok(report)
}
