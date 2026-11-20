//! #269: Measure bytes and time AwanFS compaction spends copying live large objects
//! against live small objects (`perform_online_compaction` / `compact_oldest_volume`).
//!
//!   cargo run --release --example compaction_cost -- --dir <DIR> \
//!       --small-count 90 --small-size 4096 --large-count 10 --large-size 67108864 \
//!       --delete-fraction 0.50

use bytes::Bytes;
use s3_server::{compute_md5, split_offset, AWAN_FLAG_ACTIVE, Durability, S3ServiceState};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

struct Args {
    dir: PathBuf,
    bucket: String,
    small_count: usize,
    small_size: usize,
    large_count: usize,
    large_size: usize,
    delete_fraction: f64,
    compaction_rate: u64,
    seed: u64,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        dir: PathBuf::new(),
        bucket: "compaction-cost".to_string(),
        small_count: 90,
        small_size: 4096,
        large_count: 10,
        large_size: 64 * 1024 * 1024,
        delete_fraction: 0.50,
        compaction_rate: 0,
        seed: 42,
    };
    while let Some(flag) = it.next() {
        let v = it.next().unwrap_or_else(|| panic!("{} needs a value", flag));
        match flag.as_str() {
            "--dir" => a.dir = PathBuf::from(v),
            "--bucket" => a.bucket = v,
            "--small-count" => a.small_count = v.parse().unwrap(),
            "--small-size" => a.small_size = v.parse().unwrap(),
            "--large-count" => a.large_count = v.parse().unwrap(),
            "--large-size" => a.large_size = v.parse().unwrap(),
            "--delete-fraction" => a.delete_fraction = v.parse().unwrap(),
            "--compaction-rate" => a.compaction_rate = v.parse().unwrap(),
            "--seed" => a.seed = v.parse().unwrap(),
            other => panic!("unknown flag {}", other),
        }
    }
    assert!(!a.dir.as_os_str().is_empty(), "--dir is required");
    assert!(
        (0.0..=1.0).contains(&a.delete_fraction),
        "--delete-fraction must be in 0.0..=1.0"
    );
    a
}

fn fill_pattern(buf: &mut [u8], seed: u64, tag: u64) {
    if buf.is_empty() {
        return;
    }
    let mut x = seed ^ tag.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let len = buf.len().min(64);
    for chunk in buf[..len].chunks_mut(8) {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let bytes = x.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

fn small_key(i: usize) -> String {
    format!("small/{:06}", i)
}

fn large_key(i: usize) -> String {
    format!("large/{:06}", i)
}

async fn put_object(
    state: &S3ServiceState,
    b_id: u32,
    bucket: &str,
    key: &str,
    data: &Bytes,
    created_ns: u64,
) -> Result<(), String> {
    let (etag_hi, etag_lo, _, _) = compute_md5(data);
    let (vol_offset, flags) = state
        .store_object(
            b_id,
            bucket,
            key,
            &[],
            data,
            AWAN_FLAG_ACTIVE,
            etag_hi,
            etag_lo,
            Some(created_ns),
        )
        .await
        .map_err(|e| format!("store_object {}: {:?}", key, e))?;
    state
        .engine
        .write()
        .put_object_at(
            b_id,
            key.as_bytes(),
            vol_offset,
            data.len() as u64,
            etag_hi,
            etag_lo,
            0,
            flags,
            created_ns / 1_000_000,
        )
        .map_err(|c| format!("put_object_at {}: {}", key, c))
}

async fn run_measurement(a: &Args) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let state = Arc::new(S3ServiceState::try_with_data_dir(&a.dir)?);
    state
        .compaction_rate
        .store(a.compaction_rate, Ordering::Relaxed);

    let b_id = state
        .engine
        .write()
        .create_bucket(&a.bucket)
        .map_err(|c| format!("create_bucket: {}", c))?;
    let created_ns = state.clock.now_ns();
    state.bucket_created_ns.write().insert(b_id, created_ns);
    let _ = state
        .bucket_configs
        .lock()
        .apply_bucket_create(b_id, &a.bucket, created_ns, "root", "");

    // Setup only: avoid one fsync per object while populating test data.
    for vm in state.volumes.read().values() {
        vm.set_durability(Durability::Fast);
    }

    let mut small_template = if a.small_count > 0 {
        vec![0x5Au8; a.small_size]
    } else {
        Vec::new()
    };
    fill_pattern(&mut small_template, a.seed, 1);
    let mut large_template = if a.large_count > 0 {
        vec![0xA5u8; a.large_size]
    } else {
        Vec::new()
    };
    fill_pattern(&mut large_template, a.seed, 2);

    // Interleave small and large objects across the volume.
    let total = a.small_count + a.large_count;
    let mut small_written = 0usize;
    let mut large_written = 0usize;
    for step in 0..total {
        let want_large = large_written < a.large_count
            && (small_written >= a.small_count
                || ((step + 1) * a.large_count) / total > large_written);
        if want_large {
            fill_pattern(&mut large_template, a.seed, 1_000_000 + large_written as u64);
            let data = Bytes::copy_from_slice(&large_template);
            let key = large_key(large_written);
            put_object(&state, b_id, &a.bucket, &key, &data, created_ns + step as u64).await?;
            large_written += 1;
        } else {
            fill_pattern(&mut small_template, a.seed, small_written as u64);
            let data = Bytes::copy_from_slice(&small_template);
            let key = small_key(small_written);
            put_object(&state, b_id, &a.bucket, &key, &data, created_ns + step as u64).await?;
            small_written += 1;
        }
    }

    let small_deleted = ((a.small_count as f64) * a.delete_fraction)
        .round()
        .clamp(0.0, a.small_count as f64) as usize;
    let mut deleted_set = HashSet::with_capacity(small_deleted);
    for k in 0..small_deleted {
        let idx = (k * a.small_count) / small_deleted;
        assert!(deleted_set.insert(idx), "duplicate delete index {}", idx);
        state.delete_key(b_id, &small_key(idx)).await?;
    }

    state
        .sync_all_volumes()
        .map_err(|e| format!("sync_all_volumes: {}", e))?;
    state.payload_cache.clear();
    for vm in state.volumes.read().values() {
        vm.set_durability(Durability::Group);
    }

    let vol_ids_before = state.volume_ids();
    assert_eq!(
        vol_ids_before.len(),
        1,
        "expected 1 volume before compaction, got {:?}",
        vol_ids_before
    );
    let volume_bytes_before: u64 = state.volumes.read().values().map(|v| v.offset()).sum();
    let work_before = state.work.snapshot();

    let t0 = Instant::now();
    let report = state
        .perform_online_compaction()
        .await
        .map_err(|e| format!("perform_online_compaction: {}", e))?;
    let seconds = t0.elapsed().as_secs_f64();

    let work_diff = state.work.snapshot().since(&work_before);
    let vol_ids_after = state.volume_ids();
    let volume_bytes_after: u64 = state.volumes.read().values().map(|v| v.offset()).sum();

    let mut small_needles_copied = 0usize;
    let mut large_needles_copied = 0usize;
    let mut small_bytes_copied = 0u64;
    let mut large_bytes_copied = 0u64;
    let mut small_payload_bytes_copied = 0u64;
    let mut large_payload_bytes_copied = 0u64;

    for (key, loc) in &report.new_locations {
        if key.starts_with("small/") {
            small_needles_copied += 1;
            small_bytes_copied += loc.total_len;
            small_payload_bytes_copied += loc.payload_size;
        } else if key.starts_with("large/") {
            large_needles_copied += 1;
            large_bytes_copied += loc.total_len;
            large_payload_bytes_copied += loc.payload_size;
        } else {
            return Err(format!("unexpected key in compaction report: {}", key).into());
        }
    }

    assert_eq!(
        small_needles_copied + large_needles_copied,
        report.live_needles_copied
    );
    assert_eq!(
        small_bytes_copied + large_bytes_copied,
        work_diff.disk_write_bytes
    );
    assert_eq!(
        small_payload_bytes_copied + large_payload_bytes_copied,
        work_diff.payload_copy_bytes
    );

    let active_vol_id = *vol_ids_after.last().expect("active volume after compaction");
    {
        let eng = state.engine.read();
        for i in 0..a.small_count {
            let res = eng.get_object(b_id, small_key(i).as_bytes());
            if deleted_set.contains(&i) {
                assert!(res.is_err(), "deleted small key {} still present", i);
            } else {
                let meta = res.expect("surviving small key missing");
                assert_eq!(split_offset(meta.vol_offset).0, active_vol_id);
            }
        }
        for i in 0..a.large_count {
            let meta = eng
                .get_object(b_id, large_key(i).as_bytes())
                .expect("surviving large key missing");
            assert_eq!(split_offset(meta.vol_offset).0, active_vol_id);
        }
    }

    let bytes_copied = small_bytes_copied + large_bytes_copied;
    let payload_bytes_copied = small_payload_bytes_copied + large_payload_bytes_copied;
    let bytes_reclaimed = if report.volume_removed {
        volume_bytes_before.saturating_sub(volume_bytes_after)
    } else {
        0
    };
    let large_share = if bytes_copied == 0 {
        0.0
    } else {
        large_bytes_copied as f64 / bytes_copied as f64
    };
    let large_payload_share = if payload_bytes_copied == 0 {
        0.0
    } else {
        large_payload_bytes_copied as f64 / payload_bytes_copied as f64
    };

    Ok(serde_json::json!({
        "small_count": a.small_count,
        "small_size": a.small_size,
        "large_count": a.large_count,
        "large_size": a.large_size,
        "delete_fraction": a.delete_fraction,
        "small_deleted": small_deleted,
        "live_needles_copied": report.live_needles_copied,
        "dead_needles_reclaimed": report.dead_needles_reclaimed,
        "small_needles_copied": small_needles_copied,
        "large_needles_copied": large_needles_copied,
        "small_bytes_copied": small_bytes_copied,
        "large_bytes_copied": large_bytes_copied,
        "bytes_copied": bytes_copied,
        "small_payload_bytes_copied": small_payload_bytes_copied,
        "large_payload_bytes_copied": large_payload_bytes_copied,
        "payload_bytes_copied": payload_bytes_copied,
        "volume_bytes_before": volume_bytes_before,
        "volume_bytes_after": volume_bytes_after,
        "bytes_reclaimed": bytes_reclaimed,
        "seconds": seconds,
        "large_share": large_share,
        "large_payload_share": large_payload_share,
        "volume_removed": report.volume_removed,
        "busy": report.busy,
        "work_disk_writes": work_diff.disk_writes,
        "work_disk_write_bytes": work_diff.disk_write_bytes,
        "work_disk_syncs": work_diff.disk_syncs,
        "work_payload_copy_bytes": work_diff.payload_copy_bytes,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = parse_args();
    let out = run_measurement(&a).await?;
    println!("{}", out);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn measures_small_and_large_compaction_bytes() {
        let dir = std::env::temp_dir().join(format!(
            "awan_cc_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let args = Args {
            dir: dir.clone(),
            bucket: "compaction-cost".to_string(),
            small_count: 10,
            small_size: 4096,
            large_count: 2,
            large_size: 131072,
            delete_fraction: 0.5,
            compaction_rate: 0,
            seed: 42,
        };
        let out = run_measurement(&args).await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(out["small_needles_copied"], 5);
        assert_eq!(out["large_needles_copied"], 2);
        assert!(out["volume_removed"].as_bool().unwrap());
        assert!(out["bytes_reclaimed"].as_u64().unwrap() > 0);
        assert!(out["large_share"].as_f64().unwrap() > 0.8);
    }
}
