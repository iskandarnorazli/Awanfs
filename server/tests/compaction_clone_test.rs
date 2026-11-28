//! #524: compaction Linux FICLONERANGE fast path, automatic read+write fallback,
//! tail-before-header clone ordering, and crash/restart recovery tests.

use reqwest::StatusCode;
use s3_server::clock::SystemClock;
use s3_server::direct_io::{AlignedFile, SECTOR_SIZE};
use s3_server::meta_index::FLAG_ATTRS;
use s3_server::sim::{CrashMode, SimFile, SimFs, SimRng};
use s3_server::vfile::{DiskFs, FileKind, VolFile, VolFs};
use s3_server::volume::{ScannedNeedle, VolumeManager};
use s3_server::{start_s3_reactor, S3ServiceState};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_cclone_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 17 + 29) % 251) as u8).collect()
}

fn scan_all<Fs: VolFs>(vm: &VolumeManager<Fs>) -> Vec<ScannedNeedle> {
    let mut out = Vec::new();
    vm.scan_needles(|n| out.push(n)).expect("scan_needles");
    out
}

#[test]
fn sim_compaction_uses_reflink_clone_when_supported_and_preserves_needles_across_reopen() {
    let sim = SimFs::new();
    sim.set_reflink_supported(true);
    let dir = Path::new("sim/clone_ok");
    let clock = Arc::new(SystemClock);

    let src = VolumeManager::open_with(&sim, dir, 1, false, clock.clone()).expect("open src");
    let dst = VolumeManager::open_with(&sim, dir, 2, false, clock.clone()).expect("open dst");

    let mut attrs = s3_server::attrs::Attrs::new();
    attrs.set(s3_server::attrs::TAG_CONTENT_TYPE, b"application/x-awanfs-test");
    let attrs_bytes = attrs.encode().expect("encode attrs");

    let small_payload = pattern(128, 1);
    let large_payload = pattern(12 * 1024, 2);
    let attrs_payload = pattern(6 * 1024, 3);

    src.write_needle_with_attrs(7, "small-key", &[], &small_payload, 0, 11, 22, Some(1001)).expect("write small");
    src.write_needle_with_attrs(7, "large-key", &[], &large_payload, 0, 33, 44, Some(1002)).expect("write large");
    src.write_needle_with_attrs(7, "attrs-key", &attrs_bytes, &attrs_payload, FLAG_ATTRS, 55, 66, Some(1003))
        .expect("write attrs");
    src.sync_now().expect("sync src");

    let scanned = scan_all(&src);
    assert_eq!(scanned.len(), 3);

    let src_file: Arc<SimFile> = sim.file(&dir.join("volume_0001.dat")).expect("src dat");
    let dst_file: Arc<SimFile> = sim.file(&dir.join("volume_0002.dat")).expect("dst dat");

    let reads_before = src_file.bytes_read();
    let writes_before = dst_file.bytes_written();
    let clones_before = dst_file.clone_calls();
    let cloned_bytes_before = dst_file.bytes_cloned();

    let mut expected_cloned_bytes = 0u64;
    for n in &scanned {
        let (loc, cloned) = dst.copy_needle_from(&src, n.offset, &n.header).expect("clone needle");
        assert!(cloned, "reflink should be used for {}", n.key);
        expected_cloned_bytes += loc.total_len;
    }
    dst.sync_now().expect("sync dst");

    // Cloning must transfer extents via Clone without copying bytes through read/write.
    assert_eq!(src_file.bytes_read(), reads_before, "clone must not read source payload bytes");
    assert_eq!(dst_file.bytes_written(), writes_before, "clone must not write target payload bytes");
    assert_eq!(dst_file.bytes_cloned() - cloned_bytes_before, expected_cloned_bytes);
    // Single-sector small needle = 1 clone call; two multi-sector needles = 2 clone calls each (tail first, header second).
    assert_eq!(dst_file.clone_calls() - clones_before, 5);

    drop(src);
    drop(dst);

    let reopened = VolumeManager::open_with(&sim, dir, 2, false, clock).expect("reopen dst");
    let after = scan_all(&reopened);
    assert_eq!(after.len(), 3);
    assert_eq!(reopened.read_needle_unmapped_verified(7, after[0].offset).unwrap().as_ref(), &small_payload[..]);
    assert_eq!(reopened.read_needle_unmapped_verified(7, after[1].offset).unwrap().as_ref(), &large_payload[..]);
    assert_eq!(reopened.read_needle_unmapped_verified(7, after[2].offset).unwrap().as_ref(), &attrs_payload[..]);
    let mut raw_attrs = vec![0u8; after[2].header.attrs_len as usize];
    reopened
        .read_cached(after[2].offset + after[2].header.attrs_start() as u64, &mut raw_attrs)
        .unwrap();
    let decoded_attrs = s3_server::attrs::Attrs::decode(&raw_attrs).unwrap();
    assert_eq!(decoded_attrs.content_type(), Some("application/x-awanfs-test"));
}

#[test]
fn sim_compaction_fallback_on_unsupported_caches_flag_and_enospc_stays_gap_free() {
    let sim = SimFs::new();
    sim.set_reflink_supported(true);
    let dir = Path::new("sim/clone_fallback");
    let clock = Arc::new(SystemClock);

    let src = VolumeManager::open_with(&sim, dir, 1, false, clock.clone()).expect("open src");
    let dst = VolumeManager::open_with(&sim, dir, 2, false, clock.clone()).expect("open dst");

    let p1 = pattern(9 * 1024, 10);
    let p2 = pattern(5 * 1024, 11);
    let p3 = pattern(200, 12);
    let p4 = pattern(6 * 1024, 13);

    src.write_needle_with_attrs(3, "k1", &[], &p1, 0, 1, 2, Some(10)).unwrap();
    src.write_needle_with_attrs(3, "k2", &[], &p2, 0, 3, 4, Some(20)).unwrap();
    src.write_needle_with_attrs(3, "k3", &[], &p3, 0, 5, 6, Some(30)).unwrap();
    src.write_needle_with_attrs(3, "k4", &[], &p4, 0, 7, 8, Some(40)).unwrap();
    src.sync_now().unwrap();

    let scanned = scan_all(&src);
    let dst_file: Arc<SimFile> = sim.file(&dir.join("volume_0002.dat")).expect("dst dat");

    // Part A: transient ENOSPC on k1's clone falls back in place without disabling reflink globally.
    dst_file.arm_clone_error(io::ErrorKind::StorageFull);
    let (loc1, cloned1) = dst.copy_needle_from(&src, scanned[0].offset, &scanned[0].header).unwrap();
    assert!(!cloned1, "ENOSPC must fall back to read+write");
    assert_eq!(loc1.offset, SECTOR_SIZE as u64, "fallback stays at same reserved offset");

    // Because ENOSPC is transient, k2 should still use clone and succeed.
    let (loc2, cloned2) = dst.copy_needle_from(&src, scanned[1].offset, &scanned[1].header).unwrap();
    assert!(cloned2, "transient ENOSPC must not disable reflink for k2");
    assert_eq!(loc2.offset, loc1.offset + loc1.total_len, "contiguous without gaps");

    // Part B: Unsupported on k3 caches reflink_supported = false and avoids Clone calls on k4.
    dst_file.arm_clone_error(io::ErrorKind::Unsupported);
    let (loc3, cloned3) = dst.copy_needle_from(&src, scanned[2].offset, &scanned[2].header).unwrap();
    assert!(!cloned3);
    assert_eq!(loc3.offset, loc2.offset + loc2.total_len);

    let clone_calls_before_k4 = dst_file.clone_calls();
    let (loc4, cloned4) = dst.copy_needle_from(&src, scanned[3].offset, &scanned[3].header).unwrap();
    assert!(!cloned4);
    assert_eq!(loc4.offset, loc3.offset + loc3.total_len);
    assert_eq!(
        dst_file.clone_calls(),
        clone_calls_before_k4,
        "after Unsupported, k4 must skip Clone and not retry ioctl"
    );

    dst.sync_now().unwrap();
    drop(src);
    drop(dst);

    let reopened = VolumeManager::open_with(&sim, dir, 2, false, clock).unwrap();
    let after = scan_all(&reopened);
    assert_eq!(after.len(), 4);
    assert_eq!(after[0].offset, SECTOR_SIZE as u64);
    assert_eq!(after[1].offset, after[0].offset + after[0].total_len);
    assert_eq!(after[2].offset, after[1].offset + after[1].total_len);
    assert_eq!(after[3].offset, after[2].offset + after[2].total_len);
    assert_eq!(reopened.read_needle_unmapped_verified(3, after[0].offset).unwrap().as_ref(), &p1[..]);
    assert_eq!(reopened.read_needle_unmapped_verified(3, after[1].offset).unwrap().as_ref(), &p2[..]);
    assert_eq!(reopened.read_needle_unmapped_verified(3, after[2].offset).unwrap().as_ref(), &p3[..]);
    assert_eq!(reopened.read_needle_unmapped_verified(3, after[3].offset).unwrap().as_ref(), &p4[..]);
}

#[test]
fn sim_clone_crash_recovery_tail_before_header_across_seeded_prefixes() {
    let dir = Path::new("sim/clone_crash");
    let clock = Arc::new(SystemClock);
    let payload = pattern(16 * 1024, 77);

    let mut saw_torn_drop = false;
    let mut saw_survived = false;

    for seed in 0..32u64 {
        let sim = SimFs::new();
        sim.set_reflink_supported(true);
        let src = VolumeManager::open_with(&sim, dir, 1, false, clock.clone()).unwrap();
        let dst = VolumeManager::open_with(&sim, dir, 2, false, clock.clone()).unwrap();
        // Sync volume 2 superblock first so crash tests only pending needle clone operations.
        dst.sync_now().unwrap();

        src.write_needle_with_attrs(9, "crash-k", &[], &payload, 0, 12, 34, Some(500)).unwrap();
        src.sync_now().unwrap();
        let n = scan_all(&src).pop().unwrap();

        let (_, cloned) = dst.copy_needle_from(&src, n.offset, &n.header).unwrap();
        assert!(cloned);

        let mut rng = SimRng::new(seed);
        let crashed = sim.crashed(CrashMode::SeededPrefix, &mut rng);
        drop(src);
        drop(dst);

        let recovered = VolumeManager::open_with(&crashed, dir, 2, false, clock.clone()).unwrap();
        let rows = scan_all(&recovered);
        if rows.is_empty() {
            saw_torn_drop = true;
        } else {
            saw_survived = true;
            assert_eq!(rows.len(), 1);
            let got = recovered
                .read_needle_unmapped_verified(9, rows[0].offset)
                .expect("recovered cloned needle must verify cleanly against payload CRC (tail must predate header)");
            assert_eq!(got.as_ref(), &payload[..]);
        }
    }

    assert!(saw_torn_drop && saw_survived, "seeded prefix sweep should cover both dropped-before-header and full-clone cases");
}

#[test]
fn direct_io_clone_range_from_validates_alignment_and_platform() {
    let dir = temp_dir("direct");
    let src_p = dir.join("volume_0001.dat");
    let dst_p = dir.join("volume_0002.dat");

    let src = DiskFs.open(&src_p, FileKind::Data, true).unwrap();
    let dst = DiskFs.open(&dst_p, FileKind::Data, true).unwrap();
    let buf = vec![0xABu8; 2 * SECTOR_SIZE];
    src.write_aligned_at(0, &buf).unwrap();
    src.sync_data().unwrap();

    // Misaligned offsets/lengths must fail with InvalidInput on all platforms.
    let misaligned = dst.clone_range_from(&src, 128, SECTOR_SIZE as u64, 0).unwrap_err();
    assert_eq!(misaligned.kind(), io::ErrorKind::InvalidInput);
    let misaligned_len = dst.clone_range_from(&src, 0, 128, 0).unwrap_err();
    assert_eq!(misaligned_len.kind(), io::ErrorKind::InvalidInput);

    #[cfg(not(target_os = "linux"))]
    {
        let err = dst.clone_range_from(&src, 0, SECTOR_SIZE as u64, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    // AlignedFile::clone_range_from smoke check.
    let af_src = AlignedFile::open_direct(&src_p, true).unwrap();
    let af_dst = AlignedFile::open_direct(&dst_p, false).unwrap();
    let res = af_dst.clone_range_from(&af_src, 0, SECTOR_SIZE as u64, 0);
    #[cfg(not(target_os = "linux"))]
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::Unsupported);
    #[cfg(target_os = "linux")]
    let _ = res; // Either Ok on XFS/Btrfs or EOPNOTSUPP/EXDEV on ext4/tmpfs.

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn end_to_end_compact_oldest_volume_preserves_mixed_needles_and_restarts() {
    let dir = temp_dir("e2e");
    let c = reqwest::Client::new();
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    state.set_volume_span(64 * 1024);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);

    assert_eq!(c.put(format!("{}/compact-clone-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let mut expect: HashMap<usize, Option<Vec<u8>>> = HashMap::new();
    for k in 0..32 {
        let len = if k % 3 == 0 { 200 } else { 5100 + k * 37 };
        let body = pattern(len, k);
        let res = c
            .put(format!("{}/compact-clone-bkt/obj-{}", url, k))
            .header("content-type", "application/x-custom-ct")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        expect.insert(k, Some(body));
    }
    for k in 0..16 {
        let res = c.delete(format!("{}/compact-clone-bkt/obj-{}", url, k)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        expect.insert(k, None);
    }

    assert!(state.volumes.read().len() >= 3, "need multiple volumes before compaction");
    let mut total_moved = 0usize;
    while state.volumes.read().len() > 2 {
        let r = state.compact_oldest_volume().await.expect("compact_oldest_volume");
        total_moved += r.live_needles_copied;
        assert!(r.live_needles_cloned <= r.live_needles_copied);
    }
    assert!(total_moved > 0);

    state.payload_cache.clear();
    for (k, exp) in &expect {
        let res = c.get(format!("{}/compact-clone-bkt/obj-{}", url, k)).send().await.unwrap();
        match exp {
            None => assert_eq!(res.status(), StatusCode::NOT_FOUND),
            Some(bytes) => {
                assert_eq!(res.status(), StatusCode::OK);
                assert_eq!(
                    res.headers().get("content-type").and_then(|v| v.to_str().ok()),
                    Some("application/x-custom-ct")
                );
                assert_eq!(res.bytes().await.unwrap().as_ref(), &bytes[..]);
            }
        }
    }

    handle.shutdown();
    drop(state);

    let state2 = Arc::new(S3ServiceState::with_data_dir(&dir));
    state2.set_volume_span(64 * 1024);
    let handle2 = start_s3_reactor("127.0.0.1:0".parse().unwrap(), Arc::clone(&state2)).await.unwrap();
    let url2 = format!("http://{}", handle2.local_addr);
    for (k, exp) in &expect {
        let res = c.get(format!("{}/compact-clone-bkt/obj-{}", url2, k)).send().await.unwrap();
        match exp {
            None => assert_eq!(res.status(), StatusCode::NOT_FOUND),
            Some(bytes) => {
                assert_eq!(res.status(), StatusCode::OK);
                assert_eq!(res.bytes().await.unwrap().as_ref(), &bytes[..]);
            }
        }
    }
    handle2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
