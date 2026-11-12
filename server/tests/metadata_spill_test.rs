//! awan-storage/server/tests/metadata_spill_test.rs
//! Dynamic Metadata Spilling & Secondary KV Index Tests for Issue #474

use reqwest::StatusCode;
use s3_server::{
    pack_bucket_flags, start_s3_reactor, ObjectMeta, S3ServiceState, SpillIndex, StorageEngine,
    FLAG_ACTIVE,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awan_spill_test_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

#[tokio::test]
async fn test_s3_reactor_spill_database_end_to_end() {
    let dir = temp_dir("reactor_spill");
    let client = reqwest::Client::new();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let handle = start_s3_reactor(addr, state).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);

    // 1. Create bucket
    let bucket = "spill-bucket";
    let res = client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 2. Put objects
    for i in 0..10 {
        let key = format!("data_{:02}.txt", i);
        let res = client
            .put(&format!("{}/{}/{}", base_url, bucket, key))
            .body(format!("payload-content-{}", i))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    // 3. Verify metadata.db was created in data_dir
    let db_path = dir.join("metadata.db");
    assert!(db_path.exists(), "metadata.db must exist in data_dir");

    // 4. Verify GET returns correct data
    for i in 0..10 {
        let key = format!("data_{:02}.txt", i);
        let res = client
            .get(&format!("{}/{}/{}", base_url, bucket, key))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.text().await.unwrap();
        assert_eq!(body, format!("payload-content-{}", i));
    }

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_spill_index_crud_and_prefix_scan() {
    let dir = temp_dir("spill_crud");
    let db_path = dir.join("metadata.db");
    let spill = SpillIndex::open(&db_path).expect("open spill db");

    let b_id = 1u32;
    let meta1 = ObjectMeta {
        key_hash: 1001,
        bucket_flags: pack_bucket_flags(b_id, 0, FLAG_ACTIVE),
        vol_offset: 4096,
        size: 1024,
        etag_hi: 0xAAAA,
        etag_lo: 0xBBBB,
        created_at: 1700000000,
        key_slice: 0,
    };

    // 1. Put
    spill.put(b_id, b"photos/vacation.jpg", &meta1).expect("put");

    // 2. Get
    let fetched = spill.get(b_id, b"photos/vacation.jpg").expect("get").expect("found");
    assert_eq!(fetched.vol_offset, 4096);
    assert_eq!(fetched.size, 1024);
    assert_eq!(fetched.etag_hi, 0xAAAA);

    // 3. Prefix Scan
    for i in 0..20 {
        let k = format!("photos/2026/img_{:02}.jpg", i);
        let mut m = meta1;
        m.vol_offset = 4096 * (i + 2);
        spill.put(b_id, k.as_bytes(), &m).expect("put batch");
    }

    let scan = spill.scan_prefix(b_id, b"photos/2026/", None, 100).expect("scan");
    assert_eq!(scan.len(), 20);
    assert_eq!(scan[0].0, b"photos/2026/img_00.jpg");
    assert_eq!(scan[19].0, b"photos/2026/img_19.jpg");

    // Pagination test (start_after)
    let paged = spill
        .scan_prefix(b_id, b"photos/2026/", Some(b"photos/2026/img_09.jpg"), 100)
        .expect("paged");
    assert_eq!(paged.len(), 10);
    assert_eq!(paged[0].0, b"photos/2026/img_10.jpg");

    // 4. Overwrite
    let mut updated_meta = meta1;
    updated_meta.size = 2048;
    spill.put(b_id, b"photos/vacation.jpg", &updated_meta).expect("overwrite");
    let after_update = spill.get(b_id, b"photos/vacation.jpg").expect("get").expect("found");
    assert_eq!(after_update.size, 2048);

    // 5. Delete
    let deleted = spill.delete(b_id, b"photos/vacation.jpg").expect("delete");
    assert!(deleted);
    let after_delete = spill.get(b_id, b"photos/vacation.jpg").expect("get");
    assert!(after_delete.is_none());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_storage_engine_spill_on_capacity_limit() {
    let dir = temp_dir("engine_spill");
    let db_path = dir.join("metadata.db");
    let spill = SpillIndex::open(&db_path).expect("open spill db");

    // Create an engine configured with max_objects = 20 (small memory arena)
    let mut engine = StorageEngine::new(20, 1024 * 1024, 65536);
    engine.attach_spill_index(spill);

    let b_id = engine.create_bucket("test-bucket").expect("create bucket");

    // Ingest 50 objects: first 20 stay in-memory, remaining 30 spill to redb
    for i in 0..50 {
        let key = format!("item_{:03}.dat", i);
        engine
            .put_object(
                b_id,
                key.as_bytes(),
                4096 * (i as u64 + 1),
                100 + i as u64,
                0x1111,
                0x2222,
                0,
                0,
            )
            .expect("put object");
    }

    // Verify all 50 objects are retrievable
    for i in 0..50 {
        let key = format!("item_{:03}.dat", i);
        let meta = engine.get_object(b_id, key.as_bytes()).expect("get object");
        assert_eq!(meta.size, 100 + i as u64);
        assert_eq!(meta.vol_offset, 4096 * (i as u64 + 1));
    }

    // Verify scan_objects seamlessly returns across memory and spilled boundaries
    let mut scanned = vec![ObjectMeta::default(); 50];
    let count = engine.scan_objects(b_id, b"item_", b"", 50, &mut scanned);
    assert_eq!(count, 50);

    // Overwrite a spilled object (e.g. item_045)
    engine
        .put_object(
            b_id,
            b"item_045.dat",
            99999,
            8888,
            0x3333,
            0x4444,
            0,
            0,
        )
        .expect("overwrite spilled object");
    let updated = engine.get_object(b_id, b"item_045.dat").expect("get updated");
    assert_eq!(updated.size, 8888);
    assert_eq!(updated.vol_offset, 99999);

    // Delete a spilled object
    engine.delete_object(b_id, b"item_045.dat").expect("delete spilled object");
    assert!(engine.get_object(b_id, b"item_045.dat").is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

/// The #13 gate: 1.2 million keys in one bucket (100,000 in memory, the rest spilled), every one
/// readable, and the process's peak resident memory at most 200 MB. Run in CI on Linux in a
/// release build (`-- --ignored`); peak RSS comes from /proc/self/status (VmHWM).
#[test]
#[ignore]
fn spill_holds_1_2_million_keys_in_bounded_memory() {
    let dir = temp_dir("scale");
    let spill = SpillIndex::open(&dir.join("metadata.db")).expect("open spill db");
    let mut engine = StorageEngine::new(100_000, 4 * 1024 * 1024, 65536);
    engine.attach_spill_index(spill);
    let b_id = engine.create_bucket("scale").unwrap();
    let n = 1_200_000u64;
    for i in 0..n {
        let key = format!("obj-{:07}", i);
        engine.put_object(b_id, key.as_bytes(), 4096 * (i + 1), i, i, !i, 0, 0).expect("put");
        if i % 200_000 == 0 {
            println!("{} keys written", i);
        }
    }
    for i in 0..n {
        let key = format!("obj-{:07}", i);
        let m = engine.get_object(b_id, key.as_bytes()).unwrap_or_else(|_| panic!("{} missing", key));
        assert_eq!((m.size, m.vol_offset), (i, 4096 * (i + 1)), "{}", key);
    }
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let kb: u64 = status
            .lines()
            .find(|l| l.starts_with("VmHWM:"))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
            .unwrap();
        println!("peak RSS for {} keys: {} MB", n, kb / 1024);
        assert!(kb <= 200 * 1024, "peak RSS {} MB above 200 MB", kb / 1024);
    }
}
