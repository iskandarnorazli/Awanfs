//! awan-storage/server/tests/property_tests.rs
//! Property-Based Generative Invariant and S3 Slicing Testing Suite (#471)

use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use awan_storage_server::ffi_bridge::AwanStorageEngine;
use awan_storage_server::s3_api::{compute_md5, md5_to_etag_hex};

static INJECT_RANGE_OFF_BY_ONE: AtomicBool = AtomicBool::new(false);

/// Helper: calculates RFC 9110 range slice on a payload
fn calculate_range_slice(payload: &[u8], start: usize, end: usize) -> Option<&[u8]> {
    let size = payload.len();
    if size == 0 || start >= size || start > end {
        return None;
    }
    let actual_end = std::cmp::min(end, size - 1);

    if INJECT_RANGE_OFF_BY_ONE.load(Ordering::Relaxed) {
        // Off-by-one fault injection for negative control:
        if actual_end + 2 <= size {
            return Some(&payload[start..actual_end + 2]);
        } else {
            return Some(&payload[start..actual_end]);
        }
    }

    Some(&payload[start..=actual_end])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_put_get_roundtrip_identity(
        key in "[a-zA-Z0-9_.-]{1,64}",
        payload in proptest::collection::vec(any::<u8>(), 0..8192)
    ) {
        let mut engine = AwanStorageEngine::new(4096, 64 * 1024, 64 * 1024);
        let b_id = engine.create_bucket("prop-bucket").unwrap();

        let (hi, lo, hex, _) = compute_md5(&payload);
        engine.put_object(b_id, key.as_bytes(), 0, payload.len() as u64, hi, lo, 1, 1).unwrap();

        let meta = engine.get_object(b_id, key.as_bytes()).unwrap();
        prop_assert_eq!(meta.size, payload.len() as u64);
        prop_assert_eq!(meta.etag_hi, hi);
        prop_assert_eq!(meta.etag_lo, lo);
        prop_assert_eq!(md5_to_etag_hex(meta.etag_hi, meta.etag_lo), hex);
    }

    #[test]
    fn prop_rfc9110_range_slicing_matches_reference(
        payload in proptest::collection::vec(any::<u8>(), 1..2048),
        start in 0usize..2048,
        length in 1usize..2048
    ) {
        let size = payload.len();
        if start < size {
            let end = start + length;
            let expected_end = std::cmp::min(end, size - 1);
            let expected_slice = &payload[start..=expected_end];

            let computed = calculate_range_slice(&payload, start, end);
            prop_assert!(computed.is_some());
            prop_assert_eq!(computed.unwrap(), expected_slice);
        }
    }

    #[test]
    fn prop_stateful_model_equivalence_btreemap(
        ops in proptest::collection::vec((
            prop_oneof![Just(0), Just(1), Just(2)], // 0: Put, 1: Get, 2: Delete
            "[a-z0-9]{1,8}",                         // Key
            proptest::collection::vec(any::<u8>(), 0..256) // Value
        ), 10..50)
    ) {
        let mut engine = AwanStorageEngine::new(2048, 64 * 1024, 64 * 1024);
        let b_id = engine.create_bucket("stateful-bucket").unwrap();
        let mut model: BTreeMap<String, Vec<u8>> = BTreeMap::new();

        for (op, key, val) in ops {
            match op {
                0 => {
                    // PUT
                    let (hi, lo, _, _) = compute_md5(&val);
                    engine.put_object(b_id, key.as_bytes(), 0, val.len() as u64, hi, lo, 1, 1).unwrap();
                    model.insert(key, val);
                }
                1 => {
                    // GET
                    let res = engine.get_object(b_id, key.as_bytes());
                    match model.get(&key) {
                        Some(expected) => {
                            prop_assert!(res.is_ok());
                            let meta = res.unwrap();
                            prop_assert_eq!(meta.size, expected.len() as u64);
                        }
                        None => {
                            prop_assert!(res.is_err());
                        }
                    }
                }
                2 => {
                    // DELETE
                    let res = engine.delete_object(b_id, key.as_bytes());
                    match model.remove(&key) {
                        Some(_) => {
                            prop_assert!(res.is_ok());
                        }
                        None => {
                            prop_assert!(res.is_err());
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn test_negative_control_range_shrink_on_fault() {
    INJECT_RANGE_OFF_BY_ONE.store(true, Ordering::Relaxed);
    let payload = vec![0x42u8; 10];
    let computed = calculate_range_slice(&payload, 0, 5);
    INJECT_RANGE_OFF_BY_ONE.store(false, Ordering::Relaxed);

    // Negative control: off-by-one injection must produce mismatch with standard slice
    assert_ne!(computed.unwrap(), &payload[0..=5], "Injected off-by-one must cause slice mismatch");
}
