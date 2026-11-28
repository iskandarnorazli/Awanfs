use bytes::Bytes;
use proptest::prelude::*;
use s3_server::bucket_config::{
    BucketConfigStore, ConfigError, ConfigKind, ParsedConfig, ValidatorRegistry,
    MAX_CONFIG_BYTES, MAX_POLICY_BYTES,
};
use s3_server::fsm::{bucket_step, BucketEvent, BucketState, BucketStatus};
use s3_server::s3_api::{render_metrics, S3ServiceState};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile_helper::TempDir;

mod tempfile_helper {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    pub struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        pub fn new(prefix: &str) -> Self {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("{}_{}_{}", prefix, std::process::id(), n));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        pub fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

fn accept_all_validators() -> ValidatorRegistry {
    let mut reg = ValidatorRegistry::new();
    for &kind in ConfigKind::ALL {
        reg.register(kind, Arc::new(|bytes: &[u8]| Ok(ParsedConfig::raw(Bytes::copy_from_slice(bytes)))));
    }
    reg
}

#[test]
fn config_survives_restart_byte_for_byte_and_delete_stays_deleted() {
    let dir = TempDir::new("bcfg_restart");
    let manifest = dir.path().join("buckets.manifest");

    {
        let mut store = BucketConfigStore::open(Some(manifest.clone()), accept_all_validators()).unwrap();
        store.apply_bucket_create(1, "alpha-bucket", 100, "root", "").unwrap();
        for (i, &kind) in ConfigKind::ALL.iter().enumerate() {
            let payload = format!("{{\"kind\":\"{}\",\"idx\":{}}}", kind.as_str(), i).into_bytes();
            store.put_config(1, kind, &payload).unwrap();
        }
        // Delete one kind and verify it stays deleted across restart.
        store.delete_config(1, ConfigKind::Cors).unwrap();
    }

    // Restart from disk.
    let store = BucketConfigStore::open(Some(manifest), accept_all_validators()).unwrap();
    let cfg = store.load_bucket(1).expect("bucket 1 should exist after restart");
    for (i, &kind) in ConfigKind::ALL.iter().enumerate() {
        if kind == ConfigKind::Cors {
            assert!(cfg.get(kind).is_none(), "deleted kind cors should remain deleted");
        } else {
            let expected = format!("{{\"kind\":\"{}\",\"idx\":{}}}", kind.as_str(), i).into_bytes();
            let got = cfg.get(kind).expect("kind should survive restart");
            assert_eq!(got.raw.as_ref(), expected.as_slice(), "byte-for-byte mismatch for {}", kind.as_str());
        }
    }
}

#[test]
fn latest_record_wins_and_delete_bucket_drops_all_kinds() {
    let dir = TempDir::new("bcfg_latest");
    let manifest = dir.path().join("buckets.manifest");

    {
        let mut store = BucketConfigStore::open(Some(manifest.clone()), accept_all_validators()).unwrap();
        store.apply_bucket_create(1, "my-bucket", 1, "root", "").unwrap();
        store.put_config(1, ConfigKind::Policy, b"{\"v\":1}").unwrap();
        store.put_config(1, ConfigKind::Policy, b"{\"v\":2}").unwrap();
        assert_eq!(store.load_bucket(1).unwrap().get(ConfigKind::Policy).unwrap().raw.as_ref(), b"{\"v\":2}");

        // Delete bucket drops all kinds; recreating with a new id starts clean.
        for &kind in ConfigKind::ALL {
            store.put_config(1, kind, b"data").unwrap();
        }
        store.apply_bucket_delete(1, "my-bucket", true).unwrap();
        assert!(store.load_bucket(1).is_none());

        store.apply_bucket_create(2, "my-bucket", 2, "root", "").unwrap();
        let cfg2 = store.load_bucket(2).unwrap();
        for &kind in ConfigKind::ALL {
            assert!(cfg2.get(kind).is_none(), "recreated bucket must not inherit {:?}", kind);
        }
    }

    let store = BucketConfigStore::open(Some(manifest), accept_all_validators()).unwrap();
    assert!(store.load_bucket(1).is_none());
    let cfg2 = store.load_bucket(2).unwrap();
    for &kind in ConfigKind::ALL {
        assert!(cfg2.get(kind).is_none(), "recreated bucket after restart must be clean for {:?}", kind);
    }
}

#[test]
fn existing_versioning_tilde_lines_still_read_and_written() {
    let dir = TempDir::new("bcfg_tilde");
    let manifest = dir.path().join("buckets.manifest");
    std::fs::write(&manifest, "+1:ver-bucket\n~1:Enabled\n").unwrap();

    let state = S3ServiceState::try_with_data_dir(dir.path()).expect("startup should read ~ line");
    assert_eq!(
        state.versioning.read().get(&1).copied(),
        Some(s3_server::versions::Versioning::Enabled)
    );
    state.set_versioning(1, s3_server::versions::Versioning::Suspended).unwrap();

    let raw = std::fs::read_to_string(&manifest).unwrap();
    assert!(raw.contains("~1:Suspended"), "set_versioning must still write ~id:status");

    let state2 = S3ServiceState::try_with_data_dir(dir.path()).expect("restart should read ~Suspended");
    assert_eq!(
        state2.versioning.read().get(&1).copied(),
        Some(s3_server::versions::Versioning::Suspended)
    );
}

#[test]
fn unknown_kind_stops_startup_naming_the_kind() {
    let dir = TempDir::new("bcfg_unknown");
    let manifest = dir.path().join("buckets.manifest");
    std::fs::write(&manifest, "+1:test-bucket\n=1:future-magic-kind:AQID\n").unwrap();

    let err = BucketConfigStore::open(Some(manifest), accept_all_validators())
        .err()
        .expect("unknown kind must stop startup");
    assert!(
        err.contains("future-magic-kind"),
        "startup error must name the unknown kind, got: {}",
        err
    );
}

#[test]
fn corrupt_middle_line_stops_startup_with_line_number() {
    let dir = TempDir::new("bcfg_corrupt_mid");
    let manifest = dir.path().join("buckets.manifest");
    std::fs::write(
        &manifest,
        "+1:alpha-bucket\nthis-is-not-a-valid-record\n+2:beta-bucket\n",
    )
    .unwrap();

    let err = BucketConfigStore::open(Some(manifest), accept_all_validators())
        .err()
        .expect("corrupt middle line must stop startup");
    assert!(
        err.contains("2"),
        "startup error must include 1-based line number 2, got: {}",
        err
    );
}

#[test]
fn torn_last_line_at_every_truncation_offset_is_ignored_with_warning() {
    let dir = TempDir::new("bcfg_torn");
    let manifest = dir.path().join("buckets.manifest");
    let full = "+1:alpha-bucket\n=1:policy:eyJ2IjoxfQ==\n=1:public-access-block:PEJsb2NrLz4=\n";

    // Every truncation point from 0..full.len() must succeed at startup without error.
    for cut in 0..=full.len() {
        std::fs::write(&manifest, &full[..cut]).unwrap();
        let res = BucketConfigStore::open(Some(manifest.clone()), accept_all_validators());
        assert!(
            res.is_ok(),
            "truncation at byte {} ({:?}) should not fail startup: {:?}",
            cut,
            &full[..cut],
            res.err()
        );
    }
}

#[test]
fn size_limits_and_validator_enforcement() {
    let dir = TempDir::new("bcfg_limits");
    let manifest = dir.path().join("buckets.manifest");

    // Empty validator registry rejects writes.
    let mut empty_reg_store = BucketConfigStore::open(Some(manifest.clone()), ValidatorRegistry::new()).unwrap();
    empty_reg_store.apply_bucket_create(1, "limit-bucket", 1, "root", "").unwrap();
    let err = empty_reg_store.put_config(1, ConfigKind::Policy, b"{}").unwrap_err();
    assert!(matches!(err, ConfigError::NoValidator(_)));

    // Validator that rejects "bad" refuses persistence.
    let mut reg = accept_all_validators();
    reg.register(
        ConfigKind::Policy,
        Arc::new(|bytes: &[u8]| {
            if bytes == b"bad" {
                Err(ConfigError::Rejected { status: 400, code: "MalformedPolicy", message: "bad policy".into() })
            } else {
                Ok(ParsedConfig::raw(Bytes::copy_from_slice(bytes)))
            }
        }),
    );
    let mut store = BucketConfigStore::open(Some(manifest), reg).unwrap();
    let err = store.put_config(1, ConfigKind::Policy, b"bad").unwrap_err();
    assert_eq!(err.status(), 400);
    assert!(store.load_bucket(1).unwrap().get(ConfigKind::Policy).is_none());

    // 20 KiB limit for policy, 64 KiB for others.
    let ok_policy = vec![b'a'; MAX_POLICY_BYTES];
    store.put_config(1, ConfigKind::Policy, &ok_policy).unwrap();
    let too_big_policy = vec![b'a'; MAX_POLICY_BYTES + 1];
    let err = store.put_config(1, ConfigKind::Policy, &too_big_policy).unwrap_err();
    assert_eq!(err.status(), 400);

    let ok_cors = vec![b'b'; MAX_CONFIG_BYTES];
    store.put_config(1, ConfigKind::Cors, &ok_cors).unwrap();
    let too_big_cors = vec![b'b'; MAX_CONFIG_BYTES + 1];
    let err = store.put_config(1, ConfigKind::Cors, &too_big_cors).unwrap_err();
    assert_eq!(err.status(), 400);
}

#[test]
fn plus_record_optional_trailing_fields_parse() {
    let dir = TempDir::new("bcfg_plus");
    let manifest = dir.path().join("buckets.manifest");
    std::fs::write(
        &manifest,
        "+1:bkt-one\n+2:bkt-two:1700000000\n+3:bkt-three:1700000001:alice\n+4:bkt-four:1700000002:bob:tenant-x\n",
    )
    .unwrap();

    let store = BucketConfigStore::open(Some(manifest), accept_all_validators()).unwrap();
    assert!(store.load_bucket(1).is_some());
    assert!(store.load_bucket(2).is_some());
    assert!(store.load_bucket(3).is_some());
    assert!(store.load_bucket(4).is_some());
}

#[test]
fn concurrent_readers_see_consistent_snapshot_without_locks() {
    let dir = TempDir::new("bcfg_conc");
    let manifest = dir.path().join("buckets.manifest");
    let store = Arc::new(parking_lot::Mutex::new(
        BucketConfigStore::open(Some(manifest), accept_all_validators()).unwrap(),
    ));
    let reader_handle = store.lock().reader();
    store.lock().apply_bucket_create(1, "conc-bucket", 1, "root", "").unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let r = reader_handle.clone();
        let s = Arc::clone(&stop);
        readers.push(std::thread::spawn(move || {
            while !s.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(cfg) = r.load_bucket(1) {
                    if let Some(p) = cfg.get(ConfigKind::Policy) {
                        assert!(p.raw.starts_with(b"{\"seq\":"));
                    }
                }
            }
        }));
    }

    for seq in 0..50 {
        let doc = format!("{{\"seq\":{}}}", seq).into_bytes();
        store.lock().put_config(1, ConfigKind::Policy, &doc).unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in readers {
        h.join().unwrap();
    }
}

#[test]
fn metrics_expose_writes_manifest_size_and_live_ratio() {
    let dir = TempDir::new("bcfg_metrics");
    let state = S3ServiceState::try_with_data_dir(dir.path()).unwrap();
    let out = render_metrics(&state);
    assert!(out.contains("awanfs_bucket_manifest_bytes"));
    assert!(out.contains("awanfs_bucket_manifest_live_ratio"));
    assert!(out.contains("awanfs_bucket_config_writes_total"));
}

#[derive(Clone, Debug)]
enum Op {
    Create(u8),
    Delete(u8),
    Put(u8, usize, Vec<u8>),
    DelCfg(u8, usize),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..3).prop_map(Op::Create),
        (0u8..3).prop_map(Op::Delete),
        (0u8..3, 0usize..ConfigKind::ALL.len(), prop::collection::vec(any::<u8>(), 1..32))
            .prop_map(|(b, k, d)| Op::Put(b, k, d)),
        (0u8..3, 0usize..ConfigKind::ALL.len()).prop_map(|(b, k)| Op::DelCfg(b, k)),
    ]
}

proptest! {
    #[test]
    fn prop_replay_equals_live_and_compaction_preserves_state(ops in prop::collection::vec(op_strategy(), 1..80)) {
        let dir = TempDir::new("bcfg_prop");
        let manifest = dir.path().join("buckets.manifest");

        let mut next_id: u32 = 1;
        let mut name_to_id: HashMap<String, u32> = HashMap::new();
        let mut live_expected: HashMap<u32, HashMap<ConfigKind, Vec<u8>>> = HashMap::new();

        {
            let mut store = BucketConfigStore::open(Some(manifest.clone()), accept_all_validators()).unwrap();
            for op in &ops {
                match op {
                    Op::Create(b) => {
                        let name = format!("bucket-{}", b);
                        if !name_to_id.contains_key(&name) {
                            let id = next_id;
                            next_id += 1;
                            let st = bucket_step(BucketState::absent(), BucketEvent::Create { caller_id: 0, is_us_east_1: true, acl_matches: true }).unwrap();
                            assert_eq!(st.status, BucketStatus::Active);
                            store.apply_bucket_create(id, &name, 0, "root", "").unwrap();
                            name_to_id.insert(name, id);
                            live_expected.insert(id, HashMap::new());
                        }
                    }
                    Op::Delete(b) => {
                        let name = format!("bucket-{}", b);
                        if let Some(id) = name_to_id.remove(&name) {
                            store.apply_bucket_delete(id, &name, true).unwrap();
                            live_expected.remove(&id);
                        }
                    }
                    Op::Put(b, k_idx, data) => {
                        let name = format!("bucket-{}", b);
                        if let Some(&id) = name_to_id.get(&name) {
                            let kind = ConfigKind::ALL[*k_idx];
                            store.put_config(id, kind, data).unwrap();
                            live_expected.get_mut(&id).unwrap().insert(kind, data.clone());
                        }
                    }
                    Op::DelCfg(b, k_idx) => {
                        let name = format!("bucket-{}", b);
                        if let Some(&id) = name_to_id.get(&name) {
                            let kind = ConfigKind::ALL[*k_idx];
                            store.delete_config(id, kind).unwrap();
                            live_expected.get_mut(&id).unwrap().remove(&kind);
                        }
                    }
                }
            }
        }

        // First restart (may compact if dead > live).
        let replayed = BucketConfigStore::open(Some(manifest.clone()), accept_all_validators()).unwrap();
        for (&id, expected_map) in &live_expected {
            let cfg = replayed.load_bucket(id).expect("active bucket missing after replay");
            for &kind in ConfigKind::ALL {
                let got = cfg.get(kind).map(|c| c.raw.to_vec());
                let want = expected_map.get(&kind).cloned();
                prop_assert_eq!(got, want);
            }
        }

        // Force extra dead records so compaction definitely triggers on next restart, then verify again.
        drop(replayed);
        {
            let mut store = BucketConfigStore::open(Some(manifest.clone()), accept_all_validators()).unwrap();
            store.apply_bucket_create(999, "churn-bucket", 0, "root", "").unwrap();
            for i in 0..30 {
                store.put_config(999, ConfigKind::Policy, format!("{{\"i\":{}}}", i).as_bytes()).unwrap();
            }
            store.apply_bucket_delete(999, "churn-bucket", true).unwrap();
        }
        let compacted = BucketConfigStore::open(Some(manifest), accept_all_validators()).unwrap();
        prop_assert!(compacted.load_bucket(999).is_none());
        for (&id, expected_map) in &live_expected {
            let cfg = compacted.load_bucket(id).expect("active bucket missing after compaction");
            for &kind in ConfigKind::ALL {
                let got = cfg.get(kind).map(|c| c.raw.to_vec());
                let want = expected_map.get(&kind).cloned();
                prop_assert_eq!(got, want);
            }
        }
        prop_assert!((compacted.manifest_stats().live_ratio() - 1.0).abs() < 1e-9);
    }
}

#[test]
fn base64_decode_refuses_bits_under_the_padding() {
    use s3_server::bucket_config::{base64_decode, base64_encode};
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    assert_eq!(base64_decode(""), Ok(Vec::new()));
    for bad in ["A", "AA", "AAA", "AQ==AQID", "AQA=AQID", "bad!"] {
        assert_eq!(base64_decode(bad), Err(()), "malformed {:?}", bad);
    }

    for len in 0..40usize {
        let bytes: Vec<u8> = (0..len).map(|i| (i * 37 + len * 11) as u8).collect();
        let enc = base64_encode(&bytes);
        assert_eq!(base64_decode(&enc), Ok(bytes.clone()), "canonical {:?}", enc);
        let pads = enc.bytes().rev().take_while(|&b| b == b'=').count();
        if pads == 0 {
            continue;
        }
        let at = enc.len() - pads - 1;
        let unused = if pads == 1 { 0b11 } else { 0b1111 };
        let v = ALPHABET.iter().position(|&c| c == enc.as_bytes()[at]).unwrap();
        for low in 1..=unused {
            let mut alt = enc.clone().into_bytes();
            alt[at] = ALPHABET[(v & !unused) | low];
            let alt = String::from_utf8(alt).unwrap();
            assert_eq!(base64_decode(&alt), Err(()), "{:?} is a non-canonical form of {:?}", alt, enc);
        }
    }

    assert_eq!(base64_decode("rpIg5gm="), Err(()));
    assert_eq!(base64_decode("rpIg5gk=").map(|b| base64_encode(&b)), Ok("rpIg5gk=".to_string()));
    assert_eq!(base64_decode("XUFAKrxLKna5cZ2REBfFkh=="), Err(()));
    assert_eq!(
        base64_decode("XUFAKrxLKna5cZ2REBfFkg==").map(|b| base64_encode(&b)),
        Ok("XUFAKrxLKna5cZ2REBfFkg==".to_string())
    );
}
