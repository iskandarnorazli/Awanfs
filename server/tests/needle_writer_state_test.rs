//! #154: a streamed needle commits only when its writes add up to exactly the declared length,
//! and a refused write ends it (`server/src/fsm.rs::needle_step`, STATE_MACHINES.md "One
//! needle write"). Before, an over-long write was refused but the writer stayed usable: later
//! writes that made up the declared length committed a needle whose client had sent more.

use s3_server::VolumeManager;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn volume(name: &str) -> Arc<VolumeManager> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir: PathBuf = std::env::temp_dir().join(format!("awanfs_needle_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    Arc::new(VolumeManager::open(&dir, 1).unwrap())
}

fn keys_found(vm: &VolumeManager) -> Vec<String> {
    let mut keys = Vec::new();
    vm.recover_needles(|_, key, _, _, _, _, _, _| keys.push(key)).unwrap();
    keys
}

#[test]
fn a_refused_write_ends_the_needle() {
    let vm = volume("refused");
    let mut w = vm.begin_needle(1, "k", 10, 0).unwrap();
    w.write(&[1; 4]).unwrap();
    assert!(w.write(&[2; 10]).is_err(), "4 + 10 is more than the declared 10");
    assert!(w.write(&[3; 6]).is_err(), "nothing is accepted after a refusal");
    assert!(w.finish(0, 0).is_err(), "the client sent more than it declared: no commit");
    assert!(keys_found(&vm).is_empty(), "recovery finds nothing");
}

#[test]
fn a_short_needle_does_not_commit() {
    let vm = volume("short");
    let mut w = vm.begin_needle(1, "k", 10, 0).unwrap();
    w.write(&[1; 9]).unwrap();
    let e = w.finish(0, 0).unwrap_err();
    assert!(format!("{:?}", e).contains("fewer bytes"), "{:?}", e);
    assert!(keys_found(&vm).is_empty());
}

#[test]
fn an_exact_needle_commits_and_is_recovered() {
    let vm = volume("exact");
    let mut w = vm.begin_needle(1, "k", 10, 0).unwrap();
    w.write(&[1; 4]).unwrap();
    w.write(&[2; 6]).unwrap();
    w.finish(0, 0).unwrap();
    assert_eq!(keys_found(&vm), vec!["k".to_string()]);
}
