//! Not a test of behaviour: a measurement, run on demand by the probe workflow.
//!
//!     cargo test --release --test commit_cost_probe -- --ignored --nocapture
//!
//! What one commit costs at the volume layer, with no HTTP and no async runtime: 200 times,
//! write a 64 KiB needle and time the write, the volume and journal flushes together
//! (`sync_now`), and a read of the needle through the mapping. Next to
//! `benchmarks/sync_probe.py` (the same pattern on bare files) and
//! `benchmarks/put_path_probe.py` (the same through the whole server), this says which
//! layer a slow PUT's time is in.

use s3_server::VolumeManager;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn median(mut v: Vec<Duration>) -> f64 {
    v.sort();
    v[v.len() / 2].as_secs_f64() * 1000.0
}

fn run(label: &str, sparse: bool, read_back: bool) {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_commit_probe_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let vm = VolumeManager::open_mode(&dir, 1, sparse).unwrap();
    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i * 31 % 251) as u8).collect();
    let (mut writes, mut syncs, mut reads) = (Vec::new(), Vec::new(), Vec::new());
    for i in 0..200 {
        let t = Instant::now();
        let loc = vm.write_needle(1, &format!("key-{}", i), &payload, 1, 0, 0).unwrap();
        writes.push(t.elapsed());
        let t = Instant::now();
        vm.sync_now().unwrap();
        syncs.push(t.elapsed());
        if read_back {
            let t = Instant::now();
            let got = vm.read_needle_bytes(1, loc.offset, loc.payload_crc32c).unwrap();
            assert_eq!(got.len(), payload.len());
            reads.push(t.elapsed());
        }
    }
    let read = if read_back { format!("{:.3}", median(reads)) } else { "not measured".to_string() };
    println!("| {} | {:.3} | {:.3} | {} |", label, median(writes), median(syncs), read);
    drop(vm);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "a measurement, run by .github/workflows/probe-sync.yml"]
fn what_one_commit_costs_at_the_volume_layer() {
    println!("Data under {}", std::env::temp_dir().display());
    println!("| volume | write ms (median) | volume + journal flush ms (median) | read back ms (median) |");
    println!("|---|---|---|---|");
    run("growing, never read", false, false);
    run("growing, read after each write", false, true);
    run("sparse, never read", true, false);
    run("sparse, read after each write", true, true);
}
