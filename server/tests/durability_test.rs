//! Durability modes (#29): how many syncs each mode makes, and that every acknowledged object
//! survives a killed server in each mode. A kill keeps the page cache, so the kill tests show
//! the acknowledgement path is wired; the sync counts show when the data reaches the disk.

use s3_server::{Durability, VolumeManager};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_durable_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn volume(name: &str, mode: Durability) -> Arc<VolumeManager> {
    let vm = VolumeManager::open_mode(&temp_dir(name), 1, false).unwrap();
    vm.set_durability(mode);
    Arc::new(vm)
}

fn syncs(vm: &VolumeManager) -> u64 {
    vm.work_counters().snapshot().disk_syncs
}

#[test]
fn group_mode_covers_a_batch_of_writes_with_one_sync_of_volume_and_journal() {
    let vm = volume("group", Durability::Group);
    let seqs: Vec<u64> = (0..64).map(|i| vm.write_needle(1, &format!("k{}", i), b"payload", 1, 0, 0).unwrap().seq).collect();
    assert_eq!(syncs(&vm), 0, "writing does not sync");
    let waiters: Vec<_> = seqs
        .into_iter()
        .map(|seq| {
            let vm = Arc::clone(&vm);
            std::thread::spawn(move || vm.make_durable(seq).unwrap())
        })
        .collect();
    for w in waiters {
        w.join().unwrap();
    }
    assert_eq!(syncs(&vm), 2, "64 waiting writers: one volume sync and one journal sync");
    vm.make_durable(64).unwrap();
    assert_eq!(syncs(&vm), 2, "an already durable write does not sync again");

    let seq = vm.write_needle(1, "late", b"x", 1, 0, 0).unwrap().seq;
    vm.make_durable(seq).unwrap();
    assert_eq!(syncs(&vm), 4, "a later write gets its own sync");
}

#[test]
fn strict_mode_syncs_every_write() {
    let vm = volume("strict", Durability::Strict);
    for i in 0..3 {
        let seq = vm.write_needle(1, &format!("k{}", i), b"payload", 1, 0, 0).unwrap().seq;
        vm.make_durable(seq).unwrap();
    }
    assert_eq!(syncs(&vm), 6);
}

#[test]
fn fast_mode_answers_without_a_sync_and_the_flusher_syncs_later() {
    let vm = volume("fast", Durability::Fast);
    for i in 0..3 {
        let seq = vm.write_needle(1, &format!("k{}", i), b"payload", 1, 0, 0).unwrap().seq;
        vm.make_durable(seq).unwrap();
    }
    assert_eq!(syncs(&vm), 0, "no sync on the request path");
    assert!(vm.flush_if_dirty().unwrap());
    assert_eq!(syncs(&vm), 2, "the flusher's sync covers all three writes");
    assert!(!vm.flush_if_dirty().unwrap(), "nothing new: no sync");
    assert_eq!(syncs(&vm), 2);
}

mod kill {
    use reqwest::header::ETAG;
    use reqwest::StatusCode;
    use std::net::TcpListener;
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    async fn start(dir: &Path, mode: &str) -> (Child, String) {
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let child = Command::new(env!("CARGO_BIN_EXE_s3-server"))
            .args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous", "--durability", mode])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn s3-server");
        let url = format!("http://127.0.0.1:{}", port);
        let c = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if c.get(format!("{}/", url)).send().await.is_ok() {
                return (child, url);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("s3-server --durability {} did not start", mode);
    }

    async fn acknowledged_objects_survive_a_kill(mode: &str) {
        let dir = super::temp_dir(&format!("kill_{}", mode));
        let c = reqwest::Client::new();
        let (mut child, url) = start(&dir, mode).await;
        assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
        let mut objects = Vec::new();
        for i in 0..20u32 {
            let body: Vec<u8> = (0..(5000 + i as usize * 311)).map(|j| ((j as u32 * 7 + i) % 251) as u8).collect();
            let res = c.put(format!("{}/bkt/o{}", url, i)).body(body.clone()).send().await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            objects.push((format!("o{}", i), body, res.headers().get(ETAG).unwrap().to_str().unwrap().to_string()));
        }
        assert_eq!(c.delete(format!("{}/bkt/o0", url)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
        child.kill().unwrap();
        child.wait().unwrap();

        let (mut child, url) = start(&dir, mode).await;
        assert_eq!(c.get(format!("{}/bkt/o0", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND, "{}: the delete survives", mode);
        for (key, body, etag) in objects.iter().skip(1) {
            let res = c.get(format!("{}/bkt/{}", url, key)).send().await.unwrap();
            assert_eq!(res.status(), StatusCode::OK, "{}: GET {}", mode, key);
            assert_eq!(res.headers().get(ETAG).unwrap().to_str().unwrap(), etag);
            assert!(&res.bytes().await.unwrap()[..] == &body[..], "{}: bytes of {}", mode, key);
        }
        child.kill().ok();
        child.wait().ok();
    }

    #[tokio::test]
    async fn group() {
        acknowledged_objects_survive_a_kill("group").await;
    }

    #[tokio::test]
    async fn fast() {
        acknowledged_objects_survive_a_kill("fast").await;
    }

    #[tokio::test]
    async fn strict() {
        acknowledged_objects_survive_a_kill("strict").await;
    }
}

#[test]
fn an_unknown_mode_stops_the_binary() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_s3-server"))
        .args(["--port", "0", "--allow-anonymous", "--durability", "sometimes"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
