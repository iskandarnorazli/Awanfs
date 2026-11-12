//! Replays the counterexamples of `verification/tla/ObjectVersions.tla` against the real server.
//! Rule (AckedStateSurvivesRestart): when no request is in flight, what a GET shows now is what
//! it shows after a restart.
//!
//! The test hook pauses the first request on the key right after its first logged step and lets
//! a second request run. With a per-key lock the second request waits instead; the hook reports
//! that as `key_lock_contended`, so each version of the code takes a fixed path.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_order_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn(state: Arc<S3ServiceState>) -> (String, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, state).await.expect("start reactor");
    (format!("http://{}", handle.local_addr), handle)
}

/// Status and body of a GET. An error body's <RequestId> differs per request (#41), so it is
/// blanked: the comparison is about the object's state, not which request asked.
async fn get(c: &reqwest::Client, url: &str, key: &str) -> (StatusCode, Vec<u8>) {
    let res = c.get(format!("{}/bkt/{}", url, key)).send().await.unwrap();
    let status = res.status();
    let body = res.bytes().await.unwrap().to_vec();
    if status.is_success() {
        return (status, body);
    }
    let text = String::from_utf8_lossy(&body).into_owned();
    let blanked = match (text.find("<RequestId>"), text.find("</RequestId>")) {
        (Some(a), Some(b)) if a < b => format!("{}<RequestId/>{}", &text[..a], &text[b + "</RequestId>".len()..]),
        _ => text,
    };
    (status, blanked.into_bytes())
}

#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn wait(&self) {
        let mut g = self.open.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
    }
    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

enum Second {
    Put(&'static [u8]),
    Delete,
}

/// PUT "version A" pauses after its needle is written; then `second` runs. Returns what a GET
/// shows before and after a restart.
async fn race(name: &str, second: Second) -> ((StatusCode, Vec<u8>), (StatusCode, Vec<u8>)) {
    let dir = temp_dir(name);
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let (url, _handle) = spawn(Arc::clone(&state)).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let (tx, rx) = mpsc::channel::<&'static str>();
    let gate = Arc::new(Gate::default());
    let arrivals = Arc::new(AtomicUsize::new(0));
    {
        let tx = Mutex::new(tx);
        let gate = Arc::clone(&gate);
        let arrivals = Arc::clone(&arrivals);
        *state.test_hook.write() = Some(Arc::new(move |point: &'static str, _key: &str| match point {
            "put_stored" | "delete_start" => {
                let n = arrivals.fetch_add(1, Ordering::SeqCst);
                let _ = tx.lock().unwrap().send(if n == 0 { "first" } else { "second_ran" });
                if n == 0 {
                    gate.wait();
                }
            }
            "key_lock_contended" => {
                let _ = tx.lock().unwrap().send("second_waits");
            }
            _ => {}
        }));
    }
    let recv = || tokio::task::block_in_place(|| rx.recv_timeout(Duration::from_secs(60)).expect("hook event"));

    let (ca, ua) = (c.clone(), url.clone());
    let a = tokio::spawn(async move { ca.put(format!("{}/bkt/k", ua)).body(&b"version A"[..]).send().await.unwrap().status() });
    assert_eq!(recv(), "first");

    let (cb, ub) = (c.clone(), url.clone());
    let b = tokio::spawn(async move {
        match second {
            Second::Put(body) => cb.put(format!("{}/bkt/k", ub)).body(body).send().await.unwrap().status(),
            Second::Delete => cb.delete(format!("{}/bkt/k", ub)).send().await.unwrap().status(),
        }
    });
    let (sa, sb) = match recv() {
        "second_ran" => {
            let sb = b.await.unwrap();
            gate.open();
            (a.await.unwrap(), sb)
        }
        _ => {
            gate.open();
            (a.await.unwrap(), b.await.unwrap())
        }
    };
    assert_eq!(sa, StatusCode::OK);
    assert!(sb.is_success(), "second request: {}", sb);
    *state.test_hook.write() = None;

    let live = get(&c, &url, "k").await;
    let state2 = Arc::new(S3ServiceState::with_data_dir(&dir));
    let (url2, _handle2) = spawn(state2).await;
    let after = get(&c, &url2, "k").await;
    (live, after)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_puts_on_one_key_show_the_same_version_after_restart() {
    let (live, after) = race("put_put", Second::Put(b"version B")).await;
    assert_eq!(
        (live.0, String::from_utf8_lossy(&live.1)),
        (after.0, String::from_utf8_lossy(&after.1)),
        "live view (left) and recovered view (right) differ"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_and_delete_on_one_key_show_the_same_state_after_restart() {
    let (live, after) = race("put_delete", Second::Delete).await;
    assert_eq!(
        (live.0, String::from_utf8_lossy(&live.1)),
        (after.0, String::from_utf8_lossy(&after.1)),
        "live view (left) and recovered view (right) differ"
    );
}

#[tokio::test]
async fn zero_byte_objects_and_overwrites_survive_restart() {
    let dir = temp_dir("zero");
    let c = reqwest::Client::new();
    {
        let (url, _h) = spawn(Arc::new(S3ServiceState::with_data_dir(&dir))).await;
        assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
        for (key, body) in [("k", &b"hello"[..]), ("k", &b""[..]), ("folder/", &b""[..])] {
            let res = c.put(format!("{}/bkt/{}", url, key)).body(body.to_vec()).send().await.unwrap();
            assert_eq!(res.status(), StatusCode::OK, "PUT {}", key);
        }
    }
    let (url, _h) = spawn(Arc::new(S3ServiceState::with_data_dir(&dir))).await;
    assert_eq!(get(&c, &url, "k").await, (StatusCode::OK, Vec::new()), "the empty overwrite of k");
    assert_eq!(get(&c, &url, "folder/").await, (StatusCode::OK, Vec::new()), "the zero-byte folder marker");
}

#[tokio::test]
async fn copied_object_survives_source_overwrite_and_restart() {
    let dir = temp_dir("copy");
    let c = reqwest::Client::new();
    {
        let (url, _h) = spawn(Arc::new(S3ServiceState::with_data_dir(&dir))).await;
        assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.put(format!("{}/bkt/src", url)).body(&b"original bytes"[..]).send().await.unwrap().status(), StatusCode::OK);
        let res = c.put(format!("{}/bkt/dst", url)).header("x-amz-copy-source", "/bkt/src").send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(c.put(format!("{}/bkt/src", url)).body(&b"replaced"[..]).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(get(&c, &url, "dst").await, (StatusCode::OK, b"original bytes".to_vec()), "the copy after its source was overwritten");
    }
    let (url, _h) = spawn(Arc::new(S3ServiceState::with_data_dir(&dir))).await;
    assert_eq!(get(&c, &url, "dst").await, (StatusCode::OK, b"original bytes".to_vec()), "the copy after a restart");
}

#[tokio::test]
async fn bucket_answers_head_after_restart() {
    let dir = temp_dir("bucket_head");
    let c = reqwest::Client::new();
    {
        let (url, _h) = spawn(Arc::new(S3ServiceState::with_data_dir(&dir))).await;
        assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.put(format!("{}/bkt/s", url)).body(&b"x"[..]).send().await.unwrap().status(), StatusCode::OK);
    }
    let (url, _h) = spawn(Arc::new(S3ServiceState::with_data_dir(&dir))).await;
    assert_eq!(c.head(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK, "HEAD bucket after a restart");
    assert_eq!(get(&c, &url, "s").await, (StatusCode::OK, b"x".to_vec()));
}
