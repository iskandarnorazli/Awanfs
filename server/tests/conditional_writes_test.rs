//! Integration tests for Issue #112: conditional writes, deletes, multipart completion,
//! commit-time key-lock evaluation, work counters, and crash-restart durability.

use reqwest::header::{ETAG, IF_MATCH, IF_NONE_MATCH, LAST_MODIFIED};
use reqwest::StatusCode;
use s3_server::precondition::{format_http_date, parse_http_date};
use s3_server::{start_s3_reactor, S3ServiceState, WorkSnapshot, STREAM_PUT_MIN};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const WAIT: Duration = Duration::from_secs(20);

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_condw_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Server {
    dir: PathBuf,
    state: Arc<S3ServiceState>,
    url: String,
    client: reqwest::Client,
    handle: Option<s3_server::ServerHandle>,
}

impl Server {
    async fn start_at(dir: PathBuf) -> Self {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
        Server {
            dir,
            url: format!("http://{}", handle.local_addr),
            state,
            client: reqwest::Client::new(),
            handle: Some(handle),
        }
    }

    async fn start(name: &str) -> Self {
        let dir = temp_dir(name);
        let s = Self::start_at(dir).await;
        let res = s.client.put(format!("{}/bkt", s.url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        s
    }

    async fn enable_versioning(&self, bucket: &str) {
        let res = self
            .client
            .put(format!("{}/{}?versioning", self.url, bucket))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    fn shutdown_only(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown_only();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct ChildServer {
    child: Child,
    url: String,
    dir: PathBuf,
    keep_dir: bool,
}

impl ChildServer {
    async fn spawn(dir: PathBuf, keep_dir: bool) -> Self {
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let child = Command::new(env!("CARGO_BIN_EXE_s3-server"))
            .args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn s3-server");
        let url = format!("http://127.0.0.1:{}", port);
        let c = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if c.get(format!("{}/_awanfs/health/ready", url)).send().await.is_ok() {
                return Self { child, url, dir, keep_dir };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("s3-server did not start on port {}", port);
    }

    fn kill_hard(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.keep_dir = true;
    }
}

impl Drop for ChildServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !self.keep_dir {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

struct StalledPut {
    stream: TcpStream,
    rest: Vec<u8>,
}

async fn begin_stalled_put(
    state: &S3ServiceState,
    url: &str,
    path: &str,
    body: &[u8],
    extra_headers: &str,
) -> StalledPut {
    assert!(body.len() as u64 > STREAM_PUT_MIN);
    let host = url.trim_start_matches("http://").to_string();
    let mut stream = TcpStream::connect(&host).await.unwrap();
    let head = format!(
        "PUT {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        path,
        host,
        body.len(),
        extra_headers
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    let half = body.len() / 2;
    stream.write_all(&body[..half]).await.unwrap();
    stream.flush().await.unwrap();
    for _ in 0..400 {
        if state.volume_writers_in_flight(1) == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(1));
    StalledPut {
        stream,
        rest: body[half..].to_vec(),
    }
}

impl StalledPut {
    async fn finish(mut self) -> (u16, String) {
        self.stream.write_all(&self.rest).await.unwrap();
        self.stream.flush().await.unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(WAIT, self.stream.read_to_end(&mut buf))
            .await
            .expect("stalled PUT never answered")
            .unwrap();
        let text = String::from_utf8_lossy(&buf).to_string();
        let status = text
            .split_whitespace()
            .nth(1)
            .unwrap_or_else(|| panic!("no status line in {text:?}"))
            .parse()
            .unwrap();
        (status, text)
    }
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

#[tokio::test]
async fn put_if_match_and_if_none_match_semantics_and_headers() {
    let s = Server::start("put_cond").await;
    let key_url = format!("{}/bkt/k", s.url);

    // If-Match on missing object -> 404 NoSuchKey
    for hdr in ["*", "\"badetag\""] {
        let r = s.client.put(&key_url).header(IF_MATCH, hdr).body("a").send().await.unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND, "If-Match: {hdr} on missing");
        let txt = r.text().await.unwrap();
        assert!(txt.contains("<Code>NoSuchKey</Code>"), "body: {txt}");
    }

    // If-None-Match: * creates new object -> 200
    let r1 = s.client.put(&key_url).header(IF_NONE_MATCH, "*").body("v1").send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let etag1 = r1.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    // If-None-Match: * and If-None-Match: etag1 fail with 412 + ETag + Last-Modified
    for hdr in ["*", etag1.as_str(), etag1.trim_matches('"')] {
        let r = s.client.put(&key_url).header(IF_NONE_MATCH, hdr).body("v2").send().await.unwrap();
        assert_eq!(r.status(), StatusCode::PRECONDITION_FAILED, "If-None-Match: {hdr}");
        assert_eq!(r.headers().get(ETAG).and_then(|v| v.to_str().ok()), Some(etag1.as_str()));
        assert!(r.headers().get(LAST_MODIFIED).is_some());
        let txt = r.text().await.unwrap();
        assert!(txt.contains("<Code>PreconditionFailed</Code>"), "body: {txt}");
    }

    // If-Match: badetag -> 412 + ETag + Last-Modified
    let r_bad = s.client.put(&key_url).header(IF_MATCH, "\"deadbeef\"").body("v2").send().await.unwrap();
    assert_eq!(r_bad.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(r_bad.headers().get(ETAG).and_then(|v| v.to_str().ok()), Some(etag1.as_str()));
    assert!(r_bad.headers().get(LAST_MODIFIED).is_some());

    // If-Match: etag1 -> 200
    let r2 = s.client.put(&key_url).header(IF_MATCH, &etag1).body("v2").send().await.unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let etag2 = r2.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
    assert_ne!(etag1, etag2);

    // If-None-Match: etag1 (old ETag) -> 200
    let r3 = s.client.put(&key_url).header(IF_NONE_MATCH, &etag1).body("v3").send().await.unwrap();
    assert_eq!(r3.status(), StatusCode::OK);

    // If-Match: * on existing -> 200
    let r4 = s.client.put(&key_url).header(IF_MATCH, "*").body("v4").send().await.unwrap();
    assert_eq!(r4.status(), StatusCode::OK);

    // Delete object -> If-Match fails with 404 NoSuchKey, If-None-Match: etag1 succeeds
    assert_eq!(s.client.delete(&key_url).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let r_del_im = s.client.put(&key_url).header(IF_MATCH, "*").body("v5").send().await.unwrap();
    assert_eq!(r_del_im.status(), StatusCode::NOT_FOUND);
    let r_del_inm = s.client.put(&key_url).header(IF_NONE_MATCH, &etag1).body("v5").send().await.unwrap();
    assert_eq!(r_del_inm.status(), StatusCode::OK);
}

#[tokio::test]
async fn versioned_put_treats_delete_marker_as_absent() {
    let s = Server::start("ver_put").await;
    s.enable_versioning("bkt").await;
    let key_url = format!("{}/bkt/k", s.url);

    let r1 = s.client.put(&key_url).header(IF_NONE_MATCH, "*").body("v1").send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let etag1 = r1.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    let r2 = s.client.put(&key_url).body("v2").send().await.unwrap();
    assert_eq!(r2.status(), StatusCode::OK);

    // If-Match against stale version ETag fails with 412
    let r_stale = s.client.put(&key_url).header(IF_MATCH, &etag1).body("v3").send().await.unwrap();
    assert_eq!(r_stale.status(), StatusCode::PRECONDITION_FAILED);

    // If-None-Match against stale version ETag succeeds
    let r_ok = s.client.put(&key_url).header(IF_NONE_MATCH, &etag1).body("v3").send().await.unwrap();
    assert_eq!(r_ok.status(), StatusCode::OK);

    // Create delete marker -> counts as absent for PUT
    let del = s.client.delete(&key_url).send().await.unwrap();
    assert_eq!(del.status(), StatusCode::NO_CONTENT);
    assert_eq!(del.headers().get("x-amz-delete-marker").and_then(|v| v.to_str().ok()), Some("true"));

    let r_dm_im = s.client.put(&key_url).header(IF_MATCH, "*").body("v4").send().await.unwrap();
    assert_eq!(r_dm_im.status(), StatusCode::NOT_FOUND);
    let r_dm_inm = s.client.put(&key_url).header(IF_NONE_MATCH, "*").body("v4").send().await.unwrap();
    assert_eq!(r_dm_inm.status(), StatusCode::OK);
}

#[tokio::test]
async fn complete_multipart_conditional_headers_unversioned_and_versioned() {
    let s = Server::start("mp_cond").await;
    s.enable_versioning("bkt").await;

    async fn do_mpu(s: &Server, key: &str, body: &[u8], hdr: (&str, &str)) -> (StatusCode, String) {
        let init = s
            .client
            .post(format!("{}/bkt/{}?uploads", s.url, key))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let uid = init.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
        let up = s
            .client
            .put(format!("{}/bkt/{}?partNumber=1&uploadId={}", s.url, key, uid))
            .body(body.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(up.status(), StatusCode::OK);
        let part_etag = up.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
        let xml = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>");
        let mut req = s.client.post(format!("{}/bkt/{}?uploadId={}", s.url, key, uid)).body(xml);
        if !hdr.0.is_empty() {
            req = req.header(hdr.0, hdr.1);
        }
        let resp = req.send().await.unwrap();
        let st = resp.status();
        let et = resp.headers().get(ETAG).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        (st, et)
    }

    // Absent key: If-Match fails 404
    let (st, _) = do_mpu(&s, "mp", b"part-a", ("if-match", "*")).await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // If-None-Match: * succeeds -> 200
    let (st, et1) = do_mpu(&s, "mp", b"part-a", ("if-none-match", "*")).await;
    assert_eq!(st, StatusCode::OK);

    // If-None-Match: * or et1 fails 412, work counters zero on Complete
    let before = s.state.work.snapshot();
    let (st, _) = do_mpu(&s, "mp", b"part-b", ("if-none-match", "*")).await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED);
    // Only the 1 UploadPart wrote to staging; Complete wrote 0 bytes to volume (no manifest needle).
    let d = s.state.work.snapshot().since(&before);
    assert_eq!(d.disk_writes, 1, "only the staged part wrote, Complete wrote nothing: {d:?}");

    let (st, _) = do_mpu(&s, "mp", b"part-b", ("if-match", "\"wrong\"")).await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED);

    // If-Match: et1 succeeds -> et2
    let (st, et2) = do_mpu(&s, "mp", b"part-b", ("if-match", &et1)).await;
    assert_eq!(st, StatusCode::OK);
    assert_ne!(et1, et2);

    // Delete -> delete marker counts as absent for CompleteMultipartUpload
    assert_eq!(s.client.delete(format!("{}/bkt/mp", s.url)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let (st, _) = do_mpu(&s, "mp", b"part-c", ("if-match", "*")).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = do_mpu(&s, "mp", b"part-c", ("if-none-match", "*")).await;
    assert_eq!(st, StatusCode::OK);
}

#[tokio::test]
async fn concurrent_if_none_match_star_100_rounds_one_winner_per_round() {
    let s = Server::start("race_inm").await;
    for round in 0..100 {
        let mut handles = Vec::new();
        for w in 0..4 {
            let c = s.client.clone();
            let url = format!("{}/bkt/inm-{}", s.url, round);
            handles.push(tokio::spawn(async move {
                let body = format!("round-{round}-writer-{w}");
                let resp = c.put(&url).header(IF_NONE_MATCH, "*").body(body.clone()).send().await.unwrap();
                (resp.status(), body)
            }));
        }
        let mut winners = Vec::new();
        let mut failed = 0;
        for h in handles {
            let (st, body) = h.await.unwrap();
            match st {
                StatusCode::OK => winners.push(body),
                StatusCode::PRECONDITION_FAILED => failed += 1,
                other => panic!("round {round}: unexpected status {other}"),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}: expected exactly 1 winner, got {winners:?}");
        assert_eq!(failed, 3);
        let got = s.client.get(format!("{}/bkt/inm-{}", s.url, round)).send().await.unwrap().text().await.unwrap();
        assert_eq!(got, winners[0]);
    }
}

#[tokio::test]
async fn concurrent_cas_if_match_writers_one_winner_and_survives_kill_restart() {
    let dir = temp_dir("race_cas_kill");
    let srv = ChildServer::spawn(dir.clone(), true).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(format!("{}/bkt", srv.url)).send().await.unwrap().status(), StatusCode::OK);

    let mut expected: Vec<(String, String)> = Vec::new();
    for round in 0..30 {
        let key_url = format!("{}/bkt/cas-{}", srv.url, round);
        let init = client.put(&key_url).body(format!("init-{round}")).send().await.unwrap();
        assert_eq!(init.status(), StatusCode::OK);
        let init_etag = init.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

        let mut handles = Vec::new();
        for w in 0..4 {
            let c = client.clone();
            let u = key_url.clone();
            let et = init_etag.clone();
            handles.push(tokio::spawn(async move {
                let body = format!("cas-{round}-w-{w}");
                let resp = c.put(&u).header(IF_MATCH, &et).body(body.clone()).send().await.unwrap();
                (resp.status(), body)
            }));
        }
        let mut winners = Vec::new();
        let mut failed = 0;
        for h in handles {
            let (st, body) = h.await.unwrap();
            match st {
                StatusCode::OK => winners.push(body),
                StatusCode::PRECONDITION_FAILED => failed += 1,
                other => panic!("round {round}: unexpected status {other}"),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}: exactly one CAS writer wins");
        assert_eq!(failed, 3);
        expected.push((format!("cas-{round}"), winners.pop().unwrap()));
    }

    // Hard kill (SIGKILL / TerminateProcess) and restart on same data dir
    srv.kill_hard();
    let srv2 = ChildServer::spawn(dir, false).await;
    for (k, want) in expected {
        let got = client.get(format!("{}/bkt/{}", srv2.url, k)).send().await.unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        assert_eq!(got.text().await.unwrap(), want, "CAS winner for {k} after kill -9 restart");
    }
}

#[tokio::test]
async fn failed_condition_writes_nothing_and_leaves_state_and_versions_untouched() {
    let dir = temp_dir("fail_noop");
    let mut s = Server::start("fail_noop").await;
    let dir_path = s.dir.clone();
    s.enable_versioning("bkt").await;

    let key_url = format!("{}/bkt/noop", s.url);
    let r1 = s.client.put(&key_url).body("original").send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let etag1 = r1.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    let before = s.state.work.snapshot();
    // 1. Failed buffered PUT (If-Match mismatch)
    let f1 = s.client.put(&key_url).header(IF_MATCH, "\"wrong\"").body("overwrite").send().await.unwrap();
    assert_eq!(f1.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.state.work.snapshot().since(&before), WorkSnapshot::default(), "failed PUT must do zero disk work");

    // 2. Failed buffered PUT (If-None-Match: *)
    let f2 = s.client.put(&key_url).header(IF_NONE_MATCH, "*").body("overwrite").send().await.unwrap();
    assert_eq!(f2.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.state.work.snapshot().since(&before), WorkSnapshot::default());

    // 3. Failed DELETE (If-Match mismatch)
    let f3 = s.client.delete(&key_url).header(IF_MATCH, "\"wrong\"").send().await.unwrap();
    assert_eq!(f3.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.state.work.snapshot().since(&before), WorkSnapshot::default());

    // 4. Failed DeleteObjects
    let del_xml = "<Delete><Object><Key>noop</Key><ETag>\"wrong\"</ETag></Object></Delete>";
    let f4 = s.client.post(format!("{}/bkt?delete", s.url)).body(del_xml).send().await.unwrap();
    assert_eq!(f4.status(), StatusCode::OK);
    let f4_xml = f4.text().await.unwrap();
    assert!(f4_xml.contains("<Code>PreconditionFailed</Code>"), "xml: {f4_xml}");
    assert_eq!(s.state.work.snapshot().since(&before), WorkSnapshot::default());

    // Only 1 version exists in listing
    let list = s.client.get(format!("{}/bkt?versions", s.url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(list.matches("<Version>").count(), 1, "no version added: {list}");
    assert_eq!(list.matches("<DeleteMarker>").count(), 0);

    // Restart and confirm still 1 version with original bytes and ETag
    s.shutdown_only();
    let s2 = Server::start_at(dir_path).await;
    // Note: keep s2.dir from being deleted twice by forgetting s or copying dir; easier: s's Drop will remove dir, which is fine after s2 drops? Wait: s.dir == s2.dir, so let's keep a separate path or let s2 be the one that cleans up.
    let g = s2.client.get(format!("{}/bkt/noop", s2.url)).send().await.unwrap();
    assert_eq!(g.status(), StatusCode::OK);
    assert_eq!(g.headers().get(ETAG).unwrap().to_str().unwrap(), etag1);
    assert_eq!(g.text().await.unwrap(), "original");
    let list2 = s2.client.get(format!("{}/bkt?versions", s2.url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(list2.matches("<Version>").count(), 1);
    drop(s2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn streamed_put_condition_decided_at_commit_not_body_start() {
    let dir = temp_dir("stream_commit");
    let mut s = Server::start_at(dir.clone()).await;
    assert_eq!(s.client.put(format!("{}/bkt", s.url)).send().await.unwrap().status(), StatusCode::OK);

    // Case 1: Key exists at body start with If-None-Match: *, deleted mid-stream -> commits 200!
    let k1_url = format!("{}/bkt/s1", s.url);
    assert_eq!(s.client.put(&k1_url).body("old").send().await.unwrap().status(), StatusCode::OK);
    let payload1 = pattern(STREAM_PUT_MIN as usize + 64 * 1024, 1);
    let stalled1 = begin_stalled_put(&s.state, &s.url, "/bkt/s1", &payload1, "If-None-Match: *\r\n").await;
    assert_eq!(s.client.delete(&k1_url).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let (st1, _) = stalled1.finish().await;
    assert_eq!(st1, 200, "deleted mid-stream so If-None-Match: * must succeed at commit");
    assert_eq!(s.client.get(&k1_url).send().await.unwrap().bytes().await.unwrap(), payload1);

    // Case 2: Key matches If-Match: E1 at body start, overwritten with E2 mid-stream -> fails 412 at commit!
    let k2_url = format!("{}/bkt/s2", s.url);
    let r_e1 = s.client.put(&k2_url).body("e1").send().await.unwrap();
    let e1 = r_e1.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
    let payload2 = pattern(STREAM_PUT_MIN as usize + 64 * 1024, 2);
    let stalled2 = begin_stalled_put(&s.state, &s.url, "/bkt/s2", &payload2, &format!("If-Match: {e1}\r\n")).await;
    let r_e2 = s.client.put(&k2_url).body("e2-wins").send().await.unwrap();
    assert_eq!(r_e2.status(), StatusCode::OK);
    let (st2, body2) = stalled2.finish().await;
    assert_eq!(st2, 412, "overwritten mid-stream so If-Match: E1 must fail at commit: {body2}");
    assert_eq!(s.client.get(&k2_url).send().await.unwrap().text().await.unwrap(), "e2-wins");

    // Restart: failed streamed PUT must leave no needle recovery can resurrect
    s.shutdown_only();
    let s2 = Server::start_at(dir).await;
    assert_eq!(s2.client.get(format!("{}/bkt/s1", s2.url)).send().await.unwrap().bytes().await.unwrap(), payload1);
    assert_eq!(s2.client.get(format!("{}/bkt/s2", s2.url)).send().await.unwrap().text().await.unwrap(), "e2-wins");
}

#[tokio::test]
async fn conditional_delete_and_delete_objects_by_etag_mtime_size_and_version() {
    let s = Server::start("del_cond").await;
    let key_url = format!("{}/bkt/obj", s.url);

    // Unversioned: PUT 5 bytes
    let put = s.client.put(&key_url).body("hello").send().await.unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let etag = put.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
    let head = s.client.head(&key_url).send().await.unwrap();
    let lm = head.headers().get(LAST_MODIFIED).unwrap().to_str().unwrap().to_string();
    let lm_secs = parse_http_date(&lm).unwrap();
    let bad_lm = format_http_date(lm_secs - 3600);

    // DELETE If-Match mismatch -> 412, size mismatch -> 412, lmt mismatch -> 412
    assert_eq!(s.client.delete(&key_url).header(IF_MATCH, "\"bad\"").send().await.unwrap().status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.client.delete(&key_url).header("x-amz-if-match-size", "999").send().await.unwrap().status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.client.delete(&key_url).header("x-amz-if-match-last-modified-time", &bad_lm).send().await.unwrap().status(), StatusCode::PRECONDITION_FAILED);

    // DeleteObjects mismatch -> 200 XML with Error PreconditionFailed
    let bad_xml = format!("<Delete><Object><Key>obj</Key><LastModifiedTime>{bad_lm}</LastModifiedTime></Object></Delete>");
    let r = s.client.post(format!("{}/bkt?delete", s.url)).body(bad_xml).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r_txt = r.text().await.unwrap();
    assert!(r_txt.contains("<Code>PreconditionFailed</Code>"), "xml: {r_txt}");

    // Matching size + lmt + etag -> 204
    let d_ok = s
        .client
        .delete(&key_url)
        .header(IF_MATCH, &etag)
        .header("x-amz-if-match-size", "5")
        .header("x-amz-if-match-last-modified-time", &lm)
        .send()
        .await
        .unwrap();
    assert_eq!(d_ok.status(), StatusCode::NO_CONTENT);

    // Missing object DELETE with conditions -> 204
    assert_eq!(s.client.delete(&key_url).header(IF_MATCH, "\"bad\"").send().await.unwrap().status(), StatusCode::NO_CONTENT);

    // Versioned bucket: conditional delete on never-written key must not create a delete marker
    s.enable_versioning("bkt").await;
    let never_url = format!("{}/bkt/never", s.url);
    assert_eq!(s.client.delete(&never_url).header(IF_MATCH, "*").send().await.unwrap().status(), StatusCode::NO_CONTENT);
    assert_eq!(s.client.delete(&never_url).header(IF_MATCH, "\"bad\"").send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let vers_xml = s.client.get(format!("{}/bkt?versions", s.url)).send().await.unwrap().text().await.unwrap();
    assert!(!vers_xml.contains("<Key>never</Key>"), "absent conditional delete created delete marker: {vers_xml}");

    // Versioned bucket: current delete marker vs versionId
    s.enable_versioning("bkt").await;
    let pv = s.client.put(&key_url).body("ver-data").send().await.unwrap();
    assert_eq!(pv.status(), StatusCode::OK);
    let vid = pv.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    let vetag = pv.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    // DeleteObjects with matching ETag and Size on versioned current -> creates DeleteMarker
    let ok_xml = format!("<Delete><Object><Key>obj</Key><ETag>{vetag}</ETag><Size>8</Size></Object></Delete>");
    let r = s.client.post(format!("{}/bkt?delete", s.url)).body(ok_xml).send().await.unwrap();
    let r_txt = r.text().await.unwrap();
    assert!(r_txt.contains("<DeleteMarker>true</DeleteMarker>"), "xml: {r_txt}");

    // Current is now a delete marker: badetag / badsize fails 412, '*' succeeds
    assert_eq!(s.client.delete(&key_url).header(IF_MATCH, "\"bad\"").send().await.unwrap().status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.client.delete(&key_url).header(IF_MATCH, "*").send().await.unwrap().status(), StatusCode::NO_CONTENT);

    // Specific VersionId delete with wrong ETag -> 412, right ETag -> 204
    let v_url = format!("{}/bkt/obj?versionId={}", s.url, vid);
    assert_eq!(s.client.delete(&v_url).header(IF_MATCH, "\"bad\"").send().await.unwrap().status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.client.delete(&v_url).header(IF_MATCH, &vetag).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    // Already deleted versionId -> 204 even with bad conditions
    assert_eq!(s.client.delete(&v_url).header(IF_MATCH, "\"bad\"").send().await.unwrap().status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn copy_object_destination_preconditions_evaluated_under_key_lock() {
    let s = Server::start("copy_dst").await;
    let src_url = format!("{}/bkt/src", s.url);
    let dst_url = format!("{}/bkt/dst", s.url);
    assert_eq!(s.client.put(&src_url).body("src-body").send().await.unwrap().status(), StatusCode::OK);

    // Destination absent: If-Match fails 404, If-None-Match: * succeeds 200
    let r_404 = s
        .client
        .put(&dst_url)
        .header("x-amz-copy-source", "/bkt/src")
        .header(IF_MATCH, "*")
        .send()
        .await
        .unwrap();
    assert_eq!(r_404.status(), StatusCode::NOT_FOUND);

    let r_ok = s
        .client
        .put(&dst_url)
        .header("x-amz-copy-source", "/bkt/src")
        .header(IF_NONE_MATCH, "*")
        .send()
        .await
        .unwrap();
    assert_eq!(r_ok.status(), StatusCode::OK);
    let dst_etag = r_ok.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    // Destination exists: If-None-Match: * fails 412, If-Match matching succeeds 200
    let r_412 = s
        .client
        .put(&dst_url)
        .header("x-amz-copy-source", "/bkt/src")
        .header(IF_NONE_MATCH, "*")
        .send()
        .await
        .unwrap();
    assert_eq!(r_412.status(), StatusCode::PRECONDITION_FAILED);

    let r_cas = s
        .client
        .put(&dst_url)
        .header("x-amz-copy-source", "/bkt/src")
        .header(IF_MATCH, &dst_etag)
        .send()
        .await
        .unwrap();
    assert_eq!(r_cas.status(), StatusCode::OK);
}
