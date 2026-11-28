//! Reactor accept-loop hardening tests (#531).

use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3_server::{
    accept_backoff, start_s3_reactor_with_listener, AcceptListener, S3ServiceState,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

fn emfile_error() -> io::Error {
    #[cfg(unix)]
    {
        io::Error::from_raw_os_error(libc::EMFILE)
    }
    #[cfg(windows)]
    {
        // WSAEMFILE (10024)
        io::Error::from_raw_os_error(10024)
    }
}

struct FaultInjectingListener {
    inner: TcpListener,
    addr: SocketAddr,
    injected: Mutex<VecDeque<io::Error>>,
    injected_returned: Arc<AtomicUsize>,
    all_injected_done: Arc<Notify>,
}

impl AcceptListener for FaultInjectingListener {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }

    fn accept(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send + '_>> {
        Box::pin(async move {
            let next_err = {
                let mut q = self.injected.lock().unwrap();
                q.pop_front()
            };
            if let Some(err) = next_err {
                let prev = self.injected_returned.fetch_add(1, Ordering::SeqCst);
                if prev + 1 >= 2 {
                    self.all_injected_done.notify_waiters();
                }
                return Err(err);
            }
            self.inner.accept().await
        })
    }
}

struct AlwaysEmfileListener {
    addr: SocketAddr,
    calls: Arc<AtomicUsize>,
    notify_on_first_wait: Arc<Notify>,
    signaled: Arc<AtomicBool>,
}

impl AcceptListener for AlwaysEmfileListener {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }

    fn accept(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send + '_>> {
        Box::pin(async move {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if n >= 3 && !self.signaled.swap(true, Ordering::SeqCst) {
                self.notify_on_first_wait.notify_waiters();
            }
            Err(emfile_error())
        })
    }
}

#[test]
fn accept_backoff_unit_resource_and_transient_classes() {
    let emfile = emfile_error();
    let expected = [10, 20, 40, 80, 160, 320, 640, 1000, 1000];
    let mut prev = None;
    for ms in expected {
        let d = accept_backoff(prev, &emfile);
        assert_eq!(d, Duration::from_millis(ms), "prev={:?}", prev);
        prev = Some(d);
    }

    for kind in [
        io::ErrorKind::ConnectionAborted,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::Interrupted,
    ] {
        let err = io::Error::from(kind);
        assert_eq!(accept_backoff(None, &err), Duration::ZERO);
        assert_eq!(
            accept_backoff(Some(Duration::from_millis(80)), &err),
            Duration::ZERO
        );
    }

    let unknown = io::Error::other("unexpected");
    assert_eq!(accept_backoff(None, &unknown), Duration::ZERO);
}

#[tokio::test]
async fn accept_loop_survives_emfile_and_connection_reset_then_serves_http() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp.local_addr().unwrap();
    let injected_returned = Arc::new(AtomicUsize::new(0));
    let all_injected_done = Arc::new(Notify::new());

    let mut q = VecDeque::new();
    q.push_back(emfile_error());
    q.push_back(io::Error::from(io::ErrorKind::ConnectionReset));

    let listener = FaultInjectingListener {
        inner: tcp,
        addr,
        injected: Mutex::new(q),
        injected_returned: Arc::clone(&injected_returned),
        all_injected_done: Arc::clone(&all_injected_done),
    };

    let state = Arc::new(S3ServiceState::new());
    let done_wait = all_injected_done.notified();
    let handle = start_s3_reactor_with_listener(listener, state, None)
        .await
        .unwrap();

    if injected_returned.load(Ordering::SeqCst) < 2 {
        tokio::time::timeout(Duration::from_secs(2), done_wait)
            .await
            .expect("reactor should work through EMFILE and ConnectionReset");
    }

    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).await.unwrap();

    assert!(
        resp.starts_with("HTTP/1.1 200 OK"),
        "expected HTTP 200 after accept errors, got: {:?}",
        resp
    );
    assert!(
        injected_returned.load(Ordering::SeqCst) >= 2,
        "expected both injected errors to have been processed"
    );

    assert!(
        handle.shutdown_gracefully(Duration::from_secs(2)).await,
        "server should stop cleanly after serving HTTP request"
    );
}

#[tokio::test]
async fn shutdown_interrupts_accept_backoff_wait_within_deadline() {
    // Bind a throwaway listener only to reserve an unused address for local_addr().
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let calls = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    let signaled = Arc::new(AtomicBool::new(false));
    let wait_entered = notify.notified();

    let listener = AlwaysEmfileListener {
        addr,
        calls: Arc::clone(&calls),
        notify_on_first_wait: Arc::clone(&notify),
        signaled: Arc::clone(&signaled),
    };

    let state = Arc::new(S3ServiceState::new());
    let handle = start_s3_reactor_with_listener(listener, state, None)
        .await
        .unwrap();

    if !signaled.load(Ordering::SeqCst) {
        tokio::time::timeout(Duration::from_secs(2), wait_entered)
            .await
            .expect("reactor should enter resource-error backoff loop");
    }

    assert!(
        handle.shutdown_gracefully(Duration::from_secs(2)).await,
        "shutdown during accept backoff must stop the server within 2 s"
    );
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_awanfs(args: &[&str], env: &[(&str, &str)]) -> (ChildGuard, u16, String) {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "awanfs_reactor_hardening_{}_{}_{}",
        std::process::id(),
        nanos,
        args.len()
    ));
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_awanfs"));
    cmd.args([
        "--port",
        "0",
        "--listen",
        "127.0.0.1",
        "--allow-anonymous",
    ])
    .args(args)
    .env_remove("AWANFS_MAX_CONNECTIONS")
    .env("AWANFS_LOG", "off")
    .stdout(Stdio::piped())
    .stderr(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let _ = dir;
    let mut child = ChildGuard(cmd.spawn().expect("spawn awanfs"));
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut max_line = String::new();
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if line.starts_with("Max connections:") {
                max_line = line;
                continue;
            }
            if let Some(rest) = line.strip_prefix("S3 Storage Server listening on ") {
                // Format: "http://127.0.0.1:PORT (profile ...)" or "https://..."
                let addr_part = rest.split_whitespace().next().unwrap_or("");
                if let Some((_, port_s)) = addr_part.rsplit_once(':') {
                    if let Ok(port) = port_s.parse::<u16>() {
                        let _ = tx.send((port, max_line));
                        return;
                    }
                }
            }
        }
    });
    let (port, max_line) = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("server should print Max connections and listening port");
    (child, port, max_line)
}

async fn read_http_head(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut tmp))
            .await
            .expect("timed out waiting for HTTP response")
            .expect("read HTTP response");
        assert!(n > 0, "unexpected EOF waiting for HTTP response");
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            return String::from_utf8_lossy(&buf[..pos + 4]).into_owned();
        }
    }
}

#[tokio::test]
async fn max_connections_two_holds_third_until_one_closes() {
    let (_child, port, max_line) = spawn_awanfs(&["--max-connections", "2"], &[]);
    assert_eq!(max_line, "Max connections: 2");
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Establish two active idle keep-alive connections (each has completed one request so we know
    // both permits are held inside the connection tasks).
    let mut c1 = TcpStream::connect(addr).await.unwrap();
    c1.write_all(b"GET /_awanfs/health/live HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    assert!(read_http_head(&mut c1).await.starts_with("HTTP/1.1 200 OK"));

    let mut c2 = TcpStream::connect(addr).await.unwrap();
    c2.write_all(b"GET /_awanfs/health/live HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    assert!(read_http_head(&mut c2).await.starts_with("HTTP/1.1 200 OK"));

    // Third connection waits in the kernel accept queue and gets no response while c1 and c2 are open.
    let mut c3 = TcpStream::connect(addr).await.unwrap();
    c3.write_all(b"GET /_awanfs/health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let blocked = tokio::time::timeout(Duration::from_millis(250), c3.read(&mut buf)).await;
    assert!(
        blocked.is_err(),
        "third connection must not get a response while 2 connections hold the limit"
    );

    // Closing c1 frees one permit and lets c3 complete immediately.
    drop(c1);
    assert!(
        read_http_head(&mut c3).await.starts_with("HTTP/1.1 200 OK"),
        "third connection must succeed after one idle connection closes"
    );
    drop(c2);
}

#[tokio::test]
async fn max_connections_failed_tls_handshakes_do_not_leak_permits() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let cert = fixtures.join("cert.pem");
    let key = fixtures.join("key.pem");
    let cert_s = cert.to_str().unwrap();
    let key_s = key.to_str().unwrap();

    let (_child, port, max_line) = spawn_awanfs(
        &["--cert", cert_s, "--key", key_s, "--max-connections", "2"],
        &[],
    );
    assert_eq!(max_line, "Max connections: 2");
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Send plain HTTP bytes twice to fail the TLS handshake twice; neither may leak a permit.
    for _ in 0..2 {
        let mut plain = TcpStream::connect(addr).await.unwrap();
        plain.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();
        let mut sink = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(2), plain.read_to_end(&mut sink))
            .await
            .expect("failed TLS handshake should close promptly");
    }

    let pem = std::fs::read(&cert).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&pem).unwrap())
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let resp = client
        .get(format!("https://localhost:{}/", port))
        .send()
        .await
        .expect("good TLS connection after two failed TLS handshakes should succeed");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn max_connections_zero_allows_fifty_idle_connections() {
    let (_child, port, max_line) = spawn_awanfs(&["--max-connections", "0"], &[]);
    assert_eq!(max_line, "Max connections: no limit");
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let mut conns = Vec::with_capacity(50);
    for i in 0..50 {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /_awanfs/health/live HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let head = read_http_head(&mut stream).await;
        assert!(
            head.starts_with("HTTP/1.1 200 OK"),
            "connection {} failed with --max-connections 0: {}",
            i,
            head
        );
        conns.push(stream);
    }
    assert_eq!(conns.len(), 50);

    // Env variable works as well when flag is absent.
    let (_child_env, _port_env, max_env_line) = spawn_awanfs(&[], &[("AWANFS_MAX_CONNECTIONS", "7")]);
    assert_eq!(max_env_line, "Max connections: 7");
}

