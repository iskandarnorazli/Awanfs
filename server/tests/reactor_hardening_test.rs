//! Reactor accept-loop hardening tests (#531).

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
