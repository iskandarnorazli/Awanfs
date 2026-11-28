//! awan-storage/server/src/reactor.rs
//! High-Performance Non-Blocking Network Reactor & Connection Loop
//! Reference: awan-storage/HYBRID_POC_SPECIFICATION.md Section 2

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as AutoConnBuilder;
use hyper::service::service_fn;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use hyper_util::server::graceful::GracefulShutdown;

use crate::s3_api::{handle_s3_request, S3ServiceState};

const INITIAL_ACCEPT_BACKOFF: Duration = Duration::from_millis(10);
const MAX_ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// Listener interface for the S3 accept loop so tests can inject transient
/// and resource-exhaustion errors ahead of real TCP connections.
pub trait AcceptListener: Send + 'static {
    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn accept(&self) -> Pin<Box<dyn Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send + '_>>;
}

impl AcceptListener for TcpListener {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        TcpListener::local_addr(self)
    }

    fn accept(&self) -> Pin<Box<dyn Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send + '_>> {
        Box::pin(TcpListener::accept(self))
    }
}

fn is_resource_accept_error(err: &io::Error) -> bool {
    if err.kind() == io::ErrorKind::OutOfMemory {
        return true;
    }
    #[cfg(unix)]
    if matches!(
        err.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
    ) {
        return true;
    }
    #[cfg(windows)]
    if matches!(err.raw_os_error(), Some(4 | 8 | 14 | 10024 | 10055)) {
        // ERROR_TOO_MANY_OPEN_FILES, ERROR_NOT_ENOUGH_MEMORY/WSA_NOT_ENOUGH_MEMORY,
        // ERROR_OUTOFMEMORY, WSAEMFILE, WSAENOBUFS.
        return true;
    }
    false
}

/// Pure backoff rule for `listener.accept()` errors: resource-exhaustion
/// errors wait 10 ms doubling up to 1 s; transient/unknown errors retry at once.
pub fn accept_backoff(prev: Option<Duration>, err: &io::Error) -> Duration {
    if !is_resource_accept_error(err) {
        return Duration::ZERO;
    }
    match prev {
        Some(d) if !d.is_zero() => d.saturating_mul(2).min(MAX_ACCEPT_BACKOFF),
        _ => INITIAL_ACCEPT_BACKOFF,
    }
}

pub struct ServerHandle {
    pub local_addr: SocketAddr,
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    /// Fires when the listener is closed and every connection has finished.
    done_rx: Option<oneshot::Receiver<()>>,
}

impl ServerHandle {
    /// Stops accepting connections and asks open ones to finish; does not wait.
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    /// Stops accepting connections, lets every request in flight finish, and returns once all
    /// connections are closed (`true`) or `timeout` passed (`false`) (#49).
    pub async fn shutdown_gracefully(mut self, timeout: std::time::Duration) -> bool {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        match self.done_rx.take() {
            Some(rx) => tokio::time::timeout(timeout, rx).await.is_ok(),
            None => true,
        }
    }
}

pub async fn start_s3_reactor(
    addr: SocketAddr,
    state: Arc<S3ServiceState>,
) -> Result<ServerHandle, Box<dyn std::error::Error + Send + Sync>> {
    start_s3_reactor_with(addr, state, None).await
}

/// `start_s3_reactor`, serving HTTPS with `tls` when it is given (#30).
pub async fn start_s3_reactor_with(
    addr: SocketAddr,
    state: Arc<S3ServiceState>,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> Result<ServerHandle, Box<dyn std::error::Error + Send + Sync>> {
    start_s3_reactor_with_max_connections(addr, state, tls, 0).await
}

/// Like `start_s3_reactor_with`, capping open client connections at `max_connections` (`0` = no limit, #532).
pub async fn start_s3_reactor_with_max_connections(
    addr: SocketAddr,
    state: Arc<S3ServiceState>,
    tls: Option<Arc<rustls::ServerConfig>>,
    max_connections: usize,
) -> Result<ServerHandle, Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    start_s3_reactor_with_listener_and_max_connections(listener, state, tls, max_connections).await
}

/// Starts the S3 reactor over an injected `AcceptListener` (#531).
pub async fn start_s3_reactor_with_listener<L: AcceptListener>(
    listener: L,
    state: Arc<S3ServiceState>,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> Result<ServerHandle, Box<dyn std::error::Error + Send + Sync>> {
    start_s3_reactor_with_listener_and_max_connections(listener, state, tls, 0).await
}

/// Starts the S3 reactor over an injected `AcceptListener`, with an optional open-connection cap (#532).
pub async fn start_s3_reactor_with_listener_and_max_connections<L: AcceptListener>(
    listener: L,
    state: Arc<S3ServiceState>,
    tls: Option<Arc<rustls::ServerConfig>>,
    max_connections: usize,
) -> Result<ServerHandle, Box<dyn std::error::Error + Send + Sync>> {
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let acceptor = tls.map(tokio_rustls::TlsAcceptor::from);
    let permits = (max_connections > 0).then(|| Arc::new(tokio::sync::Semaphore::new(max_connections)));

    let mut auto_builder = AutoConnBuilder::new(TokioExecutor::new());
    auto_builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(state.header_idle_timeout());

    tokio::spawn(async move {
        let graceful = GracefulShutdown::new();
        let mut backoff: Option<Duration> = None;
        let mut last_logged: Option<(io::ErrorKind, Option<i32>, Duration)> = None;
        loop {
            let permit = match &permits {
                Some(sem) => tokio::select! {
                    res = Arc::clone(sem).acquire_owned() => match res {
                        Ok(p) => Some(p),
                        Err(_) => break,
                    },
                    _ = &mut shutdown_rx => {
                        break;
                    }
                },
                None => None,
            };
            tokio::select! {
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, remote_addr)) => {
                            backoff = None;
                            last_logged = None;
                            let _ = stream.set_nodelay(true);
                            let state_clone = Arc::clone(&state);
                            let service = service_fn(move |mut req: hyper::Request<hyper::body::Incoming>| {
                                req.extensions_mut().insert(crate::s3_api::RemoteAddr(remote_addr));
                                handle_s3_request(Arc::clone(&state_clone), req)
                            });
                            let builder = auto_builder.clone();
                            let watcher = graceful.watcher();
                            let acceptor = acceptor.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                match acceptor {
                                    Some(acc) => {
                                        // A failed handshake (untrusted client, plain HTTP on the
                                        // TLS port) closes just that connection.
                                        let handshake = tokio::time::timeout(std::time::Duration::from_secs(10), acc.accept(stream));
                                        let Ok(Ok(tls_stream)) = handshake.await else { return };
                                        let conn = builder.serve_connection_with_upgrades(TokioIo::new(tls_stream), service).into_owned();
                                        let _ = watcher.watch(conn).await;
                                    }
                                    None => {
                                        let conn = builder.serve_connection_with_upgrades(TokioIo::new(stream), service).into_owned();
                                        let _ = watcher.watch(conn).await;
                                    }
                                }
                            });
                        }
                        Err(err) => {
                            drop(permit);
                            let wait = accept_backoff(backoff, &err);
                            let sig = (err.kind(), err.raw_os_error(), wait);
                            if last_logged.as_ref() != Some(&sig) {
                                if wait.is_zero() {
                                    eprintln!("TCP accept error: {:?}", err);
                                } else {
                                    eprintln!("TCP accept error (backing off {:?}): {:?}", wait, err);
                                }
                                last_logged = Some(sig);
                            }
                            if !wait.is_zero() {
                                backoff = Some(wait);
                                tokio::select! {
                                    _ = tokio::time::sleep(wait) => {}
                                    _ = &mut shutdown_rx => {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    drop(permit);
                    break;
                }
            }
        }
        drop(listener);
        graceful.shutdown().await;
        let _ = done_tx.send(());
    });

    Ok(ServerHandle {
        local_addr,
        shutdown_tx: Some(shutdown_tx),
        done_rx: Some(done_rx),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn accepted_socket_enables_tcp_nodelay() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client_fut = tokio::net::TcpStream::connect(addr);
        let (accept_res, client_res) = tokio::join!(listener.accept(), client_fut);
        let (stream, _) = accept_res.unwrap();
        let _client = client_res.unwrap();
        let _ = stream.set_nodelay(true);
        assert_eq!(stream.nodelay().unwrap(), true);
    }

    #[test]
    fn accept_backoff_doubles_resource_errors_up_to_one_second() {
        let mut resource_errors = vec![io::Error::from(io::ErrorKind::OutOfMemory)];
        #[cfg(unix)]
        {
            for code in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
                resource_errors.push(io::Error::from_raw_os_error(code));
            }
        }
        #[cfg(windows)]
        {
            for code in [4, 8, 14, 10024, 10055] {
                resource_errors.push(io::Error::from_raw_os_error(code));
            }
        }

        for err in &resource_errors {
            let mut prev = None;
            for ms in [10, 20, 40, 80, 160, 320, 640, 1000, 1000] {
                let d = accept_backoff(prev, err);
                assert_eq!(d, Duration::from_millis(ms), "err={:?} prev={:?}", err, prev);
                prev = Some(d);
            }
        }
    }

    #[test]
    fn accept_backoff_returns_zero_for_transient_and_unknown_errors() {
        for err in [
            io::Error::from(io::ErrorKind::ConnectionAborted),
            io::Error::from(io::ErrorKind::ConnectionReset),
            io::Error::from(io::ErrorKind::Interrupted),
            io::Error::from(io::ErrorKind::PermissionDenied),
            io::Error::other("unknown accept error"),
        ] {
            assert_eq!(accept_backoff(None, &err), Duration::ZERO);
            assert_eq!(
                accept_backoff(Some(Duration::from_millis(40)), &err),
                Duration::ZERO
            );
        }
    }
}
