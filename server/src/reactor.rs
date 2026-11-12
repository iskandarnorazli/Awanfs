//! awan-storage/server/src/reactor.rs
//! High-Performance Non-Blocking Network Reactor & Connection Loop
//! Reference: awan-storage/HYBRID_POC_SPECIFICATION.md Section 2

use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as AutoConnBuilder;
use hyper_util::rt::TokioExecutor;
use hyper::service::service_fn;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use hyper_util::server::graceful::GracefulShutdown;

use crate::s3_api::{handle_s3_request, S3ServiceState};

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
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();

    let auto_builder = AutoConnBuilder::new(TokioExecutor::new());

    tokio::spawn(async move {
        let graceful = GracefulShutdown::new();
        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, _remote_addr)) => {
                            let io = TokioIo::new(stream);
                            let state_clone = Arc::clone(&state);
                            let service = service_fn(move |req| {
                                handle_s3_request(Arc::clone(&state_clone), req)
                            });
                            let conn = auto_builder.serve_connection_with_upgrades(io, service).into_owned();
                            let conn = graceful.watch(conn);
                            tokio::spawn(async move {
                                let _ = conn.await;
                            });
                        }
                        Err(err) => {
                            eprintln!("TCP accept error: {:?}", err);
                            break;
                        }
                    }
                }
                _ = &mut shutdown_rx => {
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
