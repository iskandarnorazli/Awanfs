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

use crate::s3_api::{handle_s3_request, S3ServiceState};

pub struct ServerHandle {
    pub local_addr: SocketAddr,
    pub shutdown_tx: Option<oneshot::Sender<()>>,
}

impl ServerHandle {
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
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

    let auto_builder = AutoConnBuilder::new(TokioExecutor::new());

    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, _remote_addr)) => {
                            let io = TokioIo::new(stream);
                            let state_clone = Arc::clone(&state);
                            let builder = auto_builder.clone();

                            tokio::spawn(async move {
                                let service = service_fn(move |req| {
                                    handle_s3_request(Arc::clone(&state_clone), req)
                                });
                                let _ = builder.serve_connection_with_upgrades(io, service).await;
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
    });

    Ok(ServerHandle {
        local_addr,
        shutdown_tx: Some(shutdown_tx),
    })
}
