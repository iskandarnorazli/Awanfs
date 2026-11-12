//! Native TLS with rustls (#30, owner decision 18): `--cert` and `--key` (PEM), ALPN h2 and
//! http/1.1, and the certificate reloaded without a restart when its files change (checked
//! every 60 s) or on SIGHUP (Unix). A reload that fails keeps the old certificate.

use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

/// Loads a certificate chain and its private key from PEM files.
pub fn load_certified_key(cert: &Path, key: &Path) -> Result<CertifiedKey, String> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .map_err(|e| format!("{}: {}", cert.display(), e))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("{}: {}", cert.display(), e))?;
    if chain.is_empty() {
        return Err(format!("{}: no certificate in the file", cert.display()));
    }
    let key_der = PrivateKeyDer::from_pem_file(key).map_err(|e| format!("{}: {}", key.display(), e))?;
    let signing = rustls::crypto::ring::sign::any_supported_type(&key_der).map_err(|e| format!("{}: {}", key.display(), e))?;
    Ok(CertifiedKey::new(chain, signing))
}

/// The certificate the server presents; `reload` swaps it for new handshakes.
#[derive(Debug)]
pub struct ReloadingCert {
    cert_path: PathBuf,
    key_path: PathBuf,
    current: RwLock<Arc<CertifiedKey>>,
    stamp: RwLock<Option<(SystemTime, SystemTime)>>,
}

fn mtime(p: &Path) -> Option<SystemTime> {
    std::fs::metadata(p).and_then(|m| m.modified()).ok()
}

impl ReloadingCert {
    pub fn open(cert: &Path, key: &Path) -> Result<Arc<Self>, String> {
        let ck = load_certified_key(cert, key)?;
        Ok(Arc::new(ReloadingCert {
            cert_path: cert.to_path_buf(),
            key_path: key.to_path_buf(),
            current: RwLock::new(Arc::new(ck)),
            stamp: RwLock::new(mtime(cert).zip(mtime(key))),
        }))
    }

    /// Loads the files again. `Ok(true)` when the new certificate is in use.
    pub fn reload(&self) -> Result<bool, String> {
        let ck = load_certified_key(&self.cert_path, &self.key_path)?;
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(ck);
        *self.stamp.write().unwrap_or_else(|e| e.into_inner()) = mtime(&self.cert_path).zip(mtime(&self.key_path));
        Ok(true)
    }

    /// Reloads when either file's modification time changed since the last load.
    pub fn reload_if_changed(&self) -> Result<bool, String> {
        let now = mtime(&self.cert_path).zip(mtime(&self.key_path));
        if now == *self.stamp.read().unwrap_or_else(|e| e.into_inner()) {
            return Ok(false);
        }
        self.reload()
    }

    /// Checks the files every `every` and reloads on change; on Unix also on SIGHUP.
    pub fn watch(self: &Arc<Self>, every: std::time::Duration) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            #[cfg(unix)]
            let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok();
            loop {
                #[cfg(unix)]
                {
                    let forced = match hup.as_mut() {
                        Some(h) => tokio::select! {
                            _ = tokio::time::sleep(every) => false,
                            _ = h.recv() => true,
                        },
                        None => {
                            tokio::time::sleep(every).await;
                            false
                        }
                    };
                    let r = if forced { me.reload() } else { me.reload_if_changed() };
                    report(r);
                }
                #[cfg(not(unix))]
                {
                    tokio::time::sleep(every).await;
                    report(me.reload_if_changed());
                }
            }
        });
    }
}

fn report(r: Result<bool, String>) {
    match r {
        Ok(true) => eprintln!("TLS certificate reloaded"),
        Ok(false) => {}
        Err(e) => eprintln!("TLS certificate reload failed, keeping the old one: {}", e),
    }
}

impl ResolvesServerCert for ReloadingCert {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.current.read().unwrap_or_else(|e| e.into_inner())))
    }
}

/// A rustls server config presenting `cert`, with ALPN h2 and http/1.1.
pub fn server_config(cert: Arc<ReloadingCert>) -> Arc<rustls::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("ring supports the default TLS versions")
        .with_no_client_auth()
        .with_cert_resolver(cert);
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Arc::new(cfg)
}
