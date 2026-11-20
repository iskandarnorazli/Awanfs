//! #320: bare `std::net::TcpListener` and `tokio::net::TcpListener` HTTP/1.1 probes on `127.0.0.1`.
//!
//!     cargo run --release --example bare_listeners -- --mode std --port 9001
//!     cargo run --release --example bare_listeners -- --mode tokio --port 9002

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader};

type Store = Arc<RwLock<HashMap<String, Arc<Vec<u8>>>>>;

const PUT_OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
const NOT_FOUND: &[u8] = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
const BAD_REQUEST: &[u8] = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
const METHOD_NOT_ALLOWED: &[u8] = b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
const CONTINUE_100: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Std,
    Tokio,
}

struct Args {
    mode: Mode,
    port: u16,
}

struct RequestHead {
    method: String,
    path: String,
    content_length: usize,
    expect_continue: bool,
    close: bool,
}

impl RequestHead {
    fn from_lines(lines: &[String]) -> Option<Self> {
        let first = lines.first()?.trim_end_matches(['\r', '\n']);
        let mut parts = first.split_whitespace();
        let method = parts.next()?.to_string();
        let raw_target = parts.next()?;
        let version = parts.next().unwrap_or("HTTP/1.1");
        let path = raw_target.split('?').next().unwrap_or(raw_target).to_string();

        let mut content_length = 0usize;
        let mut expect_continue = false;
        let mut close = version == "HTTP/1.0";

        for line in &lines[1..] {
            let trimmed = line.trim_end_matches(['\r', '\n']);
            let Some((name, value)) = trimmed.split_once(':') else {
                continue;
            };
            let val = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = val.parse().ok()?;
            } else if name.eq_ignore_ascii_case("connection") {
                if val.eq_ignore_ascii_case("close") {
                    close = true;
                } else if val.eq_ignore_ascii_case("keep-alive") {
                    close = false;
                }
            } else if name.eq_ignore_ascii_case("expect") && val.eq_ignore_ascii_case("100-continue")
            {
                expect_continue = true;
            }
        }

        Some(Self {
            method,
            path,
            content_length,
            expect_continue,
            close,
        })
    }
}

fn parse_args() -> Result<Args, String> {
    let mut mode: Option<Mode> = None;
    let mut port: u16 = 0;
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("Usage: bare_listeners --mode <std|tokio> [--port <PORT>]");
                std::process::exit(0);
            }
            "--mode" => {
                let val = it.next().ok_or("--mode requires std or tokio")?;
                mode = Some(match val.as_str() {
                    "std" => Mode::Std,
                    "tokio" => Mode::Tokio,
                    _ => return Err(format!("--mode must be std or tokio, got {:?}", val)),
                });
            }
            "--port" => {
                let val = it.next().ok_or("--port requires a port number")?;
                port = val
                    .parse::<u16>()
                    .map_err(|e| format!("invalid --port {:?}: {}", val, e))?;
            }
            other => return Err(format!("unknown flag {}", other)),
        }
    }
    let mode = mode.ok_or("--mode <std|tokio> is required")?;
    Ok(Args { mode, port })
}

fn store_put(store: &Store, path: &str, body: &[u8]) {
    // Prebuild header + body in one buffer so GET sends it in one write_all without Nagle stalls.
    let mut resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    resp.extend_from_slice(body);
    store
        .write()
        .expect("store lock")
        .insert(path.to_string(), Arc::new(resp));
}

fn lookup_get(store: &Store, path: &str) -> Option<Arc<Vec<u8>>> {
    store.read().expect("store lock").get(path).cloned()
}

fn finish_request(head: &RequestHead, body: &[u8], store: &Store) -> Arc<Vec<u8>> {
    match head.method.as_str() {
        "PUT" => {
            store_put(store, &head.path, body);
            Arc::new(PUT_OK.to_vec())
        }
        "GET" => lookup_get(store, &head.path).unwrap_or_else(|| Arc::new(NOT_FOUND.to_vec())),
        _ => Arc::new(METHOD_NOT_ALLOWED.to_vec()),
    }
}

fn announce_listening(addr: SocketAddr) {
    println!("listening on {}", addr);
    let _ = std::io::stdout().flush();
}

fn handle_std_conn(stream: std::net::TcpStream, store: &Store) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let mut lines = Vec::new();
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                return Ok(());
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            lines.push(line);
        }
        let Some(head) = RequestHead::from_lines(&lines) else {
            reader.get_mut().write_all(BAD_REQUEST)?;
            reader.get_mut().flush()?;
            return Ok(());
        };
        if head.expect_continue {
            reader.get_mut().write_all(CONTINUE_100)?;
            reader.get_mut().flush()?;
        }
        let mut body = vec![0u8; head.content_length];
        if head.content_length > 0 {
            reader.read_exact(&mut body)?;
        }
        let resp = finish_request(&head, &body, store);
        reader.get_mut().write_all(&resp)?;
        reader.get_mut().flush()?;
        if head.close {
            return Ok(());
        }
    }
}

fn run_std(addr: SocketAddr) -> std::io::Result<()> {
    let listener = std::net::TcpListener::bind(addr)?;
    announce_listening(listener.local_addr()?);
    let store: Store = Arc::new(RwLock::new(HashMap::new()));
    for accepted in listener.incoming() {
        match accepted {
            Ok(stream) => {
                let store = Arc::clone(&store);
                std::thread::spawn(move || {
                    let _ = handle_std_conn(stream, &store);
                });
            }
            Err(err) => {
                eprintln!("std accept error: {:?}", err);
                return Err(err);
            }
        }
    }
    Ok(())
}

async fn handle_tokio_conn(stream: tokio::net::TcpStream, store: &Store) -> std::io::Result<()> {
    let mut reader = TokioBufReader::new(stream);
    loop {
        let mut lines = Vec::new();
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                return Ok(());
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            lines.push(line);
        }
        let Some(head) = RequestHead::from_lines(&lines) else {
            reader.get_mut().write_all(BAD_REQUEST).await?;
            reader.get_mut().flush().await?;
            return Ok(());
        };
        if head.expect_continue {
            reader.get_mut().write_all(CONTINUE_100).await?;
            reader.get_mut().flush().await?;
        }
        let mut body = vec![0u8; head.content_length];
        if head.content_length > 0 {
            reader.read_exact(&mut body).await?;
        }
        let resp = finish_request(&head, &body, store);
        reader.get_mut().write_all(&resp).await?;
        reader.get_mut().flush().await?;
        if head.close {
            return Ok(());
        }
    }
}

fn run_tokio(addr: SocketAddr) -> std::io::Result<()> {
    // Build the Tokio runtime only in `--mode tokio` so `--mode std` stays pure std.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        announce_listening(listener.local_addr()?);
        let store: Store = Arc::new(RwLock::new(HashMap::new()));
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let store = Arc::clone(&store);
                    tokio::spawn(async move {
                        let _ = handle_tokio_conn(stream, &store).await;
                    });
                }
                Err(err) => {
                    eprintln!("tokio accept error: {:?}", err);
                    return Err(err);
                }
            }
        }
    })
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(2);
        }
    };
    let addr: SocketAddr = format!("127.0.0.1:{}", args.port)
        .parse()
        .expect("valid loopback addr");
    let res = match args.mode {
        Mode::Std => run_std(addr),
        Mode::Tokio => run_tokio(addr),
    };
    if let Err(e) = res {
        eprintln!("listener failed: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn roundtrip(port: u16, payload: &[u8]) {
        // Probe-only connection (0 bytes) like wait_port must not disturb later requests.
        drop(TcpStream::connect(("127.0.0.1", port)).unwrap());

        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let put_req = format!(
            "PUT /probe/connect-probe HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: {}\r\n\r\n",
            payload.len()
        );
        s.write_all(put_req.as_bytes()).unwrap();
        s.write_all(payload).unwrap();
        s.flush().unwrap();

        let mut put_resp = [0u8; 64];
        let mut got_head = Vec::new();
        loop {
            let n = s.read(&mut put_resp).unwrap();
            assert!(n > 0);
            got_head.extend_from_slice(&put_resp[..n]);
            if got_head.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        assert!(String::from_utf8_lossy(&got_head).starts_with("HTTP/1.1 200 OK"));

        let get_req = format!(
            "GET /probe/connect-probe HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
        );
        s.write_all(get_req.as_bytes()).unwrap();
        s.flush().unwrap();
        let mut resp = Vec::new();
        s.read_to_end(&mut resp).unwrap();
        let sep = resp.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        assert!(String::from_utf8_lossy(&resp[..sep]).starts_with("HTTP/1.1 200 OK"));
        assert_eq!(&resp[sep..], payload);
    }

    #[test]
    fn std_listener_serves_put_and_get() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let store: Store = Arc::new(RwLock::new(HashMap::new()));
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let s = Arc::clone(&store);
                std::thread::spawn(move || {
                    let _ = handle_std_conn(stream, &s);
                });
            }
        });
        let payload: Vec<u8> = (0..65536).map(|i| (i % 251) as u8).collect();
        roundtrip(port, &payload);
    }

    #[test]
    fn tokio_listener_serves_put_and_get() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                tx.send(listener.local_addr().unwrap().port()).unwrap();
                let store: Store = Arc::new(RwLock::new(HashMap::new()));
                loop {
                    if let Ok((stream, _)) = listener.accept().await {
                        let s = Arc::clone(&store);
                        tokio::spawn(async move {
                            let _ = handle_tokio_conn(stream, &s).await;
                        });
                    }
                }
            });
        });
        let port = rx.recv().unwrap();
        let payload: Vec<u8> = (0..65536).map(|i| ((i * 7) % 251) as u8).collect();
        roundtrip(port, &payload);
    }
}
