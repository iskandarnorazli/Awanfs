//! #210: today's server past the 100,000-object hot set (`s3_api.rs`, `AwanStorageEngine::new`).
//!
//!     cargo run --release --example hot_set_bench -- load   --port P --objects 1000000
//!     cargo run --release --example hot_set_bench -- verify --port P --objects 1000000
//!
//! `load` PUTs new 1 KiB objects in windows. In each window 64 threads PUT while 8 more GET keys
//! that already exist, and after it 64 threads GET a burst of existing keys. One JSON line per
//! window. `verify` GETs random keys and checks their bytes. Plain HTTP/1.1 keep-alive over std
//! sockets, anonymous, so the client adds no dependency and is not the bottleneck of a Python
//! one. Every GET's bytes are checked: a wrong answer makes the run exit 1.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const BUCKET: &str = "hotset-bench";

struct Args {
    mode: String,
    port: u16,
    objects: u64,
    window: u64,
    threads: u64,
    get_threads: u64,
    burst: u64,
    size: usize,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let mode = it.next().unwrap_or_default();
    let mut a = Args { mode, port: 0, objects: 1_000_000, window: 50_000, threads: 64, get_threads: 8, burst: 20_000, size: 1024 };
    while let Some(flag) = it.next() {
        let v = it.next().unwrap_or_else(|| panic!("{} needs a value", flag));
        match flag.as_str() {
            "--port" => a.port = v.parse().unwrap(),
            "--objects" => a.objects = v.parse().unwrap(),
            "--window" => a.window = v.parse().unwrap(),
            "--threads" => a.threads = v.parse().unwrap(),
            "--get-threads" => a.get_threads = v.parse().unwrap(),
            "--burst" => a.burst = v.parse().unwrap(),
            "--size" => a.size = v.parse().unwrap(),
            _ => panic!("unknown flag {}", flag),
        }
    }
    assert!(a.mode == "load" || a.mode == "verify", "mode is load or verify");
    assert!(a.port != 0, "--port is required");
    a
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Keys arrive in no particular order, as S3 PUTs do.
fn key(i: u64) -> String {
    format!("data/{:03}/{:016x}", i % 1000, mix(i))
}

fn body(i: u64, size: usize) -> Vec<u8> {
    let seed = mix(i ^ 0x5555);
    (0..size).map(|j| (seed.rotate_left((j % 64) as u32) as u8) ^ (j as u8)).collect()
}

struct Conn {
    port: u16,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn open(port: u16) -> Conn {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_nodelay(true).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(600))).unwrap();
        Conn { port, reader: BufReader::new(s) }
    }

    /// One request; the status and the body. Reconnects once if the server closed the
    /// connection between requests.
    fn call(&mut self, method: &str, path: &str, payload: &[u8]) -> (u16, Vec<u8>) {
        match self.try_call(method, path, payload) {
            Ok(r) => r,
            Err(_) => {
                *self = Conn::open(self.port);
                self.try_call(method, path, payload).expect("request after reconnecting")
            }
        }
    }

    fn try_call(&mut self, method: &str, path: &str, payload: &[u8]) -> std::io::Result<(u16, Vec<u8>)> {
        let head = format!("{} {} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nContent-Length: {}\r\n\r\n", method, path, self.port, payload.len());
        let mut req = head.into_bytes();
        req.extend_from_slice(payload);
        self.reader.get_mut().write_all(&req)?;
        let mut line = String::new();
        if self.reader.read_line(&mut line)? == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "closed"));
        }
        let status: u16 = line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let mut len = 0usize;
        loop {
            line.clear();
            self.reader.read_line(&mut line)?;
            let l = line.trim_end();
            if l.is_empty() {
                break;
            }
            let lower = l.to_ascii_lowercase();
            if let Some(v) = lower.strip_prefix("content-length:") {
                len = v.trim().parse().unwrap_or(0);
            }
            assert!(!lower.starts_with("transfer-encoding:"), "chunked answers are not read by this client");
        }
        let mut out = vec![0u8; len];
        self.reader.read_exact(&mut out)?;
        Ok((status, out))
    }
}

fn pct(v: &mut [u32], p: f64) -> u32 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[(((v.len() - 1) as f64) * p).round() as usize]
}

/// GETs `n` random keys below `existing` from `threads` threads; latencies in us and the count
/// of wrong answers. With `stop`, runs until it is set instead of `n` requests.
/// The threads connect before `ready` releases them, so connecting is not timed.
fn gets(a: &Args, existing: u64, n: u64, threads: u64, seed: u64, stop: Option<Arc<AtomicBool>>) -> (Vec<u32>, u64, f64) {
    let next = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(Barrier::new(threads as usize + 1));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (next, stop, ready, port, size) = (Arc::clone(&next), stop.clone(), Arc::clone(&ready), a.port, a.size);
            std::thread::spawn(move || {
                let mut c = Conn::open(port);
                ready.wait();
                let (mut lat, mut wrong) = (Vec::new(), 0u64);
                let mut r = mix(seed ^ (t + 1));
                loop {
                    match &stop {
                        Some(s) if s.load(Ordering::Relaxed) => break,
                        None if next.fetch_add(1, Ordering::Relaxed) >= n => break,
                        _ => {}
                    }
                    r = mix(r);
                    let i = r % existing;
                    let t0 = Instant::now();
                    let (status, got) = c.call("GET", &format!("/{}/{}", BUCKET, key(i)), &[]);
                    lat.push(t0.elapsed().as_micros().min(u32::MAX as u128) as u32);
                    if status != 200 || got != body(i, size) {
                        wrong += 1;
                    }
                }
                (lat, wrong)
            })
        })
        .collect();
    ready.wait();
    let t0 = Instant::now();
    let (mut lat, mut wrong) = (Vec::new(), 0);
    for h in handles {
        let (l, w) = h.join().unwrap();
        lat.extend(l);
        wrong += w;
    }
    (lat, wrong, t0.elapsed().as_secs_f64())
}

fn load(a: &Args) -> u64 {
    let mut c = Conn::open(a.port);
    let (status, _) = c.call("PUT", &format!("/{}", BUCKET), &[]);
    assert!(status == 200 || status == 409, "create bucket: {}", status);
    let mut wrong_total = 0;
    let mut start = 0u64;
    while start < a.objects {
        let end = (start + a.window).min(a.objects);
        let stop = Arc::new(AtomicBool::new(false));
        let during = (start > 0).then(|| {
            let (args, stop) = (Args { mode: String::new(), ..*a }, Arc::clone(&stop));
            std::thread::spawn(move || gets(&args, start, 0, args.get_threads, start, Some(stop)))
        });
        let next = Arc::new(AtomicU64::new(start));
        let ready = Arc::new(Barrier::new(a.threads as usize + 1));
        let handles: Vec<_> = (0..a.threads)
            .map(|_| {
                let (next, ready, port, size) = (Arc::clone(&next), Arc::clone(&ready), a.port, a.size);
                std::thread::spawn(move || {
                    let mut c = Conn::open(port);
                    ready.wait();
                    let (mut lat, mut failed) = (Vec::new(), 0u64);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= end {
                            break;
                        }
                        let b = body(i, size);
                        let t = Instant::now();
                        let (status, _) = c.call("PUT", &format!("/{}/{}", BUCKET, key(i)), &b);
                        lat.push(t.elapsed().as_micros().min(u32::MAX as u128) as u32);
                        if status != 200 {
                            failed += 1;
                        }
                    }
                    (lat, failed)
                })
            })
            .collect();
        ready.wait();
        let t0 = Instant::now();
        let (mut put_lat, mut put_failed) = (Vec::new(), 0);
        for h in handles {
            let (l, f) = h.join().unwrap();
            put_lat.extend(l);
            put_failed += f;
        }
        let put_secs = t0.elapsed().as_secs_f64();
        stop.store(true, Ordering::Relaxed);
        let (mut during_lat, during_wrong, _) = during.map(|h| h.join().unwrap()).unwrap_or_default();
        let (mut burst_lat, burst_wrong, burst_secs) = gets(a, end, a.burst, a.threads, end, None);
        let wrong = put_failed + during_wrong + burst_wrong;
        wrong_total += wrong;
        println!(
            "{{\"phase\":\"window\",\"objects\":{},\"put_ops\":{:.0},\"put_p50_us\":{},\"put_p99_us\":{},\"get_during_count\":{},\"get_during_p50_us\":{},\"get_during_p99_us\":{},\"burst_ops\":{:.0},\"burst_p50_us\":{},\"burst_p99_us\":{},\"wrong\":{}}}",
            end,
            (end - start) as f64 / put_secs,
            pct(&mut put_lat, 0.50),
            pct(&mut put_lat, 0.99),
            during_lat.len(),
            pct(&mut during_lat, 0.50),
            pct(&mut during_lat, 0.99),
            a.burst as f64 / burst_secs,
            pct(&mut burst_lat, 0.50),
            pct(&mut burst_lat, 0.99),
            wrong
        );
        std::io::stdout().flush().unwrap();
        start = end;
    }
    wrong_total
}

fn verify(a: &Args) -> u64 {
    let (mut lat, wrong, secs) = gets(a, a.objects, a.burst, a.threads, 0xABCD, None);
    println!(
        "{{\"phase\":\"verify\",\"objects\":{},\"gets\":{},\"get_ops\":{:.0},\"get_p50_us\":{},\"get_p99_us\":{},\"wrong\":{}}}",
        a.objects,
        lat.len(),
        lat.len() as f64 / secs,
        pct(&mut lat, 0.50),
        pct(&mut lat, 0.99),
        wrong
    );
    wrong
}

fn main() {
    let a = parse_args();
    let wrong = if a.mode == "load" { load(&a) } else { verify(&a) };
    if wrong > 0 {
        eprintln!("{} wrong answers", wrong);
        std::process::exit(1);
    }
}
