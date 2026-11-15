//! A GET body for objects above 4 MiB read with positioned reads, not through the volume
//! mapping (#155, step two; decision 46).
//!
//! One reader per response runs on the blocking pool. It starts on the body's first poll, so no
//! payload byte is read before the headers are sent. It reads `read_chunk()` at a time,
//! by size (owner decision 1C): from `direct_read_min` up with direct I/O, in aligned windows,
//! so the bytes stay in no cache; below it through the page cache, so a hot object is read
//! again from memory. Neither way puts the bytes in the RSS, as reading through the volume
//! mapping did. It hands each
//! chunk over a channel of `read_ahead()` slots: the reader waits while the client is slow, and a
//! response holds at most `read_ahead() + 3` chunks: one being read, one held back until the
//! next is read (so the last is sent only once the CRC is known), the slots, one being sent.
//! Read buffers come back to a small pool when sent, so a chunk is neither allocated nor zeroed.
//! When the client goes away the receiver is dropped, the next send fails, and the reader
//! stops and frees its buffer.
//!
//! A full GET checks the payload CRC over the bytes it sends. The last chunk is held until
//! the CRC is known; on a mismatch it is not sent and the transfer is cut. A range is not
//! checked (decision 46). Every move goes through `fsm::stream_step`.

use crate::direct_io::{AlignedBuffer, SECTOR_SIZE};
use crate::fsm::{stream_step, StreamAbort, StreamEvent, StreamState};
use crate::volume::VolumeManager;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// The largest frame: tests check frames are no bigger. The size used is `read_chunk()`.
pub const READ_CHUNK: usize = 4 << 20;

/// The size of one read and one body frame: `AWAN_READ_CHUNK` (a size, at most `READ_CHUNK`),
/// or 1 MiB. Read once.
pub fn read_chunk() -> usize {
    static CHUNK: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CHUNK.get_or_init(|| {
        std::env::var("AWAN_READ_CHUNK")
            .ok()
            .and_then(|v| crate::memory::parse_size(&v))
            .map(|n| (n as usize).clamp(SECTOR_SIZE, READ_CHUNK) / SECTOR_SIZE * SECTOR_SIZE)
            .unwrap_or(1 << 20)
    })
}

/// How many chunks the reader may have read ahead of the response: `AWAN_READ_AHEAD` (1 to 8),
/// or 1. Read once.
pub fn read_ahead() -> usize {
    static AHEAD: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *AHEAD.get_or_init(|| std::env::var("AWAN_READ_AHEAD").ok().and_then(|v| v.parse().ok()).unwrap_or(1usize).clamp(1, 8))
}

/// Buffers a frame gives back when the response has sent it, so a chunk costs neither an
/// allocation nor zeroing. At most `POOL_IDLE` wait here; the rest are freed.
const POOL_IDLE: usize = 16;

fn pool() -> &'static parking_lot::Mutex<Vec<AlignedBuffer>> {
    static POOL: std::sync::OnceLock<parking_lot::Mutex<Vec<AlignedBuffer>>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| parking_lot::Mutex::new(Vec::new()))
}

fn take_buffer(len: usize) -> AlignedBuffer {
    let mut free = pool().lock();
    while let Some(b) = free.pop() {
        if b.len() == len {
            return b;
        }
    }
    drop(free);
    AlignedBuffer::new(len)
}

/// Every large GET goes through the page cache unless `AWAN_DIRECT_READ_MIN` (a size such as
/// `1GiB`) says from which size to read direct. Measured up to 256 MiB, direct reads were slower
/// at every size (`measurements/2026-11-16-large-objects-155-step2.md`).
pub const DEFAULT_DIRECT_READ_MIN: u64 = u64::MAX;

pub fn direct_read_min_from_env() -> u64 {
    std::env::var("AWAN_DIRECT_READ_MIN").ok().and_then(|v| crate::memory::parse_size(&v)).unwrap_or(DEFAULT_DIRECT_READ_MIN)
}

static READERS: AtomicUsize = AtomicUsize::new(0);

/// Readers running now, in this process. Tests wait for it to fall to 0 after a client leaves.
pub fn readers_in_flight() -> usize {
    READERS.load(Ordering::SeqCst)
}

/// What to send: `len` payload bytes from payload offset `start` of the needle at `local`.
pub struct PayloadRange {
    pub vm: Arc<VolumeManager>,
    pub local: u64,
    pub start: u64,
    pub len: u64,
    /// Read with direct I/O (aligned windows) rather than through the page cache.
    pub direct: bool,
    /// The payload CRC to check, for a range that is the whole payload.
    pub verify: Option<u32>,
    /// For the log line on a checksum mismatch.
    pub what: String,
    pub failures: Arc<AtomicU64>,
}

pub struct PreadBody {
    start: Option<(PayloadRange, u64)>,
    rx: Option<tokio::sync::mpsc::Receiver<std::io::Result<Bytes>>>,
    remaining: u64,
}

impl PreadBody {
    /// `payload_at` is where the payload starts in the volume file (`PayloadRange::open`).
    pub fn new(range: PayloadRange, payload_at: u64) -> Self {
        let remaining = range.len;
        PreadBody { start: Some((range, payload_at)), rx: None, remaining }
    }
}

impl hyper::body::Body for PreadBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        let this = &mut *self;
        if let Some((range, payload_at)) = this.start.take() {
            let (tx, rx) = tokio::sync::mpsc::channel(read_ahead());
            READERS.fetch_add(1, Ordering::SeqCst);
            tokio::task::spawn_blocking(move || {
                read_into(range, payload_at, tx);
                READERS.fetch_sub(1, Ordering::SeqCst);
            });
            this.rx = Some(rx);
        }
        let Some(rx) = this.rx.as_mut() else {
            return std::task::Poll::Ready(None);
        };
        match rx.poll_recv(cx) {
            std::task::Poll::Ready(Some(Ok(chunk))) => {
                this.remaining = this.remaining.saturating_sub(chunk.len() as u64);
                std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))))
            }
            std::task::Poll::Ready(Some(Err(e))) => {
                this.rx = None;
                std::task::Poll::Ready(Some(Err(e)))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0 && self.start.is_none()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::with_exact(self.remaining)
    }
}

/// The buffer one read lands in, handed to the response as it is: no copy. Back to the pool
/// when the response has sent it.
struct Window(Option<AlignedBuffer>);

impl AsRef<[u8]> for Window {
    fn as_ref(&self) -> &[u8] {
        self.0.as_deref().unwrap_or(&[])
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        if let Some(b) = self.0.take() {
            let mut free = pool().lock();
            if free.len() < POOL_IDLE {
                free.push(b);
            }
        }
    }
}

/// The reader: aligned windows, one chunk handed over at a time.
fn read_into(range: PayloadRange, payload_at: u64, tx: tokio::sync::mpsc::Sender<std::io::Result<Bytes>>) {
    let sector = SECTOR_SIZE as u64;
    let work = range.vm.work_counters();
    let mut state = StreamState::Opening;
    let mut crc = 0u32;
    let (begin, end) = (payload_at + range.start, payload_at + range.start + range.len);
    let mut pos = begin;
    // The chunk read last, held back until the next one is read (or the CRC is known).
    let mut held: Option<Bytes> = None;
    let step = |state: StreamState, event: StreamEvent| stream_step(state, event).unwrap_or(state);
    let chunk_len = read_chunk();
    while pos < end {
        let n = (chunk_len as u64).min(end - pos);
        // One buffer size for every chunk, so pooled buffers fit any read.
        let mut window = take_buffer(chunk_len + 2 * SECTOR_SIZE);
        let read = if range.direct {
            let lo = pos / sector * sector;
            let hi = (pos + n).div_ceil(sector) * sector;
            range.vm.read_aligned(lo, &mut window[..(hi - lo) as usize]).map(|()| (pos - lo) as usize)
        } else {
            range.vm.read_cached(pos, &mut window[..n as usize]).map(|()| 0)
        }
        .map(|at| Bytes::from_owner(Window(Some(window))).slice(at..at + n as usize));
        let chunk = match read {
            Ok(c) => c,
            Err(e) => {
                state = step(state, StreamEvent::ReadError);
                let _ = tx.blocking_send(Err(std::io::Error::other(format!("could not read the object: {:?}", e))));
                debug_assert_eq!(state, StreamState::Aborted(StreamAbort::ReadError));
                return;
            }
        };
        if range.verify.is_some() {
            crc = crc32c::crc32c_append(crc, &chunk);
            work.payload_verify(n);
        }
        pos += n;
        if let Some(prev) = held.replace(chunk) {
            if tx.blocking_send(Ok(prev)).is_err() {
                state = step(state, StreamEvent::ClientGone);
                debug_assert_eq!(state, StreamState::Aborted(StreamAbort::ClientGone));
                return;
            }
            state = step(state, StreamEvent::Chunk);
        }
    }
    let checksum_ok = range.verify.is_none_or(|want| want == crc);
    if !checksum_ok {
        range.failures.fetch_add(1, Ordering::SeqCst);
        tracing::error!(object = %range.what, expected = range.verify.unwrap_or(0), actual = crc, "payload checksum mismatch while sending; the transfer is cut");
        let _ = stream_step(state, StreamEvent::End { checksum_ok });
        let _ = tx.blocking_send(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "the object's bytes failed their checksum")));
        return;
    }
    if let Some(last) = held {
        if tx.blocking_send(Ok(last)).is_err() {
            let _ = stream_step(state, StreamEvent::ClientGone);
            return;
        }
        state = step(state, StreamEvent::Chunk);
    }
    let _ = stream_step(state, StreamEvent::End { checksum_ok });
}
