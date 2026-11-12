//! awan-storage/server/src/aws_chunked.rs
//! STREAMING-AWS4-HMAC-SHA256-PAYLOAD (aws-chunked) Framing Decoder
//! Reference: awan-storage/API_SPECIFICATION.md Section 3.2

use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};

static FAULT_TAMPER_CHUNK_SIGNATURE: AtomicBool = AtomicBool::new(false);

pub fn set_fault_tamper_chunk_signature(tamper: bool) {
    FAULT_TAMPER_CHUNK_SIGNATURE.store(tamper, Ordering::SeqCst);
}

/// Longest chunk header line accepted (`<hex size>;chunk-signature=<64 hex>` is 82 bytes).
const MAX_HEADER_LINE: usize = 4096;

enum State {
    /// Collecting a chunk header line up to its CRLF.
    Header,
    /// Inside a chunk's data; bytes left.
    Data(usize),
    /// After a chunk's data: its CRLF, of which `usize` bytes were seen.
    DataEnd(usize),
    /// The zero-size chunk was read; trailers (if any) are ignored.
    Done,
}

/// An incremental `aws-chunked` decoder (#36): bytes go in as they arrive, decoded bytes come
/// out through a callback, and nothing is held but the current header line. For every chunk,
/// the final zero-size one included, `check(hex sha256 of its data, its chunk-signature)` is
/// called when the chunk ends; `false` stops with `SignatureDoesNotMatch`.
pub struct ChunkedDecoder<C: FnMut(&str, Option<&str>) -> bool> {
    state: State,
    line: Vec<u8>,
    signature: Option<String>,
    hasher: sha2::Sha256,
    check: C,
    decoded: u64,
}

impl<C: FnMut(&str, Option<&str>) -> bool> ChunkedDecoder<C> {
    pub fn new(check: C) -> Self {
        use sha2::Digest;
        ChunkedDecoder { state: State::Header, line: Vec::new(), signature: None, hasher: sha2::Sha256::new(), check, decoded: 0 }
    }

    /// Decoded bytes produced so far.
    pub fn decoded(&self) -> u64 {
        self.decoded
    }

    fn end_chunk(&mut self) -> Result<(), &'static str> {
        use sha2::Digest;
        let digest = std::mem::replace(&mut self.hasher, sha2::Sha256::new()).finalize();
        let hex: String = digest.iter().map(|b| format!("{:02x}", b)).collect();
        if (self.check)(&hex, self.signature.as_deref()) {
            Ok(())
        } else {
            Err("SignatureDoesNotMatch")
        }
    }

    /// Feeds the next bytes of the body; decoded data goes to `out`.
    pub fn push<E>(&mut self, mut input: &[u8], out: &mut impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), PushError<E>> {
        use sha2::Digest;
        while !input.is_empty() {
            match self.state {
                State::Done => return Ok(()),
                State::Header => {
                    let take = match input.iter().position(|&b| b == b'\n') {
                        Some(i) => i + 1,
                        None => input.len(),
                    };
                    self.line.extend_from_slice(&input[..take]);
                    input = &input[take..];
                    if self.line.len() > MAX_HEADER_LINE {
                        return Err(PushError::Decode("MalformedChunkHeader"));
                    }
                    if self.line.last() != Some(&b'\n') {
                        continue;
                    }
                    if self.line.len() < 2 || self.line[self.line.len() - 2] != b'\r' {
                        return Err(PushError::Decode("MalformedChunkHeader"));
                    }
                    let text = std::str::from_utf8(&self.line[..self.line.len() - 2]).map_err(|_| PushError::Decode("MalformedChunkHeader"))?;
                    let mut fields = text.split(';');
                    let size = fields.next().unwrap_or("").trim();
                    if size.is_empty() || size.len() > 16 || !size.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(PushError::Decode("MalformedChunkHeader"));
                    }
                    let n = usize::from_str_radix(size, 16).map_err(|_| PushError::Decode("MalformedChunkHeader"))?;
                    self.signature = fields.find_map(|f| f.trim().strip_prefix("chunk-signature=")).map(String::from);
                    self.line.clear();
                    if n == 0 {
                        self.end_chunk().map_err(PushError::Decode)?;
                        self.state = State::Done;
                    } else {
                        self.state = State::Data(n);
                    }
                }
                State::Data(left) => {
                    let n = left.min(input.len());
                    self.hasher.update(&input[..n]);
                    out(&input[..n]).map_err(PushError::Sink)?;
                    self.decoded += n as u64;
                    input = &input[n..];
                    self.state = if n == left { State::DataEnd(0) } else { State::Data(left - n) };
                    if n == left {
                        self.end_chunk().map_err(PushError::Decode)?;
                    }
                }
                State::DataEnd(seen) => {
                    let want = if seen == 0 { b'\r' } else { b'\n' };
                    if input[0] != want {
                        return Err(PushError::Decode("MalformedChunkHeader"));
                    }
                    input = &input[1..];
                    self.state = if seen == 0 { State::DataEnd(1) } else { State::Header };
                }
            }
        }
        Ok(())
    }

    /// The body ended: it must have reached the zero-size chunk.
    pub fn finish(&self) -> Result<(), &'static str> {
        match self.state {
            State::Done => Ok(()),
            _ => Err("IncompleteBody"),
        }
    }
}

/// Why `ChunkedDecoder::push` stopped: the framing or a signature, or the sink of decoded
/// bytes.
#[derive(Debug)]
pub enum PushError<E> {
    Decode(&'static str),
    Sink(E),
}

/// Decodes a whole `aws-chunked` body: `<hex size>[;chunk-signature=<sig>]\r\n<data>\r\n` ...
/// ending with a zero-size chunk (trailers after it are ignored). Framing must be exact: a
/// bad chunk header, missing CRLF, or a body that ends before the zero-size chunk is an error.
/// Signatures are not checked here; see `decode_aws_chunked_checked`.
pub fn decode_aws_chunked(framed_data: &[u8]) -> Result<Bytes, &'static str> {
    decode_aws_chunked_checked(framed_data, |_, _| true)
}

/// Like `decode_aws_chunked`, and calls `check(hex sha256 of the chunk data, its
/// chunk-signature)` for every chunk, the final zero-size one included, in order. A `false`
/// stops decoding with `SignatureDoesNotMatch`. Same decoder as the streaming path.
pub fn decode_aws_chunked_checked(framed_data: &[u8], check: impl FnMut(&str, Option<&str>) -> bool) -> Result<Bytes, &'static str> {
    if FAULT_TAMPER_CHUNK_SIGNATURE.load(Ordering::Relaxed) {
        return Err("SignatureDoesNotMatch");
    }
    let mut decoded = Vec::with_capacity(framed_data.len());
    let mut dec = ChunkedDecoder::new(check);
    dec.push(framed_data, &mut |d: &[u8]| -> Result<(), ()> {
        decoded.extend_from_slice(d);
        Ok(())
    })
    .map_err(|e| match e {
        PushError::Decode(m) => m,
        PushError::Sink(()) => "InternalError",
    })?;
    dec.finish()?;
    Ok(Bytes::from(decoded))
}
