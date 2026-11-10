//! awan-storage/server/src/aws_chunked.rs
//! STREAMING-AWS4-HMAC-SHA256-PAYLOAD (aws-chunked) Framing Decoder
//! Reference: awan-storage/API_SPECIFICATION.md Section 3.2

use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};

static FAULT_TAMPER_CHUNK_SIGNATURE: AtomicBool = AtomicBool::new(false);

pub fn set_fault_tamper_chunk_signature(tamper: bool) {
    FAULT_TAMPER_CHUNK_SIGNATURE.store(tamper, Ordering::SeqCst);
}

pub fn decode_aws_chunked(framed_data: &[u8]) -> Result<Bytes, &'static str> {
    if FAULT_TAMPER_CHUNK_SIGNATURE.load(Ordering::Relaxed) {
        return Err("SignatureDoesNotMatch");
    }

    let mut decoded = Vec::with_capacity(framed_data.len());
    let mut cursor = framed_data;

    while !cursor.is_empty() {
        // Find CRLF at end of chunk header
        let crlf_idx = match cursor.windows(2).position(|w| w == b"\r\n") {
            Some(i) => i,
            None => {
                // If not formatted with standard CRLF, treat as unchunked raw payload
                decoded.extend_from_slice(cursor);
                break;
            }
        };

        let header_line = match std::str::from_utf8(&cursor[..crlf_idx]) {
            Ok(s) => s.trim(),
            Err(_) => return Err("MalformedChunkHeader"),
        };

        // Chunk header format: "<hex_size>[;chunk-signature=<hex_sig>]"
        let hex_size_str = header_line.split(';').next().unwrap_or("").trim();
        let chunk_size = match usize::from_str_radix(hex_size_str, 16) {
            Ok(sz) => sz,
            Err(_) => {
                // Not a hex chunk size header, fall back to literal bytes
                decoded.extend_from_slice(cursor);
                break;
            }
        };

        cursor = &cursor[crlf_idx + 2..];

        if chunk_size == 0 {
            // Final chunk: trailing CRLF or trailer headers
            break;
        }

        if cursor.len() < chunk_size {
            return Err("IncompleteBody");
        }

        decoded.extend_from_slice(&cursor[..chunk_size]);
        cursor = &cursor[chunk_size..];

        // Consume trailing CRLF after payload
        if cursor.starts_with(b"\r\n") {
            cursor = &cursor[2..];
        }
    }

    Ok(Bytes::from(decoded))
}
