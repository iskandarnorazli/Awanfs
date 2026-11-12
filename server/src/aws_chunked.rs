//! awan-storage/server/src/aws_chunked.rs
//! STREAMING-AWS4-HMAC-SHA256-PAYLOAD (aws-chunked) Framing Decoder
//! Reference: awan-storage/API_SPECIFICATION.md Section 3.2

use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};

static FAULT_TAMPER_CHUNK_SIGNATURE: AtomicBool = AtomicBool::new(false);

pub fn set_fault_tamper_chunk_signature(tamper: bool) {
    FAULT_TAMPER_CHUNK_SIGNATURE.store(tamper, Ordering::SeqCst);
}

/// Decodes an `aws-chunked` body: `<hex size>[;chunk-signature=<sig>]\r\n<data>\r\n` ...
/// ending with a zero-size chunk (trailers after it are ignored). Framing must be exact: a
/// bad chunk header, missing CRLF, or a body that ends before the zero-size chunk is an error.
/// Signatures are not checked here; see `decode_aws_chunked_checked`.
pub fn decode_aws_chunked(framed_data: &[u8]) -> Result<Bytes, &'static str> {
    decode_aws_chunked_checked(framed_data, |_, _| true)
}

/// Like `decode_aws_chunked`, and calls `check(chunk data, its chunk-signature)` for every
/// chunk, the final zero-size one included, in order. A `false` stops decoding with
/// `SignatureDoesNotMatch`.
pub fn decode_aws_chunked_checked(
    framed_data: &[u8],
    mut check: impl FnMut(&[u8], Option<&str>) -> bool,
) -> Result<Bytes, &'static str> {
    if FAULT_TAMPER_CHUNK_SIGNATURE.load(Ordering::Relaxed) {
        return Err("SignatureDoesNotMatch");
    }

    let mut decoded = Vec::with_capacity(framed_data.len());
    let mut cursor = framed_data;

    loop {
        let crlf_idx = cursor.windows(2).position(|w| w == b"\r\n").ok_or("IncompleteBody")?;
        let header_line = std::str::from_utf8(&cursor[..crlf_idx]).map_err(|_| "MalformedChunkHeader")?;
        let mut fields = header_line.split(';');
        let hex_size_str = fields.next().unwrap_or("").trim();
        let signature = fields.find_map(|f| f.trim().strip_prefix("chunk-signature="));
        if hex_size_str.is_empty() || hex_size_str.len() > 16 || !hex_size_str.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("MalformedChunkHeader");
        }
        let chunk_size = usize::from_str_radix(hex_size_str, 16).map_err(|_| "MalformedChunkHeader")?;
        cursor = &cursor[crlf_idx + 2..];
        if chunk_size == 0 {
            if !check(&[], signature) {
                return Err("SignatureDoesNotMatch");
            }
            break;
        }
        if cursor.len() < chunk_size {
            return Err("IncompleteBody");
        }
        if !check(&cursor[..chunk_size], signature) {
            return Err("SignatureDoesNotMatch");
        }
        decoded.extend_from_slice(&cursor[..chunk_size]);
        cursor = &cursor[chunk_size..];
        cursor = cursor.strip_prefix(b"\r\n").ok_or("MalformedChunkHeader")?;
    }

    Ok(Bytes::from(decoded))
}
