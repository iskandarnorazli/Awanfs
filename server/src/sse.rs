//! Server-Side Encryption (SSE, #121): AES-256-GCM chunk framing (`AWE1`), key wrapping/derivation,
//! header parsing, and constant-time customer key verification.
//!
//! Security properties (documented in `SECURITY.md` and `FORMAT.md`):
//! - Per-object 256-bit DEK derived via `HKDF-SHA256(salt = key_salt, ikm = customer_key, info = "awanfs-sse-c-dek-v1")`.
//! - 64 KiB plaintext chunks with per-chunk 16-byte GCM authentication tag (`65,552` bytes stored per full chunk).
//! - 12-byte GCM nonce = `nonce_prefix[4] || part_number[4 BE] || chunk_index[4 BE]`.
//! - 13-byte GCM AAD = `"AWE1" || part_number[4 BE] || chunk_index[4 BE] || is_last_chunk[1]`.
//! - Customer key bytes are never stored, logged, traced, or formatted in `Debug`; buffers are zeroized on drop.

use bytes::Bytes;
use hyper::HeaderMap;
use md5::{Digest, Md5};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::hkdf::{KeyType, Salt, HKDF_SHA256};
use ring::hmac::{self, HMAC_SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashSet;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{compiler_fence, Ordering};

use crate::bucket_config::{base64_decode, base64_encode};
use crate::stream_read::ChunkTransform;

pub const AWE1_MAGIC: [u8; 4] = *b"AWE1";
pub const AWE1_HEADER_LEN: usize = 16;
pub const AWE1_CHUNK_PLAIN: usize = 64 * 1024;
pub const AWE1_TAG_LEN: usize = 16;
pub const AWE1_CHUNK_STORED: usize = AWE1_CHUNK_PLAIN + AWE1_TAG_LEN;

pub const SSE_ATTR_VERSION: u8 = 1;
pub const SSE_MODE_C: u8 = 1;
pub const SSE_CIPHER_AES256_GCM: u8 = 1;
pub const SSE_FLAG_MULTIPART: u8 = 1;
pub const SSE_C_ATTR_LEN: usize = 80;

const HKDF_DEK_INFO: &[u8] = b"awanfs-sse-c-dek-v1";
const VERIFIER_DOMAIN: &[u8] = b"awanfs-sse-c-verifier-v1";
const PART_ETAG_DOMAIN: &[u8] = b"awanfs-ssec-part-etag-v1";

pub const HDR_SSE: &str = "x-amz-server-side-encryption";
pub const HDR_SSE_KMS_KEY_ID: &str = "x-amz-server-side-encryption-aws-kms-key-id";
pub const HDR_SSE_C_ALGO: &str = "x-amz-server-side-encryption-customer-algorithm";
pub const HDR_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
pub const HDR_SSE_C_KEY_MD5: &str = "x-amz-server-side-encryption-customer-key-md5";
pub const HDR_COPY_SSE_C_ALGO: &str = "x-amz-copy-source-server-side-encryption-customer-algorithm";
pub const HDR_COPY_SSE_C_KEY: &str = "x-amz-copy-source-server-side-encryption-customer-key";
pub const HDR_COPY_SSE_C_KEY_MD5: &str = "x-amz-copy-source-server-side-encryption-customer-key-md5";

fn zeroize(buf: &mut [u8]) {
    for b in buf.iter_mut() {
        // SAFETY: `b` is a valid, aligned mutable reference to a `u8` inside `buf`.
        unsafe { std::ptr::write_volatile(b, 0) };
    }
    compiler_fence(Ordering::SeqCst);
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    // SAFETY: `diff` is a stack-local `u8` valid for reads; volatile read prevents short-circuiting.
    unsafe { std::ptr::read_volatile(&diff) == 0 }
}

struct DekLen;
impl KeyType for DekLen {
    fn len(&self) -> usize {
        32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseError {
    TooLarge,
    Corrupt(&'static str),
    CryptoFailed(&'static str),
}

impl fmt::Display for SseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SseError::TooLarge => write!(f, "encrypted object exceeds 2^32 chunk limit"),
            SseError::Corrupt(why) => write!(f, "encrypted payload is corrupt: {}", why),
            SseError::CryptoFailed(why) => write!(f, "encryption operation failed: {}", why),
        }
    }
}

impl std::error::Error for SseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SseHeaderError {
    pub status: u16,
    pub code: &'static str,
    pub message: &'static str,
}

impl SseHeaderError {
    const fn bad(code: &'static str, message: &'static str) -> Self {
        Self { status: 400, code, message }
    }
}

/// Customer-supplied 256-bit SSE-C key parsed from request headers. Zeroized on drop and redacted
/// from `Debug`.
#[derive(Clone)]
pub struct SseCKey {
    raw: [u8; 32],
    pub key_md5: [u8; 16],
    pub key_md5_b64: String,
}

impl Drop for SseCKey {
    fn drop(&mut self) {
        zeroize(&mut self.raw);
    }
}

impl fmt::Debug for SseCKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SseCKey")
            .field("key_md5_b64", &self.key_md5_b64)
            .finish_non_exhaustive()
    }
}

/// Transient per-object 256-bit data encryption key derived via HKDF-SHA256. Zeroized on drop.
#[derive(Clone)]
pub struct SseDek {
    raw: [u8; 32],
}

impl Drop for SseDek {
    fn drop(&mut self) {
        zeroize(&mut self.raw);
    }
}

impl fmt::Debug for SseDek {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SseDek").finish_non_exhaustive()
    }
}

impl SseDek {
    pub fn from_raw(raw: [u8; 32]) -> Self {
        Self { raw }
    }

    pub fn aead_key(&self) -> Result<LessSafeKey, SseError> {
        let unbound = UnboundKey::new(&AES_256_GCM, &self.raw)
            .map_err(|_| SseError::CryptoFailed("invalid AES-256-GCM key"))?;
        Ok(LessSafeKey::new(unbound))
    }
}

/// Streaming keyed part-ETag computer for SSE-C multipart parts.
/// Computes `HMAC-SHA256(key = dek, msg = "awanfs-ssec-part-etag-v1" || part_number || plaintext)[0..16]`:
/// opaque relative to plaintext MD5 while remaining identical across retries of the same plaintext part.
pub struct SsecPartEtag {
    ctx: hmac::Context,
}

impl SsecPartEtag {
    pub fn new(dek: &SseDek, part_number: u32) -> Self {
        let hkey = hmac::Key::new(HMAC_SHA256, &dek.raw);
        let mut ctx = hmac::Context::with_key(&hkey);
        ctx.update(PART_ETAG_DOMAIN);
        ctx.update(&part_number.to_be_bytes());
        Self { ctx }
    }

    pub fn update(&mut self, plaintext: &[u8]) {
        self.ctx.update(plaintext);
    }

    pub fn finish(self) -> [u8; 16] {
        let tag = self.ctx.sign();
        let mut out = [0u8; 16];
        out.copy_from_slice(&tag.as_ref()[..16]);
        out
    }
}

pub fn ssec_part_etag(dek: &SseDek, part_number: u32, plaintext: &[u8]) -> [u8; 16] {
    let mut h = SsecPartEtag::new(dek, part_number);
    h.update(plaintext);
    h.finish()
}

/// Persisted encryption metadata stored under `TAG_SSE` (`0x0050`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SseAttr {
    SseC {
        multipart: bool,
        nonce_prefix: [u8; 4],
        plaintext_size: u64,
        key_md5: [u8; 16],
        key_salt: [u8; 16],
        verifier_mac: [u8; 32],
    },
}

impl SseAttr {
    pub fn new_ssec(key: &SseCKey, multipart: bool, plaintext_size: u64) -> Result<Self, SseError> {
        let nonce_prefix = random_bytes::<4>()?;
        let key_salt = random_bytes::<16>()?;
        let verifier_mac = compute_ssec_verifier(key, &key_salt);
        Ok(Self::SseC {
            multipart,
            nonce_prefix,
            plaintext_size,
            key_md5: key.key_md5,
            key_salt,
            verifier_mac,
        })
    }

    pub fn is_multipart(&self) -> bool {
        match self {
            SseAttr::SseC { multipart, .. } => *multipart,
        }
    }

    pub fn nonce_prefix(&self) -> [u8; 4] {
        match self {
            SseAttr::SseC { nonce_prefix, .. } => *nonce_prefix,
        }
    }

    pub fn plaintext_size(&self) -> u64 {
        match self {
            SseAttr::SseC { plaintext_size, .. } => *plaintext_size,
        }
    }

    pub fn with_plaintext_size(&self, plaintext_size: u64) -> Self {
        match self {
            SseAttr::SseC {
                multipart,
                nonce_prefix,
                key_md5,
                key_salt,
                verifier_mac,
                ..
            } => SseAttr::SseC {
                multipart: *multipart,
                nonce_prefix: *nonce_prefix,
                plaintext_size,
                key_md5: *key_md5,
                key_salt: *key_salt,
                verifier_mac: *verifier_mac,
            },
        }
    }

    /// Verifies both `key_md5` and salted `HMAC-SHA256` `verifier_mac` in constant time.
    pub fn verify_ssec_key(&self, key: &SseCKey) -> bool {
        let SseAttr::SseC {
            key_md5,
            key_salt,
            verifier_mac,
            ..
        } = self;
        let expected_mac = compute_ssec_verifier(key, key_salt);
        let md5_ok = ct_eq(key_md5, &key.key_md5);
        let mac_ok = ct_eq(verifier_mac, &expected_mac);
        md5_ok && mac_ok
    }

    pub fn derive_dek(&self, key: &SseCKey) -> Result<SseDek, SseError> {
        match self {
            SseAttr::SseC { key_salt, .. } => derive_ssec_dek(key, key_salt),
        }
    }

    pub fn response_headers(&self) -> Vec<(&'static str, String)> {
        match self {
            SseAttr::SseC { key_md5, .. } => vec![
                (HDR_SSE_C_ALGO, "AES256".to_string()),
                (HDR_SSE_C_KEY_MD5, base64_encode(key_md5)),
            ],
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            SseAttr::SseC {
                multipart,
                nonce_prefix,
                plaintext_size,
                key_md5,
                key_salt,
                verifier_mac,
            } => {
                let mut out = Vec::with_capacity(SSE_C_ATTR_LEN);
                out.push(SSE_ATTR_VERSION);
                out.push(SSE_MODE_C);
                out.push(SSE_CIPHER_AES256_GCM);
                out.push(if *multipart { SSE_FLAG_MULTIPART } else { 0 });
                out.extend_from_slice(nonce_prefix);
                out.extend_from_slice(&plaintext_size.to_le_bytes());
                out.extend_from_slice(key_md5);
                out.extend_from_slice(key_salt);
                out.extend_from_slice(verifier_mac);
                debug_assert_eq!(out.len(), SSE_C_ATTR_LEN);
                out
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SseError> {
        if bytes.len() < 16 {
            return Err(SseError::Corrupt("TAG_SSE too short"));
        }
        if bytes[0] != SSE_ATTR_VERSION {
            return Err(SseError::Corrupt("unsupported TAG_SSE version"));
        }
        if bytes[2] != SSE_CIPHER_AES256_GCM {
            return Err(SseError::Corrupt("unsupported TAG_SSE cipher"));
        }
        let mode = bytes[1];
        let flags = bytes[3];
        if flags & !SSE_FLAG_MULTIPART != 0 {
            return Err(SseError::Corrupt("unknown TAG_SSE flags"));
        }
        let multipart = flags & SSE_FLAG_MULTIPART != 0;
        let nonce_prefix: [u8; 4] = bytes[4..8].try_into().unwrap();
        let plaintext_size = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        match mode {
            SSE_MODE_C => {
                if bytes.len() != SSE_C_ATTR_LEN {
                    return Err(SseError::Corrupt("invalid SSE-C TAG_SSE length"));
                }
                Ok(SseAttr::SseC {
                    multipart,
                    nonce_prefix,
                    plaintext_size,
                    key_md5: bytes[16..32].try_into().unwrap(),
                    key_salt: bytes[32..48].try_into().unwrap(),
                    verifier_mac: bytes[48..80].try_into().unwrap(),
                })
            }
            _ => Err(SseError::Corrupt("unsupported TAG_SSE mode")),
        }
    }

    /// Extracts logical `plaintext_size` from raw `TAG_SSE` bytes during boot recovery.
    pub fn plaintext_size_from_raw(bytes: &[u8]) -> Option<u64> {
        if bytes.len() < 16 || bytes[0] != SSE_ATTR_VERSION || bytes[1] != SSE_MODE_C {
            return None;
        }
        if bytes.len() != SSE_C_ATTR_LEN {
            return None;
        }
        Some(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
    }
}

pub fn random_bytes<const N: usize>() -> Result<[u8; N], SseError> {
    let rng = SystemRandom::new();
    let mut out = [0u8; N];
    rng.fill(&mut out)
        .map_err(|_| SseError::CryptoFailed("CSPRNG failure"))?;
    Ok(out)
}

pub fn derive_ssec_dek(key: &SseCKey, salt: &[u8; 16]) -> Result<SseDek, SseError> {
    let prk = Salt::new(HKDF_SHA256, salt).extract(&key.raw);
    let okm = prk
        .expand(&[HKDF_DEK_INFO], DekLen)
        .map_err(|_| SseError::CryptoFailed("HKDF expand failed"))?;
    let mut raw = [0u8; 32];
    okm.fill(&mut raw)
        .map_err(|_| SseError::CryptoFailed("HKDF fill failed"))?;
    Ok(SseDek { raw })
}

pub fn compute_ssec_verifier(key: &SseCKey, salt: &[u8; 16]) -> [u8; 32] {
    let hkey = hmac::Key::new(HMAC_SHA256, &key.raw);
    let mut ctx = hmac::Context::with_key(&hkey);
    ctx.update(VERIFIER_DOMAIN);
    ctx.update(salt);
    let tag = ctx.sign();
    let mut out = [0u8; 32];
    out.copy_from_slice(tag.as_ref());
    out
}

pub fn chunk_count(plaintext_len: u64) -> Result<u32, SseError> {
    if plaintext_len == 0 {
        return Ok(1);
    }
    let chunks = plaintext_len.div_ceil(AWE1_CHUNK_PLAIN as u64);
    if chunks > u32::MAX as u64 {
        return Err(SseError::TooLarge);
    }
    Ok(chunks as u32)
}

pub fn stored_len(plaintext_len: u64) -> Result<u64, SseError> {
    let chunks = chunk_count(plaintext_len)? as u64;
    let total = (AWE1_HEADER_LEN as u64)
        .checked_add(plaintext_len)
        .and_then(|v| v.checked_add(chunks * (AWE1_TAG_LEN as u64)))
        .ok_or(SseError::TooLarge)?;
    Ok(total)
}

pub fn make_nonce(prefix: [u8; 4], part_number: u32, chunk_index: u32) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[0..4].copy_from_slice(&prefix);
    out[4..8].copy_from_slice(&part_number.to_be_bytes());
    out[8..12].copy_from_slice(&chunk_index.to_be_bytes());
    out
}

pub fn make_aad(part_number: u32, chunk_index: u32, is_last: bool) -> [u8; 13] {
    let mut out = [0u8; 13];
    out[0..4].copy_from_slice(&AWE1_MAGIC);
    out[4..8].copy_from_slice(&part_number.to_be_bytes());
    out[8..12].copy_from_slice(&chunk_index.to_be_bytes());
    out[12] = u8::from(is_last);
    out
}

pub fn parse_awe1_header(bytes: &[u8]) -> Result<(u64, [u8; 4]), SseError> {
    if bytes.len() < AWE1_HEADER_LEN {
        return Err(SseError::Corrupt("truncated AWE1 header"));
    }
    if bytes[0..4] != AWE1_MAGIC {
        return Err(SseError::Corrupt("bad AWE1 magic"));
    }
    let plaintext_len = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let nonce_prefix: [u8; 4] = bytes[12..16].try_into().unwrap();
    let _ = chunk_count(plaintext_len)?;
    Ok((plaintext_len, nonce_prefix))
}

fn plain_len_of_chunk(total_plaintext_len: u64, chunk_idx: u32) -> Result<usize, SseError> {
    if total_plaintext_len == 0 {
        return if chunk_idx == 0 {
            Ok(0)
        } else {
            Err(SseError::Corrupt("chunk index out of range for empty payload"))
        };
    }
    let p_off = (chunk_idx as u64)
        .checked_mul(AWE1_CHUNK_PLAIN as u64)
        .ok_or(SseError::Corrupt("chunk offset overflow"))?;
    if p_off >= total_plaintext_len {
        return Err(SseError::Corrupt("chunk index past plaintext_len"));
    }
    Ok((total_plaintext_len - p_off).min(AWE1_CHUNK_PLAIN as u64) as usize)
}

/// Computes the byte range of the stored `AWE1` payload chunks covering plaintext `[start, start + len)`.
/// Returns `(first_chunk_idx, stored_start_rel_to_payload, stored_span_len)`.
pub fn stored_range_for_plaintext(
    total_plaintext_len: u64,
    start: u64,
    len: u64,
) -> Result<(u32, u64, u64), SseError> {
    let end = start.checked_add(len).ok_or(SseError::Corrupt("range overflow"))?;
    if end > total_plaintext_len {
        return Err(SseError::Corrupt("range past plaintext_len"));
    }
    if total_plaintext_len == 0 {
        return Ok((0, AWE1_HEADER_LEN as u64, AWE1_TAG_LEN as u64));
    }
    let first_chunk = (start / AWE1_CHUNK_PLAIN as u64) as u32;
    let last_chunk = if len == 0 {
        first_chunk
    } else {
        ((end - 1) / AWE1_CHUNK_PLAIN as u64) as u32
    };
    let stored_start = (AWE1_HEADER_LEN as u64) + (first_chunk as u64) * (AWE1_CHUNK_STORED as u64);
    let last_plain_len = plain_len_of_chunk(total_plaintext_len, last_chunk)? as u64;
    let stored_end = (AWE1_HEADER_LEN as u64)
        + (last_chunk as u64) * (AWE1_CHUNK_STORED as u64)
        + last_plain_len
        + (AWE1_TAG_LEN as u64);
    Ok((first_chunk, stored_start, stored_end - stored_start))
}

fn encrypt_chunk_in_place(
    aead: &LessSafeKey,
    nonce_prefix: [u8; 4],
    part_number: u32,
    chunk_index: u32,
    is_last: bool,
    buf: &mut Vec<u8>,
) -> Result<(), SseError> {
    let nonce = Nonce::assume_unique_for_key(make_nonce(nonce_prefix, part_number, chunk_index));
    let aad = Aad::from(make_aad(part_number, chunk_index, is_last));
    aead.seal_in_place_append_tag(nonce, aad, buf)
        .map_err(|_| SseError::CryptoFailed("AES-256-GCM seal failed"))
}

pub fn decrypt_chunk(
    aead: &LessSafeKey,
    nonce_prefix: [u8; 4],
    part_number: u32,
    chunk_index: u32,
    is_last: bool,
    ciphertext_with_tag: &[u8],
) -> Result<Vec<u8>, SseError> {
    if ciphertext_with_tag.len() < AWE1_TAG_LEN {
        return Err(SseError::Corrupt("chunk shorter than GCM tag"));
    }
    let mut work = ciphertext_with_tag.to_vec();
    let nonce = Nonce::assume_unique_for_key(make_nonce(nonce_prefix, part_number, chunk_index));
    let aad = Aad::from(make_aad(part_number, chunk_index, is_last));
    let plain = aead
        .open_in_place(nonce, aad, &mut work)
        .map_err(|_| SseError::Corrupt("AES-256-GCM authentication failed"))?;
    let plain_len = plain.len();
    work.truncate(plain_len);
    Ok(work)
}

/// Encrypts a full plaintext buffer into `AWE1` format. Returns `(ciphertext, ciphertext_md5)`.
pub fn encrypt_full(
    dek: &SseDek,
    nonce_prefix: [u8; 4],
    part_number: u32,
    plaintext: &[u8],
) -> Result<(Vec<u8>, [u8; 16]), SseError> {
    let plaintext_len = plaintext.len() as u64;
    let total_stored = stored_len(plaintext_len)? as usize;
    let total_chunks = chunk_count(plaintext_len)?;
    let aead = dek.aead_key()?;
    let mut out = Vec::with_capacity(total_stored);
    out.extend_from_slice(&AWE1_MAGIC);
    out.extend_from_slice(&plaintext_len.to_le_bytes());
    out.extend_from_slice(&nonce_prefix);
    #[cfg(debug_assertions)]
    let mut seen_nonces: HashSet<[u8; 12]> = HashSet::new();

    if plaintext.is_empty() {
        let nonce_bytes = make_nonce(nonce_prefix, part_number, 0);
        #[cfg(debug_assertions)]
        assert!(seen_nonces.insert(nonce_bytes), "SSE nonce reuse detected");
        let mut buf = Vec::with_capacity(AWE1_TAG_LEN);
        encrypt_chunk_in_place(&aead, nonce_prefix, part_number, 0, true, &mut buf)?;
        out.extend_from_slice(&buf);
    } else {
        let mut scratch = Vec::with_capacity(AWE1_CHUNK_STORED);
        for (i, chunk) in plaintext.chunks(AWE1_CHUNK_PLAIN).enumerate() {
            let chunk_idx = i as u32;
            let nonce_bytes = make_nonce(nonce_prefix, part_number, chunk_idx);
            #[cfg(debug_assertions)]
            assert!(seen_nonces.insert(nonce_bytes), "SSE nonce reuse detected");
            let is_last = chunk_idx + 1 == total_chunks;
            scratch.clear();
            scratch.extend_from_slice(chunk);
            encrypt_chunk_in_place(&aead, nonce_prefix, part_number, chunk_idx, is_last, &mut scratch)?;
            out.extend_from_slice(&scratch);
        }
    }
    debug_assert_eq!(out.len(), total_stored);
    let md5_raw: [u8; 16] = Md5::digest(&out).into();
    Ok((out, md5_raw))
}

/// Streaming `AWE1` encryptor yielding 64 KiB ciphertext chunks while tracking ciphertext MD5.
pub struct Awe1EncryptWriter {
    aead: LessSafeKey,
    nonce_prefix: [u8; 4],
    part_number: u32,
    declared_plaintext_len: u64,
    total_chunks: u32,
    next_chunk_idx: u32,
    plaintext_fed: u64,
    header_written: bool,
    buf: Vec<u8>,
    scratch: Vec<u8>,
    md5: Md5,
    #[cfg(debug_assertions)]
    seen_nonces: HashSet<[u8; 12]>,
}

impl Awe1EncryptWriter {
    pub fn new(
        dek: &SseDek,
        nonce_prefix: [u8; 4],
        part_number: u32,
        declared_plaintext_len: u64,
    ) -> Result<Self, SseError> {
        let total_chunks = chunk_count(declared_plaintext_len)?;
        let aead = dek.aead_key()?;
        Ok(Self {
            aead,
            nonce_prefix,
            part_number,
            declared_plaintext_len,
            total_chunks,
            next_chunk_idx: 0,
            plaintext_fed: 0,
            header_written: false,
            buf: Vec::with_capacity(AWE1_CHUNK_PLAIN),
            scratch: Vec::with_capacity(AWE1_CHUNK_STORED),
            md5: Md5::new(),
            #[cfg(debug_assertions)]
            seen_nonces: HashSet::new(),
        })
    }

    fn emit_header<E, F>(&mut self, emit: &mut F) -> Result<(), Result<SseError, E>>
    where
        F: FnMut(&[u8]) -> Result<(), E>,
    {
        if self.header_written {
            return Ok(());
        }
        let mut hdr = [0u8; AWE1_HEADER_LEN];
        hdr[0..4].copy_from_slice(&AWE1_MAGIC);
        hdr[4..12].copy_from_slice(&self.declared_plaintext_len.to_le_bytes());
        hdr[12..16].copy_from_slice(&self.nonce_prefix);
        self.md5.update(hdr);
        emit(&hdr).map_err(Err)?;
        self.header_written = true;
        Ok(())
    }

    fn flush_chunk<E, F>(&mut self, is_last: bool, emit: &mut F) -> Result<(), Result<SseError, E>>
    where
        F: FnMut(&[u8]) -> Result<(), E>,
    {
        let idx = self.next_chunk_idx;
        let nonce_bytes = make_nonce(self.nonce_prefix, self.part_number, idx);
        #[cfg(debug_assertions)]
        assert!(self.seen_nonces.insert(nonce_bytes), "SSE nonce reuse detected");
        self.next_chunk_idx = idx.checked_add(1).ok_or(Ok(SseError::TooLarge))?;
        self.scratch.clear();
        self.scratch.extend_from_slice(&self.buf);
        self.buf.clear();
        encrypt_chunk_in_place(&self.aead, self.nonce_prefix, self.part_number, idx, is_last, &mut self.scratch)
            .map_err(Ok)?;
        self.md5.update(&self.scratch);
        emit(&self.scratch).map_err(Err)?;
        Ok(())
    }

    pub fn write<E, F>(&mut self, mut plaintext: &[u8], emit: &mut F) -> Result<(), Result<SseError, E>>
    where
        F: FnMut(&[u8]) -> Result<(), E>,
    {
        self.emit_header(emit)?;
        let new_total = self
            .plaintext_fed
            .checked_add(plaintext.len() as u64)
            .ok_or(Ok(SseError::TooLarge))?;
        if new_total > self.declared_plaintext_len {
            return Err(Ok(SseError::Corrupt("more plaintext bytes than declared")));
        }
        self.plaintext_fed = new_total;
        while !plaintext.is_empty() {
            let room = AWE1_CHUNK_PLAIN - self.buf.len();
            let take = room.min(plaintext.len());
            self.buf.extend_from_slice(&plaintext[..take]);
            plaintext = &plaintext[take..];
            if self.buf.len() == AWE1_CHUNK_PLAIN {
                let is_last = self.next_chunk_idx + 1 == self.total_chunks && self.plaintext_fed == self.declared_plaintext_len;
                self.flush_chunk(is_last, emit)?;
            }
        }
        Ok(())
    }

    pub fn finish<E, F>(mut self, mut emit: F) -> Result<[u8; 16], Result<SseError, E>>
    where
        F: FnMut(&[u8]) -> Result<(), E>,
    {
        self.emit_header(&mut emit)?;
        if self.plaintext_fed != self.declared_plaintext_len {
            return Err(Ok(SseError::Corrupt("fewer plaintext bytes than declared")));
        }
        if self.declared_plaintext_len == 0 || !self.buf.is_empty() {
            self.flush_chunk(true, &mut emit)?;
        }
        debug_assert_eq!(self.next_chunk_idx, self.total_chunks);
        let raw_md5: [u8; 16] = self.md5.finalize().into();
        Ok(raw_md5)
    }
}

pub fn decrypt_full(
    dek: &SseDek,
    expected_prefix: Option<[u8; 4]>,
    part_number: u32,
    expected_plaintext_len: u64,
    stored: &[u8],
) -> Result<Vec<u8>, SseError> {
    let (hdr_len, hdr_prefix) = parse_awe1_header(stored)?;
    if hdr_len != expected_plaintext_len {
        return Err(SseError::Corrupt("AWE1 plaintext_len mismatch"));
    }
    if let Some(exp) = expected_prefix {
        if hdr_prefix != exp {
            return Err(SseError::Corrupt("AWE1 nonce_prefix mismatch"));
        }
    }
    let want_stored = stored_len(expected_plaintext_len)?;
    if stored.len() as u64 != want_stored {
        return Err(SseError::Corrupt("AWE1 stored payload length mismatch"));
    }
    decrypt_range_from_chunks(
        dek,
        hdr_prefix,
        part_number,
        expected_plaintext_len,
        0,
        expected_plaintext_len,
        0,
        &stored[AWE1_HEADER_LEN..],
    )
}

/// Decrypts a contiguous sequence of stored chunks starting at `first_chunk_idx` covering
/// plaintext `[start, start + len)`. `chunk_bytes` must start at chunk `first_chunk_idx` and
/// end at the exact end of the last covered chunk.
#[allow(clippy::too_many_arguments)]
pub fn decrypt_range_from_chunks(
    dek: &SseDek,
    nonce_prefix: [u8; 4],
    part_number: u32,
    total_plaintext_len: u64,
    start: u64,
    len: u64,
    first_chunk_idx: u32,
    chunk_bytes: &[u8],
) -> Result<Vec<u8>, SseError> {
    let end_excl = start.checked_add(len).ok_or(SseError::Corrupt("range overflow"))?;
    if end_excl > total_plaintext_len {
        return Err(SseError::Corrupt("range past plaintext_len"));
    }
    let total_chunks = chunk_count(total_plaintext_len)?;
    let aead = dek.aead_key()?;
    let mut out = Vec::with_capacity(len as usize);
    let mut pos = 0usize;
    let mut idx = first_chunk_idx;

    while pos < chunk_bytes.len() {
        let plain_len = plain_len_of_chunk(total_plaintext_len, idx)?;
        let need = plain_len + AWE1_TAG_LEN;
        if pos + need > chunk_bytes.len() {
            return Err(SseError::Corrupt("truncated AWE1 chunk"));
        }
        let is_last = idx + 1 == total_chunks;
        let plain = decrypt_chunk(
            &aead,
            nonce_prefix,
            part_number,
            idx,
            is_last,
            &chunk_bytes[pos..pos + need],
        )?;
        let p_off = (idx as u64) * (AWE1_CHUNK_PLAIN as u64);
        let take_lo = start.saturating_sub(p_off) as usize;
        let take_hi = (end_excl.saturating_sub(p_off) as usize).min(plain_len);
        if take_lo < take_hi {
            out.extend_from_slice(&plain[take_lo..take_hi]);
        }
        pos += need;
        idx = idx.checked_add(1).ok_or(SseError::TooLarge)?;
    }

    if out.len() as u64 != len {
        return Err(SseError::Corrupt("decrypted range length mismatch"));
    }
    Ok(out)
}

/// Stateless `ChunkTransform` decrypting 64 KiB `AWE1` chunks streamed via `stream_read::PreadBody`.
pub struct Awe1ChunkTransform {
    dek: SseDek,
    nonce_prefix: [u8; 4],
    part_number: u32,
    total_plaintext_len: u64,
    total_chunks: u32,
    first_chunk: u32,
    range_start: u64,
    range_end_excl: u64,
}

impl Awe1ChunkTransform {
    pub fn new(
        dek: SseDek,
        nonce_prefix: [u8; 4],
        part_number: u32,
        total_plaintext_len: u64,
        range_start: u64,
        range_len: u64,
    ) -> Result<Self, SseError> {
        let end_excl = range_start
            .checked_add(range_len)
            .ok_or(SseError::Corrupt("range overflow"))?;
        if end_excl > total_plaintext_len {
            return Err(SseError::Corrupt("range past plaintext_len"));
        }
        let total_chunks = chunk_count(total_plaintext_len)?;
        let first_chunk = if total_plaintext_len == 0 {
            0
        } else {
            (range_start / AWE1_CHUNK_PLAIN as u64) as u32
        };
        Ok(Self {
            dek,
            nonce_prefix,
            part_number,
            total_plaintext_len,
            total_chunks,
            first_chunk,
            range_start,
            range_end_excl: end_excl,
        })
    }
}

impl ChunkTransform for Awe1ChunkTransform {
    fn transform(&self, logical_offset: u64, chunk: Bytes) -> Result<Bytes, String> {
        if logical_offset % (AWE1_CHUNK_STORED as u64) != 0 {
            return Err("unaligned encrypted chunk offset".to_string());
        }
        let delta_chunks = (logical_offset / (AWE1_CHUNK_STORED as u64)) as u32;
        let first_idx = self
            .first_chunk
            .checked_add(delta_chunks)
            .ok_or_else(|| "chunk index overflow".to_string())?;
        let aead = self.dek.aead_key().map_err(|e| e.to_string())?;
        let mut pos = 0usize;
        let mut idx = first_idx;
        let mut out = Vec::with_capacity(chunk.len());
        while pos < chunk.len() {
            let plain_len = plain_len_of_chunk(self.total_plaintext_len, idx).map_err(|e| e.to_string())?;
            let need = plain_len + AWE1_TAG_LEN;
            if pos + need > chunk.len() {
                return Err("truncated AWE1 chunk in stream".to_string());
            }
            let is_last = idx + 1 == self.total_chunks;
            let plain = decrypt_chunk(
                &aead,
                self.nonce_prefix,
                self.part_number,
                idx,
                is_last,
                &chunk[pos..pos + need],
            )
            .map_err(|e| e.to_string())?;
            let p_off = (idx as u64) * (AWE1_CHUNK_PLAIN as u64);
            let take_lo = self.range_start.saturating_sub(p_off) as usize;
            let take_hi = (self.range_end_excl.saturating_sub(p_off) as usize).min(plain_len);
            if take_lo < take_hi {
                out.extend_from_slice(&plain[take_lo..take_hi]);
            }
            pos += need;
            idx = idx.checked_add(1).ok_or_else(|| "chunk index overflow".to_string())?;
        }
        Ok(Bytes::from(out))
    }
}

/// Extracts `part_number` from a staged multipart part filename (`part_NNNNN.dat`), falling back
/// to `fallback_ordinal` if no staged filename matches.
pub fn part_number_from_staged_path(path: Option<&Path>, fallback_ordinal: u32) -> u32 {
    path.and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .and_then(|s| s.strip_prefix("part_"))
        .and_then(|s| s.strip_suffix(".dat"))
        .and_then(|n| n.parse::<u32>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(fallback_ordinal)
}

fn single_header_str<'a>(
    headers: &'a HeaderMap,
    name: &'static str,
) -> Result<Option<&'a str>, SseHeaderError> {
    let mut vals = headers.get_all(name).iter();
    let Some(first) = vals.next() else {
        return Ok(None);
    };
    if vals.next().is_some() {
        return Err(SseHeaderError::bad(
            "InvalidArgument",
            "Duplicate encryption header is not allowed.",
        ));
    }
    let s = first
        .to_str()
        .map_err(|_| SseHeaderError::bad("InvalidArgument", "Encryption header value is not valid UTF-8."))?;
    Ok(Some(s.trim()))
}

pub fn parse_ssec_triplet(
    headers: &HeaderMap,
    alg_hdr: &'static str,
    key_hdr: &'static str,
    md5_hdr: &'static str,
) -> Result<Option<SseCKey>, SseHeaderError> {
    let alg = single_header_str(headers, alg_hdr)?;
    let key = single_header_str(headers, key_hdr)?;
    let md5 = single_header_str(headers, md5_hdr)?;

    match (alg, key, md5) {
        (None, None, None) => Ok(None),
        (Some(_), None, _) => Err(SseHeaderError::bad(
            "InvalidArgument",
            "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption key.",
        )),
        (None, Some(_), _) | (None, None, Some(_)) => Err(SseHeaderError::bad(
            "InvalidArgument",
            "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm.",
        )),
        (Some(_), Some(_), None) => Err(SseHeaderError::bad(
            "InvalidArgument",
            "Requests specifying Server Side Encryption with Customer provided keys must provide a valid key MD5.",
        )),
        (Some(alg_s), Some(key_s), Some(md5_s)) => {
            if alg_s != "AES256" {
                return Err(SseHeaderError::bad(
                    "InvalidArgument",
                    "The encryption algorithm provided is not valid. Only AES256 is supported.",
                ));
            }
            let mut key_bytes = base64_decode(key_s).map_err(|_| {
                SseHeaderError::bad(
                    "InvalidArgument",
                    "The secret key was invalid - too short or invalid base64.",
                )
            })?;
            if key_bytes.len() != 32 {
                zeroize(&mut key_bytes);
                return Err(SseHeaderError::bad(
                    "InvalidArgument",
                    "The secret key was invalid - must be 256 bits (32 bytes).",
                ));
            }
            let md5_bytes = base64_decode(md5_s).map_err(|_| {
                zeroize(&mut key_bytes);
                SseHeaderError::bad("InvalidArgument", "The calculated MD5 hash of the key is invalid.")
            })?;
            if md5_bytes.len() != 16 {
                zeroize(&mut key_bytes);
                return Err(SseHeaderError::bad("InvalidArgument", "The calculated MD5 hash of the key is invalid."));
            }
            let actual_md5: [u8; 16] = Md5::digest(&key_bytes).into();
            if !ct_eq(&actual_md5, &md5_bytes) {
                zeroize(&mut key_bytes);
                return Err(SseHeaderError::bad(
                    "InvalidArgument",
                    "The calculated MD5 hash of the key did not match the hash that was provided.",
                ));
            }
            let mut raw = [0u8; 32];
            raw.copy_from_slice(&key_bytes);
            zeroize(&mut key_bytes);
            Ok(Some(SseCKey {
                raw,
                key_md5: actual_md5,
                key_md5_b64: md5_s.to_string(),
            }))
        }
    }
}

/// Parses SSE headers on write operations (`PutObject`, `CreateMultipartUpload`, `UploadPart`,
/// `CompleteMultipartUpload`, `CopyObject`, `UploadPartCopy`).
///
/// Validates SSE-C headers and refuses conflicting SSE header combinations (`InvalidArgument`).
/// Bare `AES256` / `aws:kms` writes stay pass-through until SSE-S3 / SSE-KMS PRs land so
/// existing `test_sse_kms_*` entries in `passing.txt` do not regress during PR 1.
pub fn parse_write_sse_headers(headers: &HeaderMap) -> Result<Option<SseCKey>, SseHeaderError> {
    let ssec = parse_ssec_triplet(headers, HDR_SSE_C_ALGO, HDR_SSE_C_KEY, HDR_SSE_C_KEY_MD5)?;
    let sse = single_header_str(headers, HDR_SSE)?;
    let kms_key_id = single_header_str(headers, HDR_SSE_KMS_KEY_ID)?;

    if ssec.is_some() && (sse.is_some() || kms_key_id.is_some()) {
        return Err(SseHeaderError::bad(
            "InvalidArgument",
            "Server-side encryption with customer-provided keys cannot be combined with SSE-S3 or SSE-KMS headers.",
        ));
    }
    if let Some(alg) = sse {
        match alg {
            "AES256" => {
                if kms_key_id.is_some() {
                    return Err(SseHeaderError::bad(
                        "InvalidArgument",
                        "x-amz-server-side-encryption-aws-kms-key-id cannot be used with AES256 encryption.",
                    ));
                }
            }
            "aws:kms" => {}
            _ => {
                return Err(SseHeaderError::bad(
                    "InvalidArgument",
                    "The encryption method specified is not supported.",
                ));
            }
        }
    }
    Ok(ssec)
}

/// Parses SSE headers on read operations (`GetObject`, `HeadObject`, `GetObjectAttributes`).
/// Refuses non-SSE-C `x-amz-server-side-encryption*` headers with 400 `InvalidArgument`.
pub fn parse_read_sse_headers(headers: &HeaderMap) -> Result<Option<SseCKey>, SseHeaderError> {
    if headers.contains_key(HDR_SSE) || headers.contains_key(HDR_SSE_KMS_KEY_ID) {
        return Err(SseHeaderError::bad(
            "InvalidArgument",
            "Server-side encryption headers are not valid on read requests.",
        ));
    }
    parse_ssec_triplet(headers, HDR_SSE_C_ALGO, HDR_SSE_C_KEY, HDR_SSE_C_KEY_MD5)
}

/// Parses copy-source SSE-C headers on `CopyObject` and `UploadPartCopy`.
pub fn parse_copy_source_sse_c(headers: &HeaderMap) -> Result<Option<SseCKey>, SseHeaderError> {
    parse_ssec_triplet(headers, HDR_COPY_SSE_C_ALGO, HDR_COPY_SSE_C_KEY, HDR_COPY_SSE_C_KEY_MD5)
}

/// Verifies a read request's SSE-C header against stored `TAG_SSE` metadata.
/// Returns `Ok(Some(SseDek))` when encrypted and authorized, `Ok(None)` when unencrypted.
pub fn authorize_read_ssec(
    stored: Option<&SseAttr>,
    provided: Option<&SseCKey>,
) -> Result<Option<SseDek>, SseHeaderError> {
    match (stored, provided) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(SseHeaderError::bad(
            "InvalidArgument",
            "The object was not encrypted with customer-provided encryption keys.",
        )),
        (Some(SseAttr::SseC { .. }), None) => Err(SseHeaderError::bad(
            "InvalidRequest",
            "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
        )),
        (Some(attr @ SseAttr::SseC { .. }), Some(key)) => {
            if !attr.verify_ssec_key(key) {
                return Err(SseHeaderError::bad(
                    "InvalidArgument",
                    "The secret key was invalid for this encrypted object.",
                ));
            }
            let dek = attr
                .derive_dek(key)
                .map_err(|_| SseHeaderError::bad("InvalidArgument", "Could not derive object encryption key."))?;
            Ok(Some(dek))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CEPH_KEY_B64: &str = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
    const CEPH_MD5_B64: &str = "DWygnHRtgiJ77HCm+1rvHw==";
    const CEPH_KEY_B_B64: &str = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=";
    const CEPH_MD5_B_B64: &str = "arxBvwY2V4SiOne6yppVPQ==";

    fn ceph_headers(alg: &str, key: &str, md5: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(HDR_SSE_C_ALGO, alg.parse().unwrap());
        h.insert(HDR_SSE_C_KEY, key.parse().unwrap());
        h.insert(HDR_SSE_C_KEY_MD5, md5.parse().unwrap());
        h
    }

    #[test]
    fn kat_awe1_fixed_key_nonce_plaintext_vectors() {
        let dek = SseDek::from_raw([0x42u8; 32]);
        let prefix = [0x11, 0x22, 0x33, 0x44];

        // 0-byte plaintext -> 16B header + 16B empty GCM tag = 32B deterministic vector.
        let (ct0, md5_0) = encrypt_full(&dek, prefix, 0, b"").unwrap();
        assert_eq!(
            ct0,
            [
                0x41, 0x57, 0x45, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44,
                0x0e, 0x6f, 0xd8, 0x98, 0xeb, 0x85, 0x0b, 0x0e, 0x62, 0x99, 0x9e, 0xee, 0xa6, 0x2f, 0x56, 0xf6,
            ]
        );
        assert_eq!(decrypt_full(&dek, Some(prefix), 0, 0, &ct0).unwrap(), b"");

        // 13-byte ASCII vector -> 16B header + 13B ciphertext + 16B tag = 45B deterministic vector.
        let (ct13, md5_13) = encrypt_full(&dek, prefix, 0, b"hello-awanfs!").unwrap();
        assert_eq!(
            ct13,
            [
                0x41, 0x57, 0x45, 0x31, 0x0d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44,
                0x78, 0x27, 0xa1, 0x35, 0x51, 0x25, 0xf4, 0x76, 0x0e, 0x59, 0x0c, 0x5c, 0x40, 0x52, 0x12, 0xa2,
                0x37, 0xfa, 0xfa, 0xdf, 0xb0, 0xe5, 0x9c, 0x75, 0xad, 0x14, 0xa6, 0x7b, 0x8a,
            ]
        );
        assert_eq!(decrypt_full(&dek, Some(prefix), 0, 13, &ct13).unwrap(), b"hello-awanfs!");
        assert_ne!(md5_0, md5_13);

        // Multi-chunk streaming writer vector matches single-shot encrypt_full byte for byte (64 KiB + 7 bytes).
        let msg: Vec<u8> = (0..(AWE1_CHUNK_PLAIN + 7)).map(|i| (i % 251) as u8).collect();
        let (ct_full, md5_full) = encrypt_full(&dek, prefix, 2, &msg).unwrap();
        let mut streamed = Vec::new();
        let mut w = Awe1EncryptWriter::new(&dek, prefix, 2, msg.len() as u64).unwrap();
        for piece in msg.chunks(1000) {
            w.write::<std::convert::Infallible, _>(piece, &mut |b| {
                streamed.extend_from_slice(b);
                Ok(())
            })
            .unwrap();
        }
        let md5_stream = w
            .finish::<std::convert::Infallible, _>(|b| {
                streamed.extend_from_slice(b);
                Ok(())
            })
            .unwrap();
        assert_eq!(streamed, ct_full);
        assert_eq!(md5_stream, md5_full);
        assert_eq!(decrypt_full(&dek, Some(prefix), 2, msg.len() as u64, &streamed).unwrap(), msg);
    }

    #[test]
    fn kat_hkdf_and_tag_sse_roundtrip() {
        let h = ceph_headers("AES256", CEPH_KEY_B64, CEPH_MD5_B64);
        let key = parse_write_sse_headers(&h).unwrap().unwrap();
        let salt = [0xAAu8; 16];
        let verifier = compute_ssec_verifier(&key, &salt);
        let dek1 = derive_ssec_dek(&key, &salt).unwrap();
        let dek2 = derive_ssec_dek(&key, &[0xBBu8; 16]).unwrap();
        assert_ne!(dek1.raw, dek2.raw, "different salts must yield distinct DEKs");

        let attr = SseAttr::SseC {
            multipart: false,
            nonce_prefix: [1, 2, 3, 4],
            plaintext_size: 123_456_789,
            key_md5: key.key_md5,
            key_salt: salt,
            verifier_mac: verifier,
        };
        let encoded = attr.encode();
        assert_eq!(encoded.len(), SSE_C_ATTR_LEN);
        assert_eq!(SseAttr::plaintext_size_from_raw(&encoded), Some(123_456_789));
        let decoded = SseAttr::decode(&encoded).unwrap();
        assert_eq!(decoded, attr);
        assert!(decoded.verify_ssec_key(&key));

        let hb = ceph_headers("AES256", CEPH_KEY_B_B64, CEPH_MD5_B_B64);
        let key_b = parse_write_sse_headers(&hb).unwrap().unwrap();
        assert!(!decoded.verify_ssec_key(&key_b));
        assert!(!format!("{:?}", key).contains("pO3upElr"));
    }

    #[test]
    fn header_parsing_conflicts_and_chunk_limit() {
        assert!(parse_write_sse_headers(&HeaderMap::new()).unwrap().is_none());
        assert!(parse_write_sse_headers(&ceph_headers("AES256", CEPH_KEY_B64, CEPH_MD5_B64)).is_ok());
        assert!(parse_write_sse_headers(&ceph_headers("AES192", CEPH_KEY_B64, CEPH_MD5_B64)).is_err());
        assert!(parse_write_sse_headers(&ceph_headers("AES256", CEPH_KEY_B64, "AAAAAAAAAAAAAAAAAAAAAA==")).is_err());

        // Conflicts: SSE-C + SSE-S3
        let mut h = ceph_headers("AES256", CEPH_KEY_B64, CEPH_MD5_B64);
        h.insert(HDR_SSE, "AES256".parse().unwrap());
        assert_eq!(parse_write_sse_headers(&h).unwrap_err().code, "InvalidArgument");

        // Conflicts: SSE-C + SSE-KMS
        let mut h = ceph_headers("AES256", CEPH_KEY_B64, CEPH_MD5_B64);
        h.insert(HDR_SSE, "aws:kms".parse().unwrap());
        h.insert(HDR_SSE_KMS_KEY_ID, "k1".parse().unwrap());
        assert_eq!(parse_write_sse_headers(&h).unwrap_err().code, "InvalidArgument");

        // Conflicts: AES256 + kms key id
        let mut h = HeaderMap::new();
        h.insert(HDR_SSE, "AES256".parse().unwrap());
        h.insert(HDR_SSE_KMS_KEY_ID, "k1".parse().unwrap());
        assert_eq!(parse_write_sse_headers(&h).unwrap_err().code, "InvalidArgument");

        // Conflicts: bad SSE algorithm
        let mut h = HeaderMap::new();
        h.insert(HDR_SSE, "aes:kms".parse().unwrap());
        assert_eq!(parse_write_sse_headers(&h).unwrap_err().code, "InvalidArgument");

        // Read with x-amz-server-side-encryption is refused
        let mut h = HeaderMap::new();
        h.insert(HDR_SSE, "aws:kms".parse().unwrap());
        assert_eq!(parse_read_sse_headers(&h).unwrap_err().code, "InvalidArgument");

        // Above 2^32 chunks is refused
        let max_supported = (u32::MAX as u64) * (AWE1_CHUNK_PLAIN as u64);
        assert!(chunk_count(max_supported).is_ok());
        assert_eq!(chunk_count(max_supported + 1), Err(SseError::TooLarge));
    }

    #[test]
    fn key_is_zeroized_on_drop() {
        let mut buf = [0x5Au8; 32];
        zeroize(&mut buf);
        assert_eq!(buf, [0u8; 32]);
    }
}
