//! S3 object checksums: CRC32, CRC32C, CRC64NVME, SHA-1, and SHA-256.

use crate::digest::base64_encode;
use crate::fsm::Refusal;
use hyper::HeaderMap;
use sha1::Digest as _;

const CRC64_NVME_POLY_REFLECTED: u64 = 0xAD93_D235_94C9_3659u64.reverse_bits();

/// Slice-by-8 lookup tables for CRC64-NVME (reflected polynomial 0xAD93D23594C93659).
const CRC64_NVME_TABLES: [[u64; 256]; 8] = {
    let mut tables = [[0u64; 256]; 8];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if (crc & 1) != 0 {
                (crc >> 1) ^ CRC64_NVME_POLY_REFLECTED
            } else {
                crc >> 1
            };
            bit += 1;
        }
        tables[0][i] = crc;
        i += 1;
    }
    let mut t = 1usize;
    while t < 8 {
        let mut i = 0usize;
        while i < 256 {
            let prev = tables[t - 1][i];
            tables[t][i] = (prev >> 8) ^ tables[0][(prev & 0xFF) as usize];
            i += 1;
        }
        t += 1;
    }
    tables
};

#[inline]
fn crc64_nvme_update(mut crc: u64, data: &[u8]) -> u64 {
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        let lo = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        crc ^= lo;
        crc = CRC64_NVME_TABLES[7][(crc & 0xFF) as usize]
            ^ CRC64_NVME_TABLES[6][((crc >> 8) & 0xFF) as usize]
            ^ CRC64_NVME_TABLES[5][((crc >> 16) & 0xFF) as usize]
            ^ CRC64_NVME_TABLES[4][((crc >> 24) & 0xFF) as usize]
            ^ CRC64_NVME_TABLES[3][((crc >> 32) & 0xFF) as usize]
            ^ CRC64_NVME_TABLES[2][((crc >> 40) & 0xFF) as usize]
            ^ CRC64_NVME_TABLES[1][((crc >> 48) & 0xFF) as usize]
            ^ CRC64_NVME_TABLES[0][((crc >> 56) & 0xFF) as usize];
    }
    for &b in chunks.remainder() {
        crc = (crc >> 8) ^ CRC64_NVME_TABLES[0][((crc ^ (b as u64)) & 0xFF) as usize];
    }
    crc
}

const fn invalid_request(why: &'static str) -> Refusal {
    Refusal {
        code: "InvalidRequest",
        status: 400,
        why,
    }
}

const INVALID_CHECKSUM_ALG: Refusal = invalid_request("unsupported checksum algorithm");
const CONFLICTING_CHECKSUM_ALGS: Refusal = invalid_request("conflicting checksum algorithms");
const INVALID_CHECKSUM_VALUE: Refusal = invalid_request("invalid checksum header value");

/// Supported S3 checksum algorithms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
}

impl ChecksumAlgorithm {
    const ALL: [Self; 5] = [
        Self::Crc32,
        Self::Crc32c,
        Self::Crc64Nvme,
        Self::Sha1,
        Self::Sha256,
    ];

    /// Parses an algorithm name case-insensitively (`CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256`).
    pub fn from_header_name(name: &str) -> Option<Self> {
        let trimmed = name.trim();
        if trimmed.eq_ignore_ascii_case("CRC32") {
            Some(Self::Crc32)
        } else if trimmed.eq_ignore_ascii_case("CRC32C") {
            Some(Self::Crc32c)
        } else if trimmed.eq_ignore_ascii_case("CRC64NVME") {
            Some(Self::Crc64Nvme)
        } else if trimmed.eq_ignore_ascii_case("SHA1") {
            Some(Self::Sha1)
        } else if trimmed.eq_ignore_ascii_case("SHA256") {
            Some(Self::Sha256)
        } else {
            None
        }
    }

    /// Returns the canonical S3 algorithm name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
        }
    }

    fn header_name(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Crc64Nvme => "x-amz-checksum-crc64nvme",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
        }
    }

    fn digest_len(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64Nvme => 8,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
        }
    }
}

/// Streaming checksum calculator for any [`ChecksumAlgorithm`].
#[derive(Clone)]
pub struct Checksum {
    inner: ChecksumInner,
}

#[derive(Clone)]
enum ChecksumInner {
    Crc32(crc32fast::Hasher),
    Crc32c(u32),
    Crc64Nvme(u64),
    Sha1(sha1::Sha1),
    Sha256(sha2::Sha256),
}

impl Checksum {
    pub fn new(alg: ChecksumAlgorithm) -> Self {
        let inner = match alg {
            ChecksumAlgorithm::Crc32 => ChecksumInner::Crc32(crc32fast::Hasher::new()),
            ChecksumAlgorithm::Crc32c => ChecksumInner::Crc32c(0),
            ChecksumAlgorithm::Crc64Nvme => ChecksumInner::Crc64Nvme(u64::MAX),
            ChecksumAlgorithm::Sha1 => ChecksumInner::Sha1(sha1::Sha1::new()),
            ChecksumAlgorithm::Sha256 => ChecksumInner::Sha256(sha2::Sha256::new()),
        };
        Self { inner }
    }

    pub fn update(&mut self, data: &[u8]) {
        match &mut self.inner {
            ChecksumInner::Crc32(h) => h.update(data),
            ChecksumInner::Crc32c(state) => *state = crc32c::crc32c_append(*state, data),
            ChecksumInner::Crc64Nvme(state) => *state = crc64_nvme_update(*state, data),
            ChecksumInner::Sha1(h) => h.update(data),
            ChecksumInner::Sha256(h) => h.update(data),
        }
    }

    pub fn finish(self) -> Vec<u8> {
        match self.inner {
            ChecksumInner::Crc32(h) => h.finalize().to_be_bytes().to_vec(),
            ChecksumInner::Crc32c(state) => state.to_be_bytes().to_vec(),
            ChecksumInner::Crc64Nvme(state) => (!state).to_be_bytes().to_vec(),
            ChecksumInner::Sha1(h) => h.finalize().to_vec(),
            ChecksumInner::Sha256(h) => h.finalize().to_vec(),
        }
    }

    pub fn finish_base64(self) -> String {
        base64_encode(&self.finish())
    }
}

fn record_alg(slot: &mut Option<ChecksumAlgorithm>, alg: ChecksumAlgorithm) -> Result<(), Refusal> {
    match *slot {
        Some(existing) if existing != alg => Err(CONFLICTING_CHECKSUM_ALGS),
        _ => {
            *slot = Some(alg);
            Ok(())
        }
    }
}

/// Parses `x-amz-checksum-<alg>`, `x-amz-sdk-checksum-algorithm`, and `x-amz-trailer`.
pub fn requested_checksum(headers: &HeaderMap) -> Result<Option<(ChecksumAlgorithm, Option<String>)>, Refusal> {
    let mut chosen: Option<ChecksumAlgorithm> = None;
    let mut expected_value: Option<String> = None;

    for alg in ChecksumAlgorithm::ALL {
        let mut values = headers.get_all(alg.header_name()).iter();
        if let Some(first) = values.next() {
            if values.next().is_some() {
                return Err(INVALID_CHECKSUM_VALUE);
            }
            let s = first.to_str().map_err(|_| INVALID_CHECKSUM_VALUE)?;
            let decoded = base64_decode(s).ok_or(INVALID_CHECKSUM_VALUE)?;
            if decoded.len() != alg.digest_len() {
                return Err(INVALID_CHECKSUM_VALUE);
            }
            record_alg(&mut chosen, alg)?;
            expected_value = Some(s.to_owned());
        }
    }

    for val in headers.get_all("x-amz-sdk-checksum-algorithm") {
        let s = val.to_str().map_err(|_| INVALID_CHECKSUM_ALG)?;
        let alg = ChecksumAlgorithm::from_header_name(s).ok_or(INVALID_CHECKSUM_ALG)?;
        record_alg(&mut chosen, alg)?;
    }

    for val in headers.get_all("x-amz-trailer") {
        let s = val.to_str().map_err(|_| INVALID_CHECKSUM_ALG)?;
        for token in s.split(',') {
            let lower = token.trim().to_ascii_lowercase();
            if let Some(suffix) = lower.strip_prefix("x-amz-checksum-") {
                let alg = ChecksumAlgorithm::from_header_name(suffix).ok_or(INVALID_CHECKSUM_ALG)?;
                record_alg(&mut chosen, alg)?;
            }
        }
    }

    Ok(chosen.map(|alg| (alg, expected_value)))
}

/// Computes an S3 multipart `COMPOSITE` checksum: `base64(alg(concat(parts)))-N`.
pub fn composite(alg: ChecksumAlgorithm, part_checksums: &[Vec<u8>]) -> String {
    let mut h = Checksum::new(alg);
    for part in part_checksums {
        h.update(part);
    }
    format!("{}-{}", h.finish_base64(), part_checksums.len())
}

/// Strict RFC 4648 standard base64 decoder.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes.len() % 4 != 0 {
        return None;
    }

    let decode_char = |b: u8| -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };

    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for (idx, chunk) in bytes.chunks_exact(4).enumerate() {
        let is_last = (idx + 1) * 4 == bytes.len();
        let a = decode_char(chunk[0])?;
        let b = decode_char(chunk[1])?;
        let c = if chunk[2] == b'=' {
            if !is_last || chunk[3] != b'=' {
                return None;
            }
            None
        } else {
            Some(decode_char(chunk[2])?)
        };
        let d = if chunk[3] == b'=' {
            if !is_last {
                return None;
            }
            None
        } else {
            Some(decode_char(chunk[3])?)
        };

        out.push((a << 2) | (b >> 4));
        if let Some(c_val) = c {
            out.push((b << 4) | (c_val >> 2));
            if let Some(d_val) = d {
                out.push((c_val << 6) | d_val);
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::{HeaderName, HeaderValue};
    use proptest::prelude::*;

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn algorithm_names_and_case_insensitive_parsing() {
        let cases = [
            (ChecksumAlgorithm::Crc32, "CRC32", "crc32"),
            (ChecksumAlgorithm::Crc32c, "CRC32C", "Crc32c"),
            (ChecksumAlgorithm::Crc64Nvme, "CRC64NVME", "crc64nvme"),
            (ChecksumAlgorithm::Sha1, "SHA1", "sha1"),
            (ChecksumAlgorithm::Sha256, "SHA256", "Sha256"),
        ];
        for (alg, canonical, mixed) in cases {
            assert_eq!(alg.name(), canonical);
            assert_eq!(ChecksumAlgorithm::from_header_name(canonical), Some(alg));
            assert_eq!(ChecksumAlgorithm::from_header_name(mixed), Some(alg));
        }
        assert_eq!(ChecksumAlgorithm::from_header_name(""), None);
        assert_eq!(ChecksumAlgorithm::from_header_name("MD5"), None);
        assert_eq!(ChecksumAlgorithm::from_header_name("SHA512"), None);
    }

    #[test]
    fn known_answer_123456789() {
        let input = b"123456789";

        let mut h = Checksum::new(ChecksumAlgorithm::Crc32);
        h.update(input);
        assert_eq!(h.finish(), 0xCBF4_3926u32.to_be_bytes());

        let mut h = Checksum::new(ChecksumAlgorithm::Crc32c);
        h.update(input);
        assert_eq!(h.finish(), 0xE306_9283u32.to_be_bytes());

        let mut h = Checksum::new(ChecksumAlgorithm::Crc64Nvme);
        h.update(input);
        assert_eq!(h.finish(), 0xAE8B_1486_0A79_9888u64.to_be_bytes());

        // Cross-check with s3-tests vectors over 1024 * b'A'
        let a1024 = vec![b'A'; 1024];
        let mut h = Checksum::new(ChecksumAlgorithm::Crc64Nvme);
        h.update(&a1024);
        assert_eq!(h.finish_base64(), "Qeh8oXvGiSo=");

        let mut h = Checksum::new(ChecksumAlgorithm::Sha256);
        h.update(&a1024);
        assert_eq!(h.finish_base64(), "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=");
    }

    #[test]
    fn known_answer_fips_sha1_and_sha256() {
        let mut h = Checksum::new(ChecksumAlgorithm::Sha1);
        h.update(b"");
        assert_eq!(to_hex(&h.finish()), "da39a3ee5e6b4b0d3255bfef95601890afd80709");

        let mut h = Checksum::new(ChecksumAlgorithm::Sha1);
        h.update(b"abc");
        assert_eq!(to_hex(&h.finish()), "a9993e364706816aba3e25717850c26c9cd0d89d");

        let mut h = Checksum::new(ChecksumAlgorithm::Sha256);
        h.update(b"");
        assert_eq!(
            to_hex(&h.finish()),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        let mut h = Checksum::new(ChecksumAlgorithm::Sha256);
        h.update(b"abc");
        assert_eq!(
            to_hex(&h.finish()),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    fn arb_alg() -> impl Strategy<Value = ChecksumAlgorithm> {
        prop_oneof![
            Just(ChecksumAlgorithm::Crc32),
            Just(ChecksumAlgorithm::Crc32c),
            Just(ChecksumAlgorithm::Crc64Nvme),
            Just(ChecksumAlgorithm::Sha1),
            Just(ChecksumAlgorithm::Sha256),
        ]
    }

    proptest! {
        #[test]
        fn streaming_split_matches_whole(
            alg in arb_alg(),
            body in proptest::collection::vec(any::<u8>(), 0..=4096),
            splits in proptest::collection::vec(0usize..=4096, 0..=16),
        ) {
            let mut whole = Checksum::new(alg);
            whole.update(&body);
            let expected = whole.finish();

            let mut points: Vec<usize> = splits.into_iter().map(|p| p % (body.len() + 1)).collect();
            points.sort_unstable();
            points.push(body.len());

            let mut streamed = Checksum::new(alg);
            let mut prev = 0;
            for pt in points {
                streamed.update(&body[prev..pt]);
                prev = pt;
            }
            prop_assert_eq!(streamed.finish(), expected);
        }
    }

    #[test]
    fn requested_checksum_parses_valid_headers() {
        let empty = HeaderMap::new();
        assert_eq!(requested_checksum(&empty), Ok(None));

        let mut h = HeaderMap::new();
        h.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("crc64nvme"));
        assert_eq!(requested_checksum(&h), Ok(Some((ChecksumAlgorithm::Crc64Nvme, None))));

        let mut h = HeaderMap::new();
        h.insert("x-amz-trailer", HeaderValue::from_static("x-amz-checksum-sha256"));
        assert_eq!(requested_checksum(&h), Ok(Some((ChecksumAlgorithm::Sha256, None))));

        let mut h = HeaderMap::new();
        let val = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";
        h.insert("x-amz-checksum-sha256", HeaderValue::from_static(val));
        h.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("SHA256"));
        h.insert("x-amz-checksum-type", HeaderValue::from_static("FULL_OBJECT"));
        assert_eq!(
            requested_checksum(&h),
            Ok(Some((ChecksumAlgorithm::Sha256, Some(val.to_string()))))
        );

        let mut h = HeaderMap::new();
        h.insert("x-amz-checksum-crc32", HeaderValue::from_static("y/Q5Jg=="));
        assert_eq!(
            requested_checksum(&h),
            Ok(Some((ChecksumAlgorithm::Crc32, Some("y/Q5Jg==".to_string()))))
        );
    }

    #[test]
    fn requested_checksum_rejects_conflicts_and_bad_values() {
        // Two different checksum headers
        let mut h = HeaderMap::new();
        h.insert("x-amz-checksum-crc32", HeaderValue::from_static("y/Q5Jg=="));
        h.insert("x-amz-checksum-crc32c", HeaderValue::from_static("MDaLrw=="));
        let err = requested_checksum(&h).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidRequest");

        // Header value vs sdk-checksum-algorithm mismatch
        let mut h = HeaderMap::new();
        h.insert("x-amz-checksum-crc32", HeaderValue::from_static("y/Q5Jg=="));
        h.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("SHA256"));
        let err = requested_checksum(&h).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidRequest");

        // Trailer vs sdk-checksum-algorithm mismatch
        let mut h = HeaderMap::new();
        h.insert("x-amz-trailer", HeaderValue::from_static("x-amz-checksum-crc32c"));
        h.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("SHA1"));
        let err = requested_checksum(&h).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidRequest");

        // Invalid base64
        let mut h = HeaderMap::new();
        h.insert("x-amz-checksum-sha256", HeaderValue::from_static("bad!"));
        let err = requested_checksum(&h).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidRequest");

        // Valid base64 but wrong length (4 bytes instead of 32 for SHA256)
        let mut h = HeaderMap::new();
        h.insert("x-amz-checksum-sha256", HeaderValue::from_static("y/Q5Jg=="));
        let err = requested_checksum(&h).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidRequest");

        // Unknown algorithm
        let mut h = HeaderMap::new();
        h.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("MD5"));
        let err = requested_checksum(&h).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidRequest");

        // Unknown trailer checksum header
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("x-amz-trailer"),
            HeaderValue::from_static("x-amz-checksum-blake3"),
        );
        let err = requested_checksum(&h).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidRequest");
    }

    #[test]
    fn composite_multipart_checksum() {
        // Vectors from ceph/s3-tests test_multipart_use_cksum_helper_sha1
        let part_b64 = [
            "iIaTCGbm+vdVjNqIMF2S0T7ibMk=",
            "LS/TJ32bAVKEwRu+sE3X7awh/lk=",
            "6DDwovUaHwrKNXDMzOGbuvj9kxI=",
        ];
        let parts: Vec<Vec<u8>> = part_b64
            .iter()
            .map(|s| base64_decode(s).expect("valid test vector"))
            .collect();
        assert_eq!(
            composite(ChecksumAlgorithm::Sha1, &parts),
            "sizjvY4eud3MrcHdZM3cQ/ol39o=-3"
        );
    }

    #[test]
    #[ignore]
    fn throughput_64mib() {
        const SIZE: usize = 64 * 1024 * 1024;
        let buf = vec![0xA5u8; SIZE];
        let algs = [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Crc64Nvme,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
        ];
        for alg in algs {
            let start = std::time::Instant::now();
            let mut h = Checksum::new(alg);
            h.update(&buf);
            let out = h.finish();
            let elapsed = start.elapsed();
            std::hint::black_box(out);
            let mib_per_s = (SIZE as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
            println!("{}: {:.1} MiB/s ({:.2} ms)", alg.name(), mib_per_s, elapsed.as_secs_f64() * 1000.0);
        }
    }
}
