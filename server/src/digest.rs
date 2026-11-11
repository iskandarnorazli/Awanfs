//! awan-storage/server/src/digest.rs
//! Streaming Hardware-Accelerated & Portable Software Fallback Checksums (#484)
//! Reference: RFC 1321 (MD5) and RFC 3720 / RFC 7143 (Castagnoli CRC32C)

use md5::{Digest, Md5};

/// Castagnoli CRC32C 256-entry lookup table for reflected polynomial 0x82F63B78.
const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let poly = 0x82F63B78u32;
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// RFC 4648 standard base64 encoding.
pub fn base64_encode(data: &[u8]) -> String {
    const B64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = if chunk.len() > 1 { chunk[1] as usize } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as usize } else { 0 };

        result.push(B64_CHARS[(b0 >> 2) & 0x3F] as char);
        result.push(B64_CHARS[((b0 << 4) | (b1 >> 4)) & 0x3F] as char);
        if chunk.len() > 1 {
            result.push(B64_CHARS[((b1 << 2) | (b2 >> 6)) & 0x3F] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(B64_CHARS[b2 & 0x3F] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Pure portable software Castagnoli CRC32C implementation.
/// Compatible with standard RFC 3720 Castagnoli CRC32C bit-for-bit across any CPU.
pub fn software_crc32c(prev_crc: u32, data: &[u8]) -> u32 {
    let mut crc = !prev_crc;
    for &b in data {
        let idx = ((crc ^ (b as u32)) & 0xFF) as usize;
        crc = CRC32C_TABLE[idx] ^ (crc >> 8);
    }
    !crc
}

/// Streaming digest pipeline computing MD5 and CRC32C simultaneously over incoming TCP frames.
/// Employs runtime CPU feature detection (SSE4.2/AVX2 or ARM CRC) with seamless fallback to pure software.
pub struct StreamingDigest {
    md5_hasher: Md5,
    crc32c_val: u32,
    total_bytes: u64,
    force_software: bool,
}

impl StreamingDigest {
    /// Creates a streaming digest pipeline with runtime hardware feature detection.
    pub fn new() -> Self {
        let has_hardware_accel = {
            #[cfg(target_arch = "x86_64")]
            {
                is_x86_feature_detected!("sse4.2") || is_x86_feature_detected!("avx2")
            }
            #[cfg(target_arch = "aarch64")]
            {
                std::arch::is_aarch64_feature_detected!("crc")
            }
            #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
            {
                false
            }
        };

        Self {
            md5_hasher: Md5::new(),
            crc32c_val: 0,
            total_bytes: 0,
            force_software: !has_hardware_accel,
        }
    }

    /// Creates a streaming digest pipeline explicitly forced to portable pure-software execution.
    pub fn new_software_fallback() -> Self {
        Self {
            md5_hasher: Md5::new(),
            crc32c_val: 0,
            total_bytes: 0,
            force_software: true,
        }
    }

    /// Feeds an incoming streaming frame / buffer chunk through MD5 and CRC32C simultaneously.
    pub fn update(&mut self, chunk: &[u8]) {
        self.md5_hasher.update(chunk);
        if self.force_software {
            self.crc32c_val = software_crc32c(self.crc32c_val, chunk);
        } else {
            self.crc32c_val = crc32c::crc32c_append(self.crc32c_val, chunk);
        }
        self.total_bytes += chunk.len() as u64;
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn is_software_fallback(&self) -> bool {
        self.force_software
    }

    /// Finalizes digests and returns (hi, lo, hex, raw_md5, crc32c).
    pub fn finalize(self) -> (u64, u64, String, [u8; 16], u32) {
        let result = self.md5_hasher.finalize();
        let bytes: [u8; 16] = result.into();
        let hi = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let lo = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let mut hex = String::with_capacity(32);
        for b in bytes {
            hex.push_str(&format!("{:02x}", b));
        }
        (hi, lo, hex, bytes, self.crc32c_val)
    }
}

impl Default for StreamingDigest {
    fn default() -> Self {
        Self::new()
    }
}
