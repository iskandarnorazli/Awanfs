//! awan-storage/server/src/sigv4.rs
//! AWS Signature Version 4 (AWS4-HMAC-SHA256) & Pre-Signed URL Authentication
//! Reference: awan-storage/API_SPECIFICATION.md Section 3

use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

type HmacSha256 = Hmac<Sha256>;

static FAULT_TAMPER_SIGV4: AtomicBool = AtomicBool::new(false);

pub fn set_fault_tamper_sigv4(tamper: bool) {
    FAULT_TAMPER_SIGV4.store(tamper, Ordering::SeqCst);
}

pub struct SigV4KeyCache {
    pub cache: RwLock<HashMap<(String, String, String, String), [u8; 32]>>,
}

impl SigV4KeyCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_or_derive(
        &self,
        secret_key: &str,
        date_yyyymmdd: &str,
        region: &str,
        service: &str,
    ) -> [u8; 32] {
        let key = (
            secret_key.to_string(),
            date_yyyymmdd.to_string(),
            region.to_string(),
            service.to_string(),
        );

        if let Some(derived) = self.cache.read().get(&key) {
            return *derived;
        }

        // kDate = HMAC-SHA256("AWS4" + secret_key, date)
        let mut k_date = HmacSha256::new_from_slice(format!("AWS4{}", secret_key).as_bytes())
            .expect("HMAC can take key of any size");
        k_date.update(date_yyyymmdd.as_bytes());
        let k_date_res = k_date.finalize().into_bytes();

        // kRegion = HMAC-SHA256(kDate, region)
        let mut k_region = HmacSha256::new_from_slice(&k_date_res)
            .expect("HMAC can take key of any size");
        k_region.update(region.as_bytes());
        let k_region_res = k_region.finalize().into_bytes();

        // kService = HMAC-SHA256(kRegion, service)
        let mut k_service = HmacSha256::new_from_slice(&k_region_res)
            .expect("HMAC can take key of any size");
        k_service.update(service.as_bytes());
        let k_service_res = k_service.finalize().into_bytes();

        // kSigning = HMAC-SHA256(kService, "aws4_request")
        let mut k_signing = HmacSha256::new_from_slice(&k_service_res)
            .expect("HMAC can take key of any size");
        k_signing.update(b"aws4_request");
        let k_signing_res: [u8; 32] = k_signing.finalize().into_bytes().into();

        self.cache.write().insert(key, k_signing_res);
        k_signing_res
    }
}

impl Default for SigV4KeyCache {
    fn default() -> Self {
        Self::new()
    }
}

pub fn parse_iso8601_epoch(dt: &str) -> Option<u64> {
    // Basic format: "20261110T120000Z"
    if dt.len() < 16 || !dt.ends_with('Z') {
        return None;
    }
    let year: i32 = dt[0..4].parse().ok()?;
    let month: u32 = dt[4..6].parse().ok()?;
    let day: u32 = dt[6..8].parse().ok()?;
    let hour: u32 = dt[9..11].parse().ok()?;
    let min: u32 = dt[11..13].parse().ok()?;
    let sec: u32 = dt[13..15].parse().ok()?;

    // Approximate days from epoch (valid for 2020..2030)
    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let days_in_months = [
        31,
        if is_leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];

    let mut total_days = 0u64;
    for y in 1970..year {
        let leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        total_days += if leap { 366 } else { 365 };
    }
    for m in 1..month {
        total_days += days_in_months[(m - 1) as usize];
    }
    total_days += (day - 1) as u64;

    let total_secs = total_days * 86400 + (hour as u64) * 3600 + (min as u64) * 60 + (sec as u64);
    Some(total_secs)
}

pub fn validate_sigv4_auth(
    auth_header: Option<&str>,
    query_params: &HashMap<String, String>,
    now_epoch_sec: u64,
) -> Result<(), &'static str> {
    if FAULT_TAMPER_SIGV4.load(Ordering::Relaxed) {
        return Err("SignatureDoesNotMatch");
    }

    // 1. Pre-Signed URL check
    if let Some(algo) = query_params.get("X-Amz-Algorithm") {
        if algo != "AWS4-HMAC-SHA256" {
            return Err("InvalidAlgorithm");
        }
        let date_str = query_params.get("X-Amz-Date").ok_or("MissingDate")?;
        let req_epoch = parse_iso8601_epoch(date_str).ok_or("InvalidDate")?;

        // Clock skew check: max 900s
        if req_epoch > now_epoch_sec && req_epoch - now_epoch_sec > 900 {
            return Err("RequestTimeTooSkewed");
        }
        if now_epoch_sec > req_epoch && now_epoch_sec - req_epoch > 900 {
            let expires: u64 = query_params
                .get("X-Amz-Expires")
                .and_then(|s| s.parse().ok())
                .unwrap_or(900);
            if now_epoch_sec - req_epoch > expires {
                return Err("RequestExpired");
            }
        }

        if !query_params.contains_key("X-Amz-Signature") {
            return Err("MissingSignature");
        }
        return Ok(());
    }

    // 2. Authorization Header check
    if let Some(auth) = auth_header {
        let auth = auth.trim();
        if !auth.starts_with("AWS4-HMAC-SHA256") {
            return Err("InvalidAlgorithm");
        }
        if !auth.contains("Credential=") || !auth.contains("Signature=") {
            return Err("IncompleteSignature");
        }
        return Ok(());
    }

    // Unauthenticated (or anonymous): allowed if server permits public access
    Ok(())
}

/// Cryptographically computes and verifies an AWS Signature Version 4 HMAC-SHA256 hex digest
/// against a canonical String-To-Sign using the cached 4-step derived signing key.
pub fn compute_sigv4_signature(
    cache: &SigV4KeyCache,
    secret_key: &str,
    date_yyyymmdd: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let signing_key = cache.get_or_derive(secret_key, date_yyyymmdd, region, service);
    let mut mac = HmacSha256::new_from_slice(&signing_key).expect("HMAC takes any key length");
    mac.update(string_to_sign.as_bytes());
    let result = mac.finalize().into_bytes();
    let mut hex = String::with_capacity(64);
    for b in result {
        hex.push_str(&format!("{:02x}", b));
    }
    hex
}

pub fn verify_sigv4_signature(
    cache: &SigV4KeyCache,
    secret_key: &str,
    date_yyyymmdd: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
    provided_signature_hex: &str,
) -> Result<(), &'static str> {
    if FAULT_TAMPER_SIGV4.load(Ordering::Relaxed) {
        return Err("SignatureDoesNotMatch");
    }
    let expected = compute_sigv4_signature(
        cache,
        secret_key,
        date_yyyymmdd,
        region,
        service,
        string_to_sign,
    );
    if expected.eq_ignore_ascii_case(provided_signature_hex.trim()) {
        Ok(())
    } else {
        Err("SignatureDoesNotMatch")
    }
}

