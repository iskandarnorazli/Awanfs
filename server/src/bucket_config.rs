//! Bucket configuration store backed by `buckets.manifest` (#149).
//!
//! Each bucket holds one immutable `BucketConfig` inside a lock-free swapped snapshot.
//! Writes validate the document, check `fsm::bucket_step`, append and sync `buckets.manifest`,
//! and only then swap the in-memory pointer.

use bytes::Bytes;
use parking_lot::Mutex;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::fsm::{bucket_step, BucketEvent, BucketState, BucketStatus, Refusal};
use crate::versions::Versioning;

pub const MAX_POLICY_BYTES: usize = 20 * 1024;
pub const MAX_CONFIG_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConfigKind {
    Owner = 0,
    Policy = 1,
    Acl = 2,
    Ownership = 3,
    PublicAccessBlock = 4,
    Cors = 5,
    Lifecycle = 6,
    Logging = 7,
    Encryption = 8,
    ObjectLock = 9,
    Tagging = 10,
    Versioning = 11,
}

impl ConfigKind {
    pub const COUNT: usize = 12;

    pub const ALL: &'static [ConfigKind] = &[
        ConfigKind::Owner,
        ConfigKind::Policy,
        ConfigKind::Acl,
        ConfigKind::Ownership,
        ConfigKind::PublicAccessBlock,
        ConfigKind::Cors,
        ConfigKind::Lifecycle,
        ConfigKind::Logging,
        ConfigKind::Encryption,
        ConfigKind::ObjectLock,
        ConfigKind::Tagging,
        ConfigKind::Versioning,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            ConfigKind::Owner => "owner",
            ConfigKind::Policy => "policy",
            ConfigKind::Acl => "acl",
            ConfigKind::Ownership => "ownership",
            ConfigKind::PublicAccessBlock => "public-access-block",
            ConfigKind::Cors => "cors",
            ConfigKind::Lifecycle => "lifecycle",
            ConfigKind::Logging => "logging",
            ConfigKind::Encryption => "encryption",
            ConfigKind::ObjectLock => "object-lock",
            ConfigKind::Tagging => "tagging",
            ConfigKind::Versioning => "versioning",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "owner" => Some(ConfigKind::Owner),
            "policy" => Some(ConfigKind::Policy),
            "acl" => Some(ConfigKind::Acl),
            "ownership" => Some(ConfigKind::Ownership),
            "public-access-block" => Some(ConfigKind::PublicAccessBlock),
            "cors" => Some(ConfigKind::Cors),
            "lifecycle" => Some(ConfigKind::Lifecycle),
            "logging" => Some(ConfigKind::Logging),
            "encryption" => Some(ConfigKind::Encryption),
            "object-lock" => Some(ConfigKind::ObjectLock),
            "tagging" => Some(ConfigKind::Tagging),
            "versioning" => Some(ConfigKind::Versioning),
            _ => None,
        }
    }

    pub const fn max_bytes(self) -> usize {
        match self {
            ConfigKind::Policy => MAX_POLICY_BYTES,
            _ => MAX_CONFIG_BYTES,
        }
    }

    pub const fn bit(self) -> u16 {
        1u16 << (self as u8)
    }
}

#[derive(Clone)]
pub struct ParsedConfig {
    pub raw: Bytes,
    pub parsed: Option<Arc<dyn Any + Send + Sync>>,
}

impl fmt::Debug for ParsedConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParsedConfig")
            .field("raw_len", &self.raw.len())
            .field("has_parsed", &self.parsed.is_some())
            .finish()
    }
}

impl ParsedConfig {
    pub fn raw(raw: Bytes) -> Self {
        Self { raw, parsed: None }
    }

    pub fn with_parsed(raw: Bytes, parsed: Arc<dyn Any + Send + Sync>) -> Self {
        Self {
            raw,
            parsed: Some(parsed),
        }
    }
}

#[derive(Debug)]
pub enum ConfigError {
    TooLarge {
        kind: ConfigKind,
        len: usize,
        max: usize,
    },
    NoValidator(ConfigKind),
    Rejected {
        status: u16,
        code: &'static str,
        message: String,
    },
    Fsm(Refusal),
    Io(std::io::Error),
}

impl ConfigError {
    pub fn status(&self) -> u16 {
        match self {
            ConfigError::TooLarge { .. } => 400,
            ConfigError::NoValidator(_) => 400,
            ConfigError::Rejected { status, .. } => *status,
            ConfigError::Fsm(r) => r.status,
            ConfigError::Io(_) => 500,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            ConfigError::TooLarge { kind: ConfigKind::Policy, .. } => "MalformedPolicy",
            ConfigError::TooLarge { .. } => "InvalidArgument",
            ConfigError::NoValidator(_) => "NotImplemented",
            ConfigError::Rejected { code, .. } => code,
            ConfigError::Fsm(r) => r.code,
            ConfigError::Io(_) => "InternalError",
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::TooLarge { kind, len, max } => {
                write!(f, "{} configuration ({} bytes) exceeds {} byte limit", kind.as_str(), len, max)
            }
            ConfigError::NoValidator(kind) => {
                write!(f, "no validator registered for configuration kind {}", kind.as_str())
            }
            ConfigError::Rejected { message, .. } => write!(f, "{}", message),
            ConfigError::Fsm(r) => write!(f, "{} ({})", r.code, r.why),
            ConfigError::Io(e) => write!(f, "manifest I/O error: {}", e),
        }
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        ConfigError::Io(e)
    }
}

pub type ValidatorFn = Arc<dyn Fn(&[u8]) -> Result<ParsedConfig, ConfigError> + Send + Sync>;

#[derive(Clone, Default)]
pub struct ValidatorRegistry {
    validators: [Option<ValidatorFn>; ConfigKind::COUNT],
}

impl ValidatorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Baseline validators for all built-in kinds (future issues replace individual kinds with
    /// full XML/JSON parsers).
    pub fn with_default_validators() -> Self {
        let mut reg = Self::new();
        for &kind in ConfigKind::ALL {
            reg.register(
                kind,
                Arc::new(|bytes: &[u8]| Ok(ParsedConfig::raw(Bytes::copy_from_slice(bytes)))),
            );
        }
        reg.register(
            ConfigKind::Cors,
            Arc::new(|bytes: &[u8]| {
                let xml_str = std::str::from_utf8(bytes).map_err(|_| ConfigError::Rejected {
                    status: 400,
                    code: "MalformedXML",
                    message: "CORS configuration is not valid UTF-8".to_string(),
                })?;
                let rules = crate::cors::parse_cors_xml(xml_str).map_err(|r| ConfigError::Rejected {
                    status: r.status,
                    code: r.code,
                    message: r.why.to_string(),
                })?;
                Ok(ParsedConfig::with_parsed(
                    Bytes::copy_from_slice(bytes),
                    Arc::new(rules),
                ))
            }),
        );
        reg
    }

    pub fn register(&mut self, kind: ConfigKind, f: ValidatorFn) {
        self.validators[kind as usize] = Some(f);
    }

    pub fn validate(&self, kind: ConfigKind, bytes: &[u8]) -> Result<ParsedConfig, ConfigError> {
        let max = kind.max_bytes();
        if bytes.len() > max {
            return Err(ConfigError::TooLarge {
                kind,
                len: bytes.len(),
                max,
            });
        }
        let Some(ref f) = self.validators[kind as usize] else {
            return Err(ConfigError::NoValidator(kind));
        };
        f(bytes)
    }
}

#[derive(Clone, Debug)]
pub struct BucketConfig {
    pub id: u32,
    pub name: String,
    pub created_ns: Option<u64>,
    pub owner: String,
    pub namespace: String,
    pub versioning: Versioning,
    configs: [Option<ParsedConfig>; ConfigKind::COUNT],
}

impl BucketConfig {
    fn new(id: u32, name: String, created_ns: Option<u64>, owner: String, namespace: String) -> Self {
        Self {
            id,
            name,
            created_ns,
            owner,
            namespace,
            versioning: Versioning::Unset,
            configs: Default::default(),
        }
    }

    pub fn get(&self, kind: ConfigKind) -> Option<&ParsedConfig> {
        self.configs[kind as usize].as_ref()
    }
}

pub type BucketConfigMap = HashMap<u32, Arc<BucketConfig>>;

/// Lock-free double-buffered `Arc<T>` pointer swapper.
///
/// Readers never take a lock: they increment a per-slot reader count, verify the active slot did
/// not change, clone the `Arc<T>`, and decrement the reader count. Writers are serialized by
/// `BucketConfigStore` and wait for the inactive slot's reader count to reach zero before freeing.
struct SwappedSnapshot {
    active: AtomicUsize,
    readers: [AtomicUsize; 2],
    slots: [AtomicPtr<BucketConfigMap>; 2],
    write_lock: Mutex<()>,
}

impl SwappedSnapshot {
    fn new(initial: BucketConfigMap) -> Self {
        let ptr = Arc::into_raw(Arc::new(initial)) as *mut BucketConfigMap;
        Self {
            active: AtomicUsize::new(0),
            readers: [AtomicUsize::new(0), AtomicUsize::new(0)],
            slots: [AtomicPtr::new(ptr), AtomicPtr::new(std::ptr::null_mut())],
            write_lock: Mutex::new(()),
        }
    }

    fn load(&self) -> Arc<BucketConfigMap> {
        loop {
            let slot = self.active.load(Ordering::Acquire);
            self.readers[slot].fetch_add(1, Ordering::SeqCst);
            if self.active.load(Ordering::SeqCst) == slot {
                let ptr = self.slots[slot].load(Ordering::Acquire);
                // SAFETY: `slots[slot]` is non-null and kept alive while `readers[slot] > 0`.
                let arc = unsafe {
                    Arc::increment_strong_count(ptr);
                    Arc::from_raw(ptr)
                };
                self.readers[slot].fetch_sub(1, Ordering::Release);
                return arc;
            }
            self.readers[slot].fetch_sub(1, Ordering::Release);
        }
    }

    fn swap(&self, next: BucketConfigMap) {
        let _guard = self.write_lock.lock();
        let old_slot = self.active.load(Ordering::Relaxed);
        let new_slot = 1 - old_slot;
        while self.readers[new_slot].load(Ordering::Acquire) != 0 {
            std::hint::spin_loop();
        }
        let new_ptr = Arc::into_raw(Arc::new(next)) as *mut BucketConfigMap;
        self.slots[new_slot].store(new_ptr, Ordering::Release);
        self.active.store(new_slot, Ordering::SeqCst);
        while self.readers[old_slot].load(Ordering::SeqCst) != 0 {
            std::hint::spin_loop();
        }
        let old_ptr = self.slots[old_slot].swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !old_ptr.is_null() {
            // SAFETY: no reader can hold a reference to `old_ptr` once `readers[old_slot] == 0`.
            unsafe {
                drop(Arc::from_raw(old_ptr));
            }
        }
    }
}

impl Drop for SwappedSnapshot {
    fn drop(&mut self) {
        for slot in &self.slots {
            let ptr = slot.swap(std::ptr::null_mut(), Ordering::AcqRel);
            if !ptr.is_null() {
                unsafe {
                    drop(Arc::from_raw(ptr));
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct BucketConfigReader {
    snapshot: Arc<SwappedSnapshot>,
}

impl BucketConfigReader {
    pub fn snapshot(&self) -> Arc<BucketConfigMap> {
        self.snapshot.load()
    }

    pub fn load_bucket(&self, bucket_id: u32) -> Option<Arc<BucketConfig>> {
        self.snapshot.load().get(&bucket_id).cloned()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ManifestStats {
    pub manifest_bytes: u64,
    pub total_records: u64,
    pub live_records: u64,
}

impl ManifestStats {
    pub fn live_ratio(&self) -> f64 {
        if self.total_records == 0 {
            1.0
        } else {
            self.live_records as f64 / self.total_records as f64
        }
    }
}

pub struct ConfigMetrics {
    writes_by_kind: [AtomicU64; ConfigKind::COUNT],
    manifest_bytes: AtomicU64,
    total_records: AtomicU64,
    live_records: AtomicU64,
}

impl Default for ConfigMetrics {
    fn default() -> Self {
        Self {
            writes_by_kind: std::array::from_fn(|_| AtomicU64::new(0)),
            manifest_bytes: AtomicU64::new(0),
            total_records: AtomicU64::new(0),
            live_records: AtomicU64::new(0),
        }
    }
}

impl ConfigMetrics {
    pub fn writes_for(&self, kind: ConfigKind) -> u64 {
        self.writes_by_kind[kind as usize].load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> ManifestStats {
        ManifestStats {
            manifest_bytes: self.manifest_bytes.load(Ordering::Relaxed),
            total_records: self.total_records.load(Ordering::Relaxed),
            live_records: self.live_records.load(Ordering::Relaxed),
        }
    }

    fn record_write(&self, kind: ConfigKind) {
        self.writes_by_kind[kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    fn set_manifest_stats(&self, bytes: u64, total: u64, live: u64) {
        self.manifest_bytes.store(bytes, Ordering::Relaxed);
        self.total_records.store(total, Ordering::Relaxed);
        self.live_records.store(live, Ordering::Relaxed);
    }

    pub fn render(&self, out: &mut String) {
        use std::fmt::Write;
        out.push_str("# HELP awanfs_bucket_config_writes_total Bucket configuration writes persisted to buckets.manifest.\n# TYPE awanfs_bucket_config_writes_total counter\n");
        for &kind in ConfigKind::ALL {
            let n = self.writes_for(kind);
            let _ = writeln!(out, "awanfs_bucket_config_writes_total{{kind=\"{}\"}} {}", kind.as_str(), n);
        }
        let st = self.stats();
        let _ = writeln!(
            out,
            "# HELP awanfs_bucket_manifest_bytes Size of buckets.manifest in bytes.\n# TYPE awanfs_bucket_manifest_bytes gauge\nawanfs_bucket_manifest_bytes {}",
            st.manifest_bytes
        );
        let _ = writeln!(
            out,
            "# HELP awanfs_bucket_manifest_live_ratio Ratio of live records to total records in buckets.manifest.\n# TYPE awanfs_bucket_manifest_live_ratio gauge\nawanfs_bucket_manifest_live_ratio {:.6}",
            st.live_ratio()
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestRecord {
    Plus {
        id: u32,
        name: String,
        created_ns: Option<u64>,
        owner: Option<String>,
        namespace: Option<String>,
    },
    Minus {
        id: u32,
        name: String,
    },
    Tilde {
        id: u32,
        status: Versioning,
    },
    SetConfig {
        id: u32,
        kind: ConfigKind,
        data: Vec<u8>,
    },
    DeleteConfig {
        id: u32,
        kind: ConfigKind,
    },
}

enum LineError {
    Malformed(String),
    UnknownKind(String),
}

fn parse_manifest_line(line: &str) -> Result<ManifestRecord, LineError> {
    if let Some(rest) = line.strip_prefix('+') {
        let mut parts = rest.splitn(5, ':');
        let id_str = parts.next().ok_or_else(|| LineError::Malformed("missing id".into()))?;
        let name = parts.next().ok_or_else(|| LineError::Malformed("missing bucket name".into()))?;
        if name.is_empty() {
            return Err(LineError::Malformed("empty bucket name".into()));
        }
        let id = id_str.parse::<u32>().map_err(|_| LineError::Malformed("invalid bucket id".into()))?;
        let created_ns = match parts.next() {
            None | Some("") => None,
            Some(s) => Some(s.parse::<u64>().map_err(|_| LineError::Malformed("invalid created_ns".into()))?),
        };
        let owner = parts.next().map(|s| s.to_string());
        let namespace = parts.next().map(|s| s.to_string());
        return Ok(ManifestRecord::Plus {
            id,
            name: name.to_string(),
            created_ns,
            owner,
            namespace,
        });
    }
    if let Some(rest) = line.strip_prefix('-') {
        let (id_str, name) = rest.split_once(':').ok_or_else(|| LineError::Malformed("missing ':'".into()))?;
        let id = id_str.parse::<u32>().map_err(|_| LineError::Malformed("invalid bucket id".into()))?;
        return Ok(ManifestRecord::Minus {
            id,
            name: name.to_string(),
        });
    }
    if let Some(rest) = line.strip_prefix('~') {
        let (id_str, status_str) = rest.split_once(':').ok_or_else(|| LineError::Malformed("missing ':'".into()))?;
        let id = id_str.parse::<u32>().map_err(|_| LineError::Malformed("invalid bucket id".into()))?;
        let status = Versioning::parse(status_str).ok_or_else(|| LineError::Malformed("invalid versioning status".into()))?;
        return Ok(ManifestRecord::Tilde { id, status });
    }
    if let Some(rest) = line.strip_prefix('=') {
        let (id_str, after_id) = rest.split_once(':').ok_or_else(|| LineError::Malformed("missing ':' after id".into()))?;
        let id = id_str.parse::<u32>().map_err(|_| LineError::Malformed("invalid bucket id".into()))?;
        let (kind_str, payload) = after_id.split_once(':').ok_or_else(|| LineError::Malformed("missing ':' after kind".into()))?;
        if payload == "-" || payload.is_empty() {
            let kind = ConfigKind::parse(kind_str).ok_or_else(|| LineError::UnknownKind(kind_str.to_string()))?;
            return Ok(ManifestRecord::DeleteConfig { id, kind });
        }
        let data = base64_decode(payload).map_err(|_| LineError::Malformed("invalid base64 payload".into()))?;
        let kind = ConfigKind::parse(kind_str).ok_or_else(|| LineError::UnknownKind(kind_str.to_string()))?;
        return Ok(ManifestRecord::SetConfig { id, kind, data });
    }
    Err(LineError::Malformed("unknown record prefix".into()))
}

pub fn parse_manifest(content: &str) -> Result<Vec<ManifestRecord>, String> {
    let ends_with_newline = content.ends_with('\n');
    let raw_lines: Vec<&str> = content.split('\n').collect();
    let last_non_empty_idx = raw_lines.iter().rposition(|l| !l.trim().is_empty());
    let mut records = Vec::new();

    for (idx, raw) in raw_lines.iter().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let is_unterminated_last = !ends_with_newline && Some(idx) == last_non_empty_idx;
        match parse_manifest_line(line) {
            Ok(rec) => records.push(rec),
            Err(LineError::UnknownKind(k)) => {
                return Err(format!("buckets.manifest line {}: unknown configuration kind '{}'", idx + 1, k));
            }
            Err(LineError::Malformed(msg)) => {
                if is_unterminated_last {
                    tracing::warn!(line = idx + 1, reason = %msg, "ignoring torn last line in buckets.manifest");
                } else {
                    return Err(format!("buckets.manifest line {}: {}", idx + 1, msg));
                }
            }
        }
    }
    Ok(records)
}

pub struct BucketConfigStore {
    manifest_path: Option<PathBuf>,
    validators: ValidatorRegistry,
    snapshot: Arc<SwappedSnapshot>,
    fsm_states: HashMap<u32, BucketState>,
    metrics: Arc<ConfigMetrics>,
    total_records: u64,
    live_records: u64,
    manifest_bytes: u64,
}

impl BucketConfigStore {
    pub fn in_memory(validators: ValidatorRegistry) -> Self {
        Self {
            manifest_path: None,
            validators,
            snapshot: Arc::new(SwappedSnapshot::new(HashMap::new())),
            fsm_states: HashMap::new(),
            metrics: Arc::new(ConfigMetrics::default()),
            total_records: 0,
            live_records: 0,
            manifest_bytes: 0,
        }
    }

    pub fn open(manifest_path: Option<PathBuf>, validators: ValidatorRegistry) -> Result<Self, String> {
        let mut store = Self::in_memory(validators);
        store.manifest_path = manifest_path.clone();

        let Some(ref path) = manifest_path else {
            return Ok(store);
        };
        if !path.exists() {
            return Ok(store);
        }

        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {}", path.display(), e))?;
        let file_bytes = content.len() as u64;
        let records = parse_manifest(&content)?;
        let total_records = records.len() as u64;

        let mut map: BucketConfigMap = HashMap::new();
        for rec in records {
            match rec {
                ManifestRecord::Plus { id, name, created_ns, owner, namespace } => {
                    let prev = store.fsm_states.get(&id).copied().unwrap_or_else(BucketState::absent);
                    let next = bucket_step(prev, BucketEvent::Create { caller_id: 0, is_us_east_1: true })
                        .map_err(|r| format!("buckets.manifest replay error on +{}:{}: {}", id, name, r.code))?;
                    store.fsm_states.insert(id, next);
                    map.insert(
                        id,
                        Arc::new(BucketConfig::new(
                            id,
                            name,
                            created_ns,
                            owner.unwrap_or_default(),
                            namespace.unwrap_or_default(),
                        )),
                    );
                }
                ManifestRecord::Minus { id, .. } => {
                    let prev = store.fsm_states.get(&id).copied().unwrap_or_else(BucketState::absent);
                    if let Ok(next) = bucket_step(prev, BucketEvent::Delete { empty: true }) {
                        store.fsm_states.insert(id, next);
                    }
                    map.remove(&id);
                }
                ManifestRecord::Tilde { id, status } => {
                    if let Some(existing) = map.get(&id) {
                        let prev = store.fsm_states.get(&id).copied().unwrap_or_else(BucketState::absent);
                        let next = bucket_step(prev, BucketEvent::PutConfig(ConfigKind::Versioning))
                            .map_err(|r| format!("buckets.manifest replay error on ~{}: {}", id, r.code))?;
                        store.fsm_states.insert(id, next);
                        let mut cfg = (**existing).clone();
                        cfg.versioning = status;
                        if status != Versioning::Unset {
                            cfg.configs[ConfigKind::Versioning as usize] =
                                Some(ParsedConfig::raw(Bytes::from(status.as_str().as_bytes().to_vec())));
                        }
                        map.insert(id, Arc::new(cfg));
                    }
                }
                ManifestRecord::SetConfig { id, kind, data } => {
                    if let Some(existing) = map.get(&id) {
                        let prev = store.fsm_states.get(&id).copied().unwrap_or_else(BucketState::absent);
                        let next = bucket_step(prev, BucketEvent::PutConfig(kind))
                            .map_err(|r| format!("buckets.manifest replay error on ={}:{}: {}", id, kind.as_str(), r.code))?;
                        store.fsm_states.insert(id, next);
                        let parsed = store
                            .validators
                            .validate(kind, &data)
                            .unwrap_or_else(|_| ParsedConfig::raw(Bytes::from(data)));
                        let mut cfg = (**existing).clone();
                        cfg.configs[kind as usize] = Some(parsed);
                        map.insert(id, Arc::new(cfg));
                    }
                }
                ManifestRecord::DeleteConfig { id, kind } => {
                    if let Some(existing) = map.get(&id) {
                        let prev = store.fsm_states.get(&id).copied().unwrap_or_else(BucketState::absent);
                        let next = bucket_step(prev, BucketEvent::DeleteConfig(kind))
                            .map_err(|r| format!("buckets.manifest replay error on ={}:{}:-: {}", id, kind.as_str(), r.code))?;
                        store.fsm_states.insert(id, next);
                        let mut cfg = (**existing).clone();
                        cfg.configs[kind as usize] = None;
                        map.insert(id, Arc::new(cfg));
                    }
                }
            }
        }

        let live_records = count_live_records(&map);
        let dead_records = total_records.saturating_sub(live_records);

        if dead_records > live_records {
            let new_bytes = compact_manifest_file(path, &map)
                .map_err(|e| format!("failed to compact {}: {}", path.display(), e))?;
            store.total_records = live_records;
            store.live_records = live_records;
            store.manifest_bytes = new_bytes;
        } else {
            store.total_records = total_records;
            store.live_records = live_records;
            store.manifest_bytes = file_bytes;
        }

        store.metrics.set_manifest_stats(store.manifest_bytes, store.total_records, store.live_records);
        store.snapshot.swap(map);
        Ok(store)
    }

    pub fn reader(&self) -> BucketConfigReader {
        BucketConfigReader {
            snapshot: Arc::clone(&self.snapshot),
        }
    }

    pub fn metrics(&self) -> Arc<ConfigMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn manifest_stats(&self) -> ManifestStats {
        ManifestStats {
            manifest_bytes: self.manifest_bytes,
            total_records: self.total_records,
            live_records: self.live_records,
        }
    }

    pub fn load_bucket(&self, bucket_id: u32) -> Option<Arc<BucketConfig>> {
        self.snapshot.load().get(&bucket_id).cloned()
    }

    pub fn snapshot(&self) -> Arc<BucketConfigMap> {
        self.snapshot.load()
    }

    pub fn fsm_state(&self, bucket_id: u32) -> BucketState {
        self.fsm_states.get(&bucket_id).copied().unwrap_or_else(BucketState::absent)
    }

    pub fn apply_bucket_create(
        &mut self,
        id: u32,
        name: &str,
        created_ns: u64,
        owner: &str,
        namespace: &str,
    ) -> Result<(), ConfigError> {
        let prev = self.fsm_state(id);
        let next = bucket_step(prev, BucketEvent::Create { caller_id: 0, is_us_east_1: true })
            .map_err(ConfigError::Fsm)?;
        if prev.status == BucketStatus::Active {
            return Ok(());
        }

        let line = if owner.is_empty() && namespace.is_empty() && created_ns == 0 {
            format!("+{}:{}\n", id, name)
        } else {
            format!("+{}:{}:{}:{}:{}\n", id, name, created_ns, owner, namespace)
        };
        self.append_and_sync(&line)?;

        self.fsm_states.insert(id, next);
        let mut map = (*self.snapshot.load()).clone();
        map.insert(
            id,
            Arc::new(BucketConfig::new(
                id,
                name.to_string(),
                Some(created_ns),
                owner.to_string(),
                namespace.to_string(),
            )),
        );
        self.total_records += 1;
        self.live_records = count_live_records(&map);
        self.metrics.set_manifest_stats(self.manifest_bytes, self.total_records, self.live_records);
        self.snapshot.swap(map);
        Ok(())
    }

    pub fn apply_bucket_delete(&mut self, id: u32, name: &str, empty: bool) -> Result<(), ConfigError> {
        let prev = self.fsm_state(id);
        let next = bucket_step(prev, BucketEvent::Delete { empty }).map_err(ConfigError::Fsm)?;

        let line = format!("-{}:{}\n", id, name);
        self.append_and_sync(&line)?;

        self.fsm_states.insert(id, next);
        let mut map = (*self.snapshot.load()).clone();
        map.remove(&id);
        self.total_records += 1;
        self.live_records = count_live_records(&map);
        self.metrics.set_manifest_stats(self.manifest_bytes, self.total_records, self.live_records);
        self.snapshot.swap(map);
        Ok(())
    }

    pub fn apply_versioning(&mut self, id: u32, status: Versioning) -> Result<(), ConfigError> {
        let prev = self.fsm_state(id);
        let next = bucket_step(prev, BucketEvent::PutConfig(ConfigKind::Versioning)).map_err(ConfigError::Fsm)?;

        let line = format!("~{}:{}\n", id, status.as_str());
        self.append_and_sync(&line)?;

        self.fsm_states.insert(id, next);
        let mut map = (*self.snapshot.load()).clone();
        if let Some(existing) = map.get(&id) {
            let mut cfg = (**existing).clone();
            cfg.versioning = status;
            if status != Versioning::Unset {
                cfg.configs[ConfigKind::Versioning as usize] =
                    Some(ParsedConfig::raw(Bytes::from(status.as_str().as_bytes().to_vec())));
            }
            map.insert(id, Arc::new(cfg));
        }
        self.total_records += 1;
        self.live_records = count_live_records(&map);
        self.metrics.record_write(ConfigKind::Versioning);
        self.metrics.set_manifest_stats(self.manifest_bytes, self.total_records, self.live_records);
        self.snapshot.swap(map);
        Ok(())
    }

    pub fn put_config(&mut self, id: u32, kind: ConfigKind, bytes: &[u8]) -> Result<(), ConfigError> {
        if bytes.is_empty() {
            return self.delete_config(id, kind);
        }
        let parsed = self.validators.validate(kind, bytes)?;
        let prev = self.fsm_state(id);
        let next = bucket_step(prev, BucketEvent::PutConfig(kind)).map_err(ConfigError::Fsm)?;

        let encoded = base64_encode(bytes);
        let line = format!("={}:{}:{}\n", id, kind.as_str(), encoded);
        self.append_and_sync(&line)?;

        self.fsm_states.insert(id, next);
        let mut map = (*self.snapshot.load()).clone();
        if let Some(existing) = map.get(&id) {
            let mut cfg = (**existing).clone();
            cfg.configs[kind as usize] = Some(parsed);
            map.insert(id, Arc::new(cfg));
        }
        self.total_records += 1;
        self.live_records = count_live_records(&map);
        self.metrics.record_write(kind);
        self.metrics.set_manifest_stats(self.manifest_bytes, self.total_records, self.live_records);
        self.snapshot.swap(map);
        Ok(())
    }

    pub fn delete_config(&mut self, id: u32, kind: ConfigKind) -> Result<(), ConfigError> {
        let prev = self.fsm_state(id);
        let next = bucket_step(prev, BucketEvent::DeleteConfig(kind)).map_err(ConfigError::Fsm)?;

        let line = format!("={}:{}:-\n", id, kind.as_str());
        self.append_and_sync(&line)?;

        self.fsm_states.insert(id, next);
        let mut map = (*self.snapshot.load()).clone();
        if let Some(existing) = map.get(&id) {
            let mut cfg = (**existing).clone();
            cfg.configs[kind as usize] = None;
            map.insert(id, Arc::new(cfg));
        }
        self.total_records += 1;
        self.live_records = count_live_records(&map);
        self.metrics.record_write(kind);
        self.metrics.set_manifest_stats(self.manifest_bytes, self.total_records, self.live_records);
        self.snapshot.swap(map);
        Ok(())
    }

    fn append_and_sync(&mut self, line: &str) -> std::io::Result<()> {
        if let Some(ref path) = self.manifest_path {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let mut f = OpenOptions::new().create(true).append(true).open(path)?;
            f.write_all(line.as_bytes())?;
            f.sync_all()?;
            self.manifest_bytes += line.len() as u64;
        }
        Ok(())
    }
}

fn count_live_records(map: &BucketConfigMap) -> u64 {
    let mut live = 0u64;
    for cfg in map.values() {
        live += 1; // + record
        if cfg.versioning != Versioning::Unset {
            live += 1; // ~ record
        }
        for &kind in ConfigKind::ALL {
            if kind == ConfigKind::Versioning && cfg.versioning != Versioning::Unset {
                continue;
            }
            if cfg.configs[kind as usize].is_some() {
                live += 1; // = record
            }
        }
    }
    live
}

fn compact_manifest_file(path: &Path, map: &BucketConfigMap) -> std::io::Result<u64> {
    let tmp_path = path.with_extension("manifest.tmp");
    let mut buf = String::new();
    let mut ids: Vec<u32> = map.keys().copied().collect();
    ids.sort_unstable();
    for id in ids {
        let cfg = &map[&id];
        match cfg.created_ns {
            Some(ns) if !cfg.owner.is_empty() || !cfg.namespace.is_empty() || ns != 0 => {
                buf.push_str(&format!("+{}:{}:{}:{}:{}\n", cfg.id, cfg.name, ns, cfg.owner, cfg.namespace));
            }
            Some(ns) => {
                buf.push_str(&format!("+{}:{}:{}\n", cfg.id, cfg.name, ns));
            }
            None => {
                buf.push_str(&format!("+{}:{}\n", cfg.id, cfg.name));
            }
        }
        if cfg.versioning != Versioning::Unset {
            buf.push_str(&format!("~{}:{}\n", cfg.id, cfg.versioning.as_str()));
        }
        for &kind in ConfigKind::ALL {
            if kind == ConfigKind::Versioning && cfg.versioning != Versioning::Unset {
                continue;
            }
            if let Some(ref c) = cfg.configs[kind as usize] {
                buf.push_str(&format!("={}:{}:{}\n", cfg.id, kind.as_str(), base64_encode(&c.raw)));
            }
        }
    }
    let bytes_len = buf.len() as u64;
    {
        let mut f = File::create(&tmp_path)?;
        f.write_all(buf.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
    Ok(bytes_len)
}

const B64_ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = if chunk.len() > 1 { chunk[1] as usize } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as usize } else { 0 };
        out.push(B64_ALPHA[(b0 >> 2) & 0x3F] as char);
        out.push(B64_ALPHA[((b0 << 4) | (b1 >> 4)) & 0x3F] as char);
        out.push(if chunk.len() > 1 {
            B64_ALPHA[((b1 << 2) | (b2 >> 6)) & 0x3F] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64_ALPHA[b2 & 0x3F] as char
        } else {
            '='
        });
    }
    out
}

pub fn base64_decode(input: &str) -> Result<Vec<u8>, ()> {
    let bytes = input.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err(());
    }
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let val = |c: u8| -> Result<u8, ()> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(()),
        }
    };
    let mut out = Vec::with_capacity((bytes.len() / 4) * 3);
    for (i, chunk) in bytes.chunks_exact(4).enumerate() {
        let is_last = (i + 1) * 4 == bytes.len();
        let a = val(chunk[0])?;
        let b = val(chunk[1])?;
        if chunk[2] == b'=' {
            if !is_last || chunk[3] != b'=' {
                return Err(());
            }
            out.push((a << 2) | (b >> 4));
        } else if chunk[3] == b'=' {
            if !is_last {
                return Err(());
            }
            let c = val(chunk[2])?;
            out.push((a << 2) | (b >> 4));
            out.push((b << 4) | (c >> 2));
        } else {
            let c = val(chunk[2])?;
            let d = val(chunk[3])?;
            out.push((a << 2) | (b >> 4));
            out.push((b << 4) | (c >> 2));
            out.push((c << 6) | d);
        }
    }
    Ok(out)
}
