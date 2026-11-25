//! awan-storage/server/src/meta_index.rs
//! High-Performance Columnar Struct-of-Arrays (SoA) Object Metadata Index
//! Zero-allocation in-memory metadata substrate with 64-byte packed records,
//! arena-backed key indexing, and O(1) hash table lookup.

use std::collections::{BTreeMap, HashMap};
use std::panic::catch_unwind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use xxhash_rust::xxh3::xxh3_64_with_seed;
use crate::spill::SpillIndex;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ObjectMeta {
    pub key_hash: u64,
    pub bucket_flags: u64,
    pub vol_offset: u64,
    pub size: u64,
    pub etag_hi: u64,
    pub etag_lo: u64,
    pub created_at: u64,
    pub key_slice: u64,
}

const _: () = assert!(std::mem::size_of::<ObjectMeta>() == 64);

// Backward-compatibility alias
pub type AwanObjectMeta = ObjectMeta;

pub const FLAG_ACTIVE: u16 = 0x0001;
pub const FLAG_TOMBSTONE: u16 = 0x0002;
pub const FLAG_MULTIPART: u16 = 0x0004;
pub const FLAG_SHARED_REF: u16 = 0x0008;
pub const FLAG_ZERO_BYTE: u16 = 0x0010;
/// The payload is compressed (`compress.rs`, #51); only in format 2 volumes.
pub const FLAG_COMPRESSED: u16 = 0x0020;
/// A version of a key in a versioned bucket (`versions.rs`, #31): its creation time is its id.
pub const FLAG_VERSIONED: u16 = 0x0040;
/// With `FLAG_VERSIONED`: the null version (Suspended buckets).
pub const FLAG_NULL_ID: u16 = 0x0080;
/// A tombstone removing one version; its 8-byte payload is the id (0 for null).
pub const FLAG_VERSION_DELETE: u16 = 0x0100;
/// Attribute-update delta needle (decision 43 & 56, #150 stage 2, #203). Payload holds
/// `target_version_id: u64 LE || encoded Attrs block`; `0` targets an unversioned object or null version.
pub const FLAG_ATTR_UPDATE: u16 = 0x0200;
/// The needle carries an attributes block (format 3, `crate::attrs`, #150): HEAD and GET read
/// it; rows without the flag read nothing more than they did before.
pub const FLAG_ATTRS: u16 = 0x0400;

pub const AWAN_FLAG_ACTIVE: u16 = FLAG_ACTIVE;
pub const AWAN_FLAG_TOMBSTONE: u16 = FLAG_TOMBSTONE;
pub const AWAN_FLAG_MULTIPART: u16 = FLAG_MULTIPART;
pub const AWAN_FLAG_SHARED_REF: u16 = FLAG_SHARED_REF;
pub const AWAN_FLAG_ZERO_BYTE: u16 = FLAG_ZERO_BYTE;

pub const OK: i32 = 0;
pub const ERR_NOT_FOUND: i32 = -1;
pub const ERR_BUCKET_NOT_FOUND: i32 = -2;
pub const ERR_BUCKET_NOT_EMPTY: i32 = -3;
pub const ERR_PRECONDITION: i32 = -4;
pub const ERR_CAPACITY: i32 = -5;
pub const ERR_INVALID_ARG: i32 = -6;
pub const ERR_SPILL: i32 = -7;
pub const ERR_PANIC: i32 = -999;

pub const AWAN_OK: i32 = OK;
pub const AWAN_ERR_NOT_FOUND: i32 = ERR_NOT_FOUND;
pub const AWAN_ERR_BUCKET_NOT_FOUND: i32 = ERR_BUCKET_NOT_FOUND;
pub const AWAN_ERR_BUCKET_NOT_EMPTY: i32 = ERR_BUCKET_NOT_EMPTY;
pub const AWAN_ERR_PRECONDITION: i32 = ERR_PRECONDITION;
pub const AWAN_ERR_CAPACITY: i32 = ERR_CAPACITY;
pub const AWAN_ERR_INVALID_ARG: i32 = ERR_INVALID_ARG;
pub const AWAN_ERR_SPILL: i32 = ERR_SPILL;
pub const AWAN_ERR_PANIC: i32 = ERR_PANIC;

#[inline]
pub fn pack_bucket_flags(bucket_id: u32, part_count: u16, flags: u16) -> u64 {
    ((bucket_id as u64) << 32) | ((part_count as u64) << 16) | (flags as u64)
}

#[inline]
pub fn unpack_bucket_id(bucket_flags: u64) -> u32 {
    (bucket_flags >> 32) as u32
}

#[inline]
pub fn unpack_part_count(bucket_flags: u64) -> u16 {
    ((bucket_flags >> 16) & 0xFFFF) as u16
}

#[inline]
pub fn unpack_flags(bucket_flags: u64) -> u16 {
    (bucket_flags & 0xFFFF) as u16
}

#[inline]
pub fn pack_key_slice(arena_offset: usize, key_len: u16) -> u64 {
    ((arena_offset as u64) << 16) | (key_len as u64)
}

#[inline]
pub fn unpack_key_slice(key_slice: u64) -> (usize, usize) {
    let offset = (key_slice >> 16) as usize;
    let len = (key_slice & 0xFFFF) as usize;
    (offset, len)
}

// Global negative-control fault injection flag for Gate testing
static FAULT_DISABLE_KEY_VERIFICATION: AtomicBool = AtomicBool::new(false);

pub fn set_fault_disable_key_verification(disable: bool) {
    FAULT_DISABLE_KEY_VERIFICATION.store(disable, Ordering::SeqCst);
}

/// Columnar Struct-of-Arrays (SoA) Index Tables
pub struct ColumnarObjectTable {
    pub key_hashes: Vec<u64>,
    pub bucket_flags: Vec<u64>,
    pub vol_offsets: Vec<u64>,
    pub sizes: Vec<u64>,
    pub etags_hi: Vec<u64>,
    pub etags_lo: Vec<u64>,
    pub created_ats: Vec<u64>,
    pub key_slices: Vec<u64>,
}

impl ColumnarObjectTable {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            key_hashes: Vec::with_capacity(cap),
            bucket_flags: Vec::with_capacity(cap),
            vol_offsets: Vec::with_capacity(cap),
            sizes: Vec::with_capacity(cap),
            etags_hi: Vec::with_capacity(cap),
            etags_lo: Vec::with_capacity(cap),
            created_ats: Vec::with_capacity(cap),
            key_slices: Vec::with_capacity(cap),
        }
    }

    pub fn push(&mut self, meta: &ObjectMeta) -> usize {
        let idx = self.key_hashes.len();
        self.key_hashes.push(meta.key_hash);
        self.bucket_flags.push(meta.bucket_flags);
        self.vol_offsets.push(meta.vol_offset);
        self.sizes.push(meta.size);
        self.etags_hi.push(meta.etag_hi);
        self.etags_lo.push(meta.etag_lo);
        self.created_ats.push(meta.created_at);
        self.key_slices.push(meta.key_slice);
        idx
    }

    pub fn update(&mut self, idx: usize, meta: &ObjectMeta) {
        self.key_hashes[idx] = meta.key_hash;
        self.bucket_flags[idx] = meta.bucket_flags;
        self.vol_offsets[idx] = meta.vol_offset;
        self.sizes[idx] = meta.size;
        self.etags_hi[idx] = meta.etag_hi;
        self.etags_lo[idx] = meta.etag_lo;
        self.created_ats[idx] = meta.created_at;
        self.key_slices[idx] = meta.key_slice;
    }

    pub fn get(&self, idx: usize) -> ObjectMeta {
        ObjectMeta {
            key_hash: self.key_hashes[idx],
            bucket_flags: self.bucket_flags[idx],
            vol_offset: self.vol_offsets[idx],
            size: self.sizes[idx],
            etag_hi: self.etags_hi[idx],
            etag_lo: self.etags_lo[idx],
            created_at: self.created_ats[idx],
            key_slice: self.key_slices[idx],
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BucketUsage {
    pub objects: u64,
    pub bytes: u64,
}

pub struct BucketEntry {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    /// Lexicographical map from UTF-8 key bytes -> SoA row index
    pub key_index: BTreeMap<Vec<u8>, usize>,
    pub current_objects: u64,
    pub current_bytes: u64,
}

pub struct StorageEngine {
    pub max_objects: usize,
    pub key_arena: Vec<u8>,
    pub table: ColumnarObjectTable,
    pub buckets: HashMap<u32, BucketEntry>,
    pub bucket_names: HashMap<String, HashMap<String, u32>>,
    pub next_bucket_id: u32,
    /// Open addressing / hash table: (bucket_id, key_hash) -> Vec of SoA row indices
    pub hash_slots: HashMap<(u32, u64), Vec<usize>>,
    pub slab_bytes: usize,
    pub spill_index: Option<SpillIndex>,
    /// Active upfront slot reservations for unindexed keys (`spill_index.is_none()`), keyed by
    /// `(bucket_id, key) -> refcount`.
    reserved_keys: HashMap<(u32, Vec<u8>), usize>,
}

/// RAII capacity guard holding a slot in `StorageEngine::reserved_keys` until committed or dropped.
pub struct CapacityReservation {
    engine: Option<Arc<RwLock<StorageEngine>>>,
    bucket_id: u32,
    key: Vec<u8>,
    reserved: bool,
}

impl std::fmt::Debug for CapacityReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapacityReservation")
            .field("bucket_id", &self.bucket_id)
            .field("key_len", &self.key.len())
            .field("reserved", &self.reserved)
            .finish()
    }
}

impl CapacityReservation {
    /// Reserves capacity for `(bucket_id, key)` when the engine is bounded (`spill_index.is_none()`)
    /// and the key is not already indexed in memory.
    pub fn acquire(
        engine: &Arc<RwLock<StorageEngine>>,
        bucket_id: u32,
        key: &[u8],
    ) -> Result<Self, i32> {
        {
            let guard = engine.read();
            if key.is_empty() || key.len() > 1024 {
                return Err(ERR_INVALID_ARG);
            }
            let bucket = guard.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
            if guard.spill_index.is_some() || bucket.key_index.contains_key(key) {
                return Ok(Self {
                    engine: None,
                    bucket_id,
                    key: Vec::new(),
                    reserved: false,
                });
            }
        }
        let reserved = engine.write().try_reserve_slot(bucket_id, key)?;
        Ok(Self {
            engine: reserved.then(|| Arc::clone(engine)),
            bucket_id,
            key: if reserved { key.to_vec() } else { Vec::new() },
            reserved,
        })
    }

    /// Re-verifies capacity (or acquires a reservation if an existing key was deleted while
    /// streaming without `lock_key`) before committing a streamed needle.
    pub fn ensure_room(
        &mut self,
        engine: &Arc<RwLock<StorageEngine>>,
        bucket_id: u32,
        key: &[u8],
    ) -> Result<(), i32> {
        if self.reserved {
            let res = engine.read().has_room_with_reserved(bucket_id, key);
            if res.is_ok() {
                return Ok(());
            }
            if let Some(eng) = self.engine.take() {
                eng.write().release_slot(self.bucket_id, &self.key);
            }
            self.reserved = false;
            return res;
        }
        {
            let guard = engine.read();
            let bucket = guard.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
            if guard.spill_index.is_some() || bucket.key_index.contains_key(key) {
                return Ok(());
            }
        }
        let reserved = engine.write().try_reserve_slot(bucket_id, key)?;
        if reserved {
            self.engine = Some(Arc::clone(engine));
            self.bucket_id = bucket_id;
            self.key = key.to_vec();
            self.reserved = true;
        }
        Ok(())
    }

    pub(crate) fn is_reserved_for(&self, bucket_id: u32, key: &[u8]) -> bool {
        self.reserved && self.bucket_id == bucket_id && self.key.as_slice() == key
    }

    /// Releases this reservation while the caller already holds `&mut StorageEngine`.
    pub(crate) fn release_in(&mut self, engine: &mut StorageEngine) {
        if self.reserved {
            engine.release_slot(self.bucket_id, &self.key);
            self.engine = None;
            self.reserved = false;
        }
    }

    /// Consumes this reservation and inserts the object atomically under one `engine.write()`.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_put_at(
        mut self,
        engine: &Arc<RwLock<StorageEngine>>,
        bucket_id: u32,
        key: &[u8],
        vol_offset: u64,
        size: u64,
        etag_hi: u64,
        etag_lo: u64,
        part_count: u16,
        flags: u16,
        created_at_ms: u64,
    ) -> Result<(), i32> {
        let mut guard = engine.write();
        self.release_in(&mut guard);
        guard.put_object_at(
            bucket_id,
            key,
            vol_offset,
            size,
            etag_hi,
            etag_lo,
            part_count,
            flags,
            created_at_ms,
        )
    }
}

impl Drop for CapacityReservation {
    fn drop(&mut self) {
        if self.reserved {
            if let Some(engine) = self.engine.take() {
                engine.write().release_slot(self.bucket_id, &self.key);
            }
            self.reserved = false;
        }
    }
}

pub type AwanStorageEngine = StorageEngine;

/// Milliseconds since the epoch, for the callers of `put_object` that pass no time (the FFI
/// path). Before #158 every insert read it here; the server's own paths pass their clock's.
fn wall_clock_ms() -> u64 {
    use crate::clock::Clock;
    crate::clock::SystemClock.now_ns() / 1_000_000
}

impl StorageEngine {
    pub fn new(max_objects: usize, key_arena_bytes: usize, slab_bytes: usize) -> Self {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::HotTable);
        Self {
            max_objects,
            key_arena: Vec::with_capacity(key_arena_bytes),
            table: ColumnarObjectTable::with_capacity(max_objects.min(1_000_000)),
            buckets: HashMap::new(),
            bucket_names: HashMap::new(),
            next_bucket_id: 1,
            hash_slots: HashMap::with_capacity(max_objects.min(1_000_000)),
            slab_bytes,
            spill_index: None,
            reserved_keys: HashMap::new(),
        }
    }

    pub fn attach_spill_index(&mut self, spill: SpillIndex) {
        self.spill_index = Some(spill);
    }

    pub fn create_bucket(&mut self, name: &str) -> Result<u32, i32> {
        self.create_bucket_in("", name)
    }

    pub fn create_bucket_in(&mut self, namespace: &str, name: &str) -> Result<u32, i32> {
        if name.len() < 3 || name.len() > 63 {
            return Err(ERR_INVALID_ARG);
        }
        if let Some(&id) = self.bucket_names.get(namespace).and_then(|m| m.get(name)) {
            return Ok(id);
        }
        let id = self.next_bucket_id;
        self.next_bucket_id += 1;
        self.bucket_names
            .entry(namespace.to_string())
            .or_default()
            .insert(name.to_string(), id);
        self.buckets.insert(
            id,
            BucketEntry {
                id,
                namespace: namespace.to_string(),
                name: name.to_string(),
                key_index: BTreeMap::new(),
                current_objects: 0,
                current_bytes: 0,
            },
        );
        Ok(id)
    }

    pub fn restore_bucket(&mut self, id: u32, name: &str) {
        self.restore_bucket_in(id, "", name);
    }

    pub fn restore_bucket_in(&mut self, id: u32, namespace: &str, name: &str) {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::HotTable);
        if id >= self.next_bucket_id {
            self.next_bucket_id = id + 1;
        }
        self.bucket_names
            .entry(namespace.to_string())
            .or_default()
            .insert(name.to_string(), id);
        self.buckets.entry(id).or_insert_with(|| BucketEntry {
            id,
            namespace: namespace.to_string(),
            name: name.to_string(),
            key_index: BTreeMap::new(),
            current_objects: 0,
            current_bytes: 0,
        });
    }

    pub fn lookup_bucket(&self, name: &str) -> Result<u32, i32> {
        self.lookup_bucket_in("", name)
    }

    pub fn lookup_bucket_in(&self, namespace: &str, name: &str) -> Result<u32, i32> {
        self.bucket_names
            .get(namespace)
            .and_then(|m| m.get(name))
            .copied()
            .ok_or(ERR_BUCKET_NOT_FOUND)
    }

    pub fn bucket_usage(&self, bucket_id: u32) -> Option<BucketUsage> {
        self.buckets.get(&bucket_id).map(|b| BucketUsage {
            objects: b.current_objects,
            bytes: b.current_bytes,
        })
    }

    pub fn delete_bucket(&mut self, bucket_id: u32) -> Result<(), i32> {
        let bucket = self.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        if !bucket.key_index.is_empty() || bucket.current_objects > 0 {
            return Err(ERR_BUCKET_NOT_EMPTY);
        }
        let ns = bucket.namespace.clone();
        let name = bucket.name.clone();
        self.buckets.remove(&bucket_id);
        if let Some(map) = self.bucket_names.get_mut(&ns) {
            map.remove(&name);
            if map.is_empty() {
                self.bucket_names.remove(&ns);
            }
        }
        Ok(())
    }

    pub fn update_object_offset(&mut self, bucket_id: u32, key: &[u8], new_offset: u64) -> Result<(), i32> {
        let bucket = self.buckets.get_mut(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        if let Some(&row_idx) = bucket.key_index.get(key) {
            self.table.vol_offsets[row_idx] = new_offset;
            return Ok(());
        }
        if let Some(ref spill) = self.spill_index {
            if let Ok(Some(mut meta)) = spill.get(bucket_id, key) {
                meta.vol_offset = new_offset;
                let _ = spill.put(bucket_id, key, &meta);
                return Ok(());
            }
        }
        Err(ERR_NOT_FOUND)
    }

    fn reserved_slots_in_use(&self) -> usize {
        self.reserved_keys
            .keys()
            .filter(|(b_id, key)| {
                self.buckets
                    .get(b_id)
                    .is_some_and(|b| !b.key_index.contains_key(key.as_slice()))
            })
            .count()
    }

    pub fn try_reserve_slot(&mut self, bucket_id: u32, key: &[u8]) -> Result<bool, i32> {
        if key.is_empty() || key.len() > 1024 {
            return Err(ERR_INVALID_ARG);
        }
        let bucket = self.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        if self.spill_index.is_some() || bucket.key_index.contains_key(key) {
            return Ok(false);
        }
        let map_key = (bucket_id, key.to_vec());
        if let Some(count) = self.reserved_keys.get_mut(&map_key) {
            *count += 1;
            return Ok(true);
        }
        if self.table.key_hashes.len() + self.reserved_slots_in_use() >= self.max_objects {
            return Err(ERR_CAPACITY);
        }
        self.reserved_keys.insert(map_key, 1);
        Ok(true)
    }

    pub fn release_slot(&mut self, bucket_id: u32, key: &[u8]) {
        if self.reserved_keys.is_empty() {
            return;
        }
        let map_key = (bucket_id, key.to_vec());
        if let Some(count) = self.reserved_keys.get_mut(&map_key) {
            if *count > 1 {
                *count -= 1;
            } else {
                self.reserved_keys.remove(&map_key);
            }
        }
    }

    pub(crate) fn has_room_with_reserved(&self, bucket_id: u32, key: &[u8]) -> Result<(), i32> {
        let bucket = self.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        if bucket.key_index.contains_key(key)
            || self.spill_index.is_some()
            || (self.table.key_hashes.len() < self.max_objects
                && self.table.key_hashes.len() + self.reserved_slots_in_use() <= self.max_objects)
        {
            Ok(())
        } else {
            Err(ERR_CAPACITY)
        }
    }

    /// Would `put_object` find room for this key? An existing key or a spill index always
    /// has room. Accounts for upfront `reserved_keys` slots so concurrent writers cannot steal
    /// the last row (#40, #356).
    pub fn has_room_for(&self, bucket_id: u32, key: &[u8]) -> Result<(), i32> {
        let bucket = self.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        if bucket.key_index.contains_key(key) || self.spill_index.is_some() {
            return Ok(());
        }
        if self.table.key_hashes.len() + self.reserved_slots_in_use() < self.max_objects
            || (self.table.key_hashes.len() < self.max_objects
                && !self.reserved_keys.is_empty()
                && self.reserved_keys.keys().any(|(b, k)| *b == bucket_id && k.as_slice() == key))
        {
            Ok(())
        } else {
            Err(ERR_CAPACITY)
        }
    }

    pub fn put_object(
        &mut self,
        bucket_id: u32,
        key: &[u8],
        vol_offset: u64,
        size: u64,
        etag_hi: u64,
        etag_lo: u64,
        part_count: u16,
        flags: u16,
    ) -> Result<(), i32> {
        let key_hash = xxh3_64_with_seed(key, bucket_id as u64);
        self.put_object_with_hash_at(bucket_id, key, key_hash, vol_offset, size, etag_hi, etag_lo, part_count, flags, wall_clock_ms())
    }

    pub fn put_object_at(
        &mut self,
        bucket_id: u32,
        key: &[u8],
        vol_offset: u64,
        size: u64,
        etag_hi: u64,
        etag_lo: u64,
        part_count: u16,
        flags: u16,
        created_at_ms: u64,
    ) -> Result<(), i32> {
        let key_hash = xxh3_64_with_seed(key, bucket_id as u64);
        self.put_object_with_hash_at(bucket_id, key, key_hash, vol_offset, size, etag_hi, etag_lo, part_count, flags, created_at_ms)
    }

    pub fn put_object_with_hash(
        &mut self,
        bucket_id: u32,
        key: &[u8],
        key_hash: u64,
        vol_offset: u64,
        size: u64,
        etag_hi: u64,
        etag_lo: u64,
        part_count: u16,
        flags: u16,
    ) -> Result<(), i32> {
        self.put_object_with_hash_at(bucket_id, key, key_hash, vol_offset, size, etag_hi, etag_lo, part_count, flags, wall_clock_ms())
    }

    pub fn put_object_with_hash_at(
        &mut self,
        bucket_id: u32,
        key: &[u8],
        key_hash: u64,
        vol_offset: u64,
        size: u64,
        etag_hi: u64,
        etag_lo: u64,
        part_count: u16,
        flags: u16,
        now_ms: u64,
    ) -> Result<(), i32> {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::HotTable);
        if key.is_empty() || key.len() > 1024 {
            return Err(ERR_INVALID_ARG);
        }
        let existing_row = {
            let bucket = self.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
            bucket.key_index.get(key).copied()
        };

        let active_flags = flags | FLAG_ACTIVE;
        let bucket_flags = pack_bucket_flags(bucket_id, part_count, active_flags);

        if let Some(row_idx) = existing_row {
            let old_meta = self.table.get(row_idx);
            let old_flags = unpack_flags(old_meta.bucket_flags);
            let was_live = old_flags & FLAG_ACTIVE != 0 && old_flags & FLAG_TOMBSTONE == 0;
            let meta = ObjectMeta {
                key_hash,
                bucket_flags,
                vol_offset,
                size,
                etag_hi,
                etag_lo,
                created_at: now_ms,
                key_slice: old_meta.key_slice,
            };
            self.table.update(row_idx, &meta);
            let bucket = self.buckets.get_mut(&bucket_id).expect("bucket checked");
            if was_live {
                bucket.current_bytes = bucket.current_bytes.saturating_sub(old_meta.size) + size;
            } else {
                bucket.current_objects += 1;
                bucket.current_bytes += size;
            }
            return Ok(());
        }

        // Past the in-memory rows, every key that is not in them goes to the spill index. The
        // table never shrinks (a delete leaves its row), so a key in the spill index cannot later
        // get a row in memory: no set of spilled keys is needed (#208).
        if let Some(ref spill) = self.spill_index {
            if self.table.key_hashes.len() >= self.max_objects {
                let meta = ObjectMeta {
                    key_hash,
                    bucket_flags,
                    vol_offset,
                    size,
                    etag_hi,
                    etag_lo,
                    created_at: now_ms,
                    key_slice: 0,
                };
                // A failed spill write must not be acknowledged: the object would be on the
                // volume but missing from the index until a restart.
                let prev = spill.put_replacing(bucket_id, key, &meta).map_err(|_| ERR_SPILL)?;
                let prev_live_size = prev.and_then(|p| {
                    let f = unpack_flags(p.bucket_flags);
                    (f & FLAG_ACTIVE != 0 && f & FLAG_TOMBSTONE == 0).then_some(p.size)
                });
                let bucket = self.buckets.get_mut(&bucket_id).expect("bucket checked");
                match prev_live_size {
                    Some(old_sz) => {
                        bucket.current_bytes = bucket.current_bytes.saturating_sub(old_sz) + size;
                    }
                    None => {
                        bucket.current_objects += 1;
                        bucket.current_bytes += size;
                    }
                }
                return Ok(());
            }
        }

        if self.table.key_hashes.len() >= self.max_objects {
            return Err(ERR_CAPACITY);
        }
        if !self.reserved_keys.is_empty() {
            let reserved = self.reserved_slots_in_use();
            let key_reserved = self
                .reserved_keys
                .keys()
                .any(|(b, k)| *b == bucket_id && k.as_slice() == key);
            if self.table.key_hashes.len() + reserved + usize::from(!key_reserved) > self.max_objects {
                return Err(ERR_CAPACITY);
            }
        }

        let arena_offset = self.key_arena.len();
        self.key_arena.extend_from_slice(key);
        let key_slice = pack_key_slice(arena_offset, key.len() as u16);

        let meta = ObjectMeta {
            key_hash,
            bucket_flags,
            vol_offset,
            size,
            etag_hi,
            etag_lo,
            created_at: now_ms,
            key_slice,
        };

        let row_idx = self.table.push(&meta);
        let bucket = self.buckets.get_mut(&bucket_id).expect("bucket checked");
        bucket.key_index.insert(key.to_vec(), row_idx);
        bucket.current_objects += 1;
        bucket.current_bytes += size;
        self.hash_slots.entry((bucket_id, key_hash)).or_default().push(row_idx);
        Ok(())
    }

    pub fn get_object(&self, bucket_id: u32, key: &[u8]) -> Result<ObjectMeta, i32> {
        let key_hash = xxh3_64_with_seed(key, bucket_id as u64);
        self.get_object_with_hash(bucket_id, key, key_hash)
    }

    pub fn get_object_with_hash(&self, bucket_id: u32, key: &[u8], key_hash: u64) -> Result<ObjectMeta, i32> {
        let _ = self.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;

        let disable_verify = FAULT_DISABLE_KEY_VERIFICATION.load(Ordering::Relaxed);

        if let Some(slots) = self.hash_slots.get(&(bucket_id, key_hash)) {
            for &row_idx in slots.iter().rev() {
                let meta = self.table.get(row_idx);
                let flags = unpack_flags(meta.bucket_flags);
                if flags & FLAG_TOMBSTONE != 0 || flags & FLAG_ACTIVE == 0 {
                    continue;
                }

                if disable_verify {
                    return Ok(meta);
                }

                let (offset, len) = unpack_key_slice(meta.key_slice);
                if offset + len <= self.key_arena.len() && &self.key_arena[offset..offset + len] == key {
                    return Ok(meta);
                }
            }
        }

        if let Some(ref spill) = self.spill_index {
            if let Ok(Some(meta)) = spill.get(bucket_id, key) {
                let flags = unpack_flags(meta.bucket_flags);
                if flags & FLAG_ACTIVE != 0 && flags & FLAG_TOMBSTONE == 0 {
                    return Ok(meta);
                }
            }
        }

        Err(ERR_NOT_FOUND)
    }

    pub fn delete_object(&mut self, bucket_id: u32, key: &[u8]) -> Result<(), i32> {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::HotTable);
        let bucket = self.buckets.get_mut(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        let row_idx_opt = bucket.key_index.remove(key);

        let mut in_memory_deleted_size: Option<u64> = None;
        if let Some(row_idx) = row_idx_opt {
            let mut meta = self.table.get(row_idx);
            let old_flags = unpack_flags(meta.bucket_flags);
            let old_sz = if old_flags & FLAG_ACTIVE != 0 && old_flags & FLAG_TOMBSTONE == 0 {
                meta.size
            } else {
                0
            };
            let part_count = unpack_part_count(meta.bucket_flags);
            meta.bucket_flags = pack_bucket_flags(bucket_id, part_count, FLAG_TOMBSTONE);
            self.table.update(row_idx, &meta);
            in_memory_deleted_size = Some(old_sz);
        }

        let mut spilled_deleted_size: Option<u64> = None;
        if let Some(ref spill) = self.spill_index {
            if in_memory_deleted_size.is_none() {
                spilled_deleted_size = spill
                    .delete_returning(bucket_id, key)
                    .map_err(|_| ERR_SPILL)?
                    .and_then(|p| {
                        let f = unpack_flags(p.bucket_flags);
                        (f & FLAG_ACTIVE != 0 && f & FLAG_TOMBSTONE == 0).then_some(p.size)
                    });
            }
        }

        if let Some(old_sz) = in_memory_deleted_size.or(spilled_deleted_size) {
            let bucket = self.buckets.get_mut(&bucket_id).expect("bucket checked");
            bucket.current_objects = bucket.current_objects.saturating_sub(1);
            bucket.current_bytes = bucket.current_bytes.saturating_sub(old_sz);
            Ok(())
        } else {
            Err(ERR_NOT_FOUND)
        }
    }

    /// Up to `limit` live objects of a bucket whose keys start with `prefix` and sort after
    /// `after` (exclusive), in key order, with their keys. In-memory rows and the spill index
    /// are merged; spilled rows have no key in the arena, which is why this returns keys.
    pub fn list_keys(&self, bucket_id: u32, prefix: &[u8], after: &[u8], limit: usize) -> Vec<(Vec<u8>, ObjectMeta)> {
        use std::ops::Bound;
        let Some(bucket) = self.buckets.get(&bucket_id) else { return Vec::new() };
        let mut merged: BTreeMap<Vec<u8>, ObjectMeta> = BTreeMap::new();
        let lower = if after >= prefix { Bound::Excluded(after.to_vec()) } else { Bound::Included(prefix.to_vec()) };
        for (k, &row_idx) in bucket.key_index.range::<Vec<u8>, _>((lower, Bound::Unbounded)) {
            if !k.starts_with(prefix) {
                break;
            }
            let flags = unpack_flags(self.table.bucket_flags[row_idx]);
            if flags & FLAG_ACTIVE != 0 && flags & FLAG_TOMBSTONE == 0 {
                merged.insert(k.clone(), self.table.get(row_idx));
                if merged.len() >= limit {
                    break;
                }
            }
        }
        if let Some(ref spill) = self.spill_index {
            let sa = if after.is_empty() { None } else { Some(after) };
            if let Ok(items) = spill.scan_prefix(bucket_id, prefix, sa, limit) {
                for (k, meta) in items {
                    let flags = unpack_flags(meta.bucket_flags);
                    if flags & FLAG_ACTIVE != 0 && flags & FLAG_TOMBSTONE == 0 && k.as_slice() > after {
                        merged.insert(k, meta);
                    }
                }
            }
        }
        merged.into_iter().take(limit).collect()
    }

    pub fn scan_objects(
        &self,
        bucket_id: u32,
        prefix: &[u8],
        start_after: &[u8],
        max_keys: usize,
        out_records: &mut [ObjectMeta],
    ) -> usize {
        let bucket = match self.buckets.get(&bucket_id) {
            Some(b) => b,
            None => return 0,
        };

        if self.spill_index.is_none() {
            let mut count = 0;
            let range: Box<dyn Iterator<Item = (&Vec<u8>, &usize)> + '_> = if !start_after.is_empty() {
                use std::ops::Bound;
                Box::new(bucket.key_index.range::<Vec<u8>, _>((Bound::Excluded(start_after.to_vec()), Bound::Unbounded)))
            } else if !prefix.is_empty() {
                use std::ops::Bound;
                Box::new(bucket.key_index.range::<Vec<u8>, _>((Bound::Included(prefix.to_vec()), Bound::Unbounded)))
            } else {
                Box::new(bucket.key_index.iter())
            };

            for (k, &row_idx) in range {
                if count >= max_keys || count >= out_records.len() {
                    break;
                }
                if !prefix.is_empty() && !k.starts_with(prefix) {
                    if k.as_slice() > prefix {
                        break;
                    }
                    continue;
                }

                let flags = unpack_flags(self.table.bucket_flags[row_idx]);
                if flags & FLAG_ACTIVE != 0 && flags & FLAG_TOMBSTONE == 0 {
                    out_records[count] = self.table.get(row_idx);
                    count += 1;
                }
            }
            return count;
        }

        // When spill index is attached, merge sorted in-memory keys and spilled keys
        let mut merged: BTreeMap<Vec<u8>, ObjectMeta> = BTreeMap::new();

        for (k, &row_idx) in &bucket.key_index {
            if !start_after.is_empty() && k.as_slice() <= start_after {
                continue;
            }
            if !prefix.is_empty() && !k.starts_with(prefix) {
                continue;
            }
            let flags = unpack_flags(self.table.bucket_flags[row_idx]);
            if flags & FLAG_ACTIVE != 0 && flags & FLAG_TOMBSTONE == 0 {
                merged.insert(k.clone(), self.table.get(row_idx));
            }
        }

        if let Some(ref spill) = self.spill_index {
            let sa_opt = if start_after.is_empty() { None } else { Some(start_after) };
            if let Ok(spilled_items) = spill.scan_prefix(bucket_id, prefix, sa_opt, max_keys) {
                for (k, meta) in spilled_items {
                    let flags = unpack_flags(meta.bucket_flags);
                    if flags & FLAG_ACTIVE != 0 && flags & FLAG_TOMBSTONE == 0 {
                        merged.insert(k, meta);
                    }
                }
            }
        }

        let mut count = 0;
        for (_k, meta) in merged {
            if count >= max_keys || count >= out_records.len() {
                break;
            }
            out_records[count] = meta;
            count += 1;
        }

        count
    }
}

// ---------------------------------------------------------------------------
// C-ABI extern "C" Entrypoints (with catch_unwind panic safety)
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn s3_storage_init(
    max_objects: u64,
    key_arena_bytes: u64,
    slab_bytes: u64,
) -> *mut std::ffi::c_void {
    let res = catch_unwind(|| {
        let engine = Box::new(StorageEngine::new(
            max_objects as usize,
            key_arena_bytes as usize,
            slab_bytes as usize,
        ));
        Box::into_raw(engine) as *mut std::ffi::c_void
    });
    res.unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub unsafe extern "C" fn s3_storage_destroy(engine: *mut std::ffi::c_void) {
    let _ = catch_unwind(|| {
        if !engine.is_null() {
            drop(Box::from_raw(engine as *mut StorageEngine));
        }
    });
}

#[no_mangle]
pub unsafe extern "C" fn s3_storage_bucket_create(
    engine: *mut std::ffi::c_void,
    name_ptr: *const u8,
    name_len: u32,
    out_bucket_id: *mut u32,
) -> i32 {
    let res = catch_unwind(|| {
        if engine.is_null() || name_ptr.is_null() || out_bucket_id.is_null() {
            return ERR_INVALID_ARG;
        }
        let engine = &mut *(engine as *mut StorageEngine);
        let name_slice = std::slice::from_raw_parts(name_ptr, name_len as usize);
        let name_str = match std::str::from_utf8(name_slice) {
            Ok(s) => s,
            Err(_) => return ERR_INVALID_ARG,
        };
        match engine.create_bucket(name_str) {
            Ok(id) => {
                *out_bucket_id = id;
                OK
            }
            Err(e) => e,
        }
    });
    res.unwrap_or(ERR_PANIC)
}

#[no_mangle]
pub unsafe extern "C" fn s3_storage_bucket_lookup(
    engine: *mut std::ffi::c_void,
    name_ptr: *const u8,
    name_len: u32,
    out_bucket_id: *mut u32,
) -> i32 {
    let res = catch_unwind(|| {
        if engine.is_null() || name_ptr.is_null() || out_bucket_id.is_null() {
            return ERR_INVALID_ARG;
        }
        let engine = &*(engine as *const StorageEngine);
        let name_slice = std::slice::from_raw_parts(name_ptr, name_len as usize);
        let name_str = match std::str::from_utf8(name_slice) {
            Ok(s) => s,
            Err(_) => return ERR_INVALID_ARG,
        };
        match engine.lookup_bucket(name_str) {
            Ok(id) => {
                *out_bucket_id = id;
                OK
            }
            Err(e) => e,
        }
    });
    res.unwrap_or(ERR_PANIC)
}

#[no_mangle]
pub unsafe extern "C" fn s3_storage_bucket_delete(
    engine: *mut std::ffi::c_void,
    bucket_id: u32,
) -> i32 {
    let res = catch_unwind(|| {
        if engine.is_null() {
            return ERR_INVALID_ARG;
        }
        let engine = &mut *(engine as *mut StorageEngine);
        match engine.delete_bucket(bucket_id) {
            Ok(()) => OK,
            Err(e) => e,
        }
    });
    res.unwrap_or(ERR_PANIC)
}

#[no_mangle]
pub unsafe extern "C" fn s3_storage_index_put(
    engine: *mut std::ffi::c_void,
    bucket_id: u32,
    key_ptr: *const u8,
    key_len: u16,
    vol_offset: u64,
    size: u64,
    etag_hi: u64,
    etag_lo: u64,
    part_count: u16,
    flags: u16,
) -> i32 {
    let res = catch_unwind(|| {
        if engine.is_null() || key_ptr.is_null() {
            return ERR_INVALID_ARG;
        }
        let engine = &mut *(engine as *mut StorageEngine);
        let key = std::slice::from_raw_parts(key_ptr, key_len as usize);
        match engine.put_object(
            bucket_id,
            key,
            vol_offset,
            size,
            etag_hi,
            etag_lo,
            part_count,
            flags,
        ) {
            Ok(()) => OK,
            Err(e) => e,
        }
    });
    res.unwrap_or(ERR_PANIC)
}

#[no_mangle]
pub unsafe extern "C" fn s3_storage_index_get(
    engine: *mut std::ffi::c_void,
    bucket_id: u32,
    key_ptr: *const u8,
    key_len: u16,
    out_meta: *mut ObjectMeta,
) -> i32 {
    let res = catch_unwind(|| {
        if engine.is_null() || key_ptr.is_null() || out_meta.is_null() {
            return ERR_INVALID_ARG;
        }
        let engine = &*(engine as *const StorageEngine);
        let key = std::slice::from_raw_parts(key_ptr, key_len as usize);
        match engine.get_object(bucket_id, key) {
            Ok(meta) => {
                *out_meta = meta;
                OK
            }
            Err(e) => e,
        }
    });
    res.unwrap_or(ERR_PANIC)
}

#[no_mangle]
pub unsafe extern "C" fn s3_storage_index_delete(
    engine: *mut std::ffi::c_void,
    bucket_id: u32,
    key_ptr: *const u8,
    key_len: u16,
) -> i32 {
    let res = catch_unwind(|| {
        if engine.is_null() || key_ptr.is_null() {
            return ERR_INVALID_ARG;
        }
        let engine = &mut *(engine as *mut StorageEngine);
        let key = std::slice::from_raw_parts(key_ptr, key_len as usize);
        match engine.delete_object(bucket_id, key) {
            Ok(()) => OK,
            Err(e) => e,
        }
    });
    res.unwrap_or(ERR_PANIC)
}

#[no_mangle]
pub unsafe extern "C" fn s3_storage_index_scan(
    engine: *mut std::ffi::c_void,
    bucket_id: u32,
    prefix_ptr: *const u8,
    prefix_len: u16,
    start_after_ptr: *const u8,
    start_after_len: u16,
    max_keys: u32,
    out_records: *mut ObjectMeta,
) -> i64 {
    let res = catch_unwind(|| {
        if engine.is_null() || out_records.is_null() {
            return -1;
        }
        let engine = &*(engine as *const StorageEngine);
        let prefix = if prefix_ptr.is_null() || prefix_len == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(prefix_ptr, prefix_len as usize)
        };
        let start_after = if start_after_ptr.is_null() || start_after_len == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(start_after_ptr, start_after_len as usize)
        };
        let out_slice = std::slice::from_raw_parts_mut(out_records, max_keys as usize);
        let count = engine.scan_objects(bucket_id, prefix, start_after, max_keys as usize, out_slice);
        count as i64
    });
    res.unwrap_or(ERR_PANIC as i64)
}

// ---------------------------------------------------------------------------
// Backward Compatibility C-ABI Symbols
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn awan_storage_init(max_objects: u64, key_arena_bytes: u64, slab_bytes: u64) -> *mut std::ffi::c_void {
    s3_storage_init(max_objects, key_arena_bytes, slab_bytes)
}

#[no_mangle]
pub unsafe extern "C" fn awan_storage_destroy(engine: *mut std::ffi::c_void) {
    s3_storage_destroy(engine)
}

#[no_mangle]
pub unsafe extern "C" fn awan_storage_bucket_create(engine: *mut std::ffi::c_void, name_ptr: *const u8, name_len: u32, out_bucket_id: *mut u32) -> i32 {
    s3_storage_bucket_create(engine, name_ptr, name_len, out_bucket_id)
}

#[no_mangle]
pub unsafe extern "C" fn awan_storage_bucket_lookup(engine: *mut std::ffi::c_void, name_ptr: *const u8, name_len: u32, out_bucket_id: *mut u32) -> i32 {
    s3_storage_bucket_lookup(engine, name_ptr, name_len, out_bucket_id)
}

#[no_mangle]
pub unsafe extern "C" fn awan_storage_bucket_delete(engine: *mut std::ffi::c_void, bucket_id: u32) -> i32 {
    s3_storage_bucket_delete(engine, bucket_id)
}

#[no_mangle]
pub unsafe extern "C" fn awan_storage_index_put(engine: *mut std::ffi::c_void, bucket_id: u32, key_ptr: *const u8, key_len: u16, vol_offset: u64, size: u64, etag_hi: u64, etag_lo: u64, part_count: u16, flags: u16) -> i32 {
    s3_storage_index_put(engine, bucket_id, key_ptr, key_len, vol_offset, size, etag_hi, etag_lo, part_count, flags)
}

#[no_mangle]
pub unsafe extern "C" fn awan_storage_index_get(engine: *mut std::ffi::c_void, bucket_id: u32, key_ptr: *const u8, key_len: u16, out_meta: *mut ObjectMeta) -> i32 {
    s3_storage_index_get(engine, bucket_id, key_ptr, key_len, out_meta)
}

#[no_mangle]
pub unsafe extern "C" fn awan_storage_index_delete(engine: *mut std::ffi::c_void, bucket_id: u32, key_ptr: *const u8, key_len: u16) -> i32 {
    s3_storage_index_delete(engine, bucket_id, key_ptr, key_len)
}

#[no_mangle]
pub unsafe extern "C" fn awan_storage_index_scan(engine: *mut std::ffi::c_void, bucket_id: u32, prefix_ptr: *const u8, prefix_len: u16, start_after_ptr: *const u8, start_after_len: u16, max_keys: u32, out_records: *mut ObjectMeta) -> i64 {
    s3_storage_index_scan(engine, bucket_id, prefix_ptr, prefix_len, start_after_ptr, start_after_len, max_keys, out_records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_reservation_blocks_other_keys_shares_same_key_and_releases_on_drop() {
        let engine = Arc::new(RwLock::new(StorageEngine::new(2, 4096, 4096)));
        let b_id = engine.write().create_bucket("bkt").unwrap();
        engine.write().put_object_at(b_id, b"k0", 4096, 1, 0, 0, 0, 0, 1).unwrap();

        let r1 = CapacityReservation::acquire(&engine, b_id, b"k1").unwrap();
        assert_eq!(CapacityReservation::acquire(&engine, b_id, b"k2").unwrap_err(), ERR_CAPACITY);
        assert_eq!(engine.read().has_room_for(b_id, b"k2").unwrap_err(), ERR_CAPACITY);
        assert_eq!(engine.write().put_object_at(b_id, b"k2", 8192, 1, 0, 0, 0, 0, 1).unwrap_err(), ERR_CAPACITY);

        // Concurrent writer on the same key shares the reserved slot, then drops without leaking.
        let r1_dup = CapacityReservation::acquire(&engine, b_id, b"k1").unwrap();
        r1_dup.commit_put_at(&engine, b_id, b"k1", 8192, 2, 0, 0, 0, 0, 2).unwrap();
        assert_eq!(engine.read().reserved_slots_in_use(), 0);

        // Existing key still fits for r1, and dropping r1 clears reserved_keys.
        let mut r1 = r1;
        r1.ensure_room(&engine, b_id, b"k1").unwrap();
        drop(r1);
        assert!(engine.read().reserved_keys.is_empty());
    }

    #[test]
    fn capacity_reservation_ensure_room_detects_midstream_commit_and_delete() {
        let engine = Arc::new(RwLock::new(StorageEngine::new(2, 4096, 4096)));
        let b_id = engine.write().create_bucket("bkt").unwrap();
        engine.write().put_object_at(b_id, b"k0", 4096, 1, 0, 0, 0, 0, 1).unwrap();

        let mut r_stream = CapacityReservation::acquire(&engine, b_id, b"k1").unwrap();
        let r_buf = CapacityReservation::acquire(&engine, b_id, b"k1").unwrap();
        r_buf.commit_put_at(&engine, b_id, b"k1", 8192, 2, 0, 0, 0, 0, 2).unwrap();
        engine.write().delete_object(b_id, b"k1").unwrap();

        assert_eq!(r_stream.ensure_room(&engine, b_id, b"k1").unwrap_err(), ERR_CAPACITY);
        assert!(engine.read().reserved_keys.is_empty());
    }
}
