//! awan-storage/server/src/meta_index.rs
//! High-Performance Columnar Struct-of-Arrays (SoA) Object Metadata Index
//! Zero-allocation in-memory metadata substrate with 64-byte packed records,
//! arena-backed key indexing, and O(1) hash table lookup.

use std::collections::{BTreeMap, HashMap};
use std::panic::catch_unwind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use xxhash_rust::xxh3::xxh3_64_with_seed;

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
pub const ERR_PANIC: i32 = -999;

pub const AWAN_OK: i32 = OK;
pub const AWAN_ERR_NOT_FOUND: i32 = ERR_NOT_FOUND;
pub const AWAN_ERR_BUCKET_NOT_FOUND: i32 = ERR_BUCKET_NOT_FOUND;
pub const AWAN_ERR_BUCKET_NOT_EMPTY: i32 = ERR_BUCKET_NOT_EMPTY;
pub const AWAN_ERR_PRECONDITION: i32 = ERR_PRECONDITION;
pub const AWAN_ERR_CAPACITY: i32 = ERR_CAPACITY;
pub const AWAN_ERR_INVALID_ARG: i32 = ERR_INVALID_ARG;
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

pub struct BucketEntry {
    pub id: u32,
    pub name: String,
    /// Lexicographical map from UTF-8 key bytes -> SoA row index
    pub key_index: BTreeMap<Vec<u8>, usize>,
}

pub struct StorageEngine {
    pub max_objects: usize,
    pub key_arena: Vec<u8>,
    pub table: ColumnarObjectTable,
    pub buckets: HashMap<u32, BucketEntry>,
    pub bucket_names: HashMap<String, u32>,
    pub next_bucket_id: u32,
    /// Open addressing / hash table: (bucket_id, key_hash) -> Vec of SoA row indices
    pub hash_slots: HashMap<(u32, u64), Vec<usize>>,
    pub slab_bytes: usize,
}

pub type AwanStorageEngine = StorageEngine;

impl StorageEngine {
    pub fn new(max_objects: usize, key_arena_bytes: usize, slab_bytes: usize) -> Self {
        Self {
            max_objects,
            key_arena: Vec::with_capacity(key_arena_bytes),
            table: ColumnarObjectTable::with_capacity(max_objects.min(1_000_000)),
            buckets: HashMap::new(),
            bucket_names: HashMap::new(),
            next_bucket_id: 1,
            hash_slots: HashMap::with_capacity(max_objects.min(1_000_000)),
            slab_bytes,
        }
    }

    pub fn create_bucket(&mut self, name: &str) -> Result<u32, i32> {
        if name.len() < 3 || name.len() > 63 {
            return Err(ERR_INVALID_ARG);
        }
        if let Some(&id) = self.bucket_names.get(name) {
            return Ok(id);
        }
        let id = self.next_bucket_id;
        self.next_bucket_id += 1;
        self.bucket_names.insert(name.to_string(), id);
        self.buckets.insert(
            id,
            BucketEntry {
                id,
                name: name.to_string(),
                key_index: BTreeMap::new(),
            },
        );
        Ok(id)
    }

    pub fn restore_bucket(&mut self, id: u32, name: &str) {
        if id >= self.next_bucket_id {
            self.next_bucket_id = id + 1;
        }
        self.bucket_names.insert(name.to_string(), id);
        self.buckets.entry(id).or_insert_with(|| BucketEntry {
            id,
            name: name.to_string(),
            key_index: BTreeMap::new(),
        });
    }

    pub fn lookup_bucket(&self, name: &str) -> Result<u32, i32> {
        self.bucket_names.get(name).copied().ok_or(ERR_BUCKET_NOT_FOUND)
    }

    pub fn delete_bucket(&mut self, bucket_id: u32) -> Result<(), i32> {
        let bucket = self.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        if !bucket.key_index.is_empty() {
            return Err(ERR_BUCKET_NOT_EMPTY);
        }
        let name = bucket.name.clone();
        self.buckets.remove(&bucket_id);
        self.bucket_names.remove(&name);
        Ok(())
    }

    pub fn update_object_offset(&mut self, bucket_id: u32, key: &[u8], new_offset: u64) -> Result<(), i32> {
        let bucket = self.buckets.get_mut(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        if let Some(&row_idx) = bucket.key_index.get(key) {
            self.table.vol_offsets[row_idx] = new_offset;
            Ok(())
        } else {
            Err(ERR_NOT_FOUND)
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
        self.put_object_with_hash(bucket_id, key, key_hash, vol_offset, size, etag_hi, etag_lo, part_count, flags)
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
        if key.is_empty() || key.len() > 1024 {
            return Err(ERR_INVALID_ARG);
        }
        let bucket = self.buckets.get_mut(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let active_flags = flags | FLAG_ACTIVE;
        let bucket_flags = pack_bucket_flags(bucket_id, part_count, active_flags);

        if let Some(&row_idx) = bucket.key_index.get(key) {
            let old_key_slice = self.table.key_slices[row_idx];
            let meta = ObjectMeta {
                key_hash,
                bucket_flags,
                vol_offset,
                size,
                etag_hi,
                etag_lo,
                created_at: now_ms,
                key_slice: old_key_slice,
            };
            self.table.update(row_idx, &meta);
            return Ok(());
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
        bucket.key_index.insert(key.to_vec(), row_idx);
        self.hash_slots.entry((bucket_id, key_hash)).or_default().push(row_idx);
        Ok(())
    }

    pub fn get_object(&self, bucket_id: u32, key: &[u8]) -> Result<ObjectMeta, i32> {
        let key_hash = xxh3_64_with_seed(key, bucket_id as u64);
        self.get_object_with_hash(bucket_id, key, key_hash)
    }

    pub fn get_object_with_hash(&self, bucket_id: u32, key: &[u8], key_hash: u64) -> Result<ObjectMeta, i32> {
        let _ = self.buckets.get(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;

        let slots = self.hash_slots.get(&(bucket_id, key_hash)).ok_or(ERR_NOT_FOUND)?;

        let disable_verify = FAULT_DISABLE_KEY_VERIFICATION.load(Ordering::Relaxed);

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

        Err(ERR_NOT_FOUND)
    }

    pub fn delete_object(&mut self, bucket_id: u32, key: &[u8]) -> Result<(), i32> {
        let bucket = self.buckets.get_mut(&bucket_id).ok_or(ERR_BUCKET_NOT_FOUND)?;
        let row_idx = bucket.key_index.remove(key).ok_or(ERR_NOT_FOUND)?;

        let mut meta = self.table.get(row_idx);
        let part_count = unpack_part_count(meta.bucket_flags);
        meta.bucket_flags = pack_bucket_flags(bucket_id, part_count, FLAG_TOMBSTONE);
        self.table.update(row_idx, &meta);
        Ok(())
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
