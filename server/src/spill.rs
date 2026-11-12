//! awan-storage/server/src/spill.rs
//! Embedded secondary key-value index for metadata overflow spilling (>1M keys)
//! Powered by redb (pure-Rust ACID zero-copy embedded B-Tree engine).

use std::path::Path;
use std::sync::Arc;
use redb::{Database, Durability, TableDefinition};
use crate::meta_index::ObjectMeta;

const META_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("object_metadata");

/// Page cache of the spill database (#13).
pub const SPILL_CACHE_BYTES: usize = 64 << 20;

#[derive(Clone)]
pub struct SpillIndex {
    db: Arc<Database>,
}

fn make_db_key(bucket_id: u32, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len());
    k.extend_from_slice(&bucket_id.to_be_bytes());
    k.extend_from_slice(key);
    k
}

fn encode_meta(meta: &ObjectMeta) -> [u8; 64] {
    let mut buf = [0u8; 64];
    buf[0..8].copy_from_slice(&meta.key_hash.to_le_bytes());
    buf[8..16].copy_from_slice(&meta.bucket_flags.to_le_bytes());
    buf[16..24].copy_from_slice(&meta.vol_offset.to_le_bytes());
    buf[24..32].copy_from_slice(&meta.size.to_le_bytes());
    buf[32..40].copy_from_slice(&meta.etag_hi.to_le_bytes());
    buf[40..48].copy_from_slice(&meta.etag_lo.to_le_bytes());
    buf[48..56].copy_from_slice(&meta.created_at.to_le_bytes());
    buf[56..64].copy_from_slice(&meta.key_slice.to_le_bytes());
    buf
}

fn decode_meta(buf: &[u8]) -> Option<ObjectMeta> {
    if buf.len() < 64 {
        return None;
    }
    Some(ObjectMeta {
        key_hash: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        bucket_flags: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        vol_offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        size: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        etag_hi: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
        etag_lo: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
        created_at: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
        key_slice: u64::from_le_bytes(buf[56..64].try_into().unwrap()),
    })
}

impl SpillIndex {
    pub fn open(path: &Path) -> Result<Self, redb::Error> {
        // A bounded page cache: the spill exists to keep memory bounded (#13). redb's default
        // is about 1 GiB.
        let db = Database::builder().set_cache_size(SPILL_CACHE_BYTES).create(path)?;

        // Initialize table if needed
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(META_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Empties the index. The server does this at boot and rebuilds it from the volume
    /// replay, so its non-durable commits never decide what exists.
    pub fn clear(&self) -> Result<(), redb::Error> {
        let mut write_txn = self.db.begin_write()?;
        write_txn.set_durability(Durability::None);
        write_txn.delete_table(META_TABLE)?;
        {
            let _ = write_txn.open_table(META_TABLE)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn put(&self, bucket_id: u32, key: &[u8], meta: &ObjectMeta) -> Result<(), redb::Error> {
        let db_key = make_db_key(bucket_id, key);
        let encoded = encode_meta(meta);
        let mut write_txn = self.db.begin_write()?;
        // Not fsynced: the volumes are the record, and boot rebuilds this index from them.
        write_txn.set_durability(Durability::None);
        {
            let mut table = write_txn.open_table(META_TABLE)?;
            table.insert(&db_key[..], &encoded[..])?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get(&self, bucket_id: u32, key: &[u8]) -> Result<Option<ObjectMeta>, redb::Error> {
        let db_key = make_db_key(bucket_id, key);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(META_TABLE)?;
        let val_opt = table.get(&db_key[..])?;
        match val_opt {
            Some(v) => Ok(decode_meta(v.value())),
            None => Ok(None),
        }
    }

    pub fn delete(&self, bucket_id: u32, key: &[u8]) -> Result<bool, redb::Error> {
        let db_key = make_db_key(bucket_id, key);
        let mut write_txn = self.db.begin_write()?;
        write_txn.set_durability(Durability::None);
        let existed = {
            let mut table = write_txn.open_table(META_TABLE)?;
            let rem = table.remove(&db_key[..])?;
            rem.is_some()
        };
        write_txn.commit()?;
        Ok(existed)
    }

    pub fn scan_prefix(
        &self,
        bucket_id: u32,
        prefix: &[u8],
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, ObjectMeta)>, redb::Error> {
        let prefix_key = make_db_key(bucket_id, prefix);
        let start_key = if let Some(sa) = start_after {
            make_db_key(bucket_id, sa)
        } else {
            prefix_key.clone()
        };

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(META_TABLE)?;

        let mut results = Vec::new();
        let b_id_prefix = bucket_id.to_be_bytes();

        use std::ops::Bound;
        let start_bound = if start_after.is_some() {
            Bound::Excluded(&start_key[..])
        } else {
            Bound::Included(&start_key[..])
        };

        let range = table.range::<&[u8]>((start_bound, Bound::Unbounded))?;
        for entry in range {
            if results.len() >= limit {
                break;
            }
            let (k, v) = entry?;
            let k_bytes = k.value();
            if k_bytes.len() < 4 || &k_bytes[0..4] != &b_id_prefix[..] {
                // Beyond this bucket's range
                break;
            }
            let user_key = &k_bytes[4..];
            if !prefix.is_empty() && !user_key.starts_with(prefix) {
                if user_key > prefix {
                    break;
                }
                continue;
            }
            if let Some(meta) = decode_meta(v.value()) {
                results.push((user_key.to_vec(), meta));
            }
        }

        Ok(results)
    }
}
