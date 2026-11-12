/* awan_storage.h — C-ABI FFI Boundary for Awan Storage Engine
 * Reference: awan-storage/HYBRID_POC_SPECIFICATION.md Section 3
 */

#ifndef AWAN_STORAGE_H
#define AWAN_STORAGE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// 64-byte cache-line-aligned metadata record
typedef struct {
    uint64_t key_hash;      // 64-bit keyed XXH3 / SipHash-1-3 of (bucket_id, key)
    uint64_t bucket_flags;  // [63:32] bucket_id | [31:16] part_count | [15:0] flags
    uint64_t vol_offset;    // [63:48] volume_id | [47:0] 4KB-aligned offset or PartExtent idx
    uint64_t size;          // Decoded payload byte length
    uint64_t etag_hi;       // Upper 64 bits of 128-bit MD5 digest
    uint64_t etag_lo;       // Lower 64 bits of 128-bit MD5 digest
    uint64_t created_at;    // Unix epoch timestamp (ms)
    uint64_t key_slice;     // [63:16] Key String Arena offset | [15:0] key_len
} AwanObjectMeta;

// Bit flags for bucket_flags [15:0]
#define AWAN_FLAG_ACTIVE      0x0001
#define AWAN_FLAG_TOMBSTONE   0x0002
#define AWAN_FLAG_MULTIPART   0x0004
#define AWAN_FLAG_SHARED_REF  0x0008
#define AWAN_FLAG_ZERO_BYTE   0x0010

// Return codes (all negative values represent structured errors; panics never unwind)
#define AWAN_OK                     0
#define AWAN_ERR_NOT_FOUND         -1
#define AWAN_ERR_BUCKET_NOT_FOUND  -2
#define AWAN_ERR_BUCKET_NOT_EMPTY  -3
#define AWAN_ERR_PRECONDITION      -4
#define AWAN_ERR_CAPACITY          -5
#define AWAN_ERR_INVALID_ARG       -6
#define AWAN_ERR_SPILL             -7
#define AWAN_ERR_PANIC           -999

// Engine lifecycle
void*   awan_storage_init(uint64_t max_objects, uint64_t key_arena_bytes, uint64_t slab_bytes);
void    awan_storage_destroy(void* engine);

// Bucket operations
int32_t awan_storage_bucket_create(void* engine, const uint8_t* name_ptr, uint32_t name_len, uint32_t* out_bucket_id);
int32_t awan_storage_bucket_lookup(void* engine, const uint8_t* name_ptr, uint32_t name_len, uint32_t* out_bucket_id);
int32_t awan_storage_bucket_delete(void* engine, uint32_t bucket_id);

// Object index operations (with full key verification in Key String Arena)
int32_t awan_storage_index_put(
    void* engine,
    uint32_t bucket_id,
    const uint8_t* key_ptr,
    uint16_t key_len,
    uint64_t vol_offset,
    uint64_t size,
    uint64_t etag_hi,
    uint64_t etag_lo,
    uint16_t part_count,
    uint16_t flags
);

int32_t awan_storage_index_get(
    void* engine,
    uint32_t bucket_id,
    const uint8_t* key_ptr,
    uint16_t key_len,
    AwanObjectMeta* out_meta
);

int32_t awan_storage_index_delete(
    void* engine,
    uint32_t bucket_id,
    const uint8_t* key_ptr,
    uint16_t key_len
);

// Lexicographical UTF-8 prefix scan for ListObjectsV2
int64_t awan_storage_index_scan(
    void* engine,
    uint32_t bucket_id,
    const uint8_t* prefix_ptr,
    uint16_t prefix_len,
    const uint8_t* start_after_ptr,
    uint16_t start_after_len,
    uint32_t max_keys,
    AwanObjectMeta* out_records
);

#ifdef __cplusplus
}
#endif

#endif /* AWAN_STORAGE_H */
