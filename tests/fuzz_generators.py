#!/usr/bin/env python3
"""
awan-storage/tests/fuzz_generators.py
Metamorphic and Randomized S3 Request Generators for Differential Fuzzing (#465)
"""

import hashlib
import random
import string

KEYS_POOL = [
    "simple_key.txt",
    "deep/nested/path/to/object.json",
    "with space/file.bin",
    "special_chars_+_&_=.dat",
    "folder_marker/",
    "nested/folder/",
    "utf8_üñîçødé_日本語.txt",
    "a" * 1024, # max 1024B key
]

SIZES_POOL = [
    0,
    1,
    13,
    4095,
    4096,
    4097,
    64 * 1024,
    5 * 1024 * 1024,
]

def generate_random_key(rng: random.Random) -> str:
    choice = rng.random()
    if choice < 0.6:
        return rng.choice(KEYS_POOL)
    elif choice < 0.8:
        # Random path with 1-4 segments
        depth = rng.randint(1, 4)
        segs = [''.join(rng.choices(string.ascii_letters + string.digits, k=rng.randint(3, 10))) for _ in range(depth)]
        return '/'.join(segs)
    else:
        # Key with special URL characters
        chars = string.ascii_letters + string.digits + "-_.~"
        base = ''.join(rng.choices(chars, k=15))
        return f"{base}_{rng.choice(['foo+bar', 'foo&bar', 'foo=bar', 'foo%20bar'])}"

def generate_random_payload(rng: random.Random, size: int) -> bytes:
    if size == 0:
        return b""
    # Deterministic pattern based on size to keep generation fast
    pattern = rng.randbytes(min(size, 256))
    repeats = (size + len(pattern) - 1) // len(pattern)
    return (pattern * repeats)[:size]

def generate_random_range(rng: random.Random, size: int):
    if size == 0:
        return "bytes=0-0"
    mode = rng.randint(0, 3)
    if mode == 0:
        # Valid closed interval
        start = rng.randint(0, size - 1)
        end = rng.randint(start, min(size - 1, start + 1024))
        return f"bytes={start}-{end}"
    elif mode == 1:
        # Start only
        start = rng.randint(0, size - 1)
        return f"bytes={start}-"
    elif mode == 2:
        # Suffix
        suffix = rng.randint(1, min(size, 1024))
        return f"bytes=-{suffix}"
    else:
        # Out of bounds / unsatisfiable
        return f"bytes={size + 100}-{size + 200}"

class ReferenceS3Model:
    """In-memory formal reference model of standard RFC 9110 / Amazon S3 semantics."""
    def __init__(self):
        self.buckets = {} # bucket_name -> { key -> (payload_bytes, etag_str) }

    def create_bucket(self, bucket: str) -> int:
        if bucket in self.buckets:
            return 200 # idempotency
        self.buckets[bucket] = {}
        return 200

    def delete_bucket(self, bucket: str) -> int:
        if bucket not in self.buckets:
            return 404
        if len(self.buckets[bucket]) > 0:
            return 409 # BucketNotEmpty
        del self.buckets[bucket]
        return 204

    def put_object(self, bucket: str, key: str, payload: bytes) -> tuple:
        if bucket not in self.buckets:
            return (404, None, None, "NoSuchBucket")
        etag = f'"{hashlib.md5(payload).hexdigest()}"'
        self.buckets[bucket][key] = (payload, etag)
        return (200, etag, len(payload), None)

    def get_object(self, bucket: str, key: str, byte_range: str = None) -> tuple:
        if bucket not in self.buckets:
            return (404, None, None, b"", "NoSuchBucket")
        if key not in self.buckets[bucket]:
            return (404, None, None, b"", "NoSuchKey")
        payload, etag = self.buckets[bucket][key]
        if byte_range is None:
            return (200, etag, len(payload), payload, None)

        # Range parsing RFC 9110
        if not byte_range.startswith("bytes="):
            return (200, etag, len(payload), payload, None)
        spec = byte_range[6:].strip()
        size = len(payload)

        if spec.startswith("-"):
            suffix = int(spec[1:])
            start = max(0, size - suffix)
            end = size - 1
        elif "-" in spec:
            parts = spec.split("-", 1)
            start = int(parts[0])
            if parts[1]:
                end = min(size - 1, int(parts[1]))
            else:
                end = size - 1
        else:
            return (200, etag, size, payload, None)

        if start >= size or start > end:
            return (416, etag, 0, b"", "InvalidRange")

        slice_data = payload[start:end + 1]
        return (206, etag, len(slice_data), slice_data, None)

    def delete_object(self, bucket: str, key: str) -> int:
        if bucket not in self.buckets:
            return 404
        self.buckets[bucket].pop(key, None)
        return 204
