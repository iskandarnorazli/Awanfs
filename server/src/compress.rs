//! Opt-in per-bucket compression (#51, owner decisions 7 and 22): zstd level 1 in independent
//! 64 KiB blocks, so a range GET decodes only the blocks it overlaps.
//!
//! A compressed payload (needle flag `FLAG_COMPRESSED`) is:
//!
//! | Bytes | Field |
//! |---|---|
//! | 0..4 | magic `AWZ1` |
//! | 4..8 | block size (logical bytes per block) |
//! | 8..16 | logical size |
//! | 16..20 | block count n |
//! | 20..20+4n | stored length of each block |
//! | then | the blocks, each one zstd frame |
//!
//! An object is stored as it is when its first block does not shrink by at least an eighth,
//! or when the whole encoding is not smaller than the object.

pub const BLOCK: usize = 64 * 1024;
const MAGIC: &[u8; 4] = b"AWZ1";
const LEVEL: i32 = 1;
/// Bytes before the block table; recovery reads the logical size from them.
pub const PREFIX_LEN: usize = 20;
/// Objects smaller than this fill one sector anyway.
const MIN_SIZE: usize = 4096;
/// Largest block size a payload may declare (guards a corrupt or hostile header).
const MAX_BLOCK: u32 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressError {
    /// The payload does not follow the layout above.
    Malformed(&'static str),
    /// A block did not decode to its expected length.
    BadBlock(usize),
}

impl std::fmt::Display for CompressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressError::Malformed(why) => write!(f, "compressed payload is malformed: {}", why),
            CompressError::BadBlock(i) => write!(f, "compressed block {} does not decode", i),
        }
    }
}

/// The compressed payload of `data`, or `None` when it should be stored as it is.
pub fn encode(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < MIN_SIZE {
        return None;
    }
    let mut blocks = data.chunks(BLOCK);
    let first_raw = blocks.next()?;
    let first = zstd::bulk::compress(first_raw, LEVEL).ok()?;
    if first.len() * 8 > first_raw.len() * 7 {
        return None;
    }
    let mut frames = vec![first];
    for b in blocks {
        frames.push(zstd::bulk::compress(b, LEVEL).ok()?);
    }
    let total = PREFIX_LEN + 4 * frames.len() + frames.iter().map(|f| f.len()).sum::<usize>();
    if total >= data.len() {
        return None;
    }
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(BLOCK as u32).to_le_bytes());
    out.extend_from_slice(&(data.len() as u64).to_le_bytes());
    out.extend_from_slice(&(frames.len() as u32).to_le_bytes());
    for f in &frames {
        out.extend_from_slice(&(f.len() as u32).to_le_bytes());
    }
    for f in &frames {
        out.extend_from_slice(f);
    }
    Some(out)
}

/// The logical size in a payload's first bytes, if they start a compressed payload.
pub fn logical_size(prefix: &[u8]) -> Option<u64> {
    if prefix.len() < PREFIX_LEN || &prefix[0..4] != MAGIC {
        return None;
    }
    Some(u64::from_le_bytes(prefix[8..16].try_into().unwrap()))
}

struct Layout {
    block: usize,
    logical: u64,
    /// Byte range of each block inside the payload.
    frames: Vec<(usize, usize)>,
}

fn layout(stored: &[u8]) -> Result<Layout, CompressError> {
    let logical = logical_size(stored).ok_or(CompressError::Malformed("no AWZ1 header"))?;
    let block = u32::from_le_bytes(stored[4..8].try_into().unwrap());
    if block == 0 || block > MAX_BLOCK {
        return Err(CompressError::Malformed("block size"));
    }
    let n = u32::from_le_bytes(stored[16..20].try_into().unwrap()) as u64;
    if n != logical.div_ceil(block as u64) {
        return Err(CompressError::Malformed("block count does not match the size"));
    }
    let table_end = (n as usize).checked_mul(4).and_then(|t| t.checked_add(PREFIX_LEN)).ok_or(CompressError::Malformed("table"))?;
    if table_end > stored.len() {
        return Err(CompressError::Malformed("table runs past the payload"));
    }
    let mut frames = Vec::with_capacity(n as usize);
    let mut at = table_end;
    for i in 0..n as usize {
        let p = PREFIX_LEN + 4 * i;
        let len = u32::from_le_bytes(stored[p..p + 4].try_into().unwrap()) as usize;
        let end = at.checked_add(len).filter(|&e| e <= stored.len()).ok_or(CompressError::Malformed("block runs past the payload"))?;
        frames.push((at, end));
        at = end;
    }
    if at != stored.len() {
        return Err(CompressError::Malformed("bytes after the last block"));
    }
    Ok(Layout { block: block as usize, logical, frames })
}

/// Logical bytes `start..end` (end exclusive, clamped to the size), and how many blocks were
/// decoded to get them: only the blocks the range overlaps.
pub fn decode_range(stored: &[u8], start: u64, end: u64) -> Result<(Vec<u8>, usize), CompressError> {
    let l = layout(stored)?;
    let end = end.min(l.logical);
    if start >= end {
        return Ok((Vec::new(), 0));
    }
    let first = (start / l.block as u64) as usize;
    let last = ((end - 1) / l.block as u64) as usize;
    let mut out = Vec::with_capacity((end - start) as usize);
    for i in first..=last {
        let block_start = i as u64 * l.block as u64;
        let want = (l.logical - block_start).min(l.block as u64) as usize;
        let (a, b) = l.frames[i];
        let raw = zstd::bulk::decompress(&stored[a..b], want).map_err(|_| CompressError::BadBlock(i))?;
        if raw.len() != want {
            return Err(CompressError::BadBlock(i));
        }
        let from = start.saturating_sub(block_start) as usize;
        let to = ((end - block_start) as usize).min(want);
        out.extend_from_slice(&raw[from..to]);
    }
    Ok((out, last - first + 1))
}

/// The whole object.
pub fn decode(stored: &[u8]) -> Result<Vec<u8>, CompressError> {
    let size = logical_size(stored).ok_or(CompressError::Malformed("no AWZ1 header"))?;
    decode_range(stored, 0, size).map(|(v, _)| v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(n: usize) -> Vec<u8> {
        (0..n).map(|i| b"the quick brown fox jumps over the lazy dog "[i % 44]).collect()
    }

    fn noise(n: usize, mut seed: u64) -> Vec<u8> {
        (0..n)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect()
    }

    #[test]
    fn compressible_data_shrinks_and_round_trips() {
        let data = text(BLOCK * 3 + 777);
        let enc = encode(&data).expect("text compresses");
        assert!(enc.len() < data.len() / 4, "{} of {}", enc.len(), data.len());
        assert_eq!(logical_size(&enc), Some(data.len() as u64));
        assert_eq!(decode(&enc).unwrap(), data);
    }

    #[test]
    fn random_and_small_data_are_stored_as_they_are() {
        assert!(encode(&noise(BLOCK * 2, 7)).is_none());
        assert!(encode(&text(MIN_SIZE - 1)).is_none());
    }

    #[test]
    fn a_range_decodes_only_the_blocks_it_overlaps_and_matches_the_object() {
        let data = text(BLOCK * 5 + 123);
        let enc = encode(&data).unwrap();
        let mut seed = 0x9E3779B97F4A7C15u64;
        for _ in 0..500 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let a = (seed >> 33) % data.len() as u64;
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let b = a + 1 + (seed >> 33) % (3 * BLOCK as u64);
            let (bytes, blocks) = decode_range(&enc, a, b).unwrap();
            let e = (b as usize).min(data.len());
            assert_eq!(bytes, &data[a as usize..e], "range {}..{}", a, b);
            let expected_blocks = (e - 1) / BLOCK - a as usize / BLOCK + 1;
            assert_eq!(blocks, expected_blocks, "range {}..{}", a, b);
        }
        // One byte at the boundary of blocks 2 and 3 decodes one block; two bytes across it, two.
        assert_eq!(decode_range(&enc, 3 * BLOCK as u64 - 1, 3 * BLOCK as u64).unwrap().1, 1);
        assert_eq!(decode_range(&enc, 3 * BLOCK as u64 - 1, 3 * BLOCK as u64 + 1).unwrap().1, 2);
    }

    #[test]
    fn corrupt_layouts_are_errors_not_panics() {
        let enc = encode(&text(BLOCK * 2 + 5)).unwrap();
        for cut in [0, 3, PREFIX_LEN, PREFIX_LEN + 4, enc.len() - 1] {
            assert!(decode(&enc[..cut]).is_err(), "cut at {}", cut);
        }
        let mut bad = enc.clone();
        bad[16] = 9; // block count
        assert!(decode(&bad).is_err());
        let mut bad = enc.clone();
        bad[4..8].copy_from_slice(&0u32.to_le_bytes());
        assert!(decode(&bad).is_err());
        // A flipped byte inside a frame is the payload CRC's job (checked before decoding).
    }
}
