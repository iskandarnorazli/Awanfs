//! The attributes block of a needle (volume format 3, decision 43, #150).
//!
//! A needle may carry a block between its key and its payload; header bytes 18..20 hold its
//! length, 0 for none, so a needle without attributes is byte for byte a format 2 needle.
//! The block is a CRC32C of the bytes after it, then entries `tag (u16) | length (u16) |
//! value`, all little-endian, tags ascending and unique. Readers skip tags they do not know
//! and keep them when they write the block again. Tag numbers are registered in `FORMAT.md`
//! and never reused; 0x8000 and above are for tests.

/// The largest block a needle can carry: its length is a u16 in the header.
pub const MAX_BLOCK: usize = u16::MAX as usize;

/// `Content-Type` as sent with the object (#150). Write-time.
pub const TAG_CONTENT_TYPE: u16 = 0x0001;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttrsError {
    /// The block would not fit in a needle: 400 `MetadataTooLarge` for the request.
    TooLarge(usize),
    /// The stored block is shorter than an entry says, or its CRC does not match. A read
    /// error: the object's attributes are never served from such a block.
    Corrupt(&'static str),
}

/// The attributes of one object, ordered by tag.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Attrs {
    entries: Vec<(u16, Vec<u8>)>,
}

impl Attrs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Sets `tag`, replacing a value it had.
    pub fn set(&mut self, tag: u16, value: &[u8]) {
        match self.entries.binary_search_by_key(&tag, |e| e.0) {
            Ok(i) => self.entries[i].1 = value.to_vec(),
            Err(i) => self.entries.insert(i, (tag, value.to_vec())),
        }
    }

    pub fn get(&self, tag: u16) -> Option<&[u8]> {
        self.entries.binary_search_by_key(&tag, |e| e.0).ok().map(|i| &self.entries[i].1[..])
    }

    pub fn content_type(&self) -> Option<&str> {
        self.get(TAG_CONTENT_TYPE).and_then(|v| std::str::from_utf8(v).ok())
    }

    /// The tags, ascending.
    pub fn tags(&self) -> impl Iterator<Item = u16> + '_ {
        self.entries.iter().map(|e| e.0)
    }

    /// The bytes to store: empty for no attributes, so the needle stays a format 2 needle.
    pub fn encode(&self) -> Result<Vec<u8>, AttrsError> {
        if self.entries.is_empty() {
            return Ok(Vec::new());
        }
        let len = 4 + self.entries.iter().map(|(_, v)| 4 + v.len()).sum::<usize>();
        if len > MAX_BLOCK || self.entries.iter().any(|(_, v)| v.len() > u16::MAX as usize) {
            return Err(AttrsError::TooLarge(len));
        }
        let mut out = Vec::with_capacity(len);
        out.extend_from_slice(&[0; 4]);
        for (tag, value) in &self.entries {
            out.extend_from_slice(&tag.to_le_bytes());
            out.extend_from_slice(&(value.len() as u16).to_le_bytes());
            out.extend_from_slice(value);
        }
        let crc = crc32c::crc32c(&out[4..]);
        out[0..4].copy_from_slice(&crc.to_le_bytes());
        Ok(out)
    }

    /// Reads a stored block. Allocates no more than the block's own length.
    pub fn decode(block: &[u8]) -> Result<Self, AttrsError> {
        if block.is_empty() {
            return Ok(Self::default());
        }
        if block.len() < 4 || block.len() > MAX_BLOCK {
            return Err(AttrsError::Corrupt("block length"));
        }
        let stored = u32::from_le_bytes(block[0..4].try_into().unwrap());
        if crc32c::crc32c(&block[4..]) != stored {
            return Err(AttrsError::Corrupt("block CRC"));
        }
        let mut entries: Vec<(u16, Vec<u8>)> = Vec::new();
        let mut rest = &block[4..];
        while !rest.is_empty() {
            if rest.len() < 4 {
                return Err(AttrsError::Corrupt("entry header"));
            }
            let tag = u16::from_le_bytes([rest[0], rest[1]]);
            let len = u16::from_le_bytes([rest[2], rest[3]]) as usize;
            if rest.len() < 4 + len {
                return Err(AttrsError::Corrupt("entry length"));
            }
            if entries.last().is_some_and(|e| e.0 >= tag) {
                return Err(AttrsError::Corrupt("tags out of order"));
            }
            entries.push((tag, rest[4..4 + len].to_vec()));
            rest = &rest[4 + len..];
        }
        Ok(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_attributes_is_an_empty_block() {
        assert_eq!(Attrs::new().encode().unwrap(), Vec::<u8>::new());
        assert_eq!(Attrs::decode(&[]).unwrap(), Attrs::new());
    }

    #[test]
    fn a_block_round_trips_and_keeps_tags_it_does_not_know() {
        let mut a = Attrs::new();
        a.set(0x8001, b"unknown to this reader");
        a.set(TAG_CONTENT_TYPE, b"text/plain");
        a.set(TAG_CONTENT_TYPE, b"image/png");
        let block = a.encode().unwrap();
        let back = Attrs::decode(&block).unwrap();
        assert_eq!(back, a);
        assert_eq!(back.content_type(), Some("image/png"));
        assert_eq!(back.tags().collect::<Vec<_>>(), vec![TAG_CONTENT_TYPE, 0x8001]);
        assert_eq!(back.encode().unwrap(), block, "written again byte for byte");
    }

    #[test]
    fn the_golden_bytes_of_a_content_type_block() {
        let mut a = Attrs::new();
        a.set(TAG_CONTENT_TYPE, b"text/plain");
        let block = a.encode().unwrap();
        let mut want = vec![0u8; 4];
        want.extend_from_slice(&[0x01, 0x00, 10, 0x00]);
        want.extend_from_slice(b"text/plain");
        let crc = crc32c::crc32c(&want[4..]);
        want[0..4].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(block, want);
    }

    #[test]
    fn a_block_over_the_limit_is_refused() {
        let mut a = Attrs::new();
        a.set(0x8001, &vec![b'x'; MAX_BLOCK - 8]);
        assert!(a.encode().is_ok(), "exactly at the limit");
        a.set(0x8001, &vec![b'x'; MAX_BLOCK - 7]);
        assert_eq!(a.encode(), Err(AttrsError::TooLarge(MAX_BLOCK + 1)));
    }

    #[test]
    fn a_damaged_block_is_an_error_never_attributes() {
        let mut a = Attrs::new();
        a.set(TAG_CONTENT_TYPE, b"text/plain");
        let block = a.encode().unwrap();
        for i in 0..block.len() {
            let mut bad = block.clone();
            bad[i] ^= 0x40;
            assert!(Attrs::decode(&bad).is_err(), "a flipped bit at byte {} is caught", i);
        }
        for n in 1..block.len() {
            assert!(Attrs::decode(&block[..n]).is_err(), "a block cut to {} bytes is caught", n);
        }
    }

    #[test]
    fn out_of_order_or_repeated_tags_are_corrupt() {
        let mut body = Vec::new();
        for tag in [2u16, 1u16] {
            body.extend_from_slice(&tag.to_le_bytes());
            body.extend_from_slice(&0u16.to_le_bytes());
        }
        let mut block = crc32c::crc32c(&body).to_le_bytes().to_vec();
        block.extend_from_slice(&body);
        assert_eq!(Attrs::decode(&block), Err(AttrsError::Corrupt("tags out of order")));
    }
}
