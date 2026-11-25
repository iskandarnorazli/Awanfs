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
/// `Content-Encoding` with `aws-chunked` stripped (#152). Write-time.
pub const TAG_CONTENT_ENCODING: u16 = 0x0002;
/// `Content-Disposition` (#152). Write-time.
pub const TAG_CONTENT_DISPOSITION: u16 = 0x0003;
/// `Content-Language` (#152). Write-time.
pub const TAG_CONTENT_LANGUAGE: u16 = 0x0004;
/// `Cache-Control` (#152). Write-time.
pub const TAG_CACHE_CONTROL: u16 = 0x0005;
/// `Expires` (#152). Write-time.
pub const TAG_EXPIRES: u16 = 0x0006;
/// `x-amz-website-redirect-location` (#152). Write-time.
pub const TAG_WEBSITE_REDIRECT: u16 = 0x0007;
/// User metadata (`x-amz-meta-*`) packed as `name_len (u16 LE) | name | val_len (u16 LE) | val` (#152).
pub const TAG_USER_META: u16 = 0x0008;

/// Object tags (`TagSet`, #115). Mutable via `0x200` attribute-update needles (#203).
pub const TAG_TAGGING: u16 = 0x0010;
/// Object ACL (#116). Replaceable via `0x200` attribute-update needles (#203).
pub const TAG_ACL: u16 = 0x0011;
/// Object Lock retention (#120). Replaceable via `0x200` attribute-update needles (#203).
pub const TAG_RETENTION: u16 = 0x0012;
/// Object Lock legal hold (#120). Replaceable via `0x200` attribute-update needles (#203).
pub const TAG_LEGAL_HOLD: u16 = 0x0013;
/// Object storage class (#159). Replaceable via `0x200` attribute-update needles (#203).
pub const TAG_STORAGE_CLASS: u16 = 0x0014;
/// Object checksum (`<ALG>:<TYPE>:<base64>`), #117. Write-time.
pub const TAG_CHECKSUM: u16 = 0x0020;

/// Multipart part sizes (little-endian `u64` per part), #117. Write-time.
pub const TAG_PART_SIZES: u16 = 0x0021;

/// Canonical owner ID / account UTF-8 (#151, Decision 3A). Write-time, stored only when it differs
/// from the bucket owner.
pub const TAG_OWNER: u16 = 0x0030;

/// Replaceable attribute group updated independently by a `0x200` attribute-update needle.
/// Only the latest update per `(key, version, group)` survives compaction and merges into `ObjectView`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AttrGroup {
    Tagging,
    Acl,
    Retention,
    LegalHold,
    StorageClass,
    Other(u16),
}

impl AttrGroup {
    /// Maps a serialized attribute tag to its replacement group.
    pub fn from_tag(tag: u16) -> Self {
        match tag {
            TAG_TAGGING => Self::Tagging,
            TAG_ACL => Self::Acl,
            TAG_RETENTION => Self::Retention,
            TAG_LEGAL_HOLD => Self::LegalHold,
            TAG_STORAGE_CLASS => Self::StorageClass,
            other => Self::Other(other),
        }
    }

    /// Returns the serialized attribute tag for this group.
    pub fn tag(self) -> u16 {
        match self {
            Self::Tagging => TAG_TAGGING,
            Self::Acl => TAG_ACL,
            Self::Retention => TAG_RETENTION,
            Self::LegalHold => TAG_LEGAL_HOLD,
            Self::StorageClass => TAG_STORAGE_CLASS,
            Self::Other(t) => t,
        }
    }
}

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

    /// Removes `tag` if present.
    pub fn remove(&mut self, tag: u16) {
        if let Ok(i) = self.entries.binary_search_by_key(&tag, |e| e.0) {
            self.entries.remove(i);
        }
    }

    pub fn content_type(&self) -> Option<&str> {
        self.get(TAG_CONTENT_TYPE).and_then(|v| std::str::from_utf8(v).ok())
    }

    pub fn tag_set(&self) -> Result<crate::tagging::TagSet, AttrsError> {
        match self.get(TAG_TAGGING) {
            Some(bytes) => decode_tags(bytes),
            None => Ok(crate::tagging::TagSet::default()),
        }
    }

    pub fn tag_count(&self) -> usize {
        self.get(TAG_TAGGING)
            .and_then(|b| decode_tags(b).ok())
            .map_or(0, |s| s.tags.len())
    }

    pub fn set_checksum(&mut self, alg: crate::checksum::ChecksumAlgorithm, checksum_type: &str, value: &str) {
        let encoded = format!("{}:{}:{}", alg.name(), checksum_type, value);
        self.set(TAG_CHECKSUM, encoded.as_bytes());
    }

    pub fn checksum(&self) -> Option<(crate::checksum::ChecksumAlgorithm, &str, &str)> {
        let s = self.get(TAG_CHECKSUM).and_then(|v| std::str::from_utf8(v).ok())?;
        let (alg_str, rest) = s.split_once(':')?;
        let alg = crate::checksum::ChecksumAlgorithm::from_header_name(alg_str)?;
        let (cksum_type, val) = rest.split_once(':')?;
        if cksum_type.is_empty() || val.is_empty() {
            return None;
        }
        Some((alg, cksum_type, val))
    }

    pub fn set_part_sizes(&mut self, sizes: &[u64]) {
        let mut buf = Vec::with_capacity(sizes.len() * 8);
        for &sz in sizes {
            buf.extend_from_slice(&sz.to_le_bytes());
        }
        self.set(TAG_PART_SIZES, &buf);
    }

    pub fn part_sizes(&self) -> Option<Vec<u64>> {
        let raw = self.get(TAG_PART_SIZES)?;
        if raw.len() % 8 != 0 {
            return None;
        }
        Some(
            raw.chunks_exact(8)
                .map(|c| u64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
                .collect(),
        )
    }
    fn get_str(&self, tag: u16) -> Option<&str> {
        self.get(tag).and_then(|v| std::str::from_utf8(v).ok())
    }

    pub fn content_encoding(&self) -> Option<&str> {
        self.get_str(TAG_CONTENT_ENCODING)
    }

    pub fn content_disposition(&self) -> Option<&str> {
        self.get_str(TAG_CONTENT_DISPOSITION)
    }

    pub fn content_language(&self) -> Option<&str> {
        self.get_str(TAG_CONTENT_LANGUAGE)
    }

    pub fn cache_control(&self) -> Option<&str> {
        self.get_str(TAG_CACHE_CONTROL)
    }

    pub fn expires(&self) -> Option<&str> {
        self.get_str(TAG_EXPIRES)
    }

    pub fn website_redirect(&self) -> Option<&str> {
        self.get_str(TAG_WEBSITE_REDIRECT)
    }

    pub fn owner(&self) -> Option<&str> {
        self.get_str(TAG_OWNER)
    }

    pub fn set_owner(&mut self, owner: &str) {
        self.set(TAG_OWNER, owner.as_bytes());
    }

    pub fn clear_owner(&mut self) {
        self.remove(TAG_OWNER);
    }

    pub fn storage_class(&self) -> Option<&str> {
        self.get_str(TAG_STORAGE_CLASS).filter(|s| !s.is_empty())
    }

    pub fn set_storage_class(&mut self, storage_class: &str) {
        self.set(TAG_STORAGE_CLASS, storage_class.as_bytes());
    }

    /// Merges every `(tag, value)` pair from `update` on top of `self`, replacing matching tags.
    pub fn merge_from(&mut self, update: &Attrs) {
        for &(tag, ref val) in &update.entries {
            self.set(tag, val);
        }
    }

    /// Iterates `(tag, value)` pairs in ascending tag order.
    pub fn iter(&self) -> impl Iterator<Item = (u16, &[u8])> + '_ {
        self.entries.iter().map(|(tag, val)| (*tag, val.as_slice()))
    }

    /// Encodes `x-amz-meta-*` entries (already lower-cased names) into `TAG_USER_META`.
    /// Does nothing when `entries` is empty so needles without metadata stay format 2.
    pub fn set_user_meta<'a, I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut buf = Vec::new();
        for (name, val) in entries {
            let nb = name.as_bytes();
            let vb = val.as_bytes();
            buf.extend_from_slice(&(nb.len() as u16).to_le_bytes());
            buf.extend_from_slice(nb);
            buf.extend_from_slice(&(vb.len() as u16).to_le_bytes());
            buf.extend_from_slice(vb);
        }
        if !buf.is_empty() {
            self.set(TAG_USER_META, &buf);
        }
    }

    /// Decodes `x-amz-meta-*` entries from `TAG_USER_META`.
    pub fn user_meta(&self) -> Vec<(&str, &str)> {
        let Some(mut rest) = self.get(TAG_USER_META) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        while rest.len() >= 2 {
            let nlen = u16::from_le_bytes([rest[0], rest[1]]) as usize;
            rest = &rest[2..];
            if rest.len() < nlen + 2 {
                break;
            }
            let Ok(name) = std::str::from_utf8(&rest[..nlen]) else {
                break;
            };
            rest = &rest[nlen..];
            let vlen = u16::from_le_bytes([rest[0], rest[1]]) as usize;
            rest = &rest[2..];
            if rest.len() < vlen {
                break;
            }
            let Ok(val) = std::str::from_utf8(&rest[..vlen]) else {
                break;
            };
            rest = &rest[vlen..];
            out.push((name, val));
        }
        out
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
        // A block with no entries is written as no block at all (length 0), so one that
        // holds only its CRC is not one this format writes.
        if block.len() <= 4 || block.len() > MAX_BLOCK {
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

/// Encodes a `TagSet` for `TAG_TAGGING`: `u16` count, then `(u16 key_len, key, u16 val_len, val)`.
pub fn encode_tags(set: &crate::tagging::TagSet) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + set.tags.len() * 16);
    out.extend_from_slice(&(set.tags.len() as u16).to_le_bytes());
    for tag in &set.tags {
        let kb = tag.key.as_bytes();
        let vb = tag.value.as_bytes();
        out.extend_from_slice(&(kb.len() as u16).to_le_bytes());
        out.extend_from_slice(kb);
        out.extend_from_slice(&(vb.len() as u16).to_le_bytes());
        out.extend_from_slice(vb);
    }
    out
}

/// Decodes a `TagSet` from `TAG_TAGGING` bytes.
pub fn decode_tags(bytes: &[u8]) -> Result<crate::tagging::TagSet, AttrsError> {
    if bytes.len() < 2 {
        return Err(AttrsError::Corrupt("tag block length"));
    }
    let count = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    let mut rest = &bytes[2..];
    if count * 4 > rest.len() {
        return Err(AttrsError::Corrupt("tag block length"));
    }
    let mut tags = Vec::with_capacity(count);
    for _ in 0..count {
        if rest.len() < 2 {
            return Err(AttrsError::Corrupt("tag key length"));
        }
        let klen = u16::from_le_bytes([rest[0], rest[1]]) as usize;
        rest = &rest[2..];
        if rest.len() < klen + 2 {
            return Err(AttrsError::Corrupt("tag key"));
        }
        let key = std::str::from_utf8(&rest[..klen])
            .map_err(|_| AttrsError::Corrupt("tag key utf8"))?
            .to_string();
        rest = &rest[klen..];
        let vlen = u16::from_le_bytes([rest[0], rest[1]]) as usize;
        rest = &rest[2..];
        if rest.len() < vlen {
            return Err(AttrsError::Corrupt("tag value"));
        }
        let value = std::str::from_utf8(&rest[..vlen])
            .map_err(|_| AttrsError::Corrupt("tag value utf8"))?
            .to_string();
        rest = &rest[vlen..];
        tags.push(crate::tagging::Tag { key, value });
    }
    if !rest.is_empty() {
        return Err(AttrsError::Corrupt("trailing tag bytes"));
    }
    Ok(crate::tagging::TagSet { tags })
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
    fn a_block_with_only_its_crc_is_corrupt() {
        // Found by the attrs_block fuzz target: [0, 0, 0, 0] is the CRC of nothing, and
        // decoding it as "no attributes" meant it did not write back the same bytes.
        let block = crc32c::crc32c(&[]).to_le_bytes();
        assert_eq!(Attrs::decode(&block), Err(AttrsError::Corrupt("block length")));
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

    #[test]
    fn decode_tags_rejects_huge_count_without_over_allocating() {
        assert_eq!(decode_tags(&[0xFF, 0xFF]), Err(AttrsError::Corrupt("tag block length")));
    }

    #[test]
    fn truncated_tag_set_returns_error() {
        let set = crate::tagging::TagSet::new(vec![
            crate::tagging::Tag::new("env", "prod"),
            crate::tagging::Tag::new("team", "core"),
        ]);
        let encoded = encode_tags(&set);
        for n in 0..encoded.len() {
            assert!(
                decode_tags(&encoded[..n]).is_err(),
                "tag set truncated to {} bytes should fail",
                n
            );
        }
    }

    #[test]
    fn truncated_part_sizes_returns_none_or_empty() {
        let mut a = Attrs::new();
        a.set_part_sizes(&[5 * 1024 * 1024]);
        let encoded = a.get(TAG_PART_SIZES).unwrap().to_vec();
        for n in 0..encoded.len() {
            let mut trunc = Attrs::new();
            trunc.set(TAG_PART_SIZES, &encoded[..n]);
            let got = trunc.part_sizes();
            if n == 0 {
                assert_eq!(got, Some(Vec::new()));
            } else {
                assert_eq!(got, None, "part_sizes truncated to {} bytes should be None", n);
            }
        }
    }

    #[test]
    fn truncated_user_meta_returns_empty() {
        let mut a = Attrs::new();
        a.set_user_meta([("owner", "alice")]);
        let encoded = a.get(TAG_USER_META).unwrap().to_vec();
        for n in 0..encoded.len() {
            let mut trunc = Attrs::new();
            trunc.set(TAG_USER_META, &encoded[..n]);
            assert!(
                trunc.user_meta().is_empty(),
                "user_meta truncated to {} bytes should be empty",
                n
            );
        }
    }

    #[test]
    fn truncated_checksum_returns_none() {
        let mut a = Attrs::new();
        a.set_checksum(crate::checksum::ChecksumAlgorithm::Crc32, "FULL_OBJECT", "A");
        let encoded = a.get(TAG_CHECKSUM).unwrap().to_vec();
        for n in 0..encoded.len() {
            let mut trunc = Attrs::new();
            trunc.set(TAG_CHECKSUM, &encoded[..n]);
            assert_eq!(
                trunc.checksum(),
                None,
                "checksum truncated to {} bytes ({:?}) should be None",
                n,
                std::str::from_utf8(&encoded[..n])
            );
        }
    }

    use proptest::prelude::*;

    fn checksum_alg_strategy() -> impl Strategy<Value = crate::checksum::ChecksumAlgorithm> {
        prop_oneof![
            Just(crate::checksum::ChecksumAlgorithm::Crc32),
            Just(crate::checksum::ChecksumAlgorithm::Crc32c),
            Just(crate::checksum::ChecksumAlgorithm::Crc64Nvme),
            Just(crate::checksum::ChecksumAlgorithm::Sha1),
            Just(crate::checksum::ChecksumAlgorithm::Sha256),
        ]
    }

    proptest! {
        #[test]
        fn prop_string_tags_round_trip(
            ct in "\\PC{0,64}",
            ce in "\\PC{0,64}",
            cd in "\\PC{0,64}",
            cl in "\\PC{0,64}",
            cc in "\\PC{0,64}",
            ex in "\\PC{0,64}",
            wr in "\\PC{0,64}",
        ) {
            let mut a = Attrs::new();
            a.set(TAG_CONTENT_TYPE, ct.as_bytes());
            a.set(TAG_CONTENT_ENCODING, ce.as_bytes());
            a.set(TAG_CONTENT_DISPOSITION, cd.as_bytes());
            a.set(TAG_CONTENT_LANGUAGE, cl.as_bytes());
            a.set(TAG_CACHE_CONTROL, cc.as_bytes());
            a.set(TAG_EXPIRES, ex.as_bytes());
            a.set(TAG_WEBSITE_REDIRECT, wr.as_bytes());
            prop_assert_eq!(a.content_type(), Some(ct.as_str()));
            prop_assert_eq!(a.content_encoding(), Some(ce.as_str()));
            prop_assert_eq!(a.content_disposition(), Some(cd.as_str()));
            prop_assert_eq!(a.content_language(), Some(cl.as_str()));
            prop_assert_eq!(a.cache_control(), Some(cc.as_str()));
            prop_assert_eq!(a.expires(), Some(ex.as_str()));
            prop_assert_eq!(a.website_redirect(), Some(wr.as_str()));
        }

        #[test]
        fn prop_tags_round_trip(
            pairs in proptest::collection::vec(("\\PC{1,64}", "\\PC{0,64}"), 0..=20)
        ) {
            let set = crate::tagging::TagSet::new(
                pairs.into_iter().map(|(k, v)| crate::tagging::Tag::new(k, v)).collect(),
            );
            let encoded = encode_tags(&set);
            prop_assert_eq!(decode_tags(&encoded), Ok(set));
        }

        #[test]
        fn prop_part_sizes_round_trip(sizes in proptest::collection::vec(any::<u64>(), 0..=64)) {
            let mut a = Attrs::new();
            a.set_part_sizes(&sizes);
            prop_assert_eq!(a.part_sizes(), Some(sizes));
        }

        #[test]
        fn prop_user_meta_round_trip(
            entries in proptest::collection::vec(("\\PC{1,64}", "\\PC{0,64}"), 0..=20)
        ) {
            let mut a = Attrs::new();
            a.set_user_meta(entries.iter().map(|(k, v)| (k.as_str(), v.as_str())));
            let expected: Vec<(&str, &str)> = entries.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            prop_assert_eq!(a.user_meta(), expected);
        }

        #[test]
        fn prop_checksum_round_trip(
            alg in checksum_alg_strategy(),
            cksum_type in "[A-Z0-9_]{1,16}",
            val in "[A-Za-z0-9+/=-]{1,64}",
        ) {
            let mut a = Attrs::new();
            a.set_checksum(alg, &cksum_type, &val);
            prop_assert_eq!(a.checksum(), Some((alg, cksum_type.as_str(), val.as_str())));
        }

        #[test]
        fn prop_attr_update_round_trip(
            target_version_id in any::<u64>(),
            sc in "[A-Z0-9_]{0,32}",
            pairs in proptest::collection::vec(("\\PC{1,32}", "\\PC{0,32}"), 0..=10),
        ) {
            let mut a = Attrs::new();
            a.set(TAG_STORAGE_CLASS, sc.as_bytes());
            let tags = crate::tagging::TagSet::new(
                pairs.into_iter().map(|(k, v)| crate::tagging::Tag::new(k, v)).collect(),
            );
            a.set(TAG_TAGGING, &encode_tags(&tags));
            let encoded = encode_attr_update(target_version_id, &a).unwrap();
            let (decoded_ver, decoded_attrs) = decode_attr_update(&encoded).unwrap();
            prop_assert_eq!(decoded_ver, target_version_id);
            prop_assert_eq!(decoded_attrs, a);
        }
    }

    #[test]
    fn attr_update_rejects_short_or_corrupt_payload() {
        assert!(matches!(decode_attr_update(&[0u8; 7]), Err(AttrsError::Corrupt(_))));
        let mut a = Attrs::new();
        a.set_storage_class("GLACIER");
        let mut payload = encode_attr_update(42, &a).unwrap();
        let last = payload.len() - 1;
        payload[last] ^= 0xff;
        assert!(matches!(decode_attr_update(&payload), Err(AttrsError::Corrupt(_))));
    }

    #[test]
    fn merge_from_replaces_updated_tags_preserving_base() {
        let mut base = Attrs::new();
        base.set(TAG_CONTENT_TYPE, b"text/plain");
        base.set(TAG_STORAGE_CLASS, b"STANDARD");
        base.set(TAG_OWNER, b"alice");

        let mut upd = Attrs::new();
        upd.set(TAG_STORAGE_CLASS, b"STANDARD_IA");
        base.merge_from(&upd);

        assert_eq!(base.content_type(), Some("text/plain"));
        assert_eq!(base.storage_class(), Some("STANDARD_IA"));
        assert_eq!(base.owner(), Some("alice"));
    }
}

/// Encodes the payload of a `FLAG_ATTR_UPDATE` (`0x200`) needle:
/// `target_version_id: u64 LE || encoded Attrs block`.
/// `target_version_id == 0` addresses an unversioned object or the `null` version.
pub fn encode_attr_update(target_version_id: u64, attrs: &Attrs) -> Result<Vec<u8>, AttrsError> {
    let block = attrs.encode()?;
    let mut out = Vec::with_capacity(8 + block.len());
    out.extend_from_slice(&target_version_id.to_le_bytes());
    out.extend_from_slice(&block);
    Ok(out)
}

/// Decodes the payload of a `FLAG_ATTR_UPDATE` (`0x200`) needle into `(target_version_id, Attrs)`.
pub fn decode_attr_update(bytes: &[u8]) -> Result<(u64, Attrs), AttrsError> {
    if bytes.len() < 8 {
        return Err(AttrsError::Corrupt("attr update payload shorter than 8-byte version id"));
    }
    let target_version_id = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    let attrs = Attrs::decode(&bytes[8..])?;
    Ok((target_version_id, attrs))
}
