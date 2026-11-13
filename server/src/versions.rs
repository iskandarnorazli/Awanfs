//! Bucket versioning (#31, owner decision 21).
//!
//! The index holds one row per key: its current version, as before. A key of a bucket that has
//! had versioning turned on also gets a history here, newest first, and the index row follows
//! the history's front (a delete marker at the front means no row). Live writes and the
//! restart replay both go through `VersionStore::apply`, so they cannot disagree.
//!
//! On disk (needle flags, `meta_index.rs`): a version is a needle flagged `FLAG_VERSIONED`
//! whose creation time (header bytes 36..44) is its version id; compaction keeps that time.
//! `FLAG_NULL_ID` makes it the null version (Suspended buckets). A delete marker is a
//! versioned tombstone. `DELETE ?versionId=` writes a tombstone flagged `FLAG_VERSION_DELETE`
//! whose 8-byte payload is the id it removes (0 for null). Replay orders a key's versions by
//! creation time, not by log position, because compaction moves old versions forward.
//!
//! Histories live in memory: one entry per version of every key in a versioned bucket.

use std::collections::{BTreeSet, HashMap};

/// A bucket's versioning status (`PUT ?versioning`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Versioning {
    /// Never set: overwrites replace, no version ids.
    #[default]
    Unset,
    Enabled,
    Suspended,
}

impl Versioning {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Enabled" => Some(Versioning::Enabled),
            "Suspended" => Some(Versioning::Suspended),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Versioning::Unset => "",
            Versioning::Enabled => "Enabled",
            Versioning::Suspended => "Suspended",
        }
    }
}

/// One version of a key: an object or a delete marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Version {
    /// Needle creation time (ns): the order of versions, and the id unless `null`.
    pub ts: u64,
    pub null: bool,
    pub marker: bool,
    pub vol_offset: u64,
    pub size: u64,
    pub etag_hi: u64,
    pub etag_lo: u64,
    pub part_count: u16,
    /// Needle flags of the object (multipart, compressed, ...).
    pub flags: u16,
}

impl Version {
    /// 0 for the null version.
    pub fn id(&self) -> u64 {
        if self.null {
            0
        } else {
            self.ts
        }
    }

    pub fn id_string(&self) -> String {
        id_string(self.id())
    }
}

pub fn id_string(id: u64) -> String {
    if id == 0 {
        "null".to_string()
    } else {
        format!("{:016x}", id)
    }
}

/// `null` is 0; other ids are 16 hex digits. Anything else names no version.
pub fn parse_id(s: &str) -> Option<u64> {
    if s == "null" {
        return Some(0);
    }
    if s.len() != 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(s, 16).ok().filter(|&n| n != 0)
}

/// `2026-11-13T01:02:03.000Z` from ns since the Unix epoch; ns 0 (before versioning) gives
/// the fixed time the listings used before #31.
pub fn iso8601(ns: u64) -> String {
    if ns == 0 {
        return "2026-11-10T00:00:00.000Z".to_string();
    }
    let secs = ns / 1_000_000_000;
    let ms = (ns / 1_000_000) % 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm, as in request_log).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", y, m, d, rem / 3600, rem / 60 % 60, rem % 60, ms)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// A new version or delete marker.
    Put(Version),
    /// `DELETE ?versionId=`: the version with this id goes.
    Remove(u64),
}

#[derive(Default, Debug)]
pub struct VersionStore {
    keys: HashMap<(u32, String), Vec<Version>>,
}

impl VersionStore {
    pub fn has(&self, b_id: u32, key: &str) -> bool {
        self.keys.contains_key(&(b_id, key.to_string()))
    }

    /// Newest first.
    pub fn versions(&self, b_id: u32, key: &str) -> Option<&[Version]> {
        self.keys.get(&(b_id, key.to_string())).map(|v| v.as_slice())
    }

    pub fn current(&self, b_id: u32, key: &str) -> Option<Version> {
        self.versions(b_id, key).and_then(|v| v.first().copied())
    }

    pub fn find(&self, b_id: u32, key: &str, id: u64) -> Option<Version> {
        self.versions(b_id, key).and_then(|v| v.iter().find(|x| x.id() == id).copied())
    }

    /// Starts a key's history with the object it had before versioning: its null version.
    pub fn start(&mut self, b_id: u32, key: &str, before: Option<Version>) {
        self.keys.entry((b_id, key.to_string())).or_insert_with(|| before.into_iter().collect());
    }

    /// The creation time for the key's next version: after every version it has, even when the
    /// clock went back.
    pub fn next_ts(&self, b_id: u32, key: &str, now_ns: u64) -> u64 {
        let newest = self.versions(b_id, key).and_then(|v| v.iter().map(|x| x.ts).max()).unwrap_or(0);
        now_ns.max(newest + 1)
    }

    /// Applies an event; returns the versions it made unreachable (their needles are dead).
    /// Starts the history if the key has none.
    pub fn apply(&mut self, b_id: u32, key: &str, ev: Event) -> Vec<Version> {
        let list = self.keys.entry((b_id, key.to_string())).or_default();
        match ev {
            Event::Remove(id) => match list.iter().position(|x| x.id() == id) {
                Some(i) => vec![list.remove(i)],
                None => Vec::new(),
            },
            Event::Put(v) => {
                // The same version twice: a compaction copy next to its original after a crash.
                if let Some(i) = list.iter().position(|x| x.ts == v.ts && x.null == v.null) {
                    list[i] = v;
                    return Vec::new();
                }
                let mut gone = Vec::new();
                if v.null {
                    if let Some(i) = list.iter().position(|x| x.null) {
                        if list[i].ts > v.ts {
                            return vec![v];
                        }
                        gone.push(list.remove(i));
                    }
                }
                let at = list.iter().position(|x| x.ts < v.ts).unwrap_or(list.len());
                list.insert(at, v);
                gone
            }
        }
    }

    /// Compaction moved a needle.
    pub fn move_offset(&mut self, b_id: u32, key: &str, old: u64, new: u64) -> bool {
        match self.keys.get_mut(&(b_id, key.to_string())).and_then(|l| l.iter_mut().find(|x| x.vol_offset == old)) {
            Some(v) => {
                v.vol_offset = new;
                true
            }
            None => false,
        }
    }

    pub fn holds_offset(&self, b_id: u32, key: &str, offset: u64) -> bool {
        self.versions(b_id, key).is_some_and(|l| l.iter().any(|x| x.vol_offset == offset))
    }

    /// Keys of a bucket with a history, sorted.
    pub fn keys_of(&self, b_id: u32, prefix: &str) -> BTreeSet<String> {
        self.keys.keys().filter(|(b, k)| *b == b_id && k.starts_with(prefix)).map(|(_, k)| k.clone()).collect()
    }

    pub fn remove_bucket(&mut self, b_id: u32) {
        self.keys.retain(|(b, _), _| *b != b_id);
    }

    /// Drops a key whose history is empty.
    pub fn tidy(&mut self, b_id: u32, key: &str) {
        let k = (b_id, key.to_string());
        if self.keys.get(&k).is_some_and(|l| l.is_empty()) {
            self.keys.remove(&k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(ts: u64) -> Version {
        Version { ts, null: false, marker: false, vol_offset: ts * 4096, size: 1, etag_hi: 0, etag_lo: 0, part_count: 0, flags: 0 }
    }

    fn ids(s: &VersionStore) -> Vec<String> {
        s.versions(1, "k").unwrap().iter().map(|x| format!("{}{}", x.id_string(), if x.marker { "m" } else { "" })).collect()
    }

    #[test]
    fn versions_are_kept_newest_first_whatever_order_they_arrive_in() {
        let mut s = VersionStore::default();
        for ts in [3, 1, 5, 2] {
            assert!(s.apply(1, "k", Event::Put(v(ts))).is_empty());
        }
        assert_eq!(ids(&s), ["0000000000000005", "0000000000000003", "0000000000000002", "0000000000000001"]);
        assert_eq!(s.current(1, "k").unwrap().ts, 5);
    }

    #[test]
    fn a_null_version_replaces_only_the_null_version() {
        let mut s = VersionStore::default();
        s.start(1, "k", Some(Version { null: true, ..v(0) }));
        s.apply(1, "k", Event::Put(v(10)));
        let gone = s.apply(1, "k", Event::Put(Version { null: true, ..v(20) }));
        assert_eq!(gone.len(), 1, "the old null version is dead");
        assert_eq!(ids(&s), ["null", "000000000000000a"]);
        // An older null arriving later (a compaction copy) loses to the newer one.
        let gone = s.apply(1, "k", Event::Put(Version { null: true, ..v(15) }));
        assert_eq!(gone[0].ts, 15);
        assert_eq!(s.current(1, "k").unwrap().ts, 20);
    }

    #[test]
    fn markers_removes_and_duplicates() {
        let mut s = VersionStore::default();
        s.apply(1, "k", Event::Put(v(1)));
        s.apply(1, "k", Event::Put(Version { marker: true, ..v(2) }));
        assert!(s.current(1, "k").unwrap().marker);
        assert_eq!(s.apply(1, "k", Event::Remove(2)).len(), 1);
        assert_eq!(s.current(1, "k").unwrap().ts, 1);
        assert!(s.apply(1, "k", Event::Remove(99)).is_empty());
        // A copy of version 1 at a new offset replaces it in place.
        assert!(s.apply(1, "k", Event::Put(Version { vol_offset: 777, ..v(1) })).is_empty());
        assert_eq!(s.versions(1, "k").unwrap().len(), 1);
        assert_eq!(s.current(1, "k").unwrap().vol_offset, 777);
        assert!(s.move_offset(1, "k", 777, 888) && s.holds_offset(1, "k", 888));
    }

    #[test]
    fn next_ts_never_goes_back_and_ids_round_trip() {
        let mut s = VersionStore::default();
        s.apply(1, "k", Event::Put(v(1_000)));
        assert_eq!(s.next_ts(1, "k", 10), 1_001, "a clock behind the newest version");
        assert_eq!(s.next_ts(1, "k", 5_000), 5_000);
        for id in [1u64, 0x1234_5678_9abc_def0, u64::MAX] {
            assert_eq!(parse_id(&id_string(id)), Some(id));
        }
        assert_eq!(parse_id("null"), Some(0));
        for bad in ["", "0000000000000000", "00000000000000001", "zzzzzzzzzzzzzzzz", "NULL"] {
            assert_eq!(parse_id(bad), None, "{:?}", bad);
        }
    }

    #[test]
    fn iso8601_matches_known_instants() {
        assert_eq!(iso8601(1_000_000), "1970-01-01T00:00:00.001Z");
        assert_eq!(iso8601(1_794_528_123_456_000_000), "2026-11-13T00:02:03.456Z");
        assert_eq!(iso8601(951_782_400_000_000_000), "2000-02-29T00:00:00.000Z");
    }
}
