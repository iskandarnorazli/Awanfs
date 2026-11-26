//! The attribute tag numbers are the ones the owner decided (Decision 3A, #151 and #159), and
//! `FORMAT.md`'s tag registry lists every tag the code writes with the same number. #402 had
//! registered ACL, retention, legal hold and storage class as 0x0011-0x0014 instead.

use s3_server::attrs::*;
use std::collections::HashMap;

fn registry() -> Vec<(u16, String)> {
    let doc = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../FORMAT.md")).unwrap();
    let start = doc.find("### Tag registry").expect("FORMAT.md has a tag registry");
    let section = &doc[start..];
    let end = section[3..].find("\n#").map(|i| i + 3).unwrap_or(section.len());
    section[..end]
        .lines()
        .filter_map(|l| {
            let cells: Vec<&str> = l.split('|').map(str::trim).collect();
            let hex = cells.get(1)?.strip_prefix("0x")?;
            Some((u16::from_str_radix(hex, 16).ok()?, cells.get(2)?.to_string()))
        })
        .collect()
}

#[test]
fn the_tags_have_the_numbers_decision_3a_gives() {
    assert_eq!(TAG_OWNER, 0x0030);
    assert_eq!(TAG_ACL, 0x0031);
    assert_eq!(TAG_RETENTION, 0x0040);
    assert_eq!(TAG_LEGAL_HOLD, 0x0041);
    assert_eq!(TAG_STORAGE_CLASS, 0x0060);
}

#[test]
fn every_tag_the_code_writes_is_in_format_md_with_its_number() {
    let reg = registry();
    let mut seen: HashMap<u16, usize> = HashMap::new();
    for (n, _) in &reg {
        *seen.entry(*n).or_default() += 1;
    }
    let twice: Vec<_> = seen.iter().filter(|(_, c)| **c > 1).map(|(n, _)| format!("{:#06x}", n)).collect();
    assert!(twice.is_empty(), "listed more than once in FORMAT.md: {:?}", twice);
    let code = [
        ("TAG_CONTENT_TYPE", TAG_CONTENT_TYPE),
        ("TAG_CONTENT_ENCODING", TAG_CONTENT_ENCODING),
        ("TAG_CONTENT_DISPOSITION", TAG_CONTENT_DISPOSITION),
        ("TAG_CONTENT_LANGUAGE", TAG_CONTENT_LANGUAGE),
        ("TAG_CACHE_CONTROL", TAG_CACHE_CONTROL),
        ("TAG_EXPIRES", TAG_EXPIRES),
        ("TAG_WEBSITE_REDIRECT", TAG_WEBSITE_REDIRECT),
        ("TAG_USER_META", TAG_USER_META),
        ("TAG_TAGGING", TAG_TAGGING),
        ("TAG_ACL", TAG_ACL),
        ("TAG_RETENTION", TAG_RETENTION),
        ("TAG_LEGAL_HOLD", TAG_LEGAL_HOLD),
        ("TAG_STORAGE_CLASS", TAG_STORAGE_CLASS),
        ("TAG_CHECKSUM", TAG_CHECKSUM),
        ("TAG_PART_SIZES", TAG_PART_SIZES),
        ("TAG_OWNER", TAG_OWNER),
    ];
    for (name, n) in code {
        let row = reg.iter().find(|(r, _)| *r == n);
        assert!(row.is_some(), "{} = {:#06x} is not in FORMAT.md's tag registry", name, n);
        assert!(!row.unwrap().1.to_ascii_lowercase().contains("retired"), "{} = {:#06x} is listed as retired", name, n);
    }
}
