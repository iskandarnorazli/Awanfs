//! Pure validation functions for S3 bucket names and object keys.

use crate::fsm::{Refusal, INVALID_BUCKET_NAME, KEY_TOO_LONG};
use std::net::Ipv4Addr;

/// Validate an S3 bucket name according to AWS / Ceph s3-tests rules.
pub fn validate_bucket_name(name: &str) -> Result<(), Refusal> {
    let bytes = name.as_bytes();
    if bytes.len() < 3 || bytes.len() > 63 {
        return Err(INVALID_BUCKET_NAME);
    }
    let is_alnum = |b: u8| matches!(b, b'a'..=b'z' | b'0'..=b'9');
    if !is_alnum(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
        return Err(INVALID_BUCKET_NAME);
    }
    if !bytes.iter().all(|&b| is_alnum(b) || b == b'.' || b == b'-') {
        return Err(INVALID_BUCKET_NAME);
    }
    if name.contains("..") || name.contains(".-") || name.contains("-.") {
        return Err(INVALID_BUCKET_NAME);
    }
    if name.parse::<Ipv4Addr>().is_ok() {
        return Err(INVALID_BUCKET_NAME);
    }
    if name.starts_with("xn--")
        || name.starts_with("sthree-")
        || name.ends_with("-s3alias")
        || name.ends_with("--ol-s3")
    {
        return Err(INVALID_BUCKET_NAME);
    }
    Ok(())
}

/// Validate an S3 object key (1 to 1024 bytes of UTF-8; empty key is bucket-level).
pub fn validate_object_key(key: &str) -> Result<(), Refusal> {
    if key.len() > 1024 {
        return Err(KEY_TOO_LONG);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsm::{INVALID_BUCKET_NAME, KEY_TOO_LONG};
    use proptest::prelude::*;

    #[test]
    fn suite_bucket_naming_table() {
        let long_60 = "a".repeat(60);
        let long_61 = "a".repeat(61);
        let long_62 = "a".repeat(62);
        let long_63 = "a".repeat(63);

        let cases: &[(&str, Result<(), Refusal>, &str)] = &[
            ("_mybucket", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_bad_starts_nonalpha"),
            ("a", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_bad_short_one"),
            ("aa", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_bad_short_two"),
            (&long_60, Ok(()), "test_bucket_create_naming_good_long_60"),
            (&long_61, Ok(()), "test_bucket_create_naming_good_long_61"),
            (&long_62, Ok(()), "test_bucket_create_naming_good_long_62"),
            (&long_63, Ok(()), "test_bucket_create_naming_good_long_63"),
            ("192.168.5.123", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_bad_ip"),
            ("foo_bar", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_dns_underscore"),
            (&long_63, Ok(()), "test_bucket_create_naming_dns_long"),
            ("foo-", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_dns_dash_at_end"),
            ("foo..bar", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_dns_dot_dot"),
            ("foo.-bar", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_dns_dot_dash"),
            ("foo-.bar", Err(INVALID_BUCKET_NAME), "test_bucket_create_naming_dns_dash_dot"),
            ("afoo", Ok(()), "test_bucket_create_naming_good_starts_alpha"),
            ("0foo", Ok(()), "test_bucket_create_naming_good_starts_digit"),
            ("aaa.111", Ok(()), "test_bucket_create_naming_good_contains_period"),
            ("aaa-111", Ok(()), "test_bucket_create_naming_good_contains_hyphen"),
        ];

        for &(name, expected, suite_test) in cases {
            assert_eq!(
                validate_bucket_name(name),
                expected,
                "suite test {suite_test} failed for bucket name {name:?}"
            );
        }
    }

    #[test]
    fn extra_bucket_name_rules() {
        assert_eq!(validate_bucket_name(""), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name(&"a".repeat(64)), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name("FooBar"), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name(".foo"), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name("foo."), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name("-foo"), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name("xn--foo"), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name("sthree-foo"), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name("foo-s3alias"), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name("foo--ol-s3"), Err(INVALID_BUCKET_NAME));
        assert_eq!(validate_bucket_name("abc"), Ok(()));
    }

    #[test]
    fn object_key_validation() {
        assert_eq!(validate_object_key(""), Ok(()));
        assert_eq!(validate_object_key("a"), Ok(()));
        assert_eq!(validate_object_key(&"a".repeat(1024)), Ok(()));
        assert_eq!(validate_object_key(&"a".repeat(1025)), Err(KEY_TOO_LONG));

        // Multi-byte UTF-8: 512 * 2 bytes = 1024 bytes (valid), 513 * 2 bytes = 1026 bytes (too long).
        let utf8_1024 = "\u{00e9}".repeat(512);
        assert_eq!(utf8_1024.len(), 1024);
        assert_eq!(validate_object_key(&utf8_1024), Ok(()));

        let utf8_1026 = "\u{00e9}".repeat(513);
        assert_eq!(utf8_1026.len(), 1026);
        assert_eq!(validate_object_key(&utf8_1026), Err(KEY_TOO_LONG));

        // Special keys from test_bucket_create_special_key_names
        for special in [" ", "\"", "$", "%", "&", "'", "<", ">", "_", "_ ", "_ _", "__"] {
            assert_eq!(validate_object_key(special), Ok(()));
        }
    }

    fn valid_bucket_name_strategy() -> impl Strategy<Value = String> {
        let alnum = prop::sample::select(
            b"abcdefghijklmnopqrstuvwxyz0123456789".to_vec(),
        );
        let mid = prop::sample::select(
            b"abcdefghijklmnopqrstuvwxyz0123456789.-".to_vec(),
        );
        (alnum.clone(), prop::collection::vec(mid, 1..=61), alnum)
            .prop_map(|(first, middle, last)| {
                let mut bytes = Vec::with_capacity(2 + middle.len());
                bytes.push(first);
                bytes.extend(middle);
                bytes.push(last);
                String::from_utf8(bytes).unwrap()
            })
            .prop_filter("must satisfy bucket naming rules", |s| {
                !s.contains("..")
                    && !s.contains(".-")
                    && !s.contains("-.")
                    && s.parse::<std::net::Ipv4Addr>().is_err()
                    && !s.starts_with("xn--")
                    && !s.starts_with("sthree-")
                    && !s.ends_with("-s3alias")
                    && !s.ends_with("--ol-s3")
            })
    }

    proptest! {
        #[test]
        fn prop_valid_bucket_names_accepted(name in valid_bucket_name_strategy()) {
            prop_assert_eq!(validate_bucket_name(&name), Ok(()));
        }

        #[test]
        fn prop_invalid_char_mutation_refused(
            name in valid_bucket_name_strategy(),
            idx in 0usize..63,
            bad_ch in prop::sample::select(vec!['_', 'A', 'Z', '/', '!', ' ']),
        ) {
            let mut chars: Vec<char> = name.chars().collect();
            let pos = idx % chars.len();
            chars[pos] = bad_ch;
            let mutated: String = chars.into_iter().collect();
            prop_assert_eq!(validate_bucket_name(&mutated), Err(INVALID_BUCKET_NAME));
        }

        #[test]
        fn prop_boundary_dot_or_dash_mutation_refused(
            name in valid_bucket_name_strategy(),
            at_start in any::<bool>(),
            ch in prop::sample::select(vec!['.', '-']),
        ) {
            let mut chars: Vec<char> = name.chars().collect();
            if at_start {
                chars[0] = ch;
            } else {
                let last = chars.len() - 1;
                chars[last] = ch;
            }
            let mutated: String = chars.into_iter().collect();
            prop_assert_eq!(validate_bucket_name(&mutated), Err(INVALID_BUCKET_NAME));
        }

        #[test]
        fn prop_valid_object_keys_accepted(key in ".{0,300}") {
            if key.len() <= 1024 {
                prop_assert_eq!(validate_object_key(&key), Ok(()));
            } else {
                prop_assert_eq!(validate_object_key(&key), Err(KEY_TOO_LONG));
            }
        }
    }
}
