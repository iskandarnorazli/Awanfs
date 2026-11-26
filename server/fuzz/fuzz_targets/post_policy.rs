#![no_main]
//! POST object policy documents and form signatures (#119): parsing and checking never panic,
//! whatever the bytes, and a policy that parses holds only conditions of known kinds.
use libfuzzer_sys::fuzz_target;
use s3_server::post_object::{
    check_policy, check_size, object_key, parse_expiration, parse_policy, read_signature, success, tagging_header, Condition,
    FormFields,
};

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let _ = parse_expiration(&text);
    let _ = tagging_header(&text);
    // As base64 text, and as the JSON inside it.
    let _ = parse_policy(&text);
    let b64 = s3_server::bucket_config::base64_encode(data);
    if let Ok(p) = parse_policy(&b64) {
        for c in &p.conditions {
            if let Condition::ContentLengthRange { min, max } = c {
                assert!(min <= max);
            }
        }
        // Split the input into form fields on newlines and `=`.
        let fields = FormFields {
            fields: text
                .lines()
                .filter_map(|l| l.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        let key = object_key(&fields, Some("f.txt")).unwrap_or_default();
        let _ = check_policy(&p, &fields, "bucket", &key, 1_800_000_000);
        let _ = check_size(Some(&p), data.len() as u64);
        let _ = read_signature(&fields);
        let _ = success(&fields, "bucket", &key, "\"etag\"");
    }
});
