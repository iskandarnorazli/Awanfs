#![no_main]
//! Bucket policy parser: never panics on arbitrary bytes and evaluates valid policies safely.
use libfuzzer_sys::fuzz_target;
use s3_server::iam::Principal;
use s3_server::policy::{parse_bucket_policy_for_bucket, PolicyRequestContext};

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    if let Ok(pol) = parse_bucket_policy_for_bucket(&text, Some("fuzz-bkt")) {
        let ctx = PolicyRequestContext::default();
        let _ = pol.evaluate(
            &Principal::Anonymous,
            &["s3:GetObject"],
            "arn:aws:s3:::fuzz-bkt/obj",
            &ctx,
        );
    }
});
