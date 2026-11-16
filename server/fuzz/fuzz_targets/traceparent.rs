#![no_main]
//! W3C traceparent (`00-<32hex>-<16hex>-<2hex>`): never panics, and accepts exactly the
//! W3C Trace Context traceparent grammar.
use libfuzzer_sys::fuzz_target;
use s3_server::trace::parse_traceparent;

fn is_lower_hex(b: &[u8]) -> bool {
    b.iter().all(|&c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_valid_w3c_traceparent(v: &str) -> bool {
    let b = v.trim().as_bytes();
    if b.len() != 55 || b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
        return false;
    }
    let version = &b[0..2];
    let trace_id = &b[3..35];
    let parent_id = &b[36..52];
    let flags = &b[53..55];
    is_lower_hex(version)
        && version != b"ff"
        && is_lower_hex(trace_id)
        && trace_id.iter().any(|&c| c != b'0')
        && is_lower_hex(parent_id)
        && parent_id.iter().any(|&c| c != b'0')
        && is_lower_hex(flags)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    let res = parse_traceparent(&s);
    assert_eq!(
        res.is_some(),
        is_valid_w3c_traceparent(&s),
        "W3C traceparent grammar mismatch on {:?}",
        s
    );
    if let Some((t, p)) = res {
        let canonical = format!("00-{}-{}-01", hex(&t), hex(&p));
        assert_eq!(parse_traceparent(&canonical), Some((t, p)));
    }
});
