//! #45: every metric the Grafana dashboard and the Prometheus alerts use exists in a scrape.

use s3_server::{render_metrics, S3ServiceState};
use std::collections::BTreeSet;

fn names_in(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let b = text.as_bytes();
    let mut i = 0;
    while let Some(pos) = text[i..].find("awanfs_") {
        let start = i + pos;
        let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
        if start > 0 && ident(b[start - 1]) {
            i = start + 1;
            continue;
        }
        let mut end = start;
        while end < b.len() && ident(b[end]) {
            end += 1;
        }
        out.insert(text[start..end].to_string());
        i = end;
    }
    out
}

#[test]
fn every_metric_in_the_dashboard_and_alerts_is_exported() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/..");
    let dashboard = std::fs::read_to_string(format!("{}/deploy/grafana/awanfs-dashboard.json", root)).unwrap();
    let alerts = std::fs::read_to_string(format!("{}/deploy/prometheus/alerts.yml", root)).unwrap();

    let scrape = render_metrics(&S3ServiceState::new());
    let families: BTreeSet<String> = scrape.lines().filter_map(|l| l.strip_prefix("# TYPE ")).filter_map(|l| l.split(' ').next()).map(String::from).collect();
    let mut used = names_in(&dashboard);
    used.extend(names_in(&alerts));
    assert!(used.len() >= 10, "found only {:?}", used);
    for name in &used {
        let family = ["_bucket", "_sum", "_count"].iter().find_map(|s| name.strip_suffix(s)).filter(|f| families.contains(*f)).unwrap_or(name);
        assert!(families.contains(family), "{} is used but not exported; exported: {:?}", name, families);
    }
}
