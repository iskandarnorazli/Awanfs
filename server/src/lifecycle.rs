//! awan-storage/server/src/lifecycle.rs
//! Bucket Lifecycle Configuration & Automated Expiration Sweep Engine
//! Reference: awan-storage/API_SPECIFICATION.md Section 2.D

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::s3_api::S3ServiceState;
use crate::xml::xml_escape;

static FAULT_DISABLE_PREFIX_CHECK: AtomicBool = AtomicBool::new(false);

pub fn set_fault_disable_prefix_check(disable: bool) {
    FAULT_DISABLE_PREFIX_CHECK.store(disable, Ordering::SeqCst);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleRule {
    pub id: String,
    pub status: bool, // true = Enabled, false = Disabled
    pub prefix: String,
    pub expiration_days: Option<u32>,
    pub abort_multipart_days: Option<u32>,
}

pub struct LifecycleManager {
    /// Bucket name -> Vec<LifecycleRule>
    pub configs: RwLock<HashMap<String, Vec<LifecycleRule>>>,
}

impl LifecycleManager {
    pub fn new() -> Self {
        Self {
            configs: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_rules(&self, bucket: &str, rules: Vec<LifecycleRule>) {
        self.configs.write().insert(bucket.to_string(), rules);
    }

    pub fn get_rules(&self, bucket: &str) -> Option<Vec<LifecycleRule>> {
        self.configs.read().get(bucket).cloned()
    }

    pub fn delete_rules(&self, bucket: &str) -> bool {
        self.configs.write().remove(bucket).is_some()
    }
}

impl Default for LifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses a lifecycle configuration with a real XML reader. The old parser searched for tag
/// strings, so an `<ID>` whose text contained `<Status>` set the status, and `&amp;` stayed
/// escaped (found by the round-trip fuzz target, #48).
pub fn parse_lifecycle_xml(xml_str: &str) -> Result<Vec<LifecycleRule>, &'static str> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    #[derive(Default)]
    struct Draft {
        id: Option<String>,
        status: Option<String>,
        prefix: Option<String>,
        expiration_days: Option<String>,
        abort_days: Option<String>,
    }

    let mut reader = Reader::from_str(xml_str);
    reader.config_mut().trim_text(true);
    let mut path: Vec<String> = Vec::new();
    let mut rules = Vec::new();
    let mut draft: Option<Draft> = None;

    fn field<'a>(d: &'a mut Draft, path: &[String]) -> Option<&'a mut Option<String>> {
        let rule_at = path.iter().rposition(|p| p == "Rule")?;
        let inner: Vec<&str> = path[rule_at + 1..].iter().map(|s| s.as_str()).collect();
        match inner.as_slice() {
            ["ID"] => Some(&mut d.id),
            ["Status"] => Some(&mut d.status),
            ["Prefix"] | ["Filter", "Prefix"] | ["Filter", "And", "Prefix"] => Some(&mut d.prefix),
            ["Expiration", "Days"] => Some(&mut d.expiration_days),
            ["AbortIncompleteMultipartUpload", "DaysAfterInitiation"] => Some(&mut d.abort_days),
            _ => None,
        }
    }

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if name == "Rule" {
                    draft = Some(Draft::default());
                }
                path.push(name);
                if let Some(d) = draft.as_mut() {
                    if let Some(f) = field(d, &path) {
                        f.get_or_insert_with(String::new);
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                path.push(name);
                if let Some(d) = draft.as_mut() {
                    if let Some(f) = field(d, &path) {
                        f.get_or_insert_with(String::new);
                    }
                }
                path.pop();
            }
            Ok(Event::Text(t)) => {
                let v = t.unescape().map_err(|_| "MalformedXML")?;
                if let Some(d) = draft.as_mut() {
                    if let Some(f) = field(d, &path) {
                        f.get_or_insert_with(String::new).push_str(&v);
                    }
                }
            }
            Ok(Event::CData(c)) => {
                let v = String::from_utf8_lossy(&c.into_inner()).into_owned();
                if let Some(d) = draft.as_mut() {
                    if let Some(f) = field(d, &path) {
                        f.get_or_insert_with(String::new).push_str(&v);
                    }
                }
            }
            Ok(Event::End(_)) => {
                if path.last().map(|s| s.as_str()) == Some("Rule") {
                    if let Some(d) = draft.take() {
                        rules.push(LifecycleRule {
                            id: d.id.unwrap_or_else(|| format!("rule-{}", rules.len() + 1)),
                            status: d.status.map_or(true, |s| s == "Enabled"),
                            prefix: d.prefix.unwrap_or_default(),
                            expiration_days: d.expiration_days.and_then(|v| v.trim().parse().ok()),
                            abort_multipart_days: d.abort_days.and_then(|v| v.trim().parse().ok()),
                        });
                    }
                }
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err("MalformedXML"),
            Ok(_) => {}
        }
    }

    if rules.is_empty() {
        return Err("MalformedXML");
    }
    Ok(rules)
}

pub fn lifecycle_config_xml(rules: &[LifecycleRule]) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n"
    );
    for r in rules {
        out.push_str("  <Rule>\n");
        out.push_str(&format!("    <ID>{}</ID>\n", xml_escape(&r.id)));
        out.push_str(&format!("    <Status>{}</Status>\n", if r.status { "Enabled" } else { "Disabled" }));
        out.push_str(&format!("    <Filter><Prefix>{}</Prefix></Filter>\n", xml_escape(&r.prefix)));
        if let Some(days) = r.expiration_days {
            out.push_str(&format!("    <Expiration><Days>{}</Days></Expiration>\n", days));
        }
        if let Some(days) = r.abort_multipart_days {
            out.push_str(&format!("    <AbortIncompleteMultipartUpload><DaysAfterInitiation>{}</DaysAfterInitiation></AbortIncompleteMultipartUpload>\n", days));
        }
        out.push_str("  </Rule>\n");
    }
    out.push_str("</LifecycleConfiguration>");
    out
}

pub fn run_lifecycle_sweep(state: &S3ServiceState, lifecycle: &LifecycleManager, now_epoch_ms: u64) {
    let configs_guard = lifecycle.configs.read();
    let disable_prefix = FAULT_DISABLE_PREFIX_CHECK.load(Ordering::Relaxed);

    for (bucket_name, rules) in configs_guard.iter() {
        for rule in rules {
            if !rule.status {
                continue;
            }

            // 1. Object Expiration Sweep
            if let Some(exp_days) = rule.expiration_days {
                let ttl_ms = (exp_days as u64) * 86_400_000;
                let b_id = match state.engine.read().lookup_bucket(bucket_name) {
                    Ok(id) => id,
                    Err(_) => continue,
                };

                // Find all keys in bucket matching prefix
                let mut keys_to_delete = Vec::new();
                {
                    let engine = state.engine.read();
                    if let Some(bucket) = engine.buckets.get(&b_id) {
                        for (key_bytes, &row_idx) in &bucket.key_index {
                            let meta = engine.table.get(row_idx);
                            let key_str = String::from_utf8_lossy(key_bytes);

                            let matches_prefix = disable_prefix || key_str.starts_with(&rule.prefix);
                            if matches_prefix {
                                if now_epoch_ms >= meta.created_at + ttl_ms {
                                    keys_to_delete.push(key_bytes.clone());
                                }
                            }
                        }
                    }
                }

                // Tombstone expired keys
                let mut engine = state.engine.write();
                for k in keys_to_delete {
                    let _ = engine.delete_object(b_id, &k);
                }
            }

            // 2. Abort Incomplete Multipart Sweep
            if let Some(abort_days) = rule.abort_multipart_days {
                let ttl_ms = (abort_days as u64) * 86_400_000;
                let mut uploads_to_abort = Vec::new();
                {
                    let uploads_guard = state.multipart.uploads.read();
                    for (upload_id, upload) in uploads_guard.iter() {
                        if &upload.bucket == bucket_name {
                            let matches_prefix = disable_prefix || upload.key.starts_with(&rule.prefix);
                            if matches_prefix && now_epoch_ms >= upload.initiated_at + ttl_ms {
                                uploads_to_abort.push(upload_id.clone());
                            }
                        }
                    }
                }

                for uid in uploads_to_abort {
                    let _ = state.multipart.abort_upload(&uid);
                }
            }
        }
    }
}
