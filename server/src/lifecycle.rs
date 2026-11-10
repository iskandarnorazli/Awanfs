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

pub fn parse_lifecycle_xml(xml_str: &str) -> Result<Vec<LifecycleRule>, &'static str> {
    let mut rules = Vec::new();
    let mut cursor = xml_str;

    while let Some(start) = cursor.find("<Rule>") {
        let rest = &cursor[start + 6..];
        let end = match rest.find("</Rule>") {
            Some(e) => e,
            None => break,
        };
        let rule_block = &rest[..end];
        cursor = &rest[end + 7..];

        let id = rule_block
            .find("<ID>")
            .and_then(|s| {
                let r = &rule_block[s + 4..];
                let e = r.find("</ID>")?;
                Some(r[..e].to_string())
            })
            .unwrap_or_else(|| format!("rule-{}", rules.len() + 1));

        let status = rule_block
            .find("<Status>")
            .and_then(|s| {
                let r = &rule_block[s + 8..];
                let e = r.find("</Status>")?;
                Some(&r[..e] == "Enabled")
            })
            .unwrap_or(true);

        let prefix = rule_block
            .find("<Prefix>")
            .and_then(|s| {
                let r = &rule_block[s + 8..];
                let e = r.find("</Prefix>")?;
                Some(r[..e].to_string())
            })
            .unwrap_or_default();

        let expiration_days = rule_block
            .find("<Expiration>")
            .and_then(|s| {
                let r = &rule_block[s + 12..];
                let e = r.find("</Expiration>")?;
                let exp_block = &r[..e];
                let d_start = exp_block.find("<Days>")?;
                let d_rest = &exp_block[d_start + 6..];
                let d_end = d_rest.find("</Days>")?;
                d_rest[..d_end].parse::<u32>().ok()
            });

        let abort_multipart_days = rule_block
            .find("<AbortIncompleteMultipartUpload>")
            .and_then(|s| {
                let r = &rule_block[s + 32..];
                let e = r.find("</AbortIncompleteMultipartUpload>")?;
                let block = &r[..e];
                let d_start = block.find("<DaysAfterInitiation>")?;
                let d_rest = &block[d_start + 21..];
                let d_end = d_rest.find("</DaysAfterInitiation>")?;
                d_rest[..d_end].parse::<u32>().ok()
            });

        rules.push(LifecycleRule {
            id,
            status,
            prefix,
            expiration_days,
            abort_multipart_days,
        });
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
