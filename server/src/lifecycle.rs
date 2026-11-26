//! Bucket Lifecycle Configuration & Automated Expiration Sweep Engine (#118).
//!
//! Lifecycle rules are persisted in [`BucketConfigStore`](crate::bucket_config::BucketConfigStore)
//! (`ConfigKind::Lifecycle` in `buckets.manifest`) and evaluated through [`crate::fsm::expiry_step`].

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::attrs::Attrs;
use crate::clock::format_http_date;
use crate::fsm::{expiry_step, ExpiryAction, ExpiryFacts, ExpiryRuleAction, LifecycleTarget, Lock};
use crate::lifecycle_config::{
    applies, parse_midnight_utc_epoch, AbortIncompleteMultipartUpload, Expiration, FilterSpec,
    LifecycleConfig, LifecycleRule as ConfigRule, NoncurrentVersionExpiration, RuleFilter,
    RuleStatus,
};
use crate::meta_index::unpack_flags;
use crate::s3_api::S3ServiceState;
use crate::versions::Versioning;
use crate::xml::xml_escape_exact;

static FAULT_DISABLE_PREFIX_CHECK: AtomicBool = AtomicBool::new(false);

pub fn set_fault_disable_prefix_check(disable: bool) {
    FAULT_DISABLE_PREFIX_CHECK.store(disable, Ordering::SeqCst);
}

/// Legacy compatibility struct used by `parser_robustness_test.rs` and `object_lock_test.rs`.
/// HTTP `/bucket?lifecycle` and conformance sweeps use [`crate::lifecycle_config::LifecycleConfig`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleRule {
    pub id: String,
    pub status: bool,
    pub prefix: String,
    pub expiration_days: Option<u32>,
    pub abort_multipart_days: Option<u32>,
}

pub struct LifecycleManager {
    /// Programmatic rules injected directly by internal unit tests (bypassing HTTP PUT ?lifecycle).
    pub configs: RwLock<HashMap<String, Vec<LifecycleRule>>>,
    expired_objects: AtomicU64,
    expired_bytes: AtomicU64,
    aborted_uploads: AtomicU64,
}

impl LifecycleManager {
    pub fn new() -> Self {
        Self {
            configs: RwLock::new(HashMap::new()),
            expired_objects: AtomicU64::new(0),
            expired_bytes: AtomicU64::new(0),
            aborted_uploads: AtomicU64::new(0),
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

    pub fn objects_expired(&self) -> u64 {
        self.expired_objects.load(Ordering::Relaxed)
    }

    pub fn bytes_expired(&self) -> u64 {
        self.expired_bytes.load(Ordering::Relaxed)
    }

    pub fn uploads_aborted(&self) -> u64 {
        self.aborted_uploads.load(Ordering::Relaxed)
    }

    fn record_expired(&self, bytes: u64) {
        self.expired_objects.fetch_add(1, Ordering::Relaxed);
        self.expired_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_aborted(&self) {
        self.aborted_uploads.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for LifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Legacy compatibility parser kept for `server/fuzz/fuzz_targets/lifecycle_xml.rs` and
/// `server/tests/parser_robustness_test.rs`.
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

/// Legacy compatibility XML serializer matching [`parse_lifecycle_xml`].
pub fn lifecycle_config_xml(rules: &[LifecycleRule]) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for r in rules {
        out.push_str("  <Rule>\n");
        out.push_str(&format!("    <ID>{}</ID>\n", xml_escape_exact(&r.id)));
        out.push_str(&format!(
            "    <Status>{}</Status>\n",
            if r.status { "Enabled" } else { "Disabled" }
        ));
        out.push_str(&format!(
            "    <Filter><Prefix>{}</Prefix></Filter>\n",
            xml_escape_exact(&r.prefix)
        ));
        if let Some(days) = r.expiration_days {
            out.push_str(&format!("    <Expiration><Days>{days}</Days></Expiration>\n"));
        }
        if let Some(days) = r.abort_multipart_days {
            out.push_str(&format!(
                "    <AbortIncompleteMultipartUpload><DaysAfterInitiation>{days}</DaysAfterInitiation></AbortIncompleteMultipartUpload>\n"
            ));
        }
        out.push_str("  </Rule>\n");
    }
    out.push_str("</LifecycleConfiguration>");
    out
}

fn legacy_rules_to_config(rules: &[LifecycleRule]) -> LifecycleConfig {
    LifecycleConfig {
        rules: rules
            .iter()
            .map(|r| ConfigRule {
                id: r.id.clone(),
                status: if r.status {
                    RuleStatus::Enabled
                } else {
                    RuleStatus::Disabled
                },
                filter: RuleFilter::Filter(FilterSpec {
                    prefix: Some(r.prefix.clone()),
                    ..Default::default()
                }),
                expiration: r.expiration_days.map(Expiration::Days),
                noncurrent_expiration: None,
                abort_incomplete_multipart_upload: r.abort_multipart_days.map(|d| {
                    AbortIncompleteMultipartUpload {
                        days_after_initiation: d,
                    }
                }),
                transitions: Vec::new(),
                noncurrent_transitions: Vec::new(),
            })
            .collect(),
    }
}

fn rule_matches(
    rule: &ConfigRule,
    key: &str,
    size: u64,
    tags: &[(&str, &str)],
    disable_prefix: bool,
) -> bool {
    if !disable_prefix {
        return applies(rule, key, size, tags);
    }
    if rule.status != RuleStatus::Enabled {
        return false;
    }
    match &rule.filter {
        RuleFilter::LegacyPrefix(_) => true,
        RuleFilter::Filter(spec) => {
            if let Some(gt) = spec.object_size_greater_than {
                if !(size > gt) {
                    return false;
                }
            }
            if let Some(lt) = spec.object_size_less_than {
                if !(size < lt) {
                    return false;
                }
            }
            spec.tags
                .iter()
                .all(|t| tags.iter().any(|&(k, v)| k == t.key && v == t.value))
        }
    }
}

fn collect_rule_actions(
    cfg: &LifecycleConfig,
    key: &str,
    size: u64,
    tags: &[(&str, &str)],
    disable_prefix: bool,
) -> Vec<ExpiryRuleAction> {
    let mut out = Vec::new();
    for rule in &cfg.rules {
        if !rule_matches(rule, key, size, tags, disable_prefix) {
            continue;
        }
        if rule.status != RuleStatus::Enabled {
            out.push(ExpiryRuleAction::Disabled);
            continue;
        }
        match &rule.expiration {
            Some(Expiration::Days(d)) => out.push(ExpiryRuleAction::ExpireDays(*d)),
            Some(Expiration::Date(dt)) => {
                if let Some(ep) = parse_midnight_utc_epoch(dt) {
                    out.push(ExpiryRuleAction::ExpireDate(ep));
                }
            }
            Some(Expiration::ExpiredObjectDeleteMarker(b)) => {
                out.push(ExpiryRuleAction::ExpiredObjectDeleteMarker(*b));
            }
            None => {}
        }
        if let Some(NoncurrentVersionExpiration {
            noncurrent_days,
            newer_noncurrent_versions,
        }) = rule.noncurrent_expiration
        {
            out.push(ExpiryRuleAction::NoncurrentExpiration {
                noncurrent_days,
                newer_noncurrent_versions,
            });
        }
        if let Some(ref a) = rule.abort_incomplete_multipart_upload {
            out.push(ExpiryRuleAction::AbortMultipart(a.days_after_initiation));
        }
    }
    out
}

/// Formats `x-amz-expiration` (`expiry-date="<HTTP-date>", rule-id="<id>"`) if an enabled
/// expiration rule (`Days` or `Date`) applies to this object.
pub fn expiration_header_for_attrs(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    size: u64,
    attrs: &Attrs,
    created_ns: u64,
) -> Option<String> {
    let cfg = state.bucket_lifecycle(b_id)?;
    let tag_set = attrs.tag_set().unwrap_or_default();
    let tag_pairs: Vec<(&str, &str)> = tag_set
        .tags
        .iter()
        .map(|t| (t.key.as_str(), t.value.as_str()))
        .collect();
    let created_sec = created_ns / 1_000_000_000;

    let mut best: Option<(u64, &str)> = None;
    for rule in &cfg.rules {
        if !applies(rule, key, size, &tag_pairs) {
            continue;
        }
        let exp_sec = match &rule.expiration {
            Some(Expiration::Days(d)) => ((created_sec / 86_400) + u64::from(*d) + 1) * 86_400,
            Some(Expiration::Date(dt)) => parse_midnight_utc_epoch(dt)?,
            Some(Expiration::ExpiredObjectDeleteMarker(_)) | None => continue,
        };
        if best.is_none_or(|(b_sec, _)| exp_sec < b_sec) {
            best = Some((exp_sec, rule.id.as_str()));
        }
    }

    let (exp_sec, rule_id) = best?;
    let date_str = format_http_date(exp_sec.saturating_mul(1_000_000_000));
    Some(format!("expiry-date=\"{date_str}\", rule-id=\"{rule_id}\""))
}

fn load_tags_and_lock(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    v_id: u64,
    vol_offset: u64,
    flags: u16,
) -> Option<(Vec<(String, String)>, Lock)> {
    let view = state.object_view_for(b_id, key, v_id, vol_offset, flags).ok()?;
    let tag_set = view.attrs.tag_set().ok()?;
    let tags = tag_set.tags.into_iter().map(|t| (t.key, t.value)).collect();
    let lock = view.attrs.lock_state().ok()?;
    Some((tags, lock))
}

fn current_bucket_lifecycle(
    state: &S3ServiceState,
    b_id: u32,
    bucket_name: &str,
) -> Option<Arc<LifecycleConfig>> {
    if let Some(cfg) = state.bucket_lifecycle(b_id) {
        return Some(cfg);
    }
    let guard = state.lifecycle.configs.read();
    guard
        .get(bucket_name)
        .map(|rules| Arc::new(legacy_rules_to_config(rules)))
}

async fn sweep_versioned_key_locked(
    state: &S3ServiceState,
    b_id: u32,
    key_str: &str,
    lc_cfg: &LifecycleConfig,
    now_sec: u64,
    day_seconds: u64,
    disable_prefix: bool,
    remaining_budget: usize,
) -> (usize, bool) {
    let Some(mut list) = state.versions.read().versions(b_id, key_str).map(|s| s.to_vec()) else {
        return (0, false);
    };
    if list.is_empty() {
        return (0, false);
    }

    let mut actions_done = 0usize;

    // Evaluate noncurrent versions oldest-to-newest so deleting an older entry keeps
    // `newer_noncurrent_count` and `list[idx - 1]` accurate for every newer entry.
    for idx in (1..list.len()).rev() {
        if actions_done >= remaining_budget {
            return (actions_done, true);
        }
        let v = list[idx];
        let newer_nc = list[1..idx].iter().filter(|x| !x.marker).count() as u32;
        let noncurrent_since_sec = list[idx - 1].ts / 1_000_000_000;
        let (tags_owned, lock) = if v.marker {
            (Vec::new(), Lock::default())
        } else {
            let Some(tl) = load_tags_and_lock(state, b_id, key_str, v.id(), v.vol_offset, v.flags) else {
                continue;
            };
            tl
        };
        let tag_refs: Vec<(&str, &str)> = tags_owned
            .iter()
            .map(|(k, val)| (k.as_str(), val.as_str()))
            .collect();
        let acts = collect_rule_actions(lc_cfg, key_str, v.size, &tag_refs, disable_prefix);
        let facts = ExpiryFacts {
            target: LifecycleTarget::NoncurrentVersion {
                noncurrent_since_sec,
                newer_noncurrent_count: newer_nc,
                is_marker: v.marker,
            },
            lock,
            now_sec,
            day_seconds,
        };
        match expiry_step(facts, &acts) {
            ExpiryAction::RemoveVersion => {
                let uncovers = idx == 1
                    && list.len() > 2
                    && !list[2].marker
                    && list[0].marker;
                if state
                    .remove_version_commit_locked(b_id, key_str, v, uncovers)
                    .await
                    .is_ok()
                {
                    list.remove(idx);
                    state.lifecycle.record_expired(v.size);
                    actions_done += 1;
                }
            }
            ExpiryAction::RemoveMarker => {
                if state
                    .remove_version_commit_locked(b_id, key_str, v, false)
                    .await
                    .is_ok()
                {
                    list.remove(idx);
                    state.lifecycle.record_expired(0);
                    actions_done += 1;
                }
            }
            _ => {}
        }
    }

    if actions_done >= remaining_budget {
        return (actions_done, true);
    }

    if let Some(&cur) = list.first() {
        if cur.marker {
            let acts = collect_rule_actions(lc_cfg, key_str, 0, &[], disable_prefix);
            let facts = ExpiryFacts {
                target: LifecycleTarget::CurrentDeleteMarker {
                    created_sec: cur.ts / 1_000_000_000,
                    sole_version: list.len() == 1,
                },
                lock: Lock::default(),
                now_sec,
                day_seconds,
            };
            if expiry_step(facts, &acts) == ExpiryAction::RemoveMarker
                && state
                    .remove_version_commit_locked(b_id, key_str, cur, false)
                    .await
                    .is_ok()
            {
                state.lifecycle.record_expired(0);
                actions_done += 1;
            }
        } else if let Some((tags_owned, lock)) =
            load_tags_and_lock(state, b_id, key_str, cur.id(), cur.vol_offset, cur.flags)
        {
            let tag_refs: Vec<(&str, &str)> = tags_owned
                .iter()
                .map(|(k, val)| (k.as_str(), val.as_str()))
                .collect();
            let acts = collect_rule_actions(lc_cfg, key_str, cur.size, &tag_refs, disable_prefix);
            let facts = ExpiryFacts {
                target: LifecycleTarget::CurrentVersion {
                    created_sec: cur.modified / 1_000_000_000,
                },
                lock,
                now_sec,
                day_seconds,
            };
            if expiry_step(facts, &acts) == ExpiryAction::AddDeleteMarker
                && state.write_delete_marker_locked(b_id, key_str).await.is_ok()
            {
                state.lifecycle.record_expired(0);
                actions_done += 1;
            }
        }
    }

    (actions_done, false)
}

async fn sweep_unversioned_or_null_key_locked(
    state: &S3ServiceState,
    b_id: u32,
    key_str: &str,
    lc_cfg: &LifecycleConfig,
    now_sec: u64,
    day_seconds: u64,
    disable_prefix: bool,
) -> usize {
    let Ok(m) = state.engine.read().get_object(b_id, key_str.as_bytes()) else {
        return 0;
    };
    let flags = unpack_flags(m.bucket_flags);
    let Some((tags_owned, lock)) = load_tags_and_lock(state, b_id, key_str, 0, m.vol_offset, flags) else {
        return 0;
    };
    let is_versioned = state
        .versioning
        .read()
        .get(&b_id)
        .copied()
        .unwrap_or_default()
        != Versioning::Unset;
    let tag_refs: Vec<(&str, &str)> = tags_owned
        .iter()
        .map(|(k, val)| (k.as_str(), val.as_str()))
        .collect();
    let acts = collect_rule_actions(lc_cfg, key_str, m.size, &tag_refs, disable_prefix);
    let target = if is_versioned {
        LifecycleTarget::CurrentVersion {
            created_sec: m.created_at / 1000,
        }
    } else {
        LifecycleTarget::UnversionedObject {
            created_sec: m.created_at / 1000,
        }
    };
    let facts = ExpiryFacts {
        target,
        lock,
        now_sec,
        day_seconds,
    };
    match expiry_step(facts, &acts) {
        ExpiryAction::Expire => {
            if let Ok(bytes) = state.expire_unversioned_locked(b_id, key_str).await {
                state.lifecycle.record_expired(bytes);
                return 1;
            }
        }
        ExpiryAction::AddDeleteMarker => {
            let before = Some(crate::ops::object::null_version_of(&m));
            state.versions.write().start(b_id, key_str, before);
            if state.write_delete_marker_locked(b_id, key_str).await.is_ok() {
                state.lifecycle.record_expired(0);
                return 1;
            }
        }
        _ => {}
    }
    0
}

/// Runs one rate-limited lifecycle sweep tick using [`fsm::expiry_step`]. Returns `true` if
/// the work budget was exhausted and another tick should run promptly.
pub async fn run_lifecycle_sweep_async(
    state: &S3ServiceState,
    now_epoch_ms: u64,
    budget: usize,
) -> bool {
    let disable_prefix = FAULT_DISABLE_PREFIX_CHECK.load(Ordering::Relaxed);
    let now_sec = now_epoch_ms / 1000;
    let day_seconds = state.lifecycle_day_seconds.load(Ordering::Relaxed).max(1);
    let budget = budget.max(1);

    let mut buckets: Vec<(u32, String)> = Vec::new();
    let mut seen_ids = HashSet::new();
    let snap = state.bucket_config_reader.snapshot();
    for (&b_id, cfg) in snap.iter() {
        if cfg.lifecycle().is_some() {
            buckets.push((b_id, cfg.name.clone()));
            seen_ids.insert(b_id);
        }
    }
    let legacy_buckets: Vec<String> = state.lifecycle.configs.read().keys().cloned().collect();
    for bname in legacy_buckets {
        if let Ok(b_id) = state.engine.read().lookup_bucket(&bname) {
            if seen_ids.insert(b_id) {
                buckets.push((b_id, bname));
            }
        }
    }
    if buckets.is_empty() {
        return false;
    }

    let mut actions_done = 0usize;

    for (b_id, bucket_name) in buckets {
        // Quick skip before scanning keys; re-checked per key under `lock_key` below.
        let Some(initial_cfg) = current_bucket_lifecycle(state, b_id, &bucket_name) else {
            continue;
        };
        if initial_cfg.rules.iter().all(|r| r.status != RuleStatus::Enabled) {
            continue;
        }

        // Collect candidate keys from `versions` and `engine` without holding both locks together
        // to preserve strict lock ordering (`engine` before `versions` elsewhere in `s3_api.rs`).
        let v_keys = state.versions.read().keys_of(b_id, "");
        let mut seen_keys: HashSet<String> = v_keys.iter().cloned().collect();
        let mut candidate_keys: Vec<String> = v_keys.into_iter().collect();
        {
            let engine = state.engine.read();
            if let Some(bucket) = engine.buckets.get(&b_id) {
                for key_bytes in bucket.key_index.keys() {
                    let key_str = String::from_utf8_lossy(key_bytes).into_owned();
                    if seen_keys.insert(key_str.clone()) {
                        candidate_keys.push(key_str);
                    }
                }
            }
        }

        for key_str in candidate_keys {
            if actions_done >= budget {
                return true;
            }
            let (did, hit_budget) = {
                let _key_guard = state.lock_key(b_id, &key_str).await;
                let Some(lc_cfg) = current_bucket_lifecycle(state, b_id, &bucket_name) else {
                    continue;
                };
                if lc_cfg.rules.iter().all(|r| r.status != RuleStatus::Enabled) {
                    continue;
                }
                let has_versions = state.versions.read().has(b_id, &key_str);
                if has_versions {
                    sweep_versioned_key_locked(
                        state,
                        b_id,
                        &key_str,
                        &lc_cfg,
                        now_sec,
                        day_seconds,
                        disable_prefix,
                        budget - actions_done,
                    )
                    .await
                } else {
                    let did = sweep_unversioned_or_null_key_locked(
                        state,
                        b_id,
                        &key_str,
                        &lc_cfg,
                        now_sec,
                        day_seconds,
                        disable_prefix,
                    )
                    .await;
                    (did, false)
                }
            };
            actions_done += did;
            if did > 0 {
                tokio::task::yield_now().await;
            }
            if hit_budget || actions_done >= budget {
                return true;
            }
        }

        // Incomplete multipart upload aborts (re-reading bucket lifecycle rule right before abort).
        let uploads: Vec<(String, String, u64)> = {
            state
                .multipart
                .uploads
                .read()
                .iter()
                .filter(|(_, u)| u.bucket == bucket_name)
                .map(|(id, u)| (id.clone(), u.key.clone(), u.initiated_at / 1000))
                .collect()
        };
        for (uid, key_str, initiated_sec) in uploads {
            if actions_done >= budget {
                return true;
            }
            let Some(lc_cfg) = current_bucket_lifecycle(state, b_id, &bucket_name) else {
                break;
            };
            let acts = collect_rule_actions(&lc_cfg, &key_str, 0, &[], disable_prefix);
            let facts = ExpiryFacts {
                target: LifecycleTarget::MultipartUpload { initiated_sec },
                lock: Lock::default(),
                now_sec,
                day_seconds,
            };
            if expiry_step(facts, &acts) == ExpiryAction::AbortUpload {
                if state.multipart.abort_upload(&uid).is_ok() {
                    state.lifecycle.record_aborted();
                    actions_done += 1;
                }
                tokio::task::yield_now().await;
            }
        }
    }

    false
}

/// Synchronous wrapper for deterministic tests that drive a sweep directly without waiting on the scheduler.
pub fn run_lifecycle_sweep(
    state: &S3ServiceState,
    _lifecycle: &LifecycleManager,
    now_epoch_ms: u64,
) {
    let exec = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("lifecycle sweep runtime")
            .block_on(run_lifecycle_sweep_async(state, now_epoch_ms, usize::MAX))
    };
    if tokio::runtime::Handle::try_current().is_ok() {
        let _ = std::thread::scope(|s| s.spawn(exec).join());
    } else {
        let _ = exec();
    }
}
