//! Full S3 Bucket Lifecycle Configuration parser, validator, serializer, and filter matcher.

use std::collections::HashSet;

use quick_xml::events::Event;
use quick_xml::reader::Reader;

use crate::fsm::Refusal;
use crate::xml::xml_escape_exact;

const MAX_RULES: usize = 1000;
const MAX_ID_CHARS: usize = 255;

const fn malformed(why: &'static str) -> Refusal {
    Refusal {
        code: "MalformedXML",
        status: 400,
        why,
    }
}

const fn invalid_arg(why: &'static str) -> Refusal {
    Refusal {
        code: "InvalidArgument",
        status: 400,
        why,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleConfig {
    pub rules: Vec<LifecycleRule>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleRule {
    pub id: String,
    pub status: RuleStatus,
    pub filter: RuleFilter,
    pub expiration: Option<Expiration>,
    pub noncurrent_expiration: Option<NoncurrentVersionExpiration>,
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
    pub transitions: Vec<Transition>,
    pub noncurrent_transitions: Vec<NoncurrentVersionTransition>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuleStatus {
    Enabled,
    Disabled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct FilterSpec {
    pub prefix: Option<String>,
    pub tags: Vec<Tag>,
    pub object_size_greater_than: Option<u64>,
    pub object_size_less_than: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuleFilter {
    /// Legacy top-level `<Prefix>...</Prefix>` directly under `<Rule>`.
    LegacyPrefix(String),
    /// Modern `<Filter>...</Filter>` (empty, single predicate, or `<And>`).
    Filter(FilterSpec),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expiration {
    Days(u32),
    /// Normalized ISO-8601 midnight UTC timestamp, e.g. `"2017-09-27T00:00:00Z"`.
    Date(String),
    ExpiredObjectDeleteMarker(bool),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoncurrentVersionExpiration {
    pub noncurrent_days: u32,
    pub newer_noncurrent_versions: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortIncompleteMultipartUpload {
    pub days_after_initiation: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransitionWhen {
    Days(u32),
    Date(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transition {
    pub when: TransitionWhen,
    pub storage_class: String,
    /// Transitions are parsed and kept so GET returns them, but have no effect.
    pub no_effect: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoncurrentVersionTransition {
    pub noncurrent_days: u32,
    pub newer_noncurrent_versions: Option<u32>,
    pub storage_class: String,
    /// Noncurrent transitions are parsed and kept so GET returns them, but have no effect.
    pub no_effect: bool,
}

pub fn parse_lifecycle_config_xml(xml: &str) -> Result<LifecycleConfig, Refusal> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut path: Vec<String> = Vec::new();
    let mut saw_root = false;
    let mut rules_drafts: Vec<RuleDraft> = Vec::new();
    let mut current_rule: Option<RuleDraft> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if path.is_empty() {
                    if name != "LifecycleConfiguration" {
                        return Err(malformed("root element must be LifecycleConfiguration"));
                    }
                    saw_root = true;
                } else if path.len() == 1 && path[0] == "LifecycleConfiguration" && name == "Rule" {
                    if rules_drafts.len() >= MAX_RULES {
                        return Err(invalid_arg("lifecycle configuration exceeds 1000 rules"));
                    }
                    current_rule = Some(RuleDraft::default());
                } else if let Some(rule) = current_rule.as_mut() {
                    rule.on_start(&path, &name)?;
                }
                path.push(name);
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if path.is_empty() {
                    if name == "LifecycleConfiguration" {
                        return Err(malformed("LifecycleConfiguration must contain at least one Rule"));
                    }
                    return Err(malformed("root element must be LifecycleConfiguration"));
                }
                if let Some(rule) = current_rule.as_mut() {
                    rule.on_start(&path, &name)?;
                    path.push(name);
                    rule.on_end(&path)?;
                    path.pop();
                }
            }
            Ok(Event::Text(t)) => {
                let v = t
                    .unescape()
                    .map_err(|_| malformed("invalid XML character reference"))?;
                if let Some(rule) = current_rule.as_mut() {
                    rule.on_text(&path, &v)?;
                }
            }
            Ok(Event::CData(c)) => {
                let v = String::from_utf8_lossy(&c.into_inner()).into_owned();
                if let Some(rule) = current_rule.as_mut() {
                    rule.on_text(&path, &v)?;
                }
            }
            Ok(Event::End(_)) => {
                if let Some(rule) = current_rule.as_mut() {
                    rule.on_end(&path)?;
                }
                if path.len() == 2 && path[0] == "LifecycleConfiguration" && path[1] == "Rule" {
                    if let Some(draft) = current_rule.take() {
                        rules_drafts.push(draft);
                    }
                }
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(malformed("invalid XML syntax")),
            Ok(_) => {}
        }
    }

    if !saw_root || rules_drafts.is_empty() {
        return Err(malformed("LifecycleConfiguration must contain at least one Rule"));
    }
    if rules_drafts.len() > MAX_RULES {
        return Err(invalid_arg("lifecycle configuration exceeds 1000 rules"));
    }

    let mut seen_ids: HashSet<String> = HashSet::with_capacity(rules_drafts.len());
    for draft in &rules_drafts {
        if let Some(ref id) = draft.id {
            if !id.is_empty() {
                if id.chars().count() > MAX_ID_CHARS {
                    return Err(invalid_arg("rule ID exceeds 255 characters"));
                }
                if !seen_ids.insert(id.clone()) {
                    return Err(invalid_arg("duplicate rule ID"));
                }
            }
        }
    }

    let mut next_gen = 1usize;
    let mut rules = Vec::with_capacity(rules_drafts.len());
    for draft in rules_drafts {
        let id = match draft.id.as_deref() {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => loop {
                let candidate = format!("rule-{next_gen}");
                next_gen += 1;
                if seen_ids.insert(candidate.clone()) {
                    break candidate;
                }
            },
        };
        rules.push(draft.finish(id)?);
    }

    Ok(LifecycleConfig { rules })
}

#[derive(Default)]
struct RuleDraft {
    id: Option<String>,
    status: Option<String>,
    legacy_prefix: Option<String>,
    has_filter: bool,
    filter: FilterSpec,
    in_tag: Option<TagDraft>,
    expiration: Option<ExpirationDraft>,
    noncurrent_expiration: Option<NoncurrentExpDraft>,
    abort_multipart: Option<AbortMultipartDraft>,
    transitions: Vec<TransitionDraft>,
    noncurrent_transitions: Vec<NoncurrentTransDraft>,
}

#[derive(Default)]
struct TagDraft {
    key: Option<String>,
    value: Option<String>,
}

#[derive(Default)]
struct ExpirationDraft {
    days: Option<String>,
    date: Option<String>,
    expired_object_delete_marker: Option<String>,
}

#[derive(Default)]
struct NoncurrentExpDraft {
    noncurrent_days: Option<String>,
    newer_noncurrent_versions: Option<String>,
}

#[derive(Default)]
struct AbortMultipartDraft {
    days_after_initiation: Option<String>,
}

#[derive(Default)]
struct TransitionDraft {
    days: Option<String>,
    date: Option<String>,
    storage_class: Option<String>,
}

#[derive(Default)]
struct NoncurrentTransDraft {
    noncurrent_days: Option<String>,
    newer_noncurrent_versions: Option<String>,
    storage_class: Option<String>,
}

impl RuleDraft {
    fn rel_path<'a>(path: &'a [String], child: &'a str) -> Vec<&'a str> {
        let mut out: Vec<&str> = path.iter().skip(2).map(|s| s.as_str()).collect();
        out.push(child);
        out
    }

    fn on_start(&mut self, path: &[String], name: &str) -> Result<(), Refusal> {
        let rel = Self::rel_path(path, name);
        match rel.as_slice() {
            ["ID"] => {
                self.id.get_or_insert_with(String::new);
            }
            ["Status"] => {
                self.status.get_or_insert_with(String::new);
            }
            ["Prefix"] => {
                self.legacy_prefix.get_or_insert_with(String::new);
            }
            ["Filter"] => {
                self.has_filter = true;
            }
            ["Filter", "Prefix"] | ["Filter", "And", "Prefix"] => {
                self.filter.prefix.get_or_insert_with(String::new);
            }
            ["Filter", "Tag"] | ["Filter", "And", "Tag"] => {
                self.in_tag = Some(TagDraft::default());
            }
            ["Filter", "Tag", "Key"] | ["Filter", "And", "Tag", "Key"] => {
                if let Some(t) = self.in_tag.as_mut() {
                    t.key.get_or_insert_with(String::new);
                }
            }
            ["Filter", "Tag", "Value"] | ["Filter", "And", "Tag", "Value"] => {
                if let Some(t) = self.in_tag.as_mut() {
                    t.value.get_or_insert_with(String::new);
                }
            }
            ["Expiration"] => {
                if self.expiration.is_some() {
                    return Err(malformed("duplicate Expiration element"));
                }
                self.expiration = Some(ExpirationDraft::default());
            }
            ["NoncurrentVersionExpiration"] => {
                if self.noncurrent_expiration.is_some() {
                    return Err(malformed("duplicate NoncurrentVersionExpiration element"));
                }
                self.noncurrent_expiration = Some(NoncurrentExpDraft::default());
            }
            ["AbortIncompleteMultipartUpload"] => {
                if self.abort_multipart.is_some() {
                    return Err(malformed("duplicate AbortIncompleteMultipartUpload element"));
                }
                self.abort_multipart = Some(AbortMultipartDraft::default());
            }
            ["Transition"] => {
                self.transitions.push(TransitionDraft::default());
            }
            ["NoncurrentVersionTransition"] => {
                self.noncurrent_transitions
                    .push(NoncurrentTransDraft::default());
            }
            _ => {}
        }
        Ok(())
    }

    fn on_text(&mut self, path: &[String], text: &str) -> Result<(), Refusal> {
        let rel: Vec<&str> = path.iter().skip(2).map(|s| s.as_str()).collect();
        match rel.as_slice() {
            ["ID"] => self.id.get_or_insert_with(String::new).push_str(text),
            ["Status"] => self.status.get_or_insert_with(String::new).push_str(text),
            ["Prefix"] => self
                .legacy_prefix
                .get_or_insert_with(String::new)
                .push_str(text),
            ["Filter", "Prefix"] | ["Filter", "And", "Prefix"] => {
                self.filter
                    .prefix
                    .get_or_insert_with(String::new)
                    .push_str(text);
            }
            ["Filter", "Tag", "Key"] | ["Filter", "And", "Tag", "Key"] => {
                if let Some(t) = self.in_tag.as_mut() {
                    t.key.get_or_insert_with(String::new).push_str(text);
                }
            }
            ["Filter", "Tag", "Value"] | ["Filter", "And", "Tag", "Value"] => {
                if let Some(t) = self.in_tag.as_mut() {
                    t.value.get_or_insert_with(String::new).push_str(text);
                }
            }
            ["Filter", "ObjectSizeGreaterThan"] | ["Filter", "And", "ObjectSizeGreaterThan"] => {
                let v = text
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| invalid_arg("invalid ObjectSizeGreaterThan"))?;
                self.filter.object_size_greater_than = Some(v);
            }
            ["Filter", "ObjectSizeLessThan"] | ["Filter", "And", "ObjectSizeLessThan"] => {
                let v = text
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| invalid_arg("invalid ObjectSizeLessThan"))?;
                self.filter.object_size_less_than = Some(v);
            }
            ["Expiration", "Days"] => {
                if let Some(e) = self.expiration.as_mut() {
                    e.days.get_or_insert_with(String::new).push_str(text);
                }
            }
            ["Expiration", "Date"] => {
                if let Some(e) = self.expiration.as_mut() {
                    e.date.get_or_insert_with(String::new).push_str(text);
                }
            }
            ["Expiration", "ExpiredObjectDeleteMarker"] => {
                if let Some(e) = self.expiration.as_mut() {
                    e.expired_object_delete_marker
                        .get_or_insert_with(String::new)
                        .push_str(text);
                }
            }
            ["NoncurrentVersionExpiration", "NoncurrentDays"] => {
                if let Some(n) = self.noncurrent_expiration.as_mut() {
                    n.noncurrent_days
                        .get_or_insert_with(String::new)
                        .push_str(text);
                }
            }
            ["NoncurrentVersionExpiration", "NewerNoncurrentVersions"] => {
                if let Some(n) = self.noncurrent_expiration.as_mut() {
                    n.newer_noncurrent_versions
                        .get_or_insert_with(String::new)
                        .push_str(text);
                }
            }
            ["AbortIncompleteMultipartUpload", "DaysAfterInitiation"] => {
                if let Some(a) = self.abort_multipart.as_mut() {
                    a.days_after_initiation
                        .get_or_insert_with(String::new)
                        .push_str(text);
                }
            }
            ["Transition", "Days"] => {
                if let Some(t) = self.transitions.last_mut() {
                    t.days.get_or_insert_with(String::new).push_str(text);
                }
            }
            ["Transition", "Date"] => {
                if let Some(t) = self.transitions.last_mut() {
                    t.date.get_or_insert_with(String::new).push_str(text);
                }
            }
            ["Transition", "StorageClass"] => {
                if let Some(t) = self.transitions.last_mut() {
                    t.storage_class
                        .get_or_insert_with(String::new)
                        .push_str(text);
                }
            }
            ["NoncurrentVersionTransition", "NoncurrentDays"] => {
                if let Some(t) = self.noncurrent_transitions.last_mut() {
                    t.noncurrent_days
                        .get_or_insert_with(String::new)
                        .push_str(text);
                }
            }
            ["NoncurrentVersionTransition", "NewerNoncurrentVersions"] => {
                if let Some(t) = self.noncurrent_transitions.last_mut() {
                    t.newer_noncurrent_versions
                        .get_or_insert_with(String::new)
                        .push_str(text);
                }
            }
            ["NoncurrentVersionTransition", "StorageClass"] => {
                if let Some(t) = self.noncurrent_transitions.last_mut() {
                    t.storage_class
                        .get_or_insert_with(String::new)
                        .push_str(text);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn on_end(&mut self, path: &[String]) -> Result<(), Refusal> {
        let rel: Vec<&str> = path.iter().skip(2).map(|s| s.as_str()).collect();
        if matches!(rel.as_slice(), ["Filter", "Tag"] | ["Filter", "And", "Tag"]) {
            if let Some(tag) = self.in_tag.take() {
                let key = tag
                    .key
                    .filter(|k| !k.is_empty())
                    .ok_or_else(|| malformed("Tag missing Key"))?;
                let value = tag.value.unwrap_or_default();
                self.filter.tags.push(Tag { key, value });
            }
        }
        Ok(())
    }

    fn finish(self, id: String) -> Result<LifecycleRule, Refusal> {
        let status = match self.status.as_deref() {
            Some("Enabled") => RuleStatus::Enabled,
            Some("Disabled") => RuleStatus::Disabled,
            _ => return Err(malformed("Status must be Enabled or Disabled")),
        };

        let filter = match (self.legacy_prefix, self.has_filter) {
            (Some(_), true) => return Err(malformed("Rule cannot have both Prefix and Filter")),
            (None, false) => return Err(malformed("Rule must have Prefix or Filter")),
            (Some(p), false) => RuleFilter::LegacyPrefix(p),
            (None, true) => {
                if let (Some(gt), Some(lt)) = (
                    self.filter.object_size_greater_than,
                    self.filter.object_size_less_than,
                ) {
                    if gt >= lt {
                        return Err(invalid_arg(
                            "ObjectSizeGreaterThan must be less than ObjectSizeLessThan",
                        ));
                    }
                }
                RuleFilter::Filter(self.filter)
            }
        };

        let expiration = match self.expiration {
            None => None,
            Some(e) => Some(parse_expiration(e)?),
        };

        let noncurrent_expiration = match self.noncurrent_expiration {
            None => None,
            Some(n) => {
                let days_s = n
                    .noncurrent_days
                    .ok_or_else(|| malformed("NoncurrentVersionExpiration missing NoncurrentDays"))?;
                let noncurrent_days = parse_positive_u32(&days_s, "NoncurrentDays")?;
                let newer_noncurrent_versions = match n.newer_noncurrent_versions {
                    Some(s) => Some(parse_positive_u32(&s, "NewerNoncurrentVersions")?),
                    None => None,
                };
                Some(NoncurrentVersionExpiration {
                    noncurrent_days,
                    newer_noncurrent_versions,
                })
            }
        };

        let abort_incomplete_multipart_upload = match self.abort_multipart {
            None => None,
            Some(a) => {
                let days_s = a.days_after_initiation.ok_or_else(|| {
                    malformed("AbortIncompleteMultipartUpload missing DaysAfterInitiation")
                })?;
                let days_after_initiation = parse_positive_u32(&days_s, "DaysAfterInitiation")?;
                Some(AbortIncompleteMultipartUpload {
                    days_after_initiation,
                })
            }
        };

        let mut transitions = Vec::with_capacity(self.transitions.len());
        for t in self.transitions {
            let storage_class = t
                .storage_class
                .filter(|s| !s.is_empty())
                .ok_or_else(|| malformed("Transition missing StorageClass"))?;
            let when = match (t.days, t.date) {
                (Some(_), Some(_)) => {
                    return Err(malformed("Transition cannot specify both Days and Date"))
                }
                (None, None) => return Err(malformed("Transition must specify Days or Date")),
                (Some(d), None) => {
                    let days = d
                        .trim()
                        .parse::<u32>()
                        .map_err(|_| invalid_arg("invalid Transition Days"))?;
                    TransitionWhen::Days(days)
                }
                (None, Some(dt)) => TransitionWhen::Date(parse_midnight_utc(&dt)?),
            };
            transitions.push(Transition {
                when,
                storage_class,
                no_effect: true,
            });
        }

        let mut noncurrent_transitions = Vec::with_capacity(self.noncurrent_transitions.len());
        for t in self.noncurrent_transitions {
            let storage_class = t
                .storage_class
                .filter(|s| !s.is_empty())
                .ok_or_else(|| malformed("NoncurrentVersionTransition missing StorageClass"))?;
            let days_s = t
                .noncurrent_days
                .ok_or_else(|| malformed("NoncurrentVersionTransition missing NoncurrentDays"))?;
            let noncurrent_days = parse_positive_u32(&days_s, "NoncurrentDays")?;
            let newer_noncurrent_versions = match t.newer_noncurrent_versions {
                Some(s) => Some(parse_positive_u32(&s, "NewerNoncurrentVersions")?),
                None => None,
            };
            noncurrent_transitions.push(NoncurrentVersionTransition {
                noncurrent_days,
                newer_noncurrent_versions,
                storage_class,
                no_effect: true,
            });
        }

        if expiration.is_none()
            && noncurrent_expiration.is_none()
            && abort_incomplete_multipart_upload.is_none()
            && transitions.is_empty()
            && noncurrent_transitions.is_empty()
        {
            return Err(malformed("Rule must specify at least one action"));
        }

        Ok(LifecycleRule {
            id,
            status,
            filter,
            expiration,
            noncurrent_expiration,
            abort_incomplete_multipart_upload,
            transitions,
            noncurrent_transitions,
        })
    }
}

fn parse_positive_u32(s: &str, field: &'static str) -> Result<u32, Refusal> {
    let val = s
        .trim()
        .parse::<i64>()
        .map_err(|_| malformed("expected integer"))?;
    if val <= 0 || val > u32::MAX as i64 {
        return Err(invalid_arg(field));
    }
    Ok(val as u32)
}

fn parse_expiration(e: ExpirationDraft) -> Result<Expiration, Refusal> {
    match (e.days, e.date, e.expired_object_delete_marker) {
        (Some(_), Some(_), _) => Err(malformed("Expiration cannot specify both Days and Date")),
        (Some(_), _, Some(_)) | (_, Some(_), Some(_)) => Err(malformed(
            "ExpiredObjectDeleteMarker cannot be combined with Days or Date",
        )),
        (Some(d), None, None) => Ok(Expiration::Days(parse_positive_u32(&d, "Days")?)),
        (None, Some(dt), None) => Ok(Expiration::Date(parse_midnight_utc(&dt)?)),
        (None, None, Some(m)) => match m.trim() {
            "true" => Ok(Expiration::ExpiredObjectDeleteMarker(true)),
            "false" => Ok(Expiration::ExpiredObjectDeleteMarker(false)),
            _ => Err(malformed("ExpiredObjectDeleteMarker must be true or false")),
        },
        (None, None, None) => Err(malformed("Expiration must specify Days, Date, or ExpiredObjectDeleteMarker")),
    }
}

fn is_leap_year(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn parse_midnight_utc(raw: &str) -> Result<String, Refusal> {
    let s = raw.trim();
    if s.len() < 10 {
        return Err(invalid_arg("invalid ISO-8601 date"));
    }
    let (date_part, rest) = s.split_at(10);
    let b = date_part.as_bytes();
    if b[4] != b'-' || b[7] != b'-' {
        return Err(invalid_arg("date must use YYYY-MM-DD"));
    }
    let year: u32 = date_part[0..4]
        .parse()
        .map_err(|_| invalid_arg("invalid year"))?;
    let month: u32 = date_part[5..7]
        .parse()
        .map_err(|_| invalid_arg("invalid month"))?;
    let day: u32 = date_part[8..10]
        .parse()
        .map_err(|_| invalid_arg("invalid day"))?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err(invalid_arg("calendar date out of range"));
    }

    let valid_time = match rest {
        "" | "T00:00:00Z" | "T00:00:00+00:00" | "T00:00:00-00:00" => true,
        r if r.starts_with("T00:00:00.") && r.ends_with('Z') => {
            let frac = &r[10..r.len() - 1];
            !frac.is_empty() && frac.bytes().all(|c| c == b'0')
        }
        _ => false,
    };
    if !valid_time {
        return Err(invalid_arg("date must be midnight UTC"));
    }

    Ok(format!("{year:04}-{month:02}-{day:02}T00:00:00Z"))
}

pub fn lifecycle_config_xml(config: &LifecycleConfig) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for r in &config.rules {
        out.push_str("  <Rule>\n");
        out.push_str(&format!("    <ID>{}</ID>\n", xml_escape_exact(&r.id)));
        let status = match r.status {
            RuleStatus::Enabled => "Enabled",
            RuleStatus::Disabled => "Disabled",
        };
        out.push_str(&format!("    <Status>{status}</Status>\n"));

        match &r.filter {
            RuleFilter::LegacyPrefix(p) => {
                out.push_str(&format!("    <Prefix>{}</Prefix>\n", xml_escape_exact(p)));
            }
            RuleFilter::Filter(spec) => {
                let count = usize::from(spec.prefix.is_some())
                    + spec.tags.len()
                    + usize::from(spec.object_size_greater_than.is_some())
                    + usize::from(spec.object_size_less_than.is_some());
                if count == 0 {
                    out.push_str("    <Filter/>\n");
                } else {
                    out.push_str("    <Filter>\n");
                    if count > 1 {
                        out.push_str("      <And>\n");
                    }
                    let indent = if count > 1 { "        " } else { "      " };
                    if let Some(ref p) = spec.prefix {
                        out.push_str(&format!(
                            "{indent}<Prefix>{}</Prefix>\n",
                            xml_escape_exact(p)
                        ));
                    }
                    for tag in &spec.tags {
                        out.push_str(&format!("{indent}<Tag>\n"));
                        out.push_str(&format!(
                            "{indent}  <Key>{}</Key>\n",
                            xml_escape_exact(&tag.key)
                        ));
                        out.push_str(&format!(
                            "{indent}  <Value>{}</Value>\n",
                            xml_escape_exact(&tag.value)
                        ));
                        out.push_str(&format!("{indent}</Tag>\n"));
                    }
                    if let Some(gt) = spec.object_size_greater_than {
                        out.push_str(&format!(
                            "{indent}<ObjectSizeGreaterThan>{gt}</ObjectSizeGreaterThan>\n"
                        ));
                    }
                    if let Some(lt) = spec.object_size_less_than {
                        out.push_str(&format!(
                            "{indent}<ObjectSizeLessThan>{lt}</ObjectSizeLessThan>\n"
                        ));
                    }
                    if count > 1 {
                        out.push_str("      </And>\n");
                    }
                    out.push_str("    </Filter>\n");
                }
            }
        }

        if let Some(ref exp) = r.expiration {
            out.push_str("    <Expiration>\n");
            match exp {
                Expiration::Days(d) => out.push_str(&format!("      <Days>{d}</Days>\n")),
                Expiration::Date(dt) => out.push_str(&format!("      <Date>{dt}</Date>\n")),
                Expiration::ExpiredObjectDeleteMarker(b) => {
                    out.push_str(&format!(
                        "      <ExpiredObjectDeleteMarker>{b}</ExpiredObjectDeleteMarker>\n"
                    ));
                }
            }
            out.push_str("    </Expiration>\n");
        }

        if let Some(ref n) = r.noncurrent_expiration {
            out.push_str("    <NoncurrentVersionExpiration>\n");
            out.push_str(&format!(
                "      <NoncurrentDays>{}</NoncurrentDays>\n",
                n.noncurrent_days
            ));
            if let Some(newer) = n.newer_noncurrent_versions {
                out.push_str(&format!(
                    "      <NewerNoncurrentVersions>{newer}</NewerNoncurrentVersions>\n"
                ));
            }
            out.push_str("    </NoncurrentVersionExpiration>\n");
        }

        if let Some(ref a) = r.abort_incomplete_multipart_upload {
            out.push_str("    <AbortIncompleteMultipartUpload>\n");
            out.push_str(&format!(
                "      <DaysAfterInitiation>{}</DaysAfterInitiation>\n",
                a.days_after_initiation
            ));
            out.push_str("    </AbortIncompleteMultipartUpload>\n");
        }

        for t in &r.transitions {
            out.push_str("    <Transition>\n");
            match &t.when {
                TransitionWhen::Days(d) => out.push_str(&format!("      <Days>{d}</Days>\n")),
                TransitionWhen::Date(dt) => out.push_str(&format!("      <Date>{dt}</Date>\n")),
            }
            out.push_str(&format!(
                "      <StorageClass>{}</StorageClass>\n",
                xml_escape_exact(&t.storage_class)
            ));
            out.push_str("    </Transition>\n");
        }

        for t in &r.noncurrent_transitions {
            out.push_str("    <NoncurrentVersionTransition>\n");
            out.push_str(&format!(
                "      <NoncurrentDays>{}</NoncurrentDays>\n",
                t.noncurrent_days
            ));
            if let Some(newer) = t.newer_noncurrent_versions {
                out.push_str(&format!(
                    "      <NewerNoncurrentVersions>{newer}</NewerNoncurrentVersions>\n"
                ));
            }
            out.push_str(&format!(
                "      <StorageClass>{}</StorageClass>\n",
                xml_escape_exact(&t.storage_class)
            ));
            out.push_str("    </NoncurrentVersionTransition>\n");
        }

        out.push_str("  </Rule>\n");
    }
    out.push_str("</LifecycleConfiguration>");
    out
}

pub fn applies(rule: &LifecycleRule, key: &str, size: u64, tags: &[(&str, &str)]) -> bool {
    if rule.status != RuleStatus::Enabled {
        return false;
    }
    match &rule.filter {
        RuleFilter::LegacyPrefix(p) => key.starts_with(p.as_str()),
        RuleFilter::Filter(spec) => {
            if let Some(ref p) = spec.prefix {
                if !key.starts_with(p.as_str()) {
                    return false;
                }
            }
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
            for tag in &spec.tags {
                if !tags.iter().any(|&(k, v)| k == tag.key && v == tag.value) {
                    return false;
                }
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn wrap_rules(inner: &str) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
            <LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
            {inner}\n\
            </LifecycleConfiguration>"
        )
    }

    #[test]
    fn table_s3_tests_cases() {
        struct Case {
            name: &'static str,
            xml: String,
            expected: Result<(), (&'static str, u16)>,
        }

        let long_id = "a".repeat(256);
        let max_id = "a".repeat(255);

        let cases = vec![
            Case {
                name: "test_lifecycle_set",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>\
                     <Rule><ID>rule2</ID><Prefix>test2/</Prefix><Status>Disabled</Status><Expiration><Days>2</Days></Expiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_get_no_id",
                xml: wrap_rules(
                    "<Rule><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Days>31</Days></Expiration></Rule>\
                     <Rule><ID></ID><Prefix>test2/</Prefix><Status>Enabled</Status><Expiration><Days>120</Days></Expiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_id_max_255",
                xml: wrap_rules(&format!(
                    "<Rule><ID>{max_id}</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule>"
                )),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_id_too_long",
                xml: wrap_rules(&format!(
                    "<Rule><ID>{long_id}</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule>"
                )),
                expected: Err(("InvalidArgument", 400)),
            },
            Case {
                name: "test_lifecycle_same_id",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>\
                     <Rule><ID>rule1</ID><Prefix>test2/</Prefix><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule>",
                ),
                expected: Err(("InvalidArgument", 400)),
            },
            Case {
                name: "test_lifecycle_invalid_status_enabled_lower",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>enabled</Status><Expiration><Days>2</Days></Expiration></Rule>",
                ),
                expected: Err(("MalformedXML", 400)),
            },
            Case {
                name: "test_lifecycle_invalid_status_disabled_lower",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>disabled</Status><Expiration><Days>2</Days></Expiration></Rule>",
                ),
                expected: Err(("MalformedXML", 400)),
            },
            Case {
                name: "test_lifecycle_invalid_status_other",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>invalid</Status><Expiration><Days>2</Days></Expiration></Rule>",
                ),
                expected: Err(("MalformedXML", 400)),
            },
            Case {
                name: "test_lifecycle_set_date_bare",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Date>2017-09-27</Date></Expiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_set_date_iso_midnight",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Date>2017-09-27T00:00:00Z</Date></Expiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_set_invalid_date",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Date>20200101</Date></Expiration></Rule>",
                ),
                expected: Err(("InvalidArgument", 400)),
            },
            Case {
                name: "test_lifecycle_set_non_midnight_date",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Date>2017-09-27T12:00:00Z</Date></Expiration></Rule>",
                ),
                expected: Err(("InvalidArgument", 400)),
            },
            Case {
                name: "test_lifecycle_expiration_days0",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>days0/</Prefix><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule>",
                ),
                expected: Err(("InvalidArgument", 400)),
            },
            Case {
                name: "test_lifecycle_both_days_and_date",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Days>1</Days><Date>2017-09-27T00:00:00Z</Date></Expiration></Rule>",
                ),
                expected: Err(("MalformedXML", 400)),
            },
            Case {
                name: "test_lifecycle_set_noncurrent",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>past/</Prefix><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>2</NoncurrentDays></NoncurrentVersionExpiration></Rule>\
                     <Rule><ID>rule2</ID><Prefix>future/</Prefix><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>3</NoncurrentDays><NewerNoncurrentVersions>5</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_set_deletemarker",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_set_filter",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Filter><Prefix>foo</Prefix></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_set_empty_filter",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Filter></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_set_multipart",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>\
                     <Rule><ID>rule2</ID><Prefix>test2/</Prefix><Status>Disabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_filter_and_tags_sizes",
                xml: wrap_rules(
                    "<Rule><ID>r1</ID><Filter><And><Prefix>logs/</Prefix><Tag><Key>k1</Key><Value>v1</Value></Tag><Tag><Key>k2</Key><Value>v2</Value></Tag><ObjectSizeGreaterThan>100</ObjectSizeGreaterThan><ObjectSizeLessThan>2000</ObjectSizeLessThan></And></Filter><Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_transition_set_invalid_date",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Expiration><Date>2023-09-27</Date></Expiration><Transition><Date>20220927</Date><StorageClass>GLACIER</StorageClass></Transition></Rule>",
                ),
                expected: Err(("InvalidArgument", 400)),
            },
            Case {
                name: "test_lifecycle_transition_days0_ok",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status><Transition><Days>0</Days><StorageClass>GLACIER</StorageClass></Transition></Rule>",
                ),
                expected: Ok(()),
            },
            Case {
                name: "test_lifecycle_no_action_refused",
                xml: wrap_rules(
                    "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status></Rule>",
                ),
                expected: Err(("MalformedXML", 400)),
            },
            Case {
                name: "test_lifecycle_empty_rules_refused",
                xml: wrap_rules(""),
                expected: Err(("MalformedXML", 400)),
            },
        ];

        for case in cases {
            let res = parse_lifecycle_config_xml(&case.xml);
            match (&case.expected, &res) {
                (Ok(()), Ok(cfg)) => {
                    assert!(!cfg.rules.is_empty(), "{}: expected rules", case.name);
                    for r in &cfg.rules {
                        assert!(!r.id.is_empty(), "{}: empty id not generated", case.name);
                        assert!(r.id.chars().count() <= 255, "{}: id > 255", case.name);
                    }
                }
                (Err((exp_code, exp_status)), Err(err)) => {
                    assert_eq!(err.code, *exp_code, "{}: wrong error code", case.name);
                    assert_eq!(err.status, *exp_status, "{}: wrong status", case.name);
                }
                _ => panic!("{}: expected {:?}, got {:?}", case.name, case.expected, res),
            }
        }
    }

    #[test]
    fn too_many_rules_refused() {
        let mut inner = String::new();
        for i in 0..1001 {
            inner.push_str(&format!(
                "<Rule><ID>r{i}</ID><Filter/><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>"
            ));
        }
        let xml = wrap_rules(&inner);
        let err = parse_lifecycle_config_xml(&xml).expect_err("1001 rules must be refused");
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidArgument");
    }

    #[test]
    fn transitions_parsed_and_marked_no_effect() {
        let xml = wrap_rules(
            "<Rule><ID>t1</ID><Filter><Prefix>archive/</Prefix></Filter><Status>Enabled</Status>\
             <Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition>\
             <NoncurrentVersionTransition><NoncurrentDays>60</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions><StorageClass>DEEP_ARCHIVE</StorageClass></NoncurrentVersionTransition>\
             </Rule>",
        );
        let cfg = parse_lifecycle_config_xml(&xml).expect("transitions should parse");
        assert_eq!(cfg.rules[0].transitions.len(), 1);
        assert!(cfg.rules[0].transitions[0].no_effect);
        assert_eq!(cfg.rules[0].transitions[0].storage_class, "GLACIER");
        assert_eq!(cfg.rules[0].noncurrent_transitions.len(), 1);
        assert!(cfg.rules[0].noncurrent_transitions[0].no_effect);
        assert_eq!(cfg.rules[0].noncurrent_transitions[0].storage_class, "DEEP_ARCHIVE");
        assert_eq!(cfg.rules[0].noncurrent_transitions[0].newer_noncurrent_versions, Some(2));
    }

    #[test]
    fn applies_table_test() {
        let make_rule = |status: RuleStatus, filter: RuleFilter| LifecycleRule {
            id: "r1".into(),
            status,
            filter,
            expiration: Some(Expiration::Days(1)),
            noncurrent_expiration: None,
            abort_incomplete_multipart_upload: None,
            transitions: vec![],
            noncurrent_transitions: vec![],
        };

        struct ApplyCase {
            name: &'static str,
            rule: LifecycleRule,
            key: &'static str,
            size: u64,
            tags: &'static [(&'static str, &'static str)],
            want: bool,
        }

        let cases = vec![
            ApplyCase {
                name: "disabled rule never applies",
                rule: make_rule(RuleStatus::Disabled, RuleFilter::Filter(FilterSpec::default())),
                key: "a/b",
                size: 100,
                tags: &[],
                want: false,
            },
            ApplyCase {
                name: "empty filter matches all",
                rule: make_rule(RuleStatus::Enabled, RuleFilter::Filter(FilterSpec::default())),
                key: "anything",
                size: 0,
                tags: &[],
                want: true,
            },
            ApplyCase {
                name: "legacy prefix match",
                rule: make_rule(RuleStatus::Enabled, RuleFilter::LegacyPrefix("logs/".into())),
                key: "logs/2026.txt",
                size: 10,
                tags: &[],
                want: true,
            },
            ApplyCase {
                name: "legacy prefix mismatch",
                rule: make_rule(RuleStatus::Enabled, RuleFilter::LegacyPrefix("logs/".into())),
                key: "data/2026.txt",
                size: 10,
                tags: &[],
                want: false,
            },
            ApplyCase {
                name: "single tag match",
                rule: make_rule(
                    RuleStatus::Enabled,
                    RuleFilter::Filter(FilterSpec {
                        tags: vec![Tag { key: "env".into(), value: "dev".into() }],
                        ..Default::default()
                    }),
                ),
                key: "k",
                size: 1,
                tags: &[("owner", "a"), ("env", "dev")],
                want: true,
            },
            ApplyCase {
                name: "tag key matches but value differs",
                rule: make_rule(
                    RuleStatus::Enabled,
                    RuleFilter::Filter(FilterSpec {
                        tags: vec![Tag { key: "env".into(), value: "dev".into() }],
                        ..Default::default()
                    }),
                ),
                key: "k",
                size: 1,
                tags: &[("env", "prod")],
                want: false,
            },
            ApplyCase {
                name: "size greater than strict boundary",
                rule: make_rule(
                    RuleStatus::Enabled,
                    RuleFilter::Filter(FilterSpec {
                        object_size_greater_than: Some(2000),
                        ..Default::default()
                    }),
                ),
                key: "k",
                size: 2000,
                tags: &[],
                want: false,
            },
            ApplyCase {
                name: "size greater than match",
                rule: make_rule(
                    RuleStatus::Enabled,
                    RuleFilter::Filter(FilterSpec {
                        object_size_greater_than: Some(2000),
                        ..Default::default()
                    }),
                ),
                key: "k",
                size: 2001,
                tags: &[],
                want: true,
            },
            ApplyCase {
                name: "size less than strict boundary",
                rule: make_rule(
                    RuleStatus::Enabled,
                    RuleFilter::Filter(FilterSpec {
                        object_size_less_than: Some(2000),
                        ..Default::default()
                    }),
                ),
                key: "k",
                size: 2000,
                tags: &[],
                want: false,
            },
            ApplyCase {
                name: "and filter all must match",
                rule: make_rule(
                    RuleStatus::Enabled,
                    RuleFilter::Filter(FilterSpec {
                        prefix: Some("days1/".into()),
                        tags: vec![
                            Tag { key: "tom".into(), value: "sawyer".into() },
                            Tag { key: "huck".into(), value: "finn".into() },
                        ],
                        object_size_greater_than: Some(10),
                        object_size_less_than: Some(100),
                    }),
                ),
                key: "days1/tom",
                size: 50,
                tags: &[("tom", "sawyer"), ("huck", "finn")],
                want: true,
            },
            ApplyCase {
                name: "and filter missing second tag",
                rule: make_rule(
                    RuleStatus::Enabled,
                    RuleFilter::Filter(FilterSpec {
                        prefix: Some("days1/".into()),
                        tags: vec![
                            Tag { key: "tom".into(), value: "sawyer".into() },
                            Tag { key: "huck".into(), value: "finn".into() },
                        ],
                        object_size_greater_than: Some(10),
                        object_size_less_than: Some(100),
                    }),
                ),
                key: "days1/tom",
                size: 50,
                tags: &[("tom", "sawyer")],
                want: false,
            },
        ];

        for c in cases {
            assert_eq!(
                applies(&c.rule, c.key, c.size, c.tags),
                c.want,
                "applies case failed: {}",
                c.name
            );
        }
    }

    fn arb_safe_text(max_len: usize) -> impl Strategy<Value = String> {
        proptest::string::string_regex(&format!("[a-zA-Z0-9_./ :-]{{0,{max_len}}}")).unwrap()
    }

    fn arb_filter() -> impl Strategy<Value = RuleFilter> {
        prop_oneof![
            arb_safe_text(32).prop_map(RuleFilter::LegacyPrefix),
            (
                proptest::option::of(arb_safe_text(32)),
                proptest::collection::vec(
                    (arb_safe_text(16), arb_safe_text(16))
                        .prop_filter("non-empty tag key", |(k, _)| !k.is_empty())
                        .prop_map(|(key, value)| Tag { key, value }),
                    0..4,
                ),
                proptest::option::of(0u64..1_000_000),
                proptest::option::of(0u64..1_000_000),
            )
                .prop_map(|(prefix, tags, gt, lt)| {
                    let (gt, lt) = match (gt, lt) {
                        (Some(a), Some(b)) if a >= b => (Some(a), Some(a + 1)),
                        other => other,
                    };
                    RuleFilter::Filter(FilterSpec {
                        prefix,
                        tags,
                        object_size_greater_than: gt,
                        object_size_less_than: lt,
                    })
                }),
        ]
    }

    fn arb_rule(idx: usize) -> impl Strategy<Value = LifecycleRule> {
        (
            prop_oneof![Just(RuleStatus::Enabled), Just(RuleStatus::Disabled)],
            arb_filter(),
            proptest::option::of(prop_oneof![
                (1u32..3650).prop_map(Expiration::Days),
                Just(Expiration::Date("2025-06-15T00:00:00Z".into())),
                any::<bool>().prop_map(Expiration::ExpiredObjectDeleteMarker),
            ]),
            proptest::option::of((1u32..3650, proptest::option::of(1u32..100)).prop_map(
                |(noncurrent_days, newer_noncurrent_versions)| NoncurrentVersionExpiration {
                    noncurrent_days,
                    newer_noncurrent_versions,
                },
            )),
            proptest::option::of((1u32..365).prop_map(|days_after_initiation| {
                AbortIncompleteMultipartUpload {
                    days_after_initiation,
                }
            })),
            proptest::collection::vec(
                (
                    prop_oneof![
                        (0u32..3650).prop_map(TransitionWhen::Days),
                        Just(TransitionWhen::Date("2026-01-01T00:00:00Z".into())),
                    ],
                    prop_oneof![Just("GLACIER".to_string()), Just("STANDARD_IA".to_string())],
                )
                    .prop_map(|(when, storage_class)| Transition {
                        when,
                        storage_class,
                        no_effect: true,
                    }),
                0..3,
            ),
            proptest::collection::vec(
                (
                    1u32..3650,
                    proptest::option::of(1u32..50),
                    prop_oneof![Just("GLACIER".to_string()), Just("DEEP_ARCHIVE".to_string())],
                )
                    .prop_map(
                        |(noncurrent_days, newer_noncurrent_versions, storage_class)| {
                            NoncurrentVersionTransition {
                                noncurrent_days,
                                newer_noncurrent_versions,
                                storage_class,
                                no_effect: true,
                            }
                        },
                    ),
                0..3,
            ),
        )
            .prop_map(
                move |(
                    status,
                    filter,
                    mut expiration,
                    noncurrent_expiration,
                    abort_incomplete_multipart_upload,
                    transitions,
                    noncurrent_transitions,
                )| {
                    if expiration.is_none()
                        && noncurrent_expiration.is_none()
                        && abort_incomplete_multipart_upload.is_none()
                        && transitions.is_empty()
                        && noncurrent_transitions.is_empty()
                    {
                        expiration = Some(Expiration::Days(1));
                    }
                    LifecycleRule {
                        id: format!("rule-{idx}"),
                        status,
                        filter,
                        expiration,
                        noncurrent_expiration,
                        abort_incomplete_multipart_upload,
                        transitions,
                        noncurrent_transitions,
                    }
                },
            )
    }

    proptest! {
        #[test]
        fn round_trip_lifecycle_config(
            rules in (1usize..6).prop_flat_map(|n| {
                (0..n).map(arb_rule).collect::<Vec<_>>()
            })
        ) {
            let cfg = LifecycleConfig { rules };
            let xml = lifecycle_config_xml(&cfg);
            let parsed = parse_lifecycle_config_xml(&xml).expect("serialized config must parse");
            prop_assert_eq!(cfg, parsed);
        }
    }
}
