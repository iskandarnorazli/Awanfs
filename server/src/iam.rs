//! Several access keys with policies (#53, owner decisions 12 and 19).
//!
//! Users live in a TOML config file (`--config`, `AWANFS_CONFIG`) that must not be readable by
//! group or others on Unix (it holds secrets: SigV4 needs them in the clear). Each user has an
//! AWS-style policy in a subset of the IAM JSON grammar:
//!
//! ```toml
//! [[users]]
//! name = "reader"
//! access_key = "AKREADER"
//! secret_key = "..."
//! policy = '''{"Statement": [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
//!                             "Resource": ["arn:aws:s3:::photos", "arn:aws:s3:::photos/*"]}]}'''
//! ```
//!
//! Effect is Allow or Deny; Action and Resource are strings or lists with `*` and `?`
//! wildcards. An explicit Deny wins, then an Allow, else the request is denied. Statements with
//! Condition, Principal, NotAction or NotResource are refused when the file is loaded, because
//! ignoring them would grant more than written. The file is reloaded when it changes (checked
//! every 60 s) and on SIGHUP (Unix); a reload that fails keeps the users already loaded.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    #[serde(default)]
    users: Vec<UserEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UserEntry {
    name: String,
    access_key: String,
    secret_key: String,
    policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone)]
struct Statement {
    effect: Effect,
    actions: Vec<String>,
    resources: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    pub secret: String,
    statements: Vec<Statement>,
}

/// Who signed a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    /// Unsigned, accepted because of `--allow-anonymous`.
    Anonymous,
    /// The `AWANFS_ACCESS_KEY` pair: full access.
    Root,
    /// A user from the config file (by access key), limited by its policy.
    User(String),
}

/// `*` matches any run of characters, `?` one character; everything else is literal.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), text.chars().collect());
    let (mut pi, mut ti) = (0, 0);
    let (mut star, mut mark) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn strings(v: &serde_json::Value, field: &str) -> Result<Vec<String>, String> {
    match v {
        serde_json::Value::String(s) => Ok(vec![s.clone()]),
        serde_json::Value::Array(a) => a
            .iter()
            .map(|x| x.as_str().map(String::from).ok_or_else(|| format!("{} must be strings", field)))
            .collect(),
        _ => Err(format!("{} must be a string or a list of strings", field)),
    }
}

fn parse_policy(text: &str) -> Result<Vec<Statement>, String> {
    let doc: serde_json::Value = serde_json::from_str(text).map_err(|e| format!("policy is not JSON: {}", e))?;
    let list = match doc.get("Statement") {
        Some(serde_json::Value::Array(a)) => a.clone(),
        Some(o @ serde_json::Value::Object(_)) => vec![o.clone()],
        _ => return Err("policy has no Statement".into()),
    };
    let mut out = Vec::new();
    for st in list {
        let obj = st.as_object().ok_or("a Statement must be an object")?;
        for unsupported in ["Condition", "Principal", "NotPrincipal", "NotAction", "NotResource"] {
            if obj.contains_key(unsupported) {
                return Err(format!("{} is not supported; it would be ignored and grant more than written", unsupported));
            }
        }
        for k in obj.keys() {
            if !["Sid", "Effect", "Action", "Resource"].contains(&k.as_str()) {
                return Err(format!("unknown statement field {}", k));
            }
        }
        let effect = match obj.get("Effect").and_then(|e| e.as_str()) {
            Some("Allow") => Effect::Allow,
            Some("Deny") => Effect::Deny,
            _ => return Err("Effect must be Allow or Deny".into()),
        };
        let actions = strings(obj.get("Action").ok_or("a Statement needs Action")?, "Action")?;
        let resources = strings(obj.get("Resource").ok_or("a Statement needs Resource")?, "Resource")?;
        out.push(Statement { effect, actions, resources });
    }
    Ok(out)
}

/// The users of one config file, by access key.
#[derive(Debug, Default)]
pub struct Iam {
    users: HashMap<String, User>,
}

impl Iam {
    pub fn parse(text: &str) -> Result<Self, String> {
        let file: ConfigFile = toml::from_str(text).map_err(|e| format!("config: {}", e))?;
        let mut users = HashMap::new();
        for u in file.users {
            if u.access_key.is_empty() || u.secret_key.is_empty() {
                return Err(format!("user {}: empty access_key or secret_key", u.name));
            }
            let statements = parse_policy(&u.policy).map_err(|e| format!("user {}: {}", u.name, e))?;
            if users.insert(u.access_key.clone(), User { name: u.name.clone(), secret: u.secret_key, statements }).is_some() {
                return Err(format!("access key of user {} is used twice", u.name));
            }
        }
        Ok(Iam { users })
    }

    /// Reads a config file; on Unix it must not be readable or writable by group or others.
    pub fn load(path: &Path) -> Result<Self, String> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(path).map_err(|e| format!("{}: {}", path.display(), e))?.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(format!("{} holds secrets and is readable by others (mode {:o}); chmod 600 it", path.display(), mode & 0o777));
            }
        }
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {}", path.display(), e))?;
        Self::parse(&text)
    }

    pub fn user(&self, access_key: &str) -> Option<&User> {
        self.users.get(access_key)
    }

    pub fn len(&self) -> usize {
        self.users.len()
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    /// May the user with `access_key` do `action` on `resource`? Deny wins, then Allow, else no.
    pub fn allows(&self, access_key: &str, action: &str, resource: &str) -> bool {
        let Some(u) = self.users.get(access_key) else { return false };
        let hits = |e: Effect| {
            u.statements.iter().any(|s| {
                s.effect == e && s.actions.iter().any(|a| glob_match(a, action)) && s.resources.iter().any(|r| glob_match(r, resource))
            })
        };
        !hits(Effect::Deny) && hits(Effect::Allow)
    }
}

/// The users in force, reloaded from their file when it changes.
#[derive(Debug)]
pub struct ReloadingIam {
    path: PathBuf,
    current: RwLock<Arc<Iam>>,
    stamp: RwLock<Option<SystemTime>>,
}

impl ReloadingIam {
    pub fn open(path: &Path) -> Result<Arc<Self>, String> {
        let iam = Iam::load(path)?;
        Ok(Arc::new(ReloadingIam {
            path: path.to_path_buf(),
            current: RwLock::new(Arc::new(iam)),
            stamp: RwLock::new(std::fs::metadata(path).and_then(|m| m.modified()).ok()),
        }))
    }

    pub fn current(&self) -> Arc<Iam> {
        Arc::clone(&self.current.read().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn reload(&self) -> Result<(), String> {
        let iam = Iam::load(&self.path)?;
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(iam);
        *self.stamp.write().unwrap_or_else(|e| e.into_inner()) = std::fs::metadata(&self.path).and_then(|m| m.modified()).ok();
        Ok(())
    }

    fn reload_if_changed(&self) -> Result<bool, String> {
        let now = std::fs::metadata(&self.path).and_then(|m| m.modified()).ok();
        if now == *self.stamp.read().unwrap_or_else(|e| e.into_inner()) {
            return Ok(false);
        }
        self.reload().map(|_| true)
    }

    /// Checks the file every `every`; on Unix also reloads on SIGHUP.
    pub fn watch(self: &Arc<Self>, every: std::time::Duration) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            #[cfg(unix)]
            let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok();
            loop {
                #[cfg(unix)]
                let forced = match hup.as_mut() {
                    Some(h) => tokio::select! {
                        _ = tokio::time::sleep(every) => false,
                        _ = h.recv() => true,
                    },
                    None => {
                        tokio::time::sleep(every).await;
                        false
                    }
                };
                #[cfg(not(unix))]
                let forced = {
                    tokio::time::sleep(every).await;
                    false
                };
                let r = if forced { me.reload().map(|_| true) } else { me.reload_if_changed() };
                match r {
                    Ok(true) => eprintln!("users reloaded from {}", me.path.display()),
                    Ok(false) => {}
                    Err(e) => eprintln!("users not reloaded, keeping the current ones: {}", e),
                }
            }
        });
    }
}

/// The IAM actions and resources a request needs, AWS names (`s3:GetObject` on
/// `arn:aws:s3:::bucket/key`). DeleteObjects is checked per key in its handler.
pub fn requirements(method: &str, bucket: Option<&str>, key: Option<&str>, query: &str, copy_source: Option<&str>) -> Vec<(&'static str, String)> {
    let has = |k: &str| query.split('&').any(|p| p == k || p.starts_with(&format!("{}=", k)));
    let arn_b = |b: &str| format!("arn:aws:s3:::{}", b);
    let arn_o = |b: &str, k: &str| format!("arn:aws:s3:::{}/{}", b, k);
    match (bucket, key) {
        (None, _) => vec![("s3:ListAllMyBuckets", "arn:aws:s3:::*".to_string())],
        (Some(b), None) => {
            let action = match method {
                "PUT" if has("lifecycle") => "s3:PutLifecycleConfiguration",
                "DELETE" if has("lifecycle") => "s3:PutLifecycleConfiguration",
                "GET" if has("lifecycle") => "s3:GetLifecycleConfiguration",
                "GET" if has("uploads") => "s3:ListBucketMultipartUploads",
                "POST" if has("delete") => return vec![],
                "PUT" => "s3:CreateBucket",
                "DELETE" => "s3:DeleteBucket",
                "GET" | "HEAD" => "s3:ListBucket",
                _ => "s3:Unsupported",
            };
            vec![(action, arn_b(b))]
        }
        (Some(b), Some(k)) => {
            let action = match method {
                "GET" if has("uploadId") => "s3:ListMultipartUploadParts",
                "DELETE" if has("uploadId") => "s3:AbortMultipartUpload",
                "PUT" | "POST" => "s3:PutObject",
                "GET" | "HEAD" => "s3:GetObject",
                "DELETE" => "s3:DeleteObject",
                _ => "s3:Unsupported",
            };
            let mut out = vec![(action, arn_o(b, k))];
            if method == "PUT" {
                if let Some(src) = copy_source {
                    let src = crate::s3_api::percent_decode(src.split('?').next().unwrap_or(src));
                    out.push(("s3:GetObject", format!("arn:aws:s3:::{}", src.trim_start_matches('/'))));
                }
            }
            out
        }
    }
}
