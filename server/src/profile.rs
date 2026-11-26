//! `--profile production` (#358): refuse to start in a configuration that is unsafe in
//! production, so no launcher has to get every flag right. The check is a pure function over the
//! startup facts; `main.rs` gathers them and exits 2 with one line per reason.

use std::sync::OnceLock;

/// Which profile the server runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Default,
    Production,
}

impl Profile {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "default" => Some(Profile::Default),
            "production" => Some(Profile::Production),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Profile::Default => "default",
            Profile::Production => "production",
        }
    }
}

static ACTIVE: OnceLock<Profile> = OnceLock::new();

/// Records the profile the process started with; set once, by `main`.
pub fn set_active(p: Profile) {
    let _ = ACTIVE.set(p);
}

/// The profile the process started with (`default` when `main` set none, as in tests).
pub fn active() -> Profile {
    ACTIVE.get().copied().unwrap_or(Profile::Default)
}

/// The shortest secret key production accepts, in bytes. AWS secret keys are 40.
pub const MIN_SECRET_LEN: usize = 32;

/// Secret keys that are public: documentation examples and the keys this repository's own
/// docs, scripts and test suites use. Production refuses each of them.
pub const KNOWN_DEFAULT_SECRETS: &[&str] = &[
    "minioadmin",
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "s3secret",
    "mysecret",
    "change-me",
    "warps3secret",
    "smokesecret",
    "DEFAULT_SECRET_KEY",
    "awanfs-main-secret-for-s3-tests-0000000000",
    "awanfs-alt-secret-for-s3-tests-00000000000",
    "awanfs-tenant-secret-for-s3-tests-0000000",
    "awanfs-main-secret-for-mint-tests-0000000",
];

/// What `main` knows at startup that decides whether production may run.
pub struct StartupFacts<'a> {
    pub anonymous: bool,
    pub tls: bool,
    pub plain_http_port: bool,
    pub listen: &'a str,
    /// `AWANFS_SECRET_KEY`, when set.
    pub root_secret: Option<&'a str>,
    /// (user name, secret) for every user in `--config`.
    pub user_secrets: Vec<(&'a str, &'a str)>,
    /// `--durability fast`: PUTs acknowledged before they are on disk.
    pub durability_fast: bool,
}

fn secret_problem(who: &str, secret: &str) -> Option<String> {
    if KNOWN_DEFAULT_SECRETS.contains(&secret) {
        Some(format!("{}'s secret key is a published default: generate a new one", who))
    } else if secret.len() < MIN_SECRET_LEN {
        Some(format!("{}'s secret key is {} bytes; production needs at least {}", who, secret.len(), MIN_SECRET_LEN))
    } else {
        None
    }
}

/// Every reason `facts` is unsafe for production; empty when it may start.
pub fn production_problems(facts: &StartupFacts) -> Vec<String> {
    let mut out = Vec::new();
    if facts.anonymous {
        out.push("anonymous access is on (--allow-anonymous or AWANFS_ALLOW_ANONYMOUS=1)".to_string());
    }
    if !facts.tls {
        out.push("TLS is off: give --cert and --key".to_string());
        let host = facts.listen.trim_start_matches('[').trim_end_matches(']');
        if host == "0.0.0.0" || host == "::" {
            out.push(format!("--listen {} binds every interface without TLS", facts.listen));
        }
    }
    if facts.plain_http_port {
        out.push("--http-port serves plain HTTP next to TLS".to_string());
    }
    if let Some(s) = facts.root_secret {
        out.extend(secret_problem("AWANFS_SECRET_KEY", s));
    }
    for (name, s) in &facts.user_secrets {
        out.extend(secret_problem(&format!("user {}", name), s));
    }
    if facts.durability_fast {
        out.push("--durability fast acknowledges PUTs before they are on disk".to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "Zq7v1Xb9Lw2Rk8Ty4Hn6Pm3Jc5Gd0Fs2Ae7Ui9Oo";

    fn safe() -> StartupFacts<'static> {
        StartupFacts {
            anonymous: false,
            tls: true,
            plain_http_port: false,
            listen: "0.0.0.0",
            root_secret: Some(GOOD),
            user_secrets: vec![("alice", GOOD)],
            durability_fast: false,
        }
    }

    #[test]
    fn a_safe_configuration_has_no_problems() {
        assert_eq!(production_problems(&safe()), Vec::<String>::new());
    }

    #[test]
    fn each_unsafe_setting_alone_gives_its_reason() {
        let one = |f: StartupFacts| {
            let p = production_problems(&f);
            assert_eq!(p.len(), 1, "{:?}", p);
            p[0].clone()
        };
        assert!(one(StartupFacts { anonymous: true, ..safe() }).contains("anonymous"));
        assert!(one(StartupFacts { tls: false, listen: "127.0.0.1", ..safe() }).contains("TLS is off"));
        assert!(one(StartupFacts { plain_http_port: true, ..safe() }).contains("--http-port"));
        assert!(one(StartupFacts { root_secret: Some("short"), ..safe() }).contains("AWANFS_SECRET_KEY"));
        assert!(one(StartupFacts { user_secrets: vec![("bob", "minioadmin")], ..safe() }).contains("user bob"));
        assert!(one(StartupFacts { durability_fast: true, ..safe() }).contains("--durability fast"));
    }

    #[test]
    fn binding_every_interface_without_tls_is_named() {
        for listen in ["0.0.0.0", "::", "[::]"] {
            let p = production_problems(&StartupFacts { tls: false, listen, ..safe() });
            assert_eq!(p.len(), 2, "{} {:?}", listen, p);
            assert!(p[1].contains("binds every interface"), "{:?}", p);
        }
    }

    #[test]
    fn the_secret_length_line_is_32_bytes() {
        let s31 = "a".repeat(31);
        let s32 = "a".repeat(32);
        assert_eq!(production_problems(&StartupFacts { root_secret: Some(&s31), user_secrets: vec![], ..safe() }).len(), 1);
        assert!(production_problems(&StartupFacts { root_secret: Some(&s32), user_secrets: vec![], ..safe() }).is_empty());
    }

    #[test]
    fn every_published_secret_is_refused_whatever_its_length() {
        for s in KNOWN_DEFAULT_SECRETS {
            let p = production_problems(&StartupFacts { root_secret: Some(s), user_secrets: vec![], ..safe() });
            assert_eq!(p.len(), 1, "{}", s);
            assert!(p[0].contains("published default"), "{}: {:?}", s, p);
        }
    }

    #[test]
    fn profile_names_parse_and_print() {
        for p in [Profile::Default, Profile::Production] {
            assert_eq!(Profile::parse(p.as_str()), Some(p));
        }
        assert_eq!(Profile::parse("prod"), None);
    }
}
