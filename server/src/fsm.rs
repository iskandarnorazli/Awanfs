//! Explicit state machines (`STATE_MACHINES.md`).
//!
//! Anything in the server that has states and rules for moving between them gets one pure
//! function here: `step(state, event) -> Result<new state, Refusal>`. No I/O, no clock, no
//! locks: the caller reads the state, calls the function under the lock that guards it, and
//! stores the result. Handlers do not decide transitions themselves, so the rules are in one
//! place, and the tests below can try every state against every event.
//!
//! Adding a machine: write the state and event types, the function, a test that covers every
//! (state, event) pair, and a row in `STATE_MACHINES.md`. `scripts/check_state_machines.py`
//! fails CI when a row names a function or test that is not there.

use crate::versions::Versioning;

/// Why a transition was refused: the S3 error code and HTTP status the caller answers with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: &'static str,
    pub status: u16,
}

const fn refuse(code: &'static str, status: u16) -> Refusal {
    Refusal { code, status }
}

pub const INVALID_BUCKET_STATE: Refusal = refuse("InvalidBucketState", 409);
pub const MALFORMED_XML: Refusal = refuse("MalformedXML", 400);
pub const ACCESS_DENIED: Refusal = refuse("AccessDenied", 403);
pub const INVALID_ARGUMENT: Refusal = refuse("InvalidArgument", 400);

/// Bucket versioning (`PUT ?versioning`).
///
/// `Unset -> Enabled | Suspended`, `Enabled <-> Suspended`. Nothing returns to `Unset`: a
/// bucket that has had versions keeps them. A bucket with object lock stays `Enabled`.
pub fn versioning_step(current: Versioning, requested: Versioning, object_lock: bool) -> Result<Versioning, Refusal> {
    match (current, requested, object_lock) {
        (_, Versioning::Unset, _) => Err(MALFORMED_XML),
        (_, Versioning::Suspended, true) => Err(INVALID_BUCKET_STATE),
        (_, Versioning::Enabled, _) => Ok(Versioning::Enabled),
        (_, Versioning::Suspended, false) => Ok(Versioning::Suspended),
    }
}

/// Object lock retention mode of one object version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionMode {
    Governance,
    Compliance,
}

/// What protects one object version from deletion (#120). Times are seconds since the epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Lock {
    pub retention: Option<(RetentionMode, u64)>,
    pub legal_hold: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockEvent {
    /// `PUT ?retention` with a mode and a retain-until time.
    PutRetention { mode: RetentionMode, until: u64, bypass_governance: bool },
    /// `PUT ?retention` with an empty document: remove the retention.
    ClearRetention { bypass_governance: bool },
    /// `PUT ?legal-hold`.
    PutLegalHold { on: bool },
}

impl Lock {
    /// The retention still in force at `now`, if any.
    pub fn active(&self, now: u64) -> Option<(RetentionMode, u64)> {
        self.retention.filter(|&(_, until)| until > now)
    }

    /// Whether this version may be deleted or overwritten at `now`.
    ///
    /// A legal hold forbids it. Compliance retention forbids it until it expires. Governance
    /// retention forbids it unless the caller may bypass governance and asked to.
    pub fn may_delete(&self, now: u64, bypass_governance: bool) -> Result<(), Refusal> {
        if self.legal_hold {
            return Err(ACCESS_DENIED);
        }
        match self.active(now) {
            None => Ok(()),
            Some((RetentionMode::Governance, _)) if bypass_governance => Ok(()),
            Some(_) => Err(ACCESS_DENIED),
        }
    }
}

/// Object lock transitions for one version at time `now`.
///
/// - A retain-until time must be in the future.
/// - Compliance in force: only the same mode with an equal or later time is accepted. Nobody
///   can shorten it, change its mode or remove it.
/// - Governance in force: the same mode with an equal or later time is accepted from anyone
///   who may set retention. Shortening it, changing its mode or removing it needs the bypass.
/// - No retention in force (none, or expired): any retention is accepted.
/// - A legal hold is independent of retention and is switched freely.
pub fn lock_step(current: Lock, event: LockEvent, now: u64) -> Result<Lock, Refusal> {
    match event {
        LockEvent::PutLegalHold { on } => Ok(Lock { legal_hold: on, ..current }),
        LockEvent::PutRetention { mode, until, bypass_governance } => {
            if until <= now {
                return Err(INVALID_ARGUMENT);
            }
            let allowed = match current.active(now) {
                None => true,
                Some((RetentionMode::Compliance, held)) => mode == RetentionMode::Compliance && until >= held,
                Some((RetentionMode::Governance, held)) => bypass_governance || (mode == RetentionMode::Governance && until >= held),
            };
            if allowed {
                Ok(Lock { retention: Some((mode, until)), ..current })
            } else {
                Err(ACCESS_DENIED)
            }
        }
        LockEvent::ClearRetention { bypass_governance } => match current.active(now) {
            None => Ok(Lock { retention: None, ..current }),
            Some((RetentionMode::Governance, _)) if bypass_governance => Ok(Lock { retention: None, ..current }),
            Some(_) => Err(ACCESS_DENIED),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use RetentionMode::{Compliance, Governance};

    const STATUSES: [Versioning; 3] = [Versioning::Unset, Versioning::Enabled, Versioning::Suspended];

    #[test]
    fn versioning_every_pair_is_decided() {
        use Versioning::{Enabled, Suspended, Unset};
        // (current, requested, object lock) -> result. Every combination appears once.
        let table = [
            (Unset, Unset, false, Err(MALFORMED_XML)),
            (Unset, Enabled, false, Ok(Enabled)),
            (Unset, Suspended, false, Ok(Suspended)),
            (Enabled, Unset, false, Err(MALFORMED_XML)),
            (Enabled, Enabled, false, Ok(Enabled)),
            (Enabled, Suspended, false, Ok(Suspended)),
            (Suspended, Unset, false, Err(MALFORMED_XML)),
            (Suspended, Enabled, false, Ok(Enabled)),
            (Suspended, Suspended, false, Ok(Suspended)),
            (Unset, Unset, true, Err(MALFORMED_XML)),
            (Unset, Enabled, true, Ok(Enabled)),
            (Unset, Suspended, true, Err(INVALID_BUCKET_STATE)),
            (Enabled, Unset, true, Err(MALFORMED_XML)),
            (Enabled, Enabled, true, Ok(Enabled)),
            (Enabled, Suspended, true, Err(INVALID_BUCKET_STATE)),
            (Suspended, Unset, true, Err(MALFORMED_XML)),
            (Suspended, Enabled, true, Ok(Enabled)),
            (Suspended, Suspended, true, Err(INVALID_BUCKET_STATE)),
        ];
        let mut seen = Vec::new();
        for (cur, req, lock, want) in table {
            assert_eq!(versioning_step(cur, req, lock), want, "{:?} -> {:?}, lock {}", cur, req, lock);
            assert!(!seen.contains(&(cur, req, lock)), "listed twice");
            seen.push((cur, req, lock));
        }
        assert_eq!(seen.len(), STATUSES.len() * STATUSES.len() * 2, "a combination is missing from the table");
    }

    #[test]
    fn versioning_never_returns_to_unset() {
        for cur in STATUSES {
            for req in STATUSES {
                for lock in [false, true] {
                    if let Ok(next) = versioning_step(cur, req, lock) {
                        assert_ne!(next, Versioning::Unset);
                    }
                }
            }
        }
    }

    /// Every lock state, event and time in a small domain that still has a time before,
    /// between and after each retain-until value.
    fn lock_domain() -> (Vec<Lock>, Vec<LockEvent>, [u64; 3]) {
        let mut states = Vec::new();
        for hold in [false, true] {
            states.push(Lock { retention: None, legal_hold: hold });
            for mode in [Governance, Compliance] {
                for until in [5, 10] {
                    states.push(Lock { retention: Some((mode, until)), legal_hold: hold });
                }
            }
        }
        let mut events = vec![LockEvent::PutLegalHold { on: true }, LockEvent::PutLegalHold { on: false }];
        for bypass in [false, true] {
            events.push(LockEvent::ClearRetention { bypass_governance: bypass });
            for mode in [Governance, Compliance] {
                for until in [4, 8, 10, 11, 20] {
                    events.push(LockEvent::PutRetention { mode, until, bypass_governance: bypass });
                }
            }
        }
        (states, events, [3, 7, 12])
    }

    #[test]
    fn compliance_in_force_is_never_weakened() {
        let (states, events, times) = lock_domain();
        let mut checked = 0;
        for s in &states {
            for &now in &times {
                let Some((Compliance, held)) = s.active(now) else { continue };
                assert_eq!(s.may_delete(now, true), Err(ACCESS_DENIED));
                for &e in &events {
                    let next = lock_step(*s, e, now).unwrap_or(*s);
                    match next.active(now) {
                        Some((Compliance, until)) => assert!(until >= held, "{:?} + {:?} at {} shortened it", s, e, now),
                        other => panic!("{:?} + {:?} at {} left {:?}", s, e, now, other),
                    }
                    checked += 1;
                }
            }
        }
        assert!(checked > 100, "the domain no longer reaches this rule: {}", checked);
    }

    #[test]
    fn governance_in_force_needs_the_bypass_to_weaken() {
        let (states, events, times) = lock_domain();
        let mut checked = 0;
        for s in &states {
            for &now in &times {
                let Some((Governance, held)) = s.active(now) else { continue };
                assert_eq!(s.may_delete(now, false), Err(ACCESS_DENIED));
                for &e in &events {
                    let bypass = matches!(e, LockEvent::PutRetention { bypass_governance: true, .. } | LockEvent::ClearRetention { bypass_governance: true });
                    if bypass {
                        continue;
                    }
                    let next = lock_step(*s, e, now).unwrap_or(*s);
                    match next.active(now) {
                        Some((Governance, until)) => assert!(until >= held, "{:?} + {:?} at {} shortened it", s, e, now),
                        other => panic!("{:?} + {:?} at {} left {:?} without the bypass", s, e, now, other),
                    }
                    checked += 1;
                }
            }
        }
        assert!(checked > 50, "the domain no longer reaches this rule: {}", checked);
    }

    #[test]
    fn a_legal_hold_forbids_delete_whatever_else_holds() {
        let (states, _, times) = lock_domain();
        for s in states.iter().filter(|s| s.legal_hold) {
            for &now in &times {
                for bypass in [false, true] {
                    assert_eq!(s.may_delete(now, bypass), Err(ACCESS_DENIED));
                }
            }
        }
    }

    #[test]
    fn with_nothing_in_force_any_future_retention_is_accepted_and_delete_is_allowed() {
        let (states, events, times) = lock_domain();
        for s in &states {
            for &now in &times {
                if s.active(now).is_some() {
                    continue;
                }
                if !s.legal_hold {
                    assert_eq!(s.may_delete(now, false), Ok(()));
                }
                for &e in &events {
                    if let LockEvent::PutRetention { mode, until, .. } = e {
                        let got = lock_step(*s, e, now);
                        if until > now {
                            assert_eq!(got, Ok(Lock { retention: Some((mode, until)), legal_hold: s.legal_hold }));
                        } else {
                            assert_eq!(got, Err(INVALID_ARGUMENT));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_retain_until_time_in_the_past_is_always_refused() {
        let (states, _, _) = lock_domain();
        for s in &states {
            for mode in [Governance, Compliance] {
                for bypass in [false, true] {
                    let e = LockEvent::PutRetention { mode, until: 7, bypass_governance: bypass };
                    assert_eq!(lock_step(*s, e, 7), Err(INVALID_ARGUMENT));
                    assert_eq!(lock_step(*s, e, 9), Err(INVALID_ARGUMENT));
                }
            }
        }
    }

    #[test]
    fn the_cases_the_s3_tests_name() {
        let gov = Lock { retention: Some((Governance, 10)), legal_hold: false };
        let comp = Lock { retention: Some((Compliance, 10)), legal_hold: false };
        // test_object_lock_changing_mode_from_governance_without_bypass
        assert_eq!(lock_step(gov, LockEvent::PutRetention { mode: Compliance, until: 10, bypass_governance: false }, 3), Err(ACCESS_DENIED));
        // ..._with_bypass
        assert_eq!(lock_step(gov, LockEvent::PutRetention { mode: Compliance, until: 10, bypass_governance: true }, 3).unwrap().retention, Some((Compliance, 10)));
        // test_object_lock_changing_mode_from_compliance
        assert_eq!(lock_step(comp, LockEvent::PutRetention { mode: Governance, until: 10, bypass_governance: true }, 3), Err(ACCESS_DENIED));
        // test_object_lock_put_obj_retention_increase_period / _shorten_period / _shorten_period_bypass
        assert!(lock_step(gov, LockEvent::PutRetention { mode: Governance, until: 20, bypass_governance: false }, 3).is_ok());
        assert_eq!(lock_step(gov, LockEvent::PutRetention { mode: Governance, until: 8, bypass_governance: false }, 3), Err(ACCESS_DENIED));
        assert!(lock_step(gov, LockEvent::PutRetention { mode: Governance, until: 8, bypass_governance: true }, 3).is_ok());
        // test_object_lock_delete_object_with_retention: denied, then allowed with the bypass
        assert_eq!(gov.may_delete(3, false), Err(ACCESS_DENIED));
        assert_eq!(gov.may_delete(3, true), Ok(()));
        // after it expires anyone may delete
        assert_eq!(comp.may_delete(10, false), Ok(()));
        // a legal hold does not touch the retention, and the reverse
        let held = lock_step(gov, LockEvent::PutLegalHold { on: true }, 3).unwrap();
        assert_eq!(held, Lock { retention: gov.retention, legal_hold: true });
        assert_eq!(lock_step(held, LockEvent::PutLegalHold { on: false }, 3).unwrap(), gov);
    }
}
