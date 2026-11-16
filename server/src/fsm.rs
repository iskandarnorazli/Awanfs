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

use crate::bucket_config::ConfigKind;
use crate::versions::Versioning;

/// Why a transition was refused: the S3 error code and HTTP status the caller answers with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: &'static str,
    pub status: u16,
    /// Which rule refused, so two refusals with the same S3 code stay distinct (and a test
    /// can tell one rule from another).
    pub why: &'static str,
}

const fn refuse(code: &'static str, status: u16) -> Refusal {
    Refusal { code, status, why: code }
}

const fn refuse_because(code: &'static str, status: u16, why: &'static str) -> Refusal {
    Refusal { code, status, why }
}

pub const INVALID_BUCKET_STATE: Refusal = refuse("InvalidBucketState", 409);
pub const MALFORMED_XML: Refusal = refuse("MalformedXML", 400);
pub const ACCESS_DENIED: Refusal = refuse("AccessDenied", 403);
pub const INVALID_ARGUMENT: Refusal = refuse("InvalidArgument", 400);
pub const NO_SUCH_BUCKET: Refusal = refuse("NoSuchBucket", 404);
pub const BUCKET_ALREADY_OWNED_BY_YOU: Refusal = refuse("BucketAlreadyOwnedByYou", 409);
pub const BUCKET_ALREADY_EXISTS: Refusal = refuse("BucketAlreadyExists", 409);
pub const BUCKET_NOT_EMPTY: Refusal = refuse("BucketNotEmpty", 409);
pub const INVALID_BUCKET_NAME: Refusal = refuse("InvalidBucketName", 400);
pub const KEY_TOO_LONG: Refusal = refuse("KeyTooLongError", 400);

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

pub const BODY_TOO_LONG: Refusal = refuse_because("IncompleteBody", 400, "more bytes than the declared length");
pub const BODY_TOO_SHORT: Refusal = refuse_because("IncompleteBody", 400, "fewer bytes than the declared length");
pub const WRITER_FAILED: Refusal = refuse_because("InternalError", 500, "a disk write of this needle failed");
pub const WRITER_COMMITTED: Refusal = refuse_because("InternalError", 500, "the needle's header is already written");

/// One needle written as its payload arrives (`volume::NeedleWriter`, #36, #154).
///
/// Its offset is reserved when the writer is made; the payload is written in pieces; the
/// header sector, which makes the needle visible to recovery, is written last, by `Finish`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NeedleState {
    /// Receiving the payload: `written` of `declared` bytes so far.
    Receiving { declared: u64, written: u64 },
    /// A disk write failed. Its header was never written, so recovery steps over it; nothing
    /// more may be written or committed.
    Failed,
    /// The header is written: the needle is complete and journaled.
    Committed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NeedleEvent {
    /// `n` more payload bytes.
    Write(u64),
    /// A write to disk returned an error.
    WriteFailed,
    /// Write the header.
    Finish,
}

/// A writer that has failed or committed takes nothing more. A payload never grows past its
/// declared length, and the header is written only when the payload is all there.
pub fn needle_step(state: NeedleState, event: NeedleEvent) -> Result<NeedleState, Refusal> {
    match (state, event) {
        (NeedleState::Receiving { declared, written }, NeedleEvent::Write(n)) => match written.checked_add(n) {
            Some(total) if total <= declared => Ok(NeedleState::Receiving { declared, written: total }),
            _ => Err(BODY_TOO_LONG),
        },
        (NeedleState::Receiving { .. }, NeedleEvent::WriteFailed) => Ok(NeedleState::Failed),
        (NeedleState::Receiving { declared, written }, NeedleEvent::Finish) => {
            if written == declared {
                Ok(NeedleState::Committed)
            } else {
                Err(BODY_TOO_SHORT)
            }
        }
        (NeedleState::Failed, _) => Err(WRITER_FAILED),
        (NeedleState::Committed, _) => Err(WRITER_COMMITTED),
    }
}

pub const STREAM_ENDED: Refusal = refuse_because("InternalError", 500, "the response stream has ended");

/// One GET response streamed from a volume with positioned reads (`stream_read`, #155).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamState {
    /// The headers are sent; no payload byte has been read yet.
    Opening,
    /// At least one chunk has been sent.
    Streaming,
    /// Every byte was sent, and its checksum (when the whole payload was sent) matched.
    Done,
    /// Cut before the end. The client sees a transfer shorter than its Content-Length.
    Aborted(StreamAbort),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamAbort {
    /// A read from the volume failed.
    ReadError,
    /// The bytes read do not match the needle's payload CRC; the last chunk is not sent.
    ChecksumMismatch,
    /// The client went away; the reader stops and frees its buffers.
    ClientGone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    /// A chunk was read and handed to the response.
    Chunk,
    ReadError,
    ClientGone,
    /// The last byte was read. `checksum_ok` is false when the whole payload was read and
    /// its CRC does not match; a range has no checksum and ends with `true`.
    End { checksum_ok: bool },
}

/// A stream that has ended takes nothing more: no chunk after `Aborted` or `Done`.
pub fn stream_step(state: StreamState, event: StreamEvent) -> Result<StreamState, Refusal> {
    use StreamState::{Aborted, Done, Opening, Streaming};
    match (state, event) {
        (Opening | Streaming, StreamEvent::Chunk) => Ok(Streaming),
        (Opening | Streaming, StreamEvent::ReadError) => Ok(Aborted(StreamAbort::ReadError)),
        (Opening | Streaming, StreamEvent::ClientGone) => Ok(Aborted(StreamAbort::ClientGone)),
        (Opening | Streaming, StreamEvent::End { checksum_ok: true }) => Ok(Done),
        (Opening | Streaming, StreamEvent::End { checksum_ok: false }) => Ok(Aborted(StreamAbort::ChecksumMismatch)),
        (Done | Aborted(_), _) => Err(STREAM_ENDED),
    }
}

pub const TASK_ALREADY_RUNNING: Refusal = refuse_because("TaskBusy", 409, "a tick is already running");
pub const TASK_NOT_RUNNING: Refusal = refuse_because("TaskIdle", 409, "no tick is running");
pub const TASK_STOPPING: Refusal = refuse_because("TaskStopping", 409, "task is stopping");
pub const TASK_STOPPED: Refusal = refuse_because("TaskStopped", 409, "task has stopped");

/// State of one scheduled background task (`scheduler.rs`, #158).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    Idle,
    Running,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskEvent {
    Tick,
    Done,
    Stop,
}

/// Transition function for a scheduled background task (#158).
///
/// `Idle -> Running -> Idle`, `Idle -> Stopped`, `Running -> Stopping -> Stopped`.
/// A second `Tick` while `Running` is refused rather than starting a second thread.
pub fn task_step(state: TaskState, event: TaskEvent) -> Result<TaskState, Refusal> {
    use TaskEvent::{Done, Stop, Tick};
    use TaskState::{Idle, Running, Stopped, Stopping};
    match (state, event) {
        (Idle, Tick) => Ok(Running),
        (Idle, Done) => Err(TASK_NOT_RUNNING),
        (Idle, Stop) => Ok(Stopped),
        (Running, Tick) => Err(TASK_ALREADY_RUNNING),
        (Running, Done) => Ok(Idle),
        (Running, Stop) => Ok(Stopping),
        (Stopping, Tick) => Err(TASK_STOPPING),
        (Stopping, Done) => Ok(Stopped),
        (Stopping, Stop) => Err(TASK_STOPPING),
        (Stopped, _) => Err(TASK_STOPPED),
    }
}

#[cfg(test)]
mod task_tests {
    use super::*;

    #[test]
    fn task_every_pair_is_decided() {
        use TaskEvent::{Done, Stop, Tick};
        use TaskState::{Idle, Running, Stopped, Stopping};
        let table = [
            (Idle, Tick, Ok(Running)),
            (Idle, Done, Err(TASK_NOT_RUNNING)),
            (Idle, Stop, Ok(Stopped)),
            (Running, Tick, Err(TASK_ALREADY_RUNNING)),
            (Running, Done, Ok(Idle)),
            (Running, Stop, Ok(Stopping)),
            (Stopping, Tick, Err(TASK_STOPPING)),
            (Stopping, Done, Ok(Stopped)),
            (Stopping, Stop, Err(TASK_STOPPING)),
            (Stopped, Tick, Err(TASK_STOPPED)),
            (Stopped, Done, Err(TASK_STOPPED)),
            (Stopped, Stop, Err(TASK_STOPPED)),
        ];
        for (state, event, want) in table {
            assert_eq!(task_step(state, event), want, "{:?} + {:?}", state, event);
        }
        assert_eq!(table.len(), 4 * 3, "every state against every event");
    }
}

/// Lifecycle of a bucket (#149).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketStatus {
    Absent,
    Active,
    Deleted,
}

/// Presence of one configuration kind on a bucket (#149).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigStatus {
    Unset,
    Set,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BucketState {
    pub status: BucketStatus,
    pub owner_id: u32,
    pub configs: u16,
}

impl BucketState {
    pub const fn absent() -> Self {
        Self {
            status: BucketStatus::Absent,
            owner_id: 0,
            configs: 0,
        }
    }

    pub const fn config_status(&self, kind: ConfigKind) -> ConfigStatus {
        if self.configs & kind.bit() != 0 {
            ConfigStatus::Set
        } else {
            ConfigStatus::Unset
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketEvent {
    Create { caller_id: u32, is_us_east_1: bool },
    Delete { empty: bool },
    PutConfig(ConfigKind),
    DeleteConfig(ConfigKind),
}

/// Bucket and per-kind configuration transitions (#149).
///
/// - `Absent -> Active` on `Create`, `Active -> Deleted` on `Delete { empty: true }`.
/// - `Create` on `Active` by owner in `us-east-1` is idempotent (`Ok`), else 409
///   `BucketAlreadyOwnedByYou`; by another account is 409 `BucketAlreadyExists`.
/// - `Delete` on non-empty `Active` is 409 `BucketNotEmpty`; on empty `Active` drops all configs.
/// - `PutConfig(kind)` sets `kind` (`Unset -> Set`); `DeleteConfig(kind)` clears `kind` (`Set -> Unset`).
/// - Any `Delete` or config event on `Absent` or `Deleted` is 404 `NoSuchBucket`.
pub fn bucket_step(state: BucketState, event: BucketEvent) -> Result<BucketState, Refusal> {
    match (state.status, event) {
        (BucketStatus::Absent, BucketEvent::Create { caller_id, .. }) => Ok(BucketState {
            status: BucketStatus::Active,
            owner_id: caller_id,
            configs: 0,
        }),
        (BucketStatus::Active, BucketEvent::Create { caller_id, is_us_east_1 }) => {
            if caller_id == state.owner_id {
                if is_us_east_1 {
                    Ok(state)
                } else {
                    Err(BUCKET_ALREADY_OWNED_BY_YOU)
                }
            } else {
                Err(BUCKET_ALREADY_EXISTS)
            }
        }
        (BucketStatus::Deleted, BucketEvent::Create { .. }) => Err(INVALID_BUCKET_STATE),
        (BucketStatus::Absent | BucketStatus::Deleted, _) => Err(NO_SUCH_BUCKET),
        (BucketStatus::Active, BucketEvent::Delete { empty: false }) => Err(BUCKET_NOT_EMPTY),
        (BucketStatus::Active, BucketEvent::Delete { empty: true }) => Ok(BucketState {
            status: BucketStatus::Deleted,
            owner_id: state.owner_id,
            configs: 0,
        }),
        (BucketStatus::Active, BucketEvent::PutConfig(kind)) => Ok(BucketState {
            configs: state.configs | kind.bit(),
            ..state
        }),
        (BucketStatus::Active, BucketEvent::DeleteConfig(kind)) => Ok(BucketState {
            configs: state.configs & !kind.bit(),
            ..state
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_every_pair_is_decided() {
        use StreamAbort::{ChecksumMismatch, ClientGone, ReadError};
        use StreamState::{Aborted, Done, Opening, Streaming};
        let chunk = StreamEvent::Chunk;
        let read_err = StreamEvent::ReadError;
        let gone = StreamEvent::ClientGone;
        let ok = StreamEvent::End { checksum_ok: true };
        let bad = StreamEvent::End { checksum_ok: false };
        let ended = [Done, Aborted(ReadError), Aborted(ChecksumMismatch), Aborted(ClientGone)];
        let mut table = vec![
            (Opening, chunk, Ok(Streaming)),
            (Streaming, chunk, Ok(Streaming)),
            (Opening, read_err, Ok(Aborted(ReadError))),
            (Streaming, read_err, Ok(Aborted(ReadError))),
            (Opening, gone, Ok(Aborted(ClientGone))),
            (Streaming, gone, Ok(Aborted(ClientGone))),
            (Opening, ok, Ok(Done)),
            (Streaming, ok, Ok(Done)),
            (Opening, bad, Ok(Aborted(ChecksumMismatch))),
            (Streaming, bad, Ok(Aborted(ChecksumMismatch))),
        ];
        for s in ended {
            for e in [chunk, read_err, gone, ok, bad] {
                table.push((s, e, Err(STREAM_ENDED)));
            }
        }
        for (state, event, want) in &table {
            assert_eq!(stream_step(*state, *event), *want, "{:?} + {:?}", state, event);
        }
        assert_eq!(table.len(), 6 * 5, "every state against every event");
    }
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

    #[test]
    fn the_needle_refusals_are_distinct() {
        let all = [BODY_TOO_LONG, BODY_TOO_SHORT, WRITER_FAILED, WRITER_COMMITTED];
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn needle_every_pair_is_decided() {
        use NeedleEvent::{Finish, Write, WriteFailed};
        use NeedleState::{Committed, Failed, Receiving};
        let r = |written| Receiving { declared: 10, written };
        // (state, event) -> result. Every state kind against every event kind, with the byte
        // counts at, below and past the declared length.
        let table = [
            (r(0), Write(0), Ok(r(0))),
            (r(0), Write(4), Ok(r(4))),
            (r(4), Write(6), Ok(r(10))),
            (r(4), Write(7), Err(BODY_TOO_LONG)),
            (r(10), Write(1), Err(BODY_TOO_LONG)),
            (r(4), Write(u64::MAX), Err(BODY_TOO_LONG)),
            (r(4), WriteFailed, Ok(Failed)),
            (r(10), WriteFailed, Ok(Failed)),
            (r(10), Finish, Ok(Committed)),
            (r(9), Finish, Err(BODY_TOO_SHORT)),
            (r(0), Finish, Err(BODY_TOO_SHORT)),
            (Failed, Write(1), Err(WRITER_FAILED)),
            (Failed, WriteFailed, Err(WRITER_FAILED)),
            (Failed, Finish, Err(WRITER_FAILED)),
            (Committed, Write(1), Err(WRITER_COMMITTED)),
            (Committed, WriteFailed, Err(WRITER_COMMITTED)),
            (Committed, Finish, Err(WRITER_COMMITTED)),
        ];
        for (state, event, want) in table {
            assert_eq!(needle_step(state, event), want, "{:?} + {:?}", state, event);
        }
        // Each (state kind, event kind) pair appears at least once.
        let kind = |s: &NeedleState| match s { Receiving { .. } => 0, Failed => 1, Committed => 2 };
        let ev = |e: &NeedleEvent| match e { Write(_) => 0, WriteFailed => 1, Finish => 2 };
        for k in 0..3 {
            for e in 0..3 {
                assert!(table.iter().any(|(s, x, _)| kind(s) == k && ev(x) == e), "state kind {} with event kind {} is not in the table", k, e);
            }
        }
    }

    #[test]
    fn a_needle_is_committed_only_with_exactly_its_declared_bytes() {
        // Any sequence of writes, in a small domain: committed iff the writes add up exactly.
        for declared in 0..6u64 {
            for a in 0..7u64 {
                for b in 0..7u64 {
                    let mut st = NeedleState::Receiving { declared, written: 0 };
                    let mut refused = false;
                    for n in [a, b] {
                        match needle_step(st, NeedleEvent::Write(n)) {
                            Ok(next) => st = next,
                            Err(e) => {
                                assert!(!refused || e == WRITER_FAILED, "{:?}", e);
                                assert!(refused || e == BODY_TOO_LONG, "{:?}", e);
                                // The writer stops at its first refusal (NeedleWriter::write).
                                st = NeedleState::Failed;
                                refused = true;
                            }
                        }
                    }
                    let done = needle_step(st, NeedleEvent::Finish);
                    let sent = if refused { None } else { Some(a + b) };
                    assert_eq!(done.is_ok(), sent == Some(declared), "declared {} writes {} {} -> {:?}", declared, a, b, done);
                    if let Ok(c) = done {
                        assert_eq!(needle_step(c, NeedleEvent::Write(0)), Err(WRITER_COMMITTED), "nothing after the header");
                    }
                }
            }
        }
    }

    #[test]
    fn bucket_every_pair_is_decided() {
        let kind = ConfigKind::Policy;
        let states = [
            BucketState { status: BucketStatus::Absent, owner_id: 1, configs: 0 },
            BucketState { status: BucketStatus::Active, owner_id: 1, configs: 0 },
            BucketState { status: BucketStatus::Active, owner_id: 1, configs: kind.bit() },
            BucketState { status: BucketStatus::Deleted, owner_id: 1, configs: 0 },
        ];
        let events = [
            BucketEvent::Create { caller_id: 1, is_us_east_1: true },
            BucketEvent::Create { caller_id: 1, is_us_east_1: false },
            BucketEvent::Create { caller_id: 2, is_us_east_1: true },
            BucketEvent::Delete { empty: false },
            BucketEvent::Delete { empty: true },
            BucketEvent::PutConfig(kind),
            BucketEvent::DeleteConfig(kind),
        ];
        let mut count = 0usize;
        for s in states {
            for e in events {
                let res = bucket_step(s, e);
                match (s.status, e) {
                    (BucketStatus::Absent, BucketEvent::Create { caller_id, .. }) => {
                        assert_eq!(res, Ok(BucketState { status: BucketStatus::Active, owner_id: caller_id, configs: 0 }));
                    }
                    (BucketStatus::Active, BucketEvent::Create { caller_id: 1, is_us_east_1: true }) => {
                        assert_eq!(res, Ok(s));
                    }
                    (BucketStatus::Active, BucketEvent::Create { caller_id: 1, is_us_east_1: false }) => {
                        assert_eq!(res, Err(BUCKET_ALREADY_OWNED_BY_YOU));
                    }
                    (BucketStatus::Active, BucketEvent::Create { .. }) => {
                        assert_eq!(res, Err(BUCKET_ALREADY_EXISTS));
                    }
                    (BucketStatus::Deleted, BucketEvent::Create { .. }) => {
                        assert_eq!(res, Err(INVALID_BUCKET_STATE));
                    }
                    (BucketStatus::Absent | BucketStatus::Deleted, _) => {
                        assert_eq!(res, Err(NO_SUCH_BUCKET));
                    }
                    (BucketStatus::Active, BucketEvent::Delete { empty: false }) => {
                        assert_eq!(res, Err(BUCKET_NOT_EMPTY));
                    }
                    (BucketStatus::Active, BucketEvent::Delete { empty: true }) => {
                        assert_eq!(res, Ok(BucketState { status: BucketStatus::Deleted, owner_id: 1, configs: 0 }));
                    }
                    (BucketStatus::Active, BucketEvent::PutConfig(k)) => {
                        let next = res.unwrap();
                        assert_eq!(next.status, BucketStatus::Active);
                        assert_eq!(next.config_status(k), ConfigStatus::Set);
                    }
                    (BucketStatus::Active, BucketEvent::DeleteConfig(k)) => {
                        let next = res.unwrap();
                        assert_eq!(next.status, BucketStatus::Active);
                        assert_eq!(next.config_status(k), ConfigStatus::Unset);
                    }
                }
                count += 1;
            }
        }
        assert_eq!(count, states.len() * events.len());
    }

    #[test]
    fn bucket_deleted_drops_configs_and_stays_deleted() {
        let mut st = BucketState::absent();
        st = bucket_step(st, BucketEvent::Create { caller_id: 7, is_us_east_1: true }).unwrap();
        for &k in ConfigKind::ALL {
            st = bucket_step(st, BucketEvent::PutConfig(k)).unwrap();
            assert_eq!(st.config_status(k), ConfigStatus::Set);
        }
        st = bucket_step(st, BucketEvent::Delete { empty: true }).unwrap();
        assert_eq!(st.status, BucketStatus::Deleted);
        assert_eq!(st.configs, 0);
        for &k in ConfigKind::ALL {
            assert_eq!(st.config_status(k), ConfigStatus::Unset);
            assert_eq!(bucket_step(st, BucketEvent::PutConfig(k)), Err(NO_SUCH_BUCKET));
            assert_eq!(bucket_step(st, BucketEvent::DeleteConfig(k)), Err(NO_SUCH_BUCKET));
        }
    }
}
