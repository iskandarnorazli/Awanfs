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
pub const NO_SUCH_KEY: Refusal = refuse("NoSuchKey", 404);
pub const NO_SUCH_VERSION: Refusal = refuse("NoSuchVersion", 404);
pub const METHOD_NOT_ALLOWED_DELETE_MARKER: Refusal =
    refuse_because("MethodNotAllowed", 405, "target is a delete marker");

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


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadEncryption {
    None,
    SseC,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartSseC {
    Absent,
    Matched,
    Mismatched,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadState {
    Open(UploadEncryption),
    Completed,
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadEvent {
    UploadPart { sse_c: PartSseC },
    ListParts,
    Complete { sse_c: PartSseC },
    Abort,
}

pub const NO_SUCH_UPLOAD: Refusal = Refusal {
    code: "NoSuchUpload",
    status: 404,
    why: "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
};

pub const SSE_C_REQUIRED: Refusal = Refusal {
    code: "InvalidRequest",
    status: 400,
    why: "The upload was created with Customer-Provided Server-Side Encryption; matching SSE-C headers are required.",
};

pub const SSE_C_MISMATCH: Refusal = Refusal {
    code: "InvalidArgument",
    status: 400,
    why: "The provided SSE-C encryption key does not match the upload's encryption parameters.",
};

pub const SSE_C_UNEXPECTED: Refusal = Refusal {
    code: "InvalidArgument",
    status: 400,
    why: "SSE-C encryption headers were provided for an unencrypted multipart upload.",
};

/// Transitions for a multipart upload (`#111`, `#121`):
/// - `Open(None)`: `UploadPart { Absent }` / `ListParts` keep `Open(None)`; `Complete { Absent }` -> `Completed`; `Abort` -> `Aborted`; any SSE-C header on part/complete -> `SSE_C_UNEXPECTED`.
/// - `Open(SseC)`: `UploadPart { Matched }` keeps `Open(SseC)`; `Complete { Matched }` -> `Completed`; `ListParts` keeps `Open(SseC)`; `Abort` -> `Aborted`; `Absent` -> `SSE_C_REQUIRED`; `Mismatched` -> `SSE_C_MISMATCH`.
/// - `Completed`: `Complete` answers the completed result (`Ok(Completed)`); other events -> `NoSuchUpload`.
/// - `Aborted`: all events -> `NoSuchUpload`.
pub fn upload_step(state: UploadState, event: UploadEvent) -> Result<UploadState, Refusal> {
    match (state, event) {
        (UploadState::Open(UploadEncryption::None), UploadEvent::UploadPart { sse_c: PartSseC::Absent }) => {
            Ok(UploadState::Open(UploadEncryption::None))
        }
        (
            UploadState::Open(UploadEncryption::None),
            UploadEvent::UploadPart { sse_c: PartSseC::Matched | PartSseC::Mismatched }
            | UploadEvent::Complete { sse_c: PartSseC::Matched | PartSseC::Mismatched },
        ) => Err(SSE_C_UNEXPECTED),
        (UploadState::Open(UploadEncryption::None), UploadEvent::Complete { sse_c: PartSseC::Absent }) => {
            Ok(UploadState::Completed)
        }
        (UploadState::Open(UploadEncryption::SseC), UploadEvent::UploadPart { sse_c: PartSseC::Matched }) => {
            Ok(UploadState::Open(UploadEncryption::SseC))
        }
        (UploadState::Open(UploadEncryption::SseC), UploadEvent::Complete { sse_c: PartSseC::Matched }) => {
            Ok(UploadState::Completed)
        }
        (
            UploadState::Open(UploadEncryption::SseC),
            UploadEvent::UploadPart { sse_c: PartSseC::Absent } | UploadEvent::Complete { sse_c: PartSseC::Absent },
        ) => Err(SSE_C_REQUIRED),
        (
            UploadState::Open(UploadEncryption::SseC),
            UploadEvent::UploadPart { sse_c: PartSseC::Mismatched } | UploadEvent::Complete { sse_c: PartSseC::Mismatched },
        ) => Err(SSE_C_MISMATCH),
        (UploadState::Open(enc), UploadEvent::ListParts) => Ok(UploadState::Open(enc)),
        (UploadState::Open(_), UploadEvent::Abort) => Ok(UploadState::Aborted),
        (UploadState::Completed, UploadEvent::Complete { .. }) => Ok(UploadState::Completed),
        (UploadState::Completed, _) | (UploadState::Aborted, _) => Err(NO_SUCH_UPLOAD),
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

    #[test]
    fn upload_every_pair_is_decided() {
        let states = [
            UploadState::Open(UploadEncryption::None),
            UploadState::Open(UploadEncryption::SseC),
            UploadState::Completed,
            UploadState::Aborted,
        ];
        let events = [
            UploadEvent::UploadPart { sse_c: PartSseC::Absent },
            UploadEvent::UploadPart { sse_c: PartSseC::Matched },
            UploadEvent::UploadPart { sse_c: PartSseC::Mismatched },
            UploadEvent::ListParts,
            UploadEvent::Complete { sse_c: PartSseC::Absent },
            UploadEvent::Complete { sse_c: PartSseC::Matched },
            UploadEvent::Complete { sse_c: PartSseC::Mismatched },
            UploadEvent::Abort,
        ];
        let mut count = 0usize;
        for s in states {
            for e in events {
                let res = upload_step(s, e);
                match (s, e) {
                    (UploadState::Open(UploadEncryption::None), UploadEvent::UploadPart { sse_c: PartSseC::Absent }) => {
                        assert_eq!(res, Ok(UploadState::Open(UploadEncryption::None)));
                    }
                    (
                        UploadState::Open(UploadEncryption::None),
                        UploadEvent::UploadPart { sse_c: PartSseC::Matched | PartSseC::Mismatched }
                        | UploadEvent::Complete { sse_c: PartSseC::Matched | PartSseC::Mismatched },
                    ) => {
                        assert_eq!(res, Err(SSE_C_UNEXPECTED));
                    }
                    (UploadState::Open(UploadEncryption::None), UploadEvent::Complete { sse_c: PartSseC::Absent }) => {
                        assert_eq!(res, Ok(UploadState::Completed));
                    }
                    (UploadState::Open(UploadEncryption::SseC), UploadEvent::UploadPart { sse_c: PartSseC::Matched }) => {
                        assert_eq!(res, Ok(UploadState::Open(UploadEncryption::SseC)));
                    }
                    (UploadState::Open(UploadEncryption::SseC), UploadEvent::Complete { sse_c: PartSseC::Matched }) => {
                        assert_eq!(res, Ok(UploadState::Completed));
                    }
                    (
                        UploadState::Open(UploadEncryption::SseC),
                        UploadEvent::UploadPart { sse_c: PartSseC::Absent }
                        | UploadEvent::Complete { sse_c: PartSseC::Absent },
                    ) => {
                        assert_eq!(res, Err(SSE_C_REQUIRED));
                    }
                    (
                        UploadState::Open(UploadEncryption::SseC),
                        UploadEvent::UploadPart { sse_c: PartSseC::Mismatched }
                        | UploadEvent::Complete { sse_c: PartSseC::Mismatched },
                    ) => {
                        assert_eq!(res, Err(SSE_C_MISMATCH));
                    }
                    (UploadState::Open(enc), UploadEvent::ListParts) => {
                        assert_eq!(res, Ok(UploadState::Open(enc)));
                    }
                    (UploadState::Open(_), UploadEvent::Abort) => {
                        assert_eq!(res, Ok(UploadState::Aborted));
                    }
                    (UploadState::Completed, UploadEvent::Complete { .. }) => {
                        assert_eq!(res, Ok(UploadState::Completed));
                    }
                    (UploadState::Completed, _) | (UploadState::Aborted, _) => {
                        assert_eq!(res, Err(NO_SUCH_UPLOAD));
                    }
                }
                count += 1;
            }
        }
        assert_eq!(count, states.len() * events.len());
    }

    #[test]
    fn completed_upload_complete_is_idempotent() {
        let s = upload_step(
            UploadState::Open(UploadEncryption::None),
            UploadEvent::Complete { sse_c: PartSseC::Absent },
        )
        .unwrap();
        assert_eq!(s, UploadState::Completed);
        assert_eq!(
            upload_step(s, UploadEvent::Complete { sse_c: PartSseC::Absent }),
            Ok(UploadState::Completed)
        );
        assert_eq!(
            upload_step(s, UploadEvent::UploadPart { sse_c: PartSseC::Absent }),
            Err(NO_SUCH_UPLOAD)
        );
        assert_eq!(upload_step(s, UploadEvent::ListParts), Err(NO_SUCH_UPLOAD));
        assert_eq!(upload_step(s, UploadEvent::Abort), Err(NO_SUCH_UPLOAD));
    }
}

/// State of a streamed PUT's bounded hash-and-write pipeline (#170).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineState {
    Filling,
    Draining,
    Failed,
    Done,
}

/// Event driving [`pipeline_step`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineEvent {
    Frame,
    EndOfBody,
    Drained,
    HashError,
    WriteError,
    Disconnect,
    LengthMismatch,
}

pub const PIPELINE_TERMINAL: Refusal = Refusal {
    status: 400,
    code: "InvalidPipelineState",
    why: "streamed PUT pipeline is not in a valid state for this event",
};

/// Pure transition function for the streamed PUT hash/write pipeline (#170).
pub fn pipeline_step(state: PipelineState, event: PipelineEvent) -> Result<PipelineState, Refusal> {
    match (state, event) {
        (PipelineState::Filling, PipelineEvent::Frame) => Ok(PipelineState::Filling),
        (PipelineState::Filling, PipelineEvent::EndOfBody) => Ok(PipelineState::Draining),
        (PipelineState::Draining, PipelineEvent::Drained) => Ok(PipelineState::Done),
        (
            PipelineState::Filling | PipelineState::Draining,
            PipelineEvent::HashError
            | PipelineEvent::WriteError
            | PipelineEvent::Disconnect
            | PipelineEvent::LengthMismatch,
        ) => Ok(PipelineState::Failed),
        _ => Err(PIPELINE_TERMINAL),
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;

    #[test]
    fn pipeline_every_pair_is_decided() {
        let states = [
            PipelineState::Filling,
            PipelineState::Draining,
            PipelineState::Failed,
            PipelineState::Done,
        ];
        let events = [
            PipelineEvent::Frame,
            PipelineEvent::EndOfBody,
            PipelineEvent::Drained,
            PipelineEvent::HashError,
            PipelineEvent::WriteError,
            PipelineEvent::Disconnect,
            PipelineEvent::LengthMismatch,
        ];
        let mut count = 0usize;
        for s in states {
            for e in events {
                let res = pipeline_step(s, e);
                match (s, e) {
                    (PipelineState::Filling, PipelineEvent::Frame) => {
                        assert_eq!(res, Ok(PipelineState::Filling));
                    }
                    (PipelineState::Filling, PipelineEvent::EndOfBody) => {
                        assert_eq!(res, Ok(PipelineState::Draining));
                    }
                    (PipelineState::Draining, PipelineEvent::Drained) => {
                        assert_eq!(res, Ok(PipelineState::Done));
                    }
                    (
                        PipelineState::Filling | PipelineState::Draining,
                        PipelineEvent::HashError
                        | PipelineEvent::WriteError
                        | PipelineEvent::Disconnect
                        | PipelineEvent::LengthMismatch,
                    ) => {
                        assert_eq!(res, Ok(PipelineState::Failed));
                    }
                    (PipelineState::Filling, PipelineEvent::Drained)
                    | (PipelineState::Draining, PipelineEvent::Frame | PipelineEvent::EndOfBody)
                    | (PipelineState::Failed, _)
                    | (PipelineState::Done, _) => {
                        assert_eq!(res, Err(PIPELINE_TERMINAL));
                    }
                }
                count += 1;
            }
        }
        assert_eq!(count, states.len() * events.len());
    }
}


/// State of a streamed UploadPart's bounded hash-and-write pipeline (#57).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartPipelineState {
    Filling,
    Draining,
    Failed,
    Done,
}

/// Event driving [`part_pipeline_step`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartPipelineEvent {
    Frame,
    EndOfBody,
    Drained,
    HashError,
    WriteError,
    Disconnect,
    LengthMismatch,
}

pub const PART_PIPELINE_TERMINAL: Refusal = Refusal {
    status: 400,
    code: "InvalidPipelineState",
    why: "streamed UploadPart pipeline is not in a valid state for this event",
};

/// Pure transition function for the streamed UploadPart hash/write pipeline (#57).
pub fn part_pipeline_step(state: PartPipelineState, event: PartPipelineEvent) -> Result<PartPipelineState, Refusal> {
    match (state, event) {
        (PartPipelineState::Filling, PartPipelineEvent::Frame) => Ok(PartPipelineState::Filling),
        (PartPipelineState::Filling, PartPipelineEvent::EndOfBody) => Ok(PartPipelineState::Draining),
        (PartPipelineState::Draining, PartPipelineEvent::Drained) => Ok(PartPipelineState::Done),
        (
            PartPipelineState::Filling | PartPipelineState::Draining,
            PartPipelineEvent::HashError
            | PartPipelineEvent::WriteError
            | PartPipelineEvent::Disconnect
            | PartPipelineEvent::LengthMismatch,
        ) => Ok(PartPipelineState::Failed),
        _ => Err(PART_PIPELINE_TERMINAL),
    }
}

#[cfg(test)]
mod part_pipeline_tests {
    use super::*;

    #[test]
    fn part_pipeline_every_pair_is_decided() {
        let states = [
            PartPipelineState::Filling,
            PartPipelineState::Draining,
            PartPipelineState::Failed,
            PartPipelineState::Done,
        ];
        let events = [
            PartPipelineEvent::Frame,
            PartPipelineEvent::EndOfBody,
            PartPipelineEvent::Drained,
            PartPipelineEvent::HashError,
            PartPipelineEvent::WriteError,
            PartPipelineEvent::Disconnect,
            PartPipelineEvent::LengthMismatch,
        ];
        let mut count = 0usize;
        for s in states {
            for e in events {
                let res = part_pipeline_step(s, e);
                match (s, e) {
                    (PartPipelineState::Filling, PartPipelineEvent::Frame) => {
                        assert_eq!(res, Ok(PartPipelineState::Filling));
                    }
                    (PartPipelineState::Filling, PartPipelineEvent::EndOfBody) => {
                        assert_eq!(res, Ok(PartPipelineState::Draining));
                    }
                    (PartPipelineState::Draining, PartPipelineEvent::Drained) => {
                        assert_eq!(res, Ok(PartPipelineState::Done));
                    }
                    (
                        PartPipelineState::Filling | PartPipelineState::Draining,
                        PartPipelineEvent::HashError
                        | PartPipelineEvent::WriteError
                        | PartPipelineEvent::Disconnect
                        | PartPipelineEvent::LengthMismatch,
                    ) => {
                        assert_eq!(res, Ok(PartPipelineState::Failed));
                    }
                    (PartPipelineState::Filling, PartPipelineEvent::Drained)
                    | (PartPipelineState::Draining, PartPipelineEvent::Frame | PartPipelineEvent::EndOfBody)
                    | (PartPipelineState::Failed, _)
                    | (PartPipelineState::Done, _) => {
                        assert_eq!(res, Err(PART_PIPELINE_TERMINAL));
                    }
                }
                count += 1;
            }
        }
        assert_eq!(count, states.len() * events.len());
    }
}

/// Latest live `0x200` attribute update location for one `AttrGroup` on one object version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyAttrUpdate {
    pub group: crate::attrs::AttrGroup,
    pub created_at_ns: u64,
    pub vol_offset: u64,
    pub total_len: u64,
}

/// Base needle metadata plus any `0x200` attribute updates for one live object version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyObject {
    /// `0` for unversioned or `null` version; otherwise the version creation timestamp (`ts`).
    pub version_id: u64,
    pub created_at_ns: u64,
    pub vol_offset: u64,
    pub flags: u16,
    pub updates: Vec<KeyAttrUpdate>,
}

impl KeyObject {
    fn apply_update(&mut self, upd: KeyAttrUpdate) {
        if upd.created_at_ns <= self.created_at_ns {
            // Superseded by a newer base object write or copy.
            return;
        }
        if let Some(existing) = self.updates.iter_mut().find(|u| u.group == upd.group) {
            if (upd.created_at_ns, upd.vol_offset) > (existing.created_at_ns, existing.vol_offset) {
                *existing = upd;
            }
        } else {
            self.updates.push(upd);
        }
    }
}

/// One entry in a versioned key's history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyVersionEntry {
    Object(KeyObject),
    DeleteMarker {
        version_id: u64,
        vol_offset: u64,
    },
}

impl KeyVersionEntry {
    pub fn version_id(&self) -> u64 {
        match self {
            Self::Object(obj) => obj.version_id,
            Self::DeleteMarker { version_id, .. } => *version_id,
        }
    }
}

/// Live state of one object key (`ObjectVersions.tla`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyState {
    Absent,
    Unversioned(KeyObject),
    Versioned { versions: Vec<KeyVersionEntry> },
}

/// One durable log record affecting a key's state or attributes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyRecord {
    Object {
        versioned: bool,
        is_null: bool,
        created_at_ns: u64,
        vol_offset: u64,
        flags: u16,
    },
    Tombstone {
        versioned: bool,
        is_null: bool,
        created_at_ns: u64,
        vol_offset: u64,
    },
    VersionDelete {
        target_version_id: u64,
    },
    AttrUpdate {
        target_version_id: u64,
        group: crate::attrs::AttrGroup,
        created_at_ns: u64,
        vol_offset: u64,
        total_len: u64,
    },
}

fn upsert_version(versions: &mut Vec<KeyVersionEntry>, entry: KeyVersionEntry) {
    let vid = entry.version_id();
    if let Some(slot) = versions.iter_mut().find(|e| e.version_id() == vid) {
        *slot = entry;
    } else {
        versions.push(entry);
    }
}

/// Pure transition fold for the live state of one object key (#150 stage 2, #203).
///
/// Handles object puts, tombstones, version deletes, and `0x200` attribute updates.
/// Both request handlers (under per-key lock) and boot recovery fold records through this
/// function. Updating an absent key/version or a delete marker returns a refusal at request
/// time (`404 NoSuchKey` / `404 NoSuchVersion` / `405 MethodNotAllowed`) and is ignored and
/// counted during recovery replay so updates never resurrect deleted data.
pub fn key_step(state: KeyState, record: KeyRecord) -> Result<KeyState, Refusal> {
    match record {
        KeyRecord::Object {
            versioned,
            is_null,
            created_at_ns,
            vol_offset,
            flags,
        } => {
            if !versioned {
                let obj = KeyObject {
                    version_id: 0,
                    created_at_ns,
                    vol_offset,
                    flags,
                    updates: Vec::new(),
                };
                match state {
                    KeyState::Absent | KeyState::Unversioned(_) => Ok(KeyState::Unversioned(obj)),
                    KeyState::Versioned { mut versions } => {
                        // Decision #186 null replay: unversioned PUT against existing history updates the null version.
                        upsert_version(&mut versions, KeyVersionEntry::Object(obj));
                        Ok(KeyState::Versioned { versions })
                    }
                }
            } else {
                let vid = if is_null { 0 } else { created_at_ns };
                let entry = KeyVersionEntry::Object(KeyObject {
                    version_id: vid,
                    created_at_ns,
                    vol_offset,
                    flags,
                    updates: Vec::new(),
                });
                let mut versions = match state {
                    KeyState::Absent => Vec::new(),
                    KeyState::Unversioned(prev) => vec![KeyVersionEntry::Object(prev)],
                    KeyState::Versioned { versions } => versions,
                };
                upsert_version(&mut versions, entry);
                Ok(KeyState::Versioned { versions })
            }
        }
        KeyRecord::Tombstone {
            versioned,
            is_null,
            created_at_ns,
            vol_offset,
        } => {
            if !versioned {
                match state {
                    KeyState::Absent | KeyState::Unversioned(_) => Ok(KeyState::Absent),
                    KeyState::Versioned { mut versions } => {
                        versions.retain(|e| e.version_id() != 0);
                        Ok(KeyState::Versioned { versions })
                    }
                }
            } else {
                let vid = if is_null { 0 } else { created_at_ns };
                let dm = KeyVersionEntry::DeleteMarker {
                    version_id: vid,
                    vol_offset,
                };
                let mut versions = match state {
                    KeyState::Absent => Vec::new(),
                    KeyState::Unversioned(prev) => vec![KeyVersionEntry::Object(prev)],
                    KeyState::Versioned { versions } => versions,
                };
                upsert_version(&mut versions, dm);
                Ok(KeyState::Versioned { versions })
            }
        }
        KeyRecord::VersionDelete { target_version_id } => match state {
            KeyState::Absent => Err(NO_SUCH_VERSION),
            KeyState::Unversioned(_) => {
                if target_version_id == 0 {
                    Ok(KeyState::Absent)
                } else {
                    Err(NO_SUCH_VERSION)
                }
            }
            KeyState::Versioned { mut versions } => {
                let before = versions.len();
                versions.retain(|e| e.version_id() != target_version_id);
                if versions.len() == before {
                    Err(NO_SUCH_VERSION)
                } else {
                    Ok(KeyState::Versioned { versions })
                }
            }
        },
        KeyRecord::AttrUpdate {
            target_version_id,
            group,
            created_at_ns,
            vol_offset,
            total_len,
        } => {
            let upd = KeyAttrUpdate {
                group,
                created_at_ns,
                vol_offset,
                total_len,
            };
            match state {
                KeyState::Absent => Err(NO_SUCH_KEY),
                KeyState::Unversioned(mut obj) => {
                    if target_version_id != 0 {
                        Err(NO_SUCH_VERSION)
                    } else {
                        obj.apply_update(upd);
                        Ok(KeyState::Unversioned(obj))
                    }
                }
                KeyState::Versioned { mut versions } => {
                    match versions.iter_mut().find(|e| e.version_id() == target_version_id) {
                        None => Err(NO_SUCH_VERSION),
                        Some(KeyVersionEntry::DeleteMarker { .. }) => Err(METHOD_NOT_ALLOWED_DELETE_MARKER),
                        Some(KeyVersionEntry::Object(obj)) => {
                            obj.apply_update(upd);
                            Ok(KeyState::Versioned { versions })
                        }
                    }
                }
            }
        }
    }
}

pub const SCRUB_ALREADY_CHECKED: Refusal = refuse_because(
    "ScrubAlreadyChecked",
    409,
    "needle was already checked or marked bad in this scrub pass",
);

/// Scrub state of one needle during a pass (`STATE_MACHINES.md`, #155).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrubState {
    /// Not yet checked in the current pass.
    Unchecked,
    /// Header and payload CRCs matched in the current pass.
    Good,
    /// Header, key/attrs, or payload CRC failed in the current pass (or on a read).
    Bad,
}

/// Events driving [`scrub_step`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrubEvent {
    /// A new scrub pass started: resets the needle to `Unchecked`.
    NewPass,
    /// Scrub verified header and payload CRCs as valid.
    VerifiedOk,
    /// Scrub or a read detected a CRC or integrity mismatch.
    Corrupted,
}

/// Pure transition function for needle scrub state (#155).
///
/// - `NewPass` resets any state to `Unchecked`.
/// - `Unchecked + VerifiedOk -> Good`; `Good` and `Bad` refuse `VerifiedOk` within the same pass.
/// - `Corrupted` moves `Unchecked | Good | Bad -> Bad`.
pub fn scrub_step(state: ScrubState, event: ScrubEvent) -> Result<ScrubState, Refusal> {
    match (state, event) {
        (_, ScrubEvent::NewPass) => Ok(ScrubState::Unchecked),
        (ScrubState::Unchecked, ScrubEvent::VerifiedOk) => Ok(ScrubState::Good),
        (ScrubState::Good | ScrubState::Bad, ScrubEvent::VerifiedOk) => Err(SCRUB_ALREADY_CHECKED),
        (_, ScrubEvent::Corrupted) => Ok(ScrubState::Bad),
    }
}

#[cfg(test)]
mod key_tests {
    use super::*;
    use crate::attrs::AttrGroup;

    fn base_obj(vid: u64, ts: u64, off: u64) -> KeyObject {
        KeyObject {
            version_id: vid,
            created_at_ns: ts,
            vol_offset: off,
            flags: 0x0001,
            updates: Vec::new(),
        }
    }

    #[test]
    fn key_every_pair_is_decided() {
        let states = [
            KeyState::Absent,
            KeyState::Unversioned(base_obj(0, 100, 1)),
            KeyState::Versioned { versions: Vec::new() },
            KeyState::Versioned {
                versions: vec![KeyVersionEntry::Object(base_obj(0, 100, 1))],
            },
            KeyState::Versioned {
                versions: vec![KeyVersionEntry::Object(base_obj(200, 200, 2))],
            },
            KeyState::Versioned {
                versions: vec![KeyVersionEntry::DeleteMarker {
                    version_id: 300,
                    vol_offset: 3,
                }],
            },
        ];

        let records = [
            KeyRecord::Object {
                versioned: false,
                is_null: false,
                created_at_ns: 400,
                vol_offset: 10,
                flags: 0x0401,
            },
            KeyRecord::Object {
                versioned: true,
                is_null: true,
                created_at_ns: 410,
                vol_offset: 11,
                flags: 0x0041,
            },
            KeyRecord::Object {
                versioned: true,
                is_null: false,
                created_at_ns: 420,
                vol_offset: 12,
                flags: 0x0041,
            },
            KeyRecord::Tombstone {
                versioned: false,
                is_null: false,
                created_at_ns: 430,
                vol_offset: 13,
            },
            KeyRecord::Tombstone {
                versioned: true,
                is_null: true,
                created_at_ns: 440,
                vol_offset: 14,
            },
            KeyRecord::Tombstone {
                versioned: true,
                is_null: false,
                created_at_ns: 450,
                vol_offset: 15,
            },
            KeyRecord::VersionDelete { target_version_id: 0 },
            KeyRecord::VersionDelete { target_version_id: 200 },
            KeyRecord::VersionDelete { target_version_id: 300 },
            KeyRecord::VersionDelete { target_version_id: 999 },
            KeyRecord::AttrUpdate {
                target_version_id: 0,
                group: AttrGroup::Tagging,
                created_at_ns: 500,
                vol_offset: 20,
                total_len: 512,
            },
            KeyRecord::AttrUpdate {
                target_version_id: 200,
                group: AttrGroup::StorageClass,
                created_at_ns: 510,
                vol_offset: 21,
                total_len: 512,
            },
            KeyRecord::AttrUpdate {
                target_version_id: 300,
                group: AttrGroup::Tagging,
                created_at_ns: 520,
                vol_offset: 22,
                total_len: 512,
            },
            KeyRecord::AttrUpdate {
                target_version_id: 999,
                group: AttrGroup::Acl,
                created_at_ns: 530,
                vol_offset: 23,
                total_len: 512,
            },
        ];

        let mut count = 0usize;
        for (si, state) in states.iter().enumerate() {
            for record in &records {
                let res = key_step(state.clone(), record.clone());
                match (si, record) {
                    (0, KeyRecord::AttrUpdate { .. }) => assert_eq!(res, Err(NO_SUCH_KEY)),
                    (_, KeyRecord::VersionDelete { target_version_id }) => {
                        let has = match state {
                            KeyState::Absent => false,
                            KeyState::Unversioned(_) => *target_version_id == 0,
                            KeyState::Versioned { versions } => {
                                versions.iter().any(|e| e.version_id() == *target_version_id)
                            }
                        };
                        assert_eq!(res.is_ok(), has);
                        if !has {
                            assert_eq!(res, Err(NO_SUCH_VERSION));
                        }
                    }
                    (_, KeyRecord::AttrUpdate { target_version_id, group, created_at_ns, .. }) => {
                        match state {
                            KeyState::Absent => assert_eq!(res, Err(NO_SUCH_KEY)),
                            KeyState::Unversioned(_) => {
                                if *target_version_id == 0 {
                                    let KeyState::Unversioned(obj) = res.unwrap() else {
                                        panic!("expected Unversioned");
                                    };
                                    assert_eq!(obj.updates.len(), 1);
                                    assert_eq!(obj.updates[0].group, *group);
                                    assert_eq!(obj.updates[0].created_at_ns, *created_at_ns);
                                } else {
                                    assert_eq!(res, Err(NO_SUCH_VERSION));
                                }
                            }
                            KeyState::Versioned { versions } => {
                                match versions.iter().find(|e| e.version_id() == *target_version_id) {
                                    None => assert_eq!(res, Err(NO_SUCH_VERSION)),
                                    Some(KeyVersionEntry::DeleteMarker { .. }) => {
                                        assert_eq!(res, Err(METHOD_NOT_ALLOWED_DELETE_MARKER))
                                    }
                                    Some(KeyVersionEntry::Object(_)) => {
                                        let KeyState::Versioned { versions: out } = res.unwrap() else {
                                            panic!("expected Versioned");
                                        };
                                        let KeyVersionEntry::Object(obj) = out
                                            .iter()
                                            .find(|e| e.version_id() == *target_version_id)
                                            .unwrap()
                                        else {
                                            panic!("expected Object");
                                        };
                                        assert!(obj.updates.iter().any(|u| u.group == *group && u.created_at_ns == *created_at_ns));
                                    }
                                }
                            }
                        }
                    }
                    (_, KeyRecord::Object { .. } | KeyRecord::Tombstone { .. }) => {
                        assert!(res.is_ok());
                    }
                }
                count += 1;
            }
        }
        assert_eq!(count, states.len() * records.len());
    }

    #[test]
    fn key_update_never_resurrects_or_hits_absent_version() {
        // Update against absent key refuses and does not resurrect.
        assert_eq!(
            key_step(
                KeyState::Absent,
                KeyRecord::AttrUpdate {
                    target_version_id: 0,
                    group: AttrGroup::Tagging,
                    created_at_ns: 100,
                    vol_offset: 1,
                    total_len: 512,
                },
            ),
            Err(NO_SUCH_KEY),
        );

        // Update whose created_at_ns <= base object ts is ignored as superseded by overwrite.
        let overwritten = KeyState::Unversioned(base_obj(0, 500, 10));
        let stale = key_step(
            overwritten.clone(),
            KeyRecord::AttrUpdate {
                target_version_id: 0,
                group: AttrGroup::StorageClass,
                created_at_ns: 400,
                vol_offset: 2,
                total_len: 512,
            },
        )
        .unwrap();
        assert_eq!(stale, overwritten);

        // Newest timestamp wins per group regardless of replay offset order (Decision 56).
        let s1 = key_step(
            overwritten,
            KeyRecord::AttrUpdate {
                target_version_id: 0,
                group: AttrGroup::StorageClass,
                created_at_ns: 700,
                vol_offset: 3,
                total_len: 512,
            },
        )
        .unwrap();
        let s2 = key_step(
            s1,
            KeyRecord::AttrUpdate {
                target_version_id: 0,
                group: AttrGroup::StorageClass,
                created_at_ns: 600,
                vol_offset: 99,
                total_len: 512,
            },
        )
        .unwrap();
        let KeyState::Unversioned(obj) = s2 else { panic!() };
        assert_eq!(obj.updates.len(), 1);
        assert_eq!(obj.updates[0].created_at_ns, 700);
        assert_eq!(obj.updates[0].vol_offset, 3);
    }
}

#[cfg(test)]
mod scrub_tests {
    use super::*;

    #[test]
    fn scrub_every_pair_is_decided() {
        use ScrubEvent::{Corrupted, NewPass, VerifiedOk};
        use ScrubState::{Bad, Good, Unchecked};
        let table = [
            (Unchecked, NewPass, Ok(Unchecked)),
            (Unchecked, VerifiedOk, Ok(Good)),
            (Unchecked, Corrupted, Ok(Bad)),
            (Good, NewPass, Ok(Unchecked)),
            (Good, VerifiedOk, Err(SCRUB_ALREADY_CHECKED)),
            (Good, Corrupted, Ok(Bad)),
            (Bad, NewPass, Ok(Unchecked)),
            (Bad, VerifiedOk, Err(SCRUB_ALREADY_CHECKED)),
            (Bad, Corrupted, Ok(Bad)),
        ];
        for (state, event, want) in table {
            assert_eq!(scrub_step(state, event), want, "{:?} + {:?}", state, event);
        }
        assert_eq!(table.len(), 3 * 3, "every state against every event");
    }
}

/// The `multipart/form-data` parser of POST object (#119): where it is in the body. The
/// parser in `server/src/form_upload.rs` finds delimiters and part headers and tells this
/// machine what it found; the machine decides the next state, or refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormState {
    /// Before the first delimiter.
    Preamble,
    /// Just after a delimiter: `\r\n` starts a part, `--` ends the body.
    AfterBoundary,
    PartHeaders,
    FieldBody,
    FileBody,
    /// After the closing delimiter: the rest is ignored.
    Epilogue,
}

/// What the parser found next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormInput {
    /// A delimiter line.
    Boundary,
    /// `\r\n` after a delimiter: another part follows.
    NextPart,
    /// `--` after a delimiter: the closing delimiter.
    Close,
    /// Part headers naming a field.
    FieldPart,
    /// Part headers naming the file.
    FilePart,
    /// Anything else after a delimiter.
    Malformed,
}

pub const FORM_MALFORMED: Refusal = refuse_because("MalformedPOSTRequest", 400, "malformed multipart/form-data POST request");
pub const FORM_NO_FILE: Refusal = refuse_because("InvalidArgument", 400, "POST requires a file part");

/// One step of the form parser. `file_seen`: the file part has started, so it must be the
/// last part (S3 ignores fields after the file; AwanFS refuses them so nothing after the
/// file can change what was stored).
pub fn form_step(state: FormState, input: FormInput, file_seen: bool) -> Result<FormState, Refusal> {
    use FormInput as I;
    use FormState as S;
    match (state, input) {
        (S::Epilogue, _) => Ok(S::Epilogue),
        (_, I::Malformed) => Err(FORM_MALFORMED),
        (S::Preamble | S::FieldBody | S::FileBody, I::Boundary) => Ok(S::AfterBoundary),
        (S::AfterBoundary, I::NextPart) if file_seen => Err(FORM_MALFORMED),
        (S::AfterBoundary, I::NextPart) => Ok(S::PartHeaders),
        (S::AfterBoundary, I::Close) if !file_seen => Err(FORM_NO_FILE),
        (S::AfterBoundary, I::Close) => Ok(S::Epilogue),
        (S::PartHeaders, I::FieldPart) => Ok(S::FieldBody),
        (S::PartHeaders, I::FilePart) => Ok(S::FileBody),
        _ => Err(FORM_MALFORMED),
    }
}

#[cfg(test)]
mod form_tests {
    use super::*;

    #[test]
    fn form_every_pair_is_decided() {
        use FormInput::*;
        use FormState::*;
        let states = [Preamble, AfterBoundary, PartHeaders, FieldBody, FileBody, Epilogue];
        let inputs = [Boundary, NextPart, Close, FieldPart, FilePart, Malformed];
        let m = Err(FORM_MALFORMED);
        let mut decided = 0;
        for &file_seen in &[false, true] {
            for &s in &states {
                for &i in &inputs {
                    let want = match (s, i) {
                        (Epilogue, _) => Ok(Epilogue),
                        (_, Malformed) => m,
                        (Preamble | FieldBody | FileBody, Boundary) => Ok(AfterBoundary),
                        (AfterBoundary, NextPart) => {
                            if file_seen {
                                m
                            } else {
                                Ok(PartHeaders)
                            }
                        }
                        (AfterBoundary, Close) => {
                            if file_seen {
                                Ok(Epilogue)
                            } else {
                                Err(FORM_NO_FILE)
                            }
                        }
                        (PartHeaders, FieldPart) => Ok(FieldBody),
                        (PartHeaders, FilePart) => Ok(FileBody),
                        _ => m,
                    };
                    assert_eq!(form_step(s, i, file_seen), want, "{:?} + {:?} (file seen: {})", s, i, file_seen);
                    decided += 1;
                }
            }
        }
        assert_eq!(decided, 2 * 6 * 6, "every state against every input, with and without the file");
    }

    #[test]
    fn form_nothing_follows_the_file_and_the_body_needs_one() {
        assert_eq!(form_step(FormState::AfterBoundary, FormInput::NextPart, true), Err(FORM_MALFORMED));
        assert_eq!(form_step(FormState::AfterBoundary, FormInput::Close, false), Err(FORM_NO_FILE));
        assert_eq!(form_step(FormState::AfterBoundary, FormInput::Close, true), Ok(FormState::Epilogue));
        assert_eq!(form_step(FormState::Epilogue, FormInput::Malformed, true), Ok(FormState::Epilogue), "the epilogue is ignored");
    }
}

/// Target evaluated by the bucket lifecycle sweeper (#118). Times are seconds since epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleTarget {
    UnversionedObject { created_sec: u64 },
    CurrentVersion { created_sec: u64 },
    CurrentDeleteMarker { created_sec: u64, sole_version: bool },
    NoncurrentVersion {
        noncurrent_since_sec: u64,
        newer_noncurrent_count: u32,
        is_marker: bool,
    },
    MultipartUpload { initiated_sec: u64 },
}

/// One action extracted from a matching enabled (or disabled) lifecycle rule (#118).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryRuleAction {
    Disabled,
    ExpireDays(u32),
    ExpireDate(u64),
    ExpiredObjectDeleteMarker(bool),
    NoncurrentExpiration {
        noncurrent_days: u32,
        newer_noncurrent_versions: Option<u32>,
    },
    AbortMultipart(u32),
}

/// Inputs to [`expiry_step`]: target facts, version lock state, current clock, and day length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpiryFacts {
    pub target: LifecycleTarget,
    pub lock: Lock,
    pub now_sec: u64,
    pub day_seconds: u64,
}

/// Decision returned by [`expiry_step`]: the sweeper only executes this decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryAction {
    Keep,
    Expire,
    AddDeleteMarker,
    RemoveVersion,
    RemoveMarker,
    AbortUpload,
}

/// Pure lifecycle state transition for one object, version, delete marker, or upload (#118).
pub fn expiry_step(facts: ExpiryFacts, actions: &[ExpiryRuleAction]) -> ExpiryAction {
    let day = facts.day_seconds.max(1);
    let age_reached = |since: u64, days: u32| -> bool {
        facts.now_sec >= since.saturating_add(u64::from(days).saturating_mul(day))
    };

    match facts.target {
        LifecycleTarget::UnversionedObject { created_sec } => {
            if facts.lock.may_delete(facts.now_sec, false).is_err() {
                return ExpiryAction::Keep;
            }
            for &act in actions {
                match act {
                    ExpiryRuleAction::ExpireDays(d) if age_reached(created_sec, d) => {
                        return ExpiryAction::Expire;
                    }
                    ExpiryRuleAction::ExpireDate(dt) if facts.now_sec >= dt => {
                        return ExpiryAction::Expire;
                    }
                    _ => {}
                }
            }
            ExpiryAction::Keep
        }
        LifecycleTarget::CurrentVersion { created_sec } => {
            for &act in actions {
                match act {
                    ExpiryRuleAction::ExpireDays(d) if age_reached(created_sec, d) => {
                        return ExpiryAction::AddDeleteMarker;
                    }
                    ExpiryRuleAction::ExpireDate(dt) if facts.now_sec >= dt => {
                        return ExpiryAction::AddDeleteMarker;
                    }
                    _ => {}
                }
            }
            ExpiryAction::Keep
        }
        LifecycleTarget::CurrentDeleteMarker {
            created_sec,
            sole_version,
        } => {
            if !sole_version {
                return ExpiryAction::Keep;
            }
            for &act in actions {
                match act {
                    ExpiryRuleAction::ExpiredObjectDeleteMarker(true) => {
                        return ExpiryAction::RemoveMarker;
                    }
                    ExpiryRuleAction::ExpireDays(d) if age_reached(created_sec, d) => {
                        return ExpiryAction::RemoveMarker;
                    }
                    ExpiryRuleAction::ExpireDate(dt) if facts.now_sec >= dt => {
                        return ExpiryAction::RemoveMarker;
                    }
                    _ => {}
                }
            }
            ExpiryAction::Keep
        }
        LifecycleTarget::NoncurrentVersion {
            noncurrent_since_sec,
            newer_noncurrent_count,
            is_marker,
        } => {
            if !is_marker && facts.lock.may_delete(facts.now_sec, false).is_err() {
                return ExpiryAction::Keep;
            }
            for &act in actions {
                if let ExpiryRuleAction::NoncurrentExpiration {
                    noncurrent_days,
                    newer_noncurrent_versions,
                } = act
                {
                    if !age_reached(noncurrent_since_sec, noncurrent_days) {
                        continue;
                    }
                    if let Some(keep_newer) = newer_noncurrent_versions {
                        if newer_noncurrent_count < keep_newer {
                            continue;
                        }
                    }
                    return if is_marker {
                        ExpiryAction::RemoveMarker
                    } else {
                        ExpiryAction::RemoveVersion
                    };
                }
            }
            ExpiryAction::Keep
        }
        LifecycleTarget::MultipartUpload { initiated_sec } => {
            for &act in actions {
                if let ExpiryRuleAction::AbortMultipart(days) = act {
                    if age_reached(initiated_sec, days) {
                        return ExpiryAction::AbortUpload;
                    }
                }
            }
            ExpiryAction::Keep
        }
    }
}

#[cfg(test)]
mod expiry_tests {
    use super::*;

    #[test]
    fn expiry_every_case_is_decided() {
        use ExpiryAction::*;
        use ExpiryRuleAction::*;
        use LifecycleTarget::*;

        let locks = [
            Lock::default(),
            Lock {
                retention: Some((RetentionMode::Governance, 2_000)),
                legal_hold: false,
            },
            Lock {
                retention: Some((RetentionMode::Governance, 500)),
                legal_hold: false,
            },
            Lock {
                retention: Some((RetentionMode::Compliance, 2_000)),
                legal_hold: false,
            },
            Lock {
                retention: None,
                legal_hold: true,
            },
        ];

        let targets = [
            UnversionedObject { created_sec: 100 },
            CurrentVersion { created_sec: 100 },
            CurrentDeleteMarker {
                created_sec: 100,
                sole_version: false,
            },
            CurrentDeleteMarker {
                created_sec: 100,
                sole_version: true,
            },
            NoncurrentVersion {
                noncurrent_since_sec: 100,
                newer_noncurrent_count: 1,
                is_marker: false,
            },
            NoncurrentVersion {
                noncurrent_since_sec: 100,
                newer_noncurrent_count: 3,
                is_marker: false,
            },
            NoncurrentVersion {
                noncurrent_since_sec: 100,
                newer_noncurrent_count: 1,
                is_marker: true,
            },
            NoncurrentVersion {
                noncurrent_since_sec: 100,
                newer_noncurrent_count: 3,
                is_marker: true,
            },
            MultipartUpload { initiated_sec: 100 },
        ];
        let rules = [
            Disabled,
            ExpireDays(1),
            ExpireDate(1_000),
            ExpireDate(2_000),
            ExpiredObjectDeleteMarker(false),
            ExpiredObjectDeleteMarker(true),
            NoncurrentExpiration {
                noncurrent_days: 1,
                newer_noncurrent_versions: None,
            },
            NoncurrentExpiration {
                noncurrent_days: 1,
                newer_noncurrent_versions: Some(2),
            },
            AbortMultipart(1),
        ];

        let mut decided = 0;
        for &now_sec in &[150u64, 1_000u64] {
            for &lock in &locks {
                for &target in &targets {
                    for &rule in &rules {
                        let facts = ExpiryFacts {
                            target,
                            lock,
                            now_sec,
                            day_seconds: 100,
                        };
                        let got = expiry_step(facts, &[rule]);
                        let age_ok = now_sec >= 200;
                        let want = match (target, rule) {
                            (_, Disabled) => Keep,
                            (UnversionedObject { .. }, ExpireDays(_)) if age_ok => {
                                if lock.may_delete(now_sec, false).is_err() { Keep } else { Expire }
                            }
                            (UnversionedObject { .. }, ExpireDate(dt)) if now_sec >= dt => {
                                if lock.may_delete(now_sec, false).is_err() { Keep } else { Expire }
                            }
                            (CurrentVersion { .. }, ExpireDays(_)) if age_ok => AddDeleteMarker,
                            (CurrentVersion { .. }, ExpireDate(dt)) if now_sec >= dt => AddDeleteMarker,
                            (CurrentDeleteMarker { sole_version: true, .. }, ExpiredObjectDeleteMarker(true)) => {
                                RemoveMarker
                            }
                            (CurrentDeleteMarker { sole_version: true, .. }, ExpireDays(_)) if age_ok => {
                                RemoveMarker
                            }
                            (CurrentDeleteMarker { sole_version: true, .. }, ExpireDate(dt)) if now_sec >= dt => {
                                RemoveMarker
                            }
                            (
                                NoncurrentVersion {
                                    newer_noncurrent_count,
                                    is_marker,
                                    ..
                                },
                                NoncurrentExpiration {
                                    newer_noncurrent_versions,
                                    ..
                                },
                            ) if age_ok => {
                                if !is_marker && lock.may_delete(now_sec, false).is_err() {
                                    Keep
                                } else if newer_noncurrent_versions
                                    .is_some_and(|n| newer_noncurrent_count < n)
                                {
                                    Keep
                                } else if is_marker {
                                    RemoveMarker
                                } else {
                                    RemoveVersion
                                }
                            }
                            (MultipartUpload { .. }, AbortMultipart(_)) if age_ok => AbortUpload,
                            _ => Keep,
                        };
                        assert_eq!(got, want, "now={now_sec} {target:?} + {rule:?} (lock={lock:?})");
                        decided += 1;
                    }
                }
            }
        }
        assert_eq!(decided, 2 * locks.len() * targets.len() * rules.len());
    }

    #[test]
    fn expiry_never_removes_locked_version() {
        let rules = [
            ExpiryRuleAction::ExpireDays(1),
            ExpiryRuleAction::ExpireDate(100),
            ExpiryRuleAction::NoncurrentExpiration {
                noncurrent_days: 1,
                newer_noncurrent_versions: None,
            },
        ];
        for lock in [
            Lock {
                retention: Some((RetentionMode::Governance, 2_000)),
                legal_hold: false,
            },
            Lock {
                retention: Some((RetentionMode::Compliance, 2_000)),
                legal_hold: false,
            },
            Lock {
                retention: None,
                legal_hold: true,
            },
        ] {
            assert_eq!(
                expiry_step(
                    ExpiryFacts {
                        target: LifecycleTarget::UnversionedObject { created_sec: 10 },
                        lock,
                        now_sec: 1_000,
                        day_seconds: 10,
                    },
                    &rules,
                ),
                ExpiryAction::Keep
            );
            assert_eq!(
                expiry_step(
                    ExpiryFacts {
                        target: LifecycleTarget::NoncurrentVersion {
                            noncurrent_since_sec: 10,
                            newer_noncurrent_count: 10,
                            is_marker: false,
                        },
                        lock,
                        now_sec: 1_000,
                        day_seconds: 10,
                    },
                    &rules,
                ),
                ExpiryAction::Keep
            );
            // Deleting current object adds a delete marker rather than deleting the locked version.
            assert_eq!(
                expiry_step(
                    ExpiryFacts {
                        target: LifecycleTarget::CurrentVersion { created_sec: 10 },
                        lock,
                        now_sec: 1_000,
                        day_seconds: 10,
                    },
                    &rules,
                ),
                ExpiryAction::AddDeleteMarker
            );
        }
    }

    #[test]
    fn expiry_respects_newer_noncurrent_and_sole_delete_marker() {
        let dm_rule = [
            ExpiryRuleAction::ExpiredObjectDeleteMarker(true),
            ExpiryRuleAction::ExpireDays(5),
        ];
        assert_eq!(
            expiry_step(
                ExpiryFacts {
                    target: LifecycleTarget::CurrentDeleteMarker {
                        created_sec: 100,
                        sole_version: false,
                    },
                    lock: Lock::default(),
                    now_sec: 1_000,
                    day_seconds: 10,
                },
                &dm_rule,
            ),
            ExpiryAction::Keep,
            "non-sole delete marker must stay"
        );
        assert_eq!(
            expiry_step(
                ExpiryFacts {
                    target: LifecycleTarget::CurrentDeleteMarker {
                        created_sec: 100,
                        sole_version: true,
                    },
                    lock: Lock::default(),
                    now_sec: 110,
                    day_seconds: 10,
                },
                &[ExpiryRuleAction::ExpireDays(5)],
            ),
            ExpiryAction::Keep,
            "sole marker waits for ExpireDays threshold"
        );
        assert_eq!(
            expiry_step(
                ExpiryFacts {
                    target: LifecycleTarget::CurrentDeleteMarker {
                        created_sec: 100,
                        sole_version: true,
                    },
                    lock: Lock::default(),
                    now_sec: 150,
                    day_seconds: 10,
                },
                &[ExpiryRuleAction::ExpireDays(5)],
            ),
            ExpiryAction::RemoveMarker
        );
    }
}
