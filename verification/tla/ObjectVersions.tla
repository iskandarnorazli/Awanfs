---- MODULE ObjectVersions ----
(*
Model of one S3 object key under concurrent PUT and DELETE, and of what boot recovery
rebuilds from the volume log. Each step names the code it stands for (server/src):

  Reserve  volume.rs  VolumeManager::write_needle   current_offset.fetch_add(total_len)
  Write    volume.rs  VolumeManager::write_needle   file.write_aligned_at(offset, buf)
  Fail     volume.rs  write_aligned_at returns Err  (the request answers 500 since #33)
  Index    s3_api.rs  PUT handler: engine.write().put_object(...)
                      DELETE handler: engine row removed; today this happens BEFORE the
                      tombstone needle is reserved and written (DeleteLogsFirst = FALSE)
  Recover  s3_api.rs  S3ServiceState::with_data_dir -> volume.rs recover_needles:
                      replays needles in offset order and stops at the first sector
                      without needle magic (a gap)

Rule (AckedStateSurvivesRestart): when no request is in flight, what a GET shows now is
what it shows after a restart. A broken rule means a client sees version A before a
reboot and version B (or a deleted object) after it.

Switches, so the same model checks the current code and the fixes:
  UseKeyLock        requests on one key hold a per-key lock from Reserve to Index
  WriteCanFail      a needle write can fail after its offset was reserved
  RecoverSkipsGaps  recovery steps over reserved-but-unwritten offsets (#38)
  DeleteLogsFirst   DELETE writes its tombstone before removing the engine row, and
                    answers 500 without removing it when the write fails (#39)
  Streamers         the writers whose body is streamed (PUT above 4 MiB, #154). A streamer
                    reserves its offset and receives its body WITHOUT the key lock, so a
                    client that stops sending blocks nobody. It takes the lock only to commit:
                      Finish    volume.rs  NeedleWriter::finish writes the header sector; only
                                           now is the needle a record recovery can see
                      Relocate  s3_api.rs  if a record of this key was logged at a later
                                           offset while the body arrived, the object is copied
                                           to a new needle at the end of the log
                      Index     as above
  Relocate          the Relocate step exists. Without it the streamer's needle sits at an
                    early offset: it is the live object, and after a restart an older write at
                    a later offset wins.
  Compactor         one compaction pass may run (s3_api.rs compact_oldest_volume, #167). The
                    volume it compacts holds every offset below `cut`, the next offset when the
                    pass starts; later writes land above it. The pass:
                      CSeal     the volume takes no new reservations (VolumeManager::seal)
                      CScan     the needles it holds are listed (scan_needles)
                      CCopy     under the key lock, the needle the index points at, if listed,
                                is written again at the end of the log
                      CDrop     the volume file is removed: every record below `cut` is gone,
                                and a write into it after that is lost
  FailLeavesRecord  a failure can come after the record reached the log: the journal append,
                    the fsync or the index update fails once the needle is on disk (#184). The
                    request answers 500, and the record is still there for recovery to replay.
  IndexAfterLateFail
                    the fix, owner decision 4B (2026-11-16): after such a failure the live
                    index applies the record exactly as recovery will, so a GET before and
                    after a restart agree. A 500 means the outcome is unknown (S3's rule).
                    Without it the live index keeps the old object and a restart shows the
                    write the client was told failed.
  CompactWaitsForWriters
                    CScan waits until no request has an offset below `cut` whose record is not
                    yet written (VolumeManager::writers_in_flight is 0). Without it a streamed
                    PUT still receiving its body is not listed, its needle is not copied, and
                    the file it lands in is removed.
  Versioning        the bucket's versioning can be switched on once, at any moment, without the
                    key lock (PutBucketVersioning). A write is versioned when the bucket is at the
                    moment it takes its stamp under the lock (s3_api.rs version_stamp), so a
                    streamer that reserved while the bucket was unversioned can commit versioned,
                    after an unversioned write of the same key that sits at a later offset (#186).
                    A versioned write's time is versions.rs next_ts: the clock, raised above every
                    version the key has. A key's history starts with its unversioned row as the
                    null version, at time 0 (ops/object.rs null_version_of); the current version
                    is the one with the largest time.
  ClockStepsBack    the wall clock can go back (NTP, a VM restore).
  NullReplay        the fix for #186: recovery applies an unversioned record of a key that already
                    has a history to its null version, as the live index did before the history
                    began: a PUT becomes the null version at time 0, a DELETE removes it. Without
                    it recovery adds the record as a null version at its own creation time, which
                    can be later than the versioned write the client saw become current.
  CondWriters       subset of Writers that send If-None-Match: * (commit only if key is absent, #112).
  CheckCondUnderLock
                    the fix for #112: conditional writers evaluate ~Visible(engine).present under
                    the key lock at commit (Acquire, after any streamed body). Without it they
                    check before acquiring the lock, so two racing creators can both pass.
*)
EXTENDS Naturals, FiniteSets

CONSTANTS Writers, Deleters, UseKeyLock, WriteCanFail, RecoverSkipsGaps, DeleteLogsFirst,
          Streamers, Relocate, Compactor, CompactWaitsForWriters,
          FailLeavesRecord, IndexAfterLateFail, Versioning, ClockStepsBack, NullReplay,
          CondWriters, CheckCondUnderLock

ASSUME Streamers \subseteq Writers
ASSUME CondWriters \subseteq Writers
ASSUME CheckCondUnderLock \in BOOLEAN

Procs == Writers \cup Deleters
Gone == [present |-> FALSE, val |-> "none"]
Ts == 1..2
Max(S) == CHOOSE m \in S : \A x \in S : x <= m

\* A key's state, live or rebuilt: its unversioned row, whether it has a version history, and
\* the history (a set of versions; at most one is the null version).
NoRow == [present |-> FALSE, val |-> "none", ts |-> 0]
Empty == [row |-> NoRow, hist |-> FALSE, store |-> {}]
RowOf(r) == IF r.kind = "put" THEN [present |-> TRUE, val |-> r.val, ts |-> r.ts] ELSE NoRow
NullOf(row) == IF row.present THEN {[val |-> row.val, ts |-> 0, null |-> TRUE, marker |-> FALSE]} ELSE {}
VersionOf(r) == [val |-> r.val, ts |-> r.ts, null |-> FALSE, marker |-> r.kind = "tomb"]
NonNull(s) == {v \in s : ~v.null}
StartHist(k) == IF k.hist THEN k.store ELSE NullOf(k.row)

\* What a GET shows: the row, or the history's version with the largest time.
Visible(k) ==
    IF ~k.hist THEN [present |-> k.row.present, val |-> k.row.val]
    ELSE IF k.store = {} THEN Gone
    ELSE LET top == CHOOSE v \in k.store : \A w \in k.store : w.ts <= v.ts IN
         IF top.marker THEN Gone ELSE [present |-> TRUE, val |-> top.val]

\* The live index applies a committed record. An unversioned record never meets a history live:
\* it was stamped while the bucket was unversioned, and a history needs a versioned write.
LiveApply(k, r) ==
    IF r.ver THEN [row |-> k.row, hist |-> TRUE, store |-> StartHist(k) \cup {VersionOf(r)}]
    ELSE [k EXCEPT !.row = RowOf(r)]

\* Recovery applies the next record in log order.
ReplayApply(k, r) ==
    IF r.ver THEN [row |-> k.row, hist |-> TRUE, store |-> StartHist(k) \cup {VersionOf(r)}]
    ELSE IF ~k.hist THEN [k EXCEPT !.row = RowOf(r)]
    ELSE IF NullReplay
    THEN [k EXCEPT !.store = NonNull(k.store) \cup
                   (IF r.kind = "put" THEN {[val |-> r.val, ts |-> 0, null |-> TRUE, marker |-> FALSE]} ELSE {})]
    ELSE [k EXCEPT !.store = NonNull(k.store) \cup
                   {[val |-> r.val, ts |-> r.ts, null |-> TRUE, marker |-> r.kind = "tomb"]}]

\* The order of a request's steps after it takes the lock.
Steps(p) == IF p \in Deleters /\ ~DeleteLogsFirst
            THEN <<"index", "reserve", "write">>
            ELSE IF p \in Streamers
            THEN <<"write", "relocate", "index">>
            ELSE <<"reserve", "write", "index">>

VARIABLES pc, step, off, nextOff, log, engine, lock, cpc, cut, snap, clock, versioning, condWon

vars == <<pc, step, off, nextOff, log, engine, lock, cpc, cut, snap, clock, versioning, condWon>>
procVars == <<pc, step, off, nextOff, log, engine, lock, condWon>>
cVars == <<cpc, cut, snap, clock, versioning>>
Dropped == cpc = "done"

\* Moves p to its next step, or to release after the last one.
Advance(p) ==
    /\ step' = [step EXCEPT ![p] = step[p] + 1]
    /\ pc' = [pc EXCEPT ![p] = IF step[p] < 3 THEN Steps(p)[step[p] + 1] ELSE "release"]

RECURSIVE Replay(_, _, _)
Replay(o, limit, acc) ==
    IF o >= limit THEN acc
    ELSE LET recs == {r \in log : r.o = o} IN
         IF recs = {}
         THEN IF RecoverSkipsGaps THEN Replay(o + 1, limit, acc) ELSE acc
         ELSE LET r == CHOOSE x \in recs : TRUE IN
              Replay(o + 1, limit, ReplayApply(acc, r))

\* A removed volume is not read: replay starts at the first offset that still exists.
Recovered == Replay(IF Dropped THEN cut ELSE 1, nextOff, Empty)

\* A versioned write's time (versions.rs next_ts), taken under the key lock.
Stamp == Max({clock} \cup {v.ts + 1 : v \in engine.store})

\* The record a request wrote, if it is in the log.
Own(p) == {r \in log : r.o = off[p]}
OwnVersioned(p) == \E r \in Own(p) : r.ver
\* The record the index applies: the request's own, or (DELETE before #39, or a write into a
\* removed volume) the record it would have written.
RecOf(p) == IF Own(p) # {} THEN CHOOSE r \in Own(p) : TRUE
            ELSE [o |-> 0, kind |-> IF p \in Writers THEN "put" ELSE "tomb", val |-> p, ver |-> versioning, ts |-> clock]

Init ==
    /\ pc = [p \in Procs |-> "start"]
    /\ step = [p \in Procs |-> 0]
    /\ off = [p \in Procs |-> 0]
    /\ nextOff = 1
    /\ log = {}
    /\ engine = Empty
    /\ lock = "free"
    /\ cpc = "idle"
    /\ cut = 0
    /\ snap = {}
    /\ clock = 1
    /\ versioning = FALSE
    /\ condWon = [p \in CondWriters |-> FALSE]

\* Broken variant (#112): condition checked BEFORE taking the key lock.
PreCheck(p) ==
    /\ p \in CondWriters
    /\ ~CheckCondUnderLock
    /\ pc[p] = "start"
    /\ pc' = [pc EXCEPT ![p] = IF Visible(engine).present THEN "done" ELSE "checked"]
    /\ UNCHANGED <<step, off, nextOff, log, engine, lock, condWon>>
    /\ UNCHANGED cVars

\* A streamer reserves before it has the lock: the body arrives next, for as long as the
\* client takes. Nothing is in the log yet (the header sector is written last).
StreamReserve(p) ==
    /\ p \in Streamers
    /\ pc[p] = IF p \in CondWriters /\ ~CheckCondUnderLock THEN "checked" ELSE "start"
    /\ off' = [off EXCEPT ![p] = nextOff]
    /\ nextOff' = nextOff + 1
    /\ pc' = [pc EXCEPT ![p] = "body"]
    /\ UNCHANGED <<step, log, engine, lock, condWon>>
    /\ UNCHANGED cVars

Acquire(p) ==
    /\ pc[p] = IF p \in Streamers THEN "body"
               ELSE IF p \in CondWriters /\ ~CheckCondUnderLock THEN "checked"
               ELSE "start"
    /\ IF UseKeyLock THEN lock = "free" /\ lock' = p ELSE UNCHANGED lock
    /\ IF p \in CondWriters /\ CheckCondUnderLock /\ Visible(engine).present
       THEN /\ pc' = [pc EXCEPT ![p] = "release"]
            /\ UNCHANGED step
       ELSE /\ step' = [step EXCEPT ![p] = 1]
            /\ pc' = [pc EXCEPT ![p] = Steps(p)[1]]
    /\ UNCHANGED <<off, nextOff, log, engine, condWon>>
    /\ UNCHANGED cVars

Reserve(p) ==
    /\ pc[p] = "reserve"
    /\ off' = [off EXCEPT ![p] = nextOff]
    /\ nextOff' = nextOff + 1
    /\ Advance(p)
    /\ UNCHANGED <<log, engine, lock, condWon>>
    /\ UNCHANGED cVars

Write(p) ==
    /\ pc[p] = "write"
    \* Into a removed volume file, the bytes go nowhere (the handle outlives the file).
    /\ log' = IF Dropped /\ off[p] < cut THEN log
              ELSE log \cup {[o |-> off[p], kind |-> IF p \in Writers THEN "put" ELSE "tomb", val |-> p,
                               ver |-> versioning, ts |-> IF versioning THEN Stamp ELSE clock]}
    /\ Advance(p)
    /\ UNCHANGED <<off, nextOff, engine, lock, condWon>>
    /\ UNCHANGED cVars

Fail(p) ==
    /\ WriteCanFail
    /\ pc[p] = "write"
    /\ pc' = [pc EXCEPT ![p] = "release"]
    /\ UNCHANGED <<step, off, nextOff, log, engine, lock, condWon>>
    /\ UNCHANGED cVars

\* Under the lock, after the streamer's own record is in the log: a record of this key at a
\* later offset means offset order no longer says this write is the newest, so it is written
\* again at the end of the log. The copy can fail like any write.
\* Only an unversioned write relocates: versions are ordered by time, not offset.
LaterRecord(p) == (\E r \in log : r.o > off[p]) /\ ~OwnVersioned(p)
CopyOf(p) == [o |-> nextOff, kind |-> "put", val |-> p, ver |-> FALSE, ts |-> clock]

RelocateStep(p) ==
    /\ pc[p] = "relocate"
    /\ IF Relocate /\ LaterRecord(p)
       THEN /\ off' = [off EXCEPT ![p] = nextOff]
            /\ nextOff' = nextOff + 1
            /\ log' = log \cup {CopyOf(p)}
       ELSE UNCHANGED <<off, nextOff, log>>
    /\ Advance(p)
    /\ UNCHANGED <<engine, lock, condWon>>
    /\ UNCHANGED cVars

RelocateFail(p) ==
    /\ WriteCanFail
    /\ pc[p] = "relocate"
    /\ Relocate /\ LaterRecord(p)
    /\ pc' = [pc EXCEPT ![p] = "release"]
    /\ UNCHANGED <<step, off, nextOff, log, engine, lock, condWon>>
    /\ UNCHANGED cVars


\* The record is in the log (Write or RelocateStep ran), then the fsync, the journal append or
\* the index update fails, and the request answers 500 (#184).
LateFail(p) ==
    /\ WriteCanFail /\ FailLeavesRecord
    /\ pc[p] = "index"
    /\ \E r \in log : r.o = off[p]
    /\ engine' = IF IndexAfterLateFail THEN LiveApply(engine, RecOf(p)) ELSE engine
    /\ pc' = [pc EXCEPT ![p] = "release"]
    /\ UNCHANGED <<step, off, nextOff, log, lock, condWon>>
    /\ UNCHANGED cVars

\* A streamer's copy reaches the log, then the same failure: the copy is the newest record.
RelocateLateFail(p) ==
    /\ WriteCanFail /\ FailLeavesRecord
    /\ pc[p] = "relocate"
    /\ Relocate /\ LaterRecord(p)
    /\ off' = [off EXCEPT ![p] = nextOff]
    /\ nextOff' = nextOff + 1
    /\ log' = log \cup {CopyOf(p)}
    /\ engine' = IF IndexAfterLateFail THEN LiveApply(engine, CopyOf(p)) ELSE engine
    /\ pc' = [pc EXCEPT ![p] = "release"]
    /\ UNCHANGED <<step, lock, condWon>>
    /\ UNCHANGED cVars

Index(p) ==
    /\ pc[p] = "index"
    /\ engine' = LiveApply(engine, RecOf(p))
    /\ condWon' = IF p \in CondWriters THEN [condWon EXCEPT ![p] = TRUE] ELSE condWon
    /\ Advance(p)
    /\ UNCHANGED <<off, nextOff, log, lock>>
    /\ UNCHANGED cVars

Release(p) ==
    /\ pc[p] = "release"
    /\ IF UseKeyLock THEN lock' = "free" ELSE UNCHANGED lock
    /\ pc' = [pc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<step, off, nextOff, log, engine, condWon>>
    /\ UNCHANGED cVars

\* The compaction pass (#167).
InFlightBelowCut == \E p \in Procs : off[p] > 0 /\ off[p] < cut /\ pc[p] \in {"body", "write"}
\* The record the index points at: the newest record of the value the engine holds.
Points(r) == Visible(engine).present /\ r.kind = "put" /\ r.val = Visible(engine).val
             /\ ~\E r2 \in log : r2.val = r.val /\ r2.o > r.o
CSeal ==
    /\ Compactor /\ cpc = "idle"
    /\ cut' = nextOff
    /\ cpc' = "sealed"
    /\ UNCHANGED <<snap, clock, versioning>> /\ UNCHANGED procVars
CScan ==
    /\ cpc = "sealed"
    /\ CompactWaitsForWriters => ~InFlightBelowCut
    /\ snap' = {r \in log : r.o < cut}
    /\ cpc' = "scanned"
    /\ UNCHANGED <<cut, clock, versioning>> /\ UNCHANGED procVars
CCopy ==
    /\ cpc = "scanned"
    /\ UseKeyLock => lock = "free"
    /\ IF \E r \in snap : Points(r)
       THEN /\ log' = log \cup {[(CHOOSE r \in snap : Points(r)) EXCEPT !.o = nextOff]}
            /\ nextOff' = nextOff + 1
       ELSE UNCHANGED <<log, nextOff>>
    /\ cpc' = "copied"
    /\ UNCHANGED <<cut, snap, pc, step, off, engine, lock, clock, versioning, condWon>>
CDrop ==
    /\ cpc = "copied"
    /\ log' = {r \in log : r.o >= cut}
    /\ cpc' = "done"
    /\ UNCHANGED <<cut, snap, pc, step, off, nextOff, engine, lock, clock, versioning, condWon>>

\* PutBucketVersioning: no key lock.
VersionOn ==
    /\ Versioning /\ ~versioning
    /\ versioning' = TRUE
    /\ UNCHANGED <<procVars, cpc, cut, snap, clock>>

\* The wall clock moves; it only matters to versioned writes.
Tick ==
    /\ Versioning
    /\ clock' \in (IF ClockStepsBack THEN Ts \ {clock} ELSE {c \in Ts : c > clock})
    /\ UNCHANGED <<procVars, cpc, cut, snap, versioning>>

AllDone == (\A p \in Procs : pc[p] = "done") /\ cpc \in {"idle", "done"}

Finished == AllDone /\ UNCHANGED vars

Next ==
    \/ \E p \in Procs : PreCheck(p)
    \/ \E p \in Procs : StreamReserve(p)
    \/ \E p \in Procs : Acquire(p)
    \/ \E p \in Procs : RelocateStep(p)
    \/ \E p \in Procs : RelocateFail(p)
    \/ \E p \in Procs : RelocateLateFail(p)
    \/ \E p \in Procs : LateFail(p)
    \/ \E p \in Procs : Reserve(p)
    \/ \E p \in Procs : Write(p)
    \/ \E p \in Procs : Fail(p)
    \/ \E p \in Procs : Index(p)
    \/ \E p \in Procs : Release(p)
    \/ CSeal \/ CScan \/ CCopy \/ CDrop
    \/ VersionOn \/ Tick
    \/ Finished

Spec == Init /\ [][Next]_vars

AckedStateSurvivesRestart == AllDone => Visible(Recovered) = Visible(engine)

\* Two conditional creates (If-None-Match: *) cannot both succeed when no deleter runs (#112).
AtMostOneConditionalCreateWins ==
    Deleters = {} => Cardinality({p \in CondWriters : condWon[p]}) <= 1

\* ListObjectVersions shows the same history before and after a restart.
HistorySurvivesRestart == AllDone => Recovered.hist = engine.hist /\ Recovered.store = engine.store

TypeOK ==
    /\ pc \in [Procs -> {"start", "checked", "body", "reserve", "write", "relocate", "index", "release", "done"}]
    /\ step \in [Procs -> 0..4]
    /\ nextOff \in 1..(Cardinality(Procs) + Cardinality(Streamers) + 2)
    /\ cpc \in {"idle", "sealed", "scanned", "copied", "done"}
    /\ clock \in Ts
    /\ versioning \in BOOLEAN
    /\ condWon \in [CondWriters -> BOOLEAN]
====
