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
*)
EXTENDS Naturals, FiniteSets

CONSTANTS Writers, Deleters, UseKeyLock, WriteCanFail, RecoverSkipsGaps, DeleteLogsFirst,
          Streamers, Relocate

ASSUME Streamers \subseteq Writers

Procs == Writers \cup Deleters
Gone == [present |-> FALSE, val |-> "none"]

\* The order of a request's steps after it takes the lock.
Steps(p) == IF p \in Deleters /\ ~DeleteLogsFirst
            THEN <<"index", "reserve", "write">>
            ELSE IF p \in Streamers
            THEN <<"write", "relocate", "index">>
            ELSE <<"reserve", "write", "index">>

VARIABLES pc, step, off, nextOff, log, engine, lock

vars == <<pc, step, off, nextOff, log, engine, lock>>

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
              Replay(o + 1, limit,
                     IF r.kind = "put" THEN [present |-> TRUE, val |-> r.val] ELSE Gone)

Recovered == Replay(1, nextOff, Gone)

Init ==
    /\ pc = [p \in Procs |-> "start"]
    /\ step = [p \in Procs |-> 0]
    /\ off = [p \in Procs |-> 0]
    /\ nextOff = 1
    /\ log = {}
    /\ engine = Gone
    /\ lock = "free"

\* A streamer reserves before it has the lock: the body arrives next, for as long as the
\* client takes. Nothing is in the log yet (the header sector is written last).
StreamReserve(p) ==
    /\ p \in Streamers
    /\ pc[p] = "start"
    /\ off' = [off EXCEPT ![p] = nextOff]
    /\ nextOff' = nextOff + 1
    /\ pc' = [pc EXCEPT ![p] = "body"]
    /\ UNCHANGED <<step, log, engine, lock>>

Acquire(p) ==
    /\ pc[p] = IF p \in Streamers THEN "body" ELSE "start"
    /\ IF UseKeyLock THEN lock = "free" /\ lock' = p ELSE UNCHANGED lock
    /\ step' = [step EXCEPT ![p] = 1]
    /\ pc' = [pc EXCEPT ![p] = Steps(p)[1]]
    /\ UNCHANGED <<off, nextOff, log, engine>>

Reserve(p) ==
    /\ pc[p] = "reserve"
    /\ off' = [off EXCEPT ![p] = nextOff]
    /\ nextOff' = nextOff + 1
    /\ Advance(p)
    /\ UNCHANGED <<log, engine, lock>>

Write(p) ==
    /\ pc[p] = "write"
    /\ log' = log \cup {[o |-> off[p], kind |-> IF p \in Writers THEN "put" ELSE "tomb", val |-> p]}
    /\ Advance(p)
    /\ UNCHANGED <<off, nextOff, engine, lock>>

Fail(p) ==
    /\ WriteCanFail
    /\ pc[p] = "write"
    /\ pc' = [pc EXCEPT ![p] = "release"]
    /\ UNCHANGED <<step, off, nextOff, log, engine, lock>>

\* Under the lock, after the streamer's own record is in the log: a record of this key at a
\* later offset means offset order no longer says this write is the newest, so it is written
\* again at the end of the log. The copy can fail like any write.
LaterRecord(p) == \E r \in log : r.o > off[p]

RelocateStep(p) ==
    /\ pc[p] = "relocate"
    /\ IF Relocate /\ LaterRecord(p)
       THEN /\ off' = [off EXCEPT ![p] = nextOff]
            /\ nextOff' = nextOff + 1
            /\ log' = log \cup {[o |-> nextOff, kind |-> "put", val |-> p]}
       ELSE UNCHANGED <<off, nextOff, log>>
    /\ Advance(p)
    /\ UNCHANGED <<engine, lock>>

RelocateFail(p) ==
    /\ WriteCanFail
    /\ pc[p] = "relocate"
    /\ Relocate /\ LaterRecord(p)
    /\ pc' = [pc EXCEPT ![p] = "release"]
    /\ UNCHANGED <<step, off, nextOff, log, engine, lock>>

Index(p) ==
    /\ pc[p] = "index"
    /\ engine' = IF p \in Writers THEN [present |-> TRUE, val |-> p] ELSE Gone
    /\ Advance(p)
    /\ UNCHANGED <<off, nextOff, log, lock>>

Release(p) ==
    /\ pc[p] = "release"
    /\ IF UseKeyLock THEN lock' = "free" ELSE UNCHANGED lock
    /\ pc' = [pc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<step, off, nextOff, log, engine>>

AllDone == \A p \in Procs : pc[p] = "done"

Finished == AllDone /\ UNCHANGED vars

Next ==
    \/ \E p \in Procs : StreamReserve(p)
    \/ \E p \in Procs : Acquire(p)
    \/ \E p \in Procs : RelocateStep(p)
    \/ \E p \in Procs : RelocateFail(p)
    \/ \E p \in Procs : Reserve(p)
    \/ \E p \in Procs : Write(p)
    \/ \E p \in Procs : Fail(p)
    \/ \E p \in Procs : Index(p)
    \/ \E p \in Procs : Release(p)
    \/ Finished

Spec == Init /\ [][Next]_vars

AckedStateSurvivesRestart == AllDone => Recovered = engine

TypeOK ==
    /\ pc \in [Procs -> {"start", "body", "reserve", "write", "relocate", "index", "release", "done"}]
    /\ step \in [Procs -> 0..4]
    /\ nextOff \in 1..(Cardinality(Procs) + Cardinality(Streamers) + 1)
====
