---------------------------- MODULE Manifest ----------------------------
(*
  #26 stage 4: the metadata index's checkpoint. Rows reach the volume log (the record) and the
  in-memory index. A flush writes the rows logged since the last checkpoint to a new segment;
  a merge (stage 5 compaction) writes two named segments as one. Either is then made current
  by writing a manifest: the live segments and the log position they cover. Boot reads the
  manifest, opens its segments and replays only the log after that position.

  The manifest has two slots (FORMAT.md, "Metadata manifest"). A new manifest goes into the
  slot that does not hold the current one, with a higher generation; boot takes the valid slot
  with the higher generation. A slot whose write was torn by a crash fails its CRC and is not
  valid.

  Switches (each fix, turned off, gives a configuration TLC must reject):
    SyncSegmentFirst      a manifest is written only after every segment it names is synced.
    GcAfterManifestSync   a segment is deleted only once no durable manifest names it.
    AlternateSlots        a new manifest never overwrites the slot holding the current one.

  Invariants:
    AcknowledgedRowsSurvive   after a crash and recovery, every row in the log is visible:
                              in a segment the recovered manifest names, or in the log after
                              its position.
    ManifestSurvivesCrash     once a manifest was durable, a crash never leaves boot without
                              one (else boot replays every volume: 70 to 98 minutes at 10M
                              keys, measurements/2026-11-16-metadata-index-redb-vs-segments.md).
*)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Rows, MaxSegs, MaxCrashes, SyncSegmentFirst, GcAfterManifestSync, AlternateSlots

Slots == {0, 1}
NoPending == [names |-> {}, pos |-> 0, merge |-> FALSE]

VARIABLES
    log,        \* the volume log: rows in write order (durable; ObjectVersions models how)
    segs,       \* segment files on disk: a set of [seq, rows, synced]
    slots,      \* the two manifest slots: [gen, names, pos, st]
    cur,        \* the slot the running server last made current
    view,       \* the running server's checkpoint: [names, pos]
    pf,         \* a flush or merge whose segment is written but whose manifest is not durable
    nextSeq,
    crashes,
    crashed,    \* TRUE in the state right after a crash and recovery
    hadManifest

vars == <<log, segs, slots, cur, view, pf, nextSeq, crashes, crashed, hadManifest>>

Seqs == {s.seq : s \in segs}
Seg(q) == CHOOSE s \in segs : s.seq = q
LogRows == {log[i] : i \in 1..Len(log)}
Other(i) == 1 - i
Valid(i) == slots[i].st = "durable"
HasValid == \E i \in Slots : Valid(i)
Best == CHOOSE i \in Slots : Valid(i) /\ \A j \in Slots : Valid(j) => slots[j].gen <= slots[i].gen
MaxGen == IF slots[0].gen >= slots[1].gen THEN slots[0].gen ELSE slots[1].gen

\* What boot sees: rows of the named segments that exist, and the log after the position.
Visible(v) == (UNION {Seg(q).rows : q \in v.names \cap Seqs}) \cup {log[i] : i \in (v.pos + 1)..Len(log)}

Init ==
    /\ log = <<>>
    /\ segs = {}
    /\ slots = [i \in Slots |-> [gen |-> 0, names |-> {}, pos |-> 0, st |-> "empty"]]
    /\ cur = 1
    /\ view = [names |-> {}, pos |-> 0]
    /\ pf = NoPending
    /\ nextSeq = 1
    /\ crashes = 0
    /\ crashed = FALSE
    /\ hadManifest = FALSE

Write(r) ==
    /\ r \notin LogRows
    /\ log' = Append(log, r)
    /\ crashed' = FALSE
    /\ UNCHANGED <<segs, slots, cur, view, pf, nextSeq, crashes, hadManifest>>

\* The rows logged since the checkpoint go to a new, unsynced segment.
Flush ==
    /\ pf = NoPending
    /\ nextSeq <= MaxSegs
    /\ Len(log) > view.pos
    /\ segs' = segs \cup {[seq |-> nextSeq, rows |-> {log[i] : i \in (view.pos + 1)..Len(log)}, synced |-> FALSE]}
    /\ pf' = [names |-> view.names \cup {nextSeq}, pos |-> Len(log), merge |-> FALSE]
    /\ nextSeq' = nextSeq + 1
    /\ crashed' = FALSE
    /\ UNCHANGED <<log, slots, cur, view, crashes, hadManifest>>

\* Two named segments become one new, unsynced segment (compaction, stage 5).
Merge ==
    /\ pf = NoPending
    /\ nextSeq <= MaxSegs
    /\ \E a, b \in view.names :
        /\ a < b
        /\ a \in Seqs /\ b \in Seqs
        /\ segs' = segs \cup {[seq |-> nextSeq, rows |-> Seg(a).rows \cup Seg(b).rows, synced |-> FALSE]}
        /\ pf' = [names |-> (view.names \ {a, b}) \cup {nextSeq}, pos |-> view.pos, merge |-> TRUE]
    /\ nextSeq' = nextSeq + 1
    /\ crashed' = FALSE
    /\ UNCHANGED <<log, slots, cur, view, crashes, hadManifest>>

SyncSegment(q) ==
    /\ q \in Seqs
    /\ ~Seg(q).synced
    /\ segs' = (segs \ {Seg(q)}) \cup {[Seg(q) EXCEPT !.synced = TRUE]}
    /\ crashed' = FALSE
    /\ UNCHANGED <<log, slots, cur, view, pf, nextSeq, crashes, hadManifest>>

Target == IF AlternateSlots THEN Other(cur) ELSE cur

WriteManifest ==
    /\ pf /= NoPending
    /\ slots[Target].st /= "pending"
    /\ SyncSegmentFirst => \A q \in pf.names : q \in Seqs /\ Seg(q).synced
    /\ slots' = [slots EXCEPT ![Target] = [gen |-> MaxGen + 1, names |-> pf.names, pos |-> pf.pos, st |-> "pending"]]
    /\ crashed' = FALSE
    /\ UNCHANGED <<log, segs, cur, view, pf, nextSeq, crashes, hadManifest>>

SyncManifest ==
    /\ pf /= NoPending
    /\ slots[Target].st = "pending"
    /\ slots' = [slots EXCEPT ![Target].st = "durable"]
    /\ cur' = Target
    /\ view' = [names |-> pf.names, pos |-> pf.pos]
    /\ pf' = NoPending
    /\ hadManifest' = TRUE
    /\ crashed' = FALSE
    /\ UNCHANGED <<log, segs, nextSeq, crashes>>

\* Deleting a segment no current or coming manifest names. With the fix, only once no manifest
\* that names it is still durable on disk or about to be: the server's view is already the new
\* one. Without it, as soon as a merge has replaced it in the coming manifest.
Gc(q) ==
    /\ q \in Seqs
    /\ q \notin pf.names
    /\ IF GcAfterManifestSync
       THEN /\ q \notin view.names
            /\ \A i \in Slots : slots[i].st \in {"durable", "pending"} => q \notin slots[i].names \/ slots[i].gen < slots[cur].gen
       ELSE pf.merge \/ q \notin view.names
    /\ segs' = segs \ {Seg(q)}
    /\ crashed' = FALSE
    /\ UNCHANGED <<log, slots, cur, view, pf, nextSeq, crashes, hadManifest>>

\* Unsynced segments are lost; a pending manifest write either landed whole or was torn.
\* Then boot: the valid slot with the higher generation, or a full replay if none.
Crash ==
    /\ crashes < MaxCrashes
    /\ \E landed \in [Slots -> BOOLEAN] :
        LET after == [i \in Slots |-> IF slots[i].st = "pending"
                                         THEN [slots[i] EXCEPT !.st = IF landed[i] THEN "durable" ELSE "torn"]
                                         ELSE slots[i]]
            valid == {i \in Slots : after[i].st = "durable"}
            best == CHOOSE i \in valid : \A j \in valid : after[j].gen <= after[i].gen
        IN /\ slots' = after
           /\ IF valid = {}
              THEN /\ view' = [names |-> {}, pos |-> 0]
                   /\ cur' = cur
              ELSE /\ view' = [names |-> after[best].names, pos |-> after[best].pos]
                   /\ cur' = best
    /\ segs' = {s \in segs : s.synced}
    /\ pf' = NoPending
    /\ crashes' = crashes + 1
    /\ crashed' = TRUE
    /\ UNCHANGED <<log, nextSeq, hadManifest>>

Next ==
    \/ \E r \in Rows : Write(r)
    \/ Flush \/ Merge \/ WriteManifest \/ SyncManifest
    \/ \E q \in 1..MaxSegs : SyncSegment(q) \/ Gc(q)
    \/ Crash

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ Len(log) <= Cardinality(Rows)
    /\ cur \in Slots
    /\ crashes <= MaxCrashes

AcknowledgedRowsSurvive == crashed => LogRows \subseteq Visible(view)

ManifestSurvivesCrash == (crashed /\ hadManifest) => \E i \in Slots : slots[i].st = "durable"
=============================================================================
