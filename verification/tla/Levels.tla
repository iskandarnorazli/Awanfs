----------------------------- MODULE Levels -----------------------------
(*
  #26 stage 5: leveled compaction of the metadata segments. Writes go to a memtable; a flush
  makes it a level-0 segment. Level-0 segments may overlap: the higher sequence number is newer.
  Each deeper level holds segments whose key ranges do not overlap. A lookup takes the memtable,
  then the level-0 segments newest first, then in each deeper level the one segment whose range
  could hold the key (the greatest first key at or below it), and stops at the first entry.

  A compaction writes one new segment one level down from its inputs and drops them:
    from level 0: the level-0 segments and the level-1 segments overlapping their key span;
    from level i: one segment and the level-(i+1) segments overlapping its range.
  Tombstones are dropped when no deeper level has a segment overlapping the output's range.
  Each compaction is one manifest switch, which Manifest.tla shows is atomic across a crash,
  so crashes are not modelled again here.

  Switches (each fix, turned off, gives a configuration TLC must reject):
    L0TakesAll           a compaction from level 0 takes every level-0 segment. Without it, a
                         newer level-0 segment can move below an older one (stage 4's merge
                         gave a merged pair a sequence number above a newer unmerged segment:
                         the same order inversion).
    OverlapAll           a compaction takes every overlapping segment of the level below.
    TombstonesAtBottom   tombstones are dropped only when nothing deeper could hold the key.

  Invariants:
    LookupCorrect    every key's lookup returns its last write, or nothing after a delete.
    LevelsDisjoint   no two segments of a level from 1 down overlap.
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Keys, MaxWrites, MaxFlushes, MaxLevel, L0TakesAll, OverlapAll, TombstonesAtBottom

Tomb == 0
Levels == 1..MaxLevel
NoData == [k \in {} |-> Tomb]

VARIABLES mem, segs, nextId, writes, flushes, truth

vars == <<mem, segs, nextId, writes, flushes, truth>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Lo(s) == Min(DOMAIN s.data)
Hi(s) == Max(DOMAIN s.data)
Overlaps(s, lo, hi) == Lo(s) <= hi /\ lo <= Hi(s)
L(i) == {s \in segs : s.level = i}

\* Higher wins: level 0 by sequence number, then shallower levels.
Prec(s) == IF s.level = 0 THEN 100 + s.id ELSE 10 - s.level

\* Each key's entry in the segment that wins it among S.
Combined(S) ==
    [k \in UNION {DOMAIN s.data : s \in S} |->
        LET w == CHOOSE s \in S : k \in DOMAIN s.data /\ \A t \in S : k \in DOMAIN t.data => Prec(t) <= Prec(s)
        IN w.data[k]]

\* The segment of level i a lookup reads for k (binary search on first keys), if any.
Picks(i, k) == {s \in L(i) : Lo(s) <= k /\ \A t \in L(i) : Lo(t) <= k => Lo(t) <= Lo(s)}

Value(k) ==
    IF k \in DOMAIN mem THEN mem[k]
    ELSE LET C == {s \in L(0) : k \in DOMAIN s.data}
                  \cup UNION {{s \in Picks(i, k) : k \in DOMAIN s.data} : i \in Levels}
         IN IF C = {} THEN Tomb
            ELSE (CHOOSE s \in C : \A t \in C : Prec(t) <= Prec(s)).data[k]

Init ==
    /\ mem = NoData
    /\ segs = {}
    /\ nextId = 1
    /\ writes = 0
    /\ flushes = 0
    /\ truth = [k \in Keys |-> Tomb]

Put(k) ==
    /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ mem' = (k :> (writes + 1)) @@ mem
    /\ truth' = [truth EXCEPT ![k] = writes + 1]
    /\ UNCHANGED <<segs, nextId, flushes>>

Del(k) ==
    /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ mem' = (k :> Tomb) @@ mem
    /\ truth' = [truth EXCEPT ![k] = Tomb]
    /\ UNCHANGED <<segs, nextId, flushes>>

Flush ==
    /\ DOMAIN mem /= {}
    /\ flushes < MaxFlushes
    /\ segs' = segs \cup {[id |-> nextId, level |-> 0, data |-> mem]}
    /\ mem' = NoData
    /\ nextId' = nextId + 1
    /\ flushes' = flushes + 1
    /\ UNCHANGED <<writes, truth>>

\* Writes the merge of In and T at level j and drops them.
Install(In, T, j) ==
    LET all == Combined(In \cup T)
        lo == Min(DOMAIN all)
        hi == Max(DOMAIN all)
        bottom == \A s \in segs : s.level > j => ~Overlaps(s, lo, hi)
        drop == IF TombstonesAtBottom THEN bottom ELSE TRUE
        keep == IF drop THEN {k \in DOMAIN all : all[k] /= Tomb} ELSE DOMAIN all
        out == [k \in keep |-> all[k]]
    IN /\ segs' = (segs \ (In \cup T)) \cup (IF keep = {} THEN {} ELSE {[id |-> nextId, level |-> j, data |-> out]})
       /\ nextId' = nextId + 1

\* The segments of level j overlapping [lo, hi] that a compaction takes.
Targets(j, lo, hi) ==
    LET ov == {s \in L(j) : Overlaps(s, lo, hi)}
    IN IF OverlapAll THEN {ov} ELSE SUBSET ov

CompactL0 ==
    /\ L(0) /= {}
    /\ \E In \in (IF L0TakesAll THEN {L(0)} ELSE {{s} : s \in L(0)}) :
        LET span == UNION {DOMAIN s.data : s \in In}
        IN \E T \in Targets(1, Min(span), Max(span)) : Install(In, T, 1)
    /\ UNCHANGED <<mem, writes, flushes, truth>>

CompactLevel(i) ==
    /\ i < MaxLevel
    /\ \E s \in L(i) :
        \E T \in Targets(i + 1, Lo(s), Hi(s)) : Install({s}, T, i + 1)
    /\ UNCHANGED <<mem, writes, flushes, truth>>

Next ==
    \/ \E k \in Keys : Put(k) \/ Del(k)
    \/ Flush
    \/ CompactL0
    \/ \E i \in Levels : CompactLevel(i)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ writes <= MaxWrites
    /\ \A s \in segs : s.level \in 0..MaxLevel /\ DOMAIN s.data /= {}

LookupCorrect == \A k \in Keys : Value(k) = truth[k]

LevelsDisjoint == \A i \in Levels : \A s, t \in L(i) : s /= t => ~Overlaps(s, Lo(t), Hi(t))
=============================================================================
