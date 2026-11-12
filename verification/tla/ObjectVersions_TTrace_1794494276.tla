---- MODULE ObjectVersions_TTrace_1794494276 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, ObjectVersions

_expression ==
    LET ObjectVersions_TEExpression == INSTANCE ObjectVersions_TEExpression
    IN ObjectVersions_TEExpression!expression
----

_trace ==
    LET ObjectVersions_TETrace == INSTANCE ObjectVersions_TETrace
    IN ObjectVersions_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        pc = ([w1 |-> "done", w2 |-> "done", d1 |-> "done"])
        /\
        log = ({[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"], [val |-> "d1", o |-> 3, kind |-> "tomb"]})
        /\
        engine = ([present |-> TRUE, val |-> "w2"])
        /\
        lock = ("free")
        /\
        nextOff = (4)
        /\
        off = ([w1 |-> 1, w2 |-> 2, d1 |-> 3])
    )
----

_init ==
    /\ lock = _TETrace[1].lock
    /\ off = _TETrace[1].off
    /\ pc = _TETrace[1].pc
    /\ log = _TETrace[1].log
    /\ engine = _TETrace[1].engine
    /\ nextOff = _TETrace[1].nextOff
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ lock  = _TETrace[i].lock
        /\ lock' = _TETrace[j].lock
        /\ off  = _TETrace[i].off
        /\ off' = _TETrace[j].off
        /\ pc  = _TETrace[i].pc
        /\ pc' = _TETrace[j].pc
        /\ log  = _TETrace[i].log
        /\ log' = _TETrace[j].log
        /\ engine  = _TETrace[i].engine
        /\ engine' = _TETrace[j].engine
        /\ nextOff  = _TETrace[i].nextOff
        /\ nextOff' = _TETrace[j].nextOff

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("ObjectVersions_TTrace_1794494276.json", _TETrace)

=============================================================================

 Note that you can extract this module `ObjectVersions_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `ObjectVersions_TEExpression.tla` file takes precedence 
  over the module `ObjectVersions_TEExpression` below).

---- MODULE ObjectVersions_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, ObjectVersions

expression == 
    [
        \* To hide variables of the `ObjectVersions` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        lock |-> lock
        ,off |-> off
        ,pc |-> pc
        ,log |-> log
        ,engine |-> engine
        ,nextOff |-> nextOff
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_lockUnchanged |-> lock = lock'
        
        \* Format the `lock` variable as Json value.
        \* ,_lockJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(lock)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_lockModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].lock # _TETrace[s-1].lock
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE ObjectVersions_TETrace ----
\*EXTENDS IOUtils, TLC, ObjectVersions
\*
\*trace == IODeserialize("ObjectVersions_TTrace_1794494276.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE ObjectVersions_TETrace ----
EXTENDS TLC, ObjectVersions

trace == 
    <<
    ([pc |-> [w1 |-> "start", w2 |-> "start", d1 |-> "start"],log |-> {},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 1,off |-> [w1 |-> 0, w2 |-> 0, d1 |-> 0]]),
    ([pc |-> [w1 |-> "reserve", w2 |-> "start", d1 |-> "start"],log |-> {},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 1,off |-> [w1 |-> 0, w2 |-> 0, d1 |-> 0]]),
    ([pc |-> [w1 |-> "reserve", w2 |-> "reserve", d1 |-> "start"],log |-> {},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 1,off |-> [w1 |-> 0, w2 |-> 0, d1 |-> 0]]),
    ([pc |-> [w1 |-> "reserve", w2 |-> "reserve", d1 |-> "reserve"],log |-> {},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 1,off |-> [w1 |-> 0, w2 |-> 0, d1 |-> 0]]),
    ([pc |-> [w1 |-> "write", w2 |-> "reserve", d1 |-> "reserve"],log |-> {},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 2,off |-> [w1 |-> 1, w2 |-> 0, d1 |-> 0]]),
    ([pc |-> [w1 |-> "write", w2 |-> "write", d1 |-> "reserve"],log |-> {},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 3,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 0]]),
    ([pc |-> [w1 |-> "write", w2 |-> "write", d1 |-> "write"],log |-> {},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "index", w2 |-> "write", d1 |-> "write"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"]},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "index", w2 |-> "index", d1 |-> "write"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"]},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "index", w2 |-> "index", d1 |-> "index"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"], [val |-> "d1", o |-> 3, kind |-> "tomb"]},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "release", w2 |-> "index", d1 |-> "index"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"], [val |-> "d1", o |-> 3, kind |-> "tomb"]},engine |-> [present |-> TRUE, val |-> "w1"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "release", w2 |-> "index", d1 |-> "release"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"], [val |-> "d1", o |-> 3, kind |-> "tomb"]},engine |-> [present |-> FALSE, val |-> "none"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "release", w2 |-> "release", d1 |-> "release"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"], [val |-> "d1", o |-> 3, kind |-> "tomb"]},engine |-> [present |-> TRUE, val |-> "w2"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "done", w2 |-> "release", d1 |-> "release"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"], [val |-> "d1", o |-> 3, kind |-> "tomb"]},engine |-> [present |-> TRUE, val |-> "w2"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "done", w2 |-> "done", d1 |-> "release"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"], [val |-> "d1", o |-> 3, kind |-> "tomb"]},engine |-> [present |-> TRUE, val |-> "w2"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]]),
    ([pc |-> [w1 |-> "done", w2 |-> "done", d1 |-> "done"],log |-> {[val |-> "w1", o |-> 1, kind |-> "put"], [val |-> "w2", o |-> 2, kind |-> "put"], [val |-> "d1", o |-> 3, kind |-> "tomb"]},engine |-> [present |-> TRUE, val |-> "w2"],lock |-> "free",nextOff |-> 4,off |-> [w1 |-> 1, w2 |-> 2, d1 |-> 3]])
    >>
----


=============================================================================

---- CONFIG ObjectVersions_TTrace_1794494276 ----
CONSTANTS
    Writers = { "w1" , "w2" }
    Deleters = { "d1" }
    UseKeyLock = FALSE
    WriteCanFail = FALSE
    RecoverSkipsGaps = FALSE

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Thu Nov 12 14:37:57 UTC 2026