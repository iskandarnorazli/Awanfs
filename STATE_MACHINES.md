# State machines

Everything in AwanFS that has states and rules for moving between them is listed here. The
rule (owner, 2026-11-14): **no indirect state machines.** A stateful thing has

1. a state type and an event type,
2. one pure transition function in `server/src/fsm.rs` (`step(state, event) -> Result<state, Refusal>`),
   which handlers call under the lock that guards the state,
3. a test that covers every (state, event) pair, and
4. when its states are shared between concurrent requests or rebuilt by recovery, a TLA+
   model in `verification/tla` whose steps name the code they stand for.

A row is **explicit** when 1 to 3 exist, **wired** when the handlers call the function, and
**implicit** when the rules still live in handler code. An issue that touches an implicit
machine makes it explicit first; an issue that adds a stateful feature adds its row.
`scripts/check_state_machines.py` runs in CI and fails when an explicit row names a function
or a test that is not in the file.

| Machine | States | Status | Function | Tests | Model | Owner |
|---|---|---|---|---|---|---|
| Bucket versioning | Unset, Enabled, Suspended | explicit | `server/src/fsm.rs::versioning_step` | `versioning_every_pair_is_decided`, `versioning_never_returns_to_unset` | none needed: one writer, the manifest | #113 wires it |
| Object lock of a version | retention (none, governance, compliance, with a time), legal hold | explicit | `server/src/fsm.rs::lock_step`, `server/src/fsm.rs::may_delete` | `compliance_in_force_is_never_weakened`, `governance_in_force_needs_the_bypass_to_weaken`, `a_legal_hold_forbids_delete_whatever_else_holds`, `with_nothing_in_force_any_future_retention_is_accepted_and_delete_is_allowed`, `a_retain_until_time_in_the_past_is_always_refused`, `the_cases_the_s3_tests_name` | to add in #120: a locked version is never removed before its time | #120 wires it |
| Live state of one object key | absent, object, delete marker, per version | implicit | `server/src/versions.rs` replay, PUT and DELETE in `server/src/s3_api.rs` | `server/tests/` version and recovery tests | `verification/tla/ObjectVersions.tla` (`AckedStateSurvivesRestart`) | #150 extends the model with attributes |
| One needle write | reserved, written, indexed, failed | implicit | `server/src/volume.rs` (`write_needle`, `NeedleWriter`) | recovery and gap tests | `ObjectVersions.tla` (Reserve, Write, Fail, Index) | #154 changes where the lock is taken; model first |
| Multipart upload | open, completed, aborted | implicit | `server/src/multipart.rs` | `server/tests/` multipart tests | none | #111 |
| Bucket | absent, active, deleted; owner | implicit | manifest replay in `server/src/s3_api.rs` | restart tests | none | #149 |
| Bucket configuration of one kind | unset, set | implicit (new) | | | none needed | #149 |
| Durability of a write | written, synced | implicit | `make_durable`, group commit in `server/src/volume.rs` | power-cut and durability tests | none | when touched |
| Volume | active, sealed, compacting, retired | implicit | `server/src/compaction.rs` | compaction tests | none | when touched |
| Lifecycle of an object under a rule | live, expired, removed | implicit | `server/src/lifecycle.rs` | lifecycle tests | none | #118 |
| Encrypted multipart upload | key and nonce state per part | not built | | | | #121 |

Not state machines, and tested as tables instead: request classification (#148) and the
authorization decision (#151).
