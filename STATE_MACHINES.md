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
| Bucket versioning | Unset, Enabled, Suspended | wired | `server/src/fsm.rs::versioning_step`, called by `put_bucket_versioning` in `server/src/ops/bucket.rs` | `versioning_every_pair_is_decided`, `versioning_never_returns_to_unset`, `server/tests/object_lock_test.rs` | none needed: one writer, the manifest | #120 |
| Object lock of a version | retention (none, governance, compliance, with a time), legal hold | wired | `server/src/fsm.rs::lock_step`, `server/src/fsm.rs::may_delete`, called by `server/src/ops/lock.rs`, `server/src/ops/object.rs`, `server/src/s3_api.rs` (`update_object_lock`, `delete_version_conditional`), and `server/src/lifecycle.rs` | `compliance_in_force_is_never_weakened`, `governance_in_force_needs_the_bypass_to_weaken`, `a_legal_hold_forbids_delete_whatever_else_holds`, `with_nothing_in_force_any_future_retention_is_accepted_and_delete_is_allowed`, `a_retain_until_time_in_the_past_is_always_refused`, `the_cases_the_s3_tests_name`, `server/tests/object_lock_test.rs` | `verification/tla/ObjectVersions.tla` (`LockedVersionIsNeverRemoved`, `fixed_object_lock.cfg`, `object_lock_skip_delete_check.cfg`) | #120 |
| Live state of one object key | absent, unversioned, versioned (object, delete marker, updates per version and group) | wired | `server/src/fsm.rs::key_step`, called by PUT, DELETE, attribute updates (`set_attribute_group`), and recovery replay in `server/src/s3_api.rs` and `server/src/ops/object.rs` | `key_every_pair_is_decided`, `key_update_never_resurrects_or_hits_absent_version`, `server/tests/attr_update_test.rs` | `verification/tla/ObjectVersions.tla` (`AckedStateSurvivesRestart`, `UpdateNeverResurrects`) | #203 (`key_step`) |
| One needle write | receiving (declared, written), failed, committed; around it: reserved, relocated, indexed | wired | `server/src/fsm.rs::needle_step`, called by the `write` and `finish` methods of `NeedleWriter` in `server/src/volume.rs` (a refused write moves the writer to failed); the streamed PUT in `server/src/s3_api.rs` (`StreamsInFlight`, `relocate_needle`) | `needle_every_pair_is_decided`, `a_needle_is_committed_only_with_exactly_its_declared_bytes`, `the_needle_refusals_are_distinct`, `server/tests/needle_writer_state_test.rs`, `server/tests/stalled_put_test.rs` | `ObjectVersions.tla` (Reserve, Write, Fail, Index; StreamReserve, RelocateStep for streamed PUTs) | wired in #154 |
| Multipart upload | open, completed, aborted | wired | `server/src/fsm.rs::upload_step`, called by `MultipartManager` in `server/src/multipart.rs` | `upload_every_pair_is_decided`, `completed_upload_complete_is_idempotent`, `server/tests/multipart_conformance_test.rs` | Complete is a writer in `ObjectVersions.tla` | #111 (`upload_step`) |
| Bucket | absent, active, deleted; owner | wired | `server/src/fsm.rs::bucket_step`, called by `BucketConfigStore` in `server/src/bucket_config.rs` (manifest replay in `server/src/s3_api.rs` and bucket create/delete in `server/src/ops/bucket.rs`) | `bucket_every_pair_is_decided`, `bucket_deleted_drops_configs_and_stays_deleted` | none needed: one serialized log | #149 (`bucket_step`) |
| Bucket configuration of one kind | unset, set | wired | `server/src/fsm.rs::bucket_step`, called by `BucketConfigStore` in `server/src/bucket_config.rs` | `bucket_every_pair_is_decided`, `bucket_deleted_drops_configs_and_stays_deleted` | none needed: one serialized log | #149 |
| Durability of a write | written, synced | implicit | `make_durable`, group commit in `server/src/volume.rs` | power-cut and durability tests | none | when touched |
| Volume | active, sealed, compacting, retired; writes in flight; written end and allocated end (#178: the file is extended ahead of the written end, trimmed at open) | implicit (the seal and the in-flight count are in `server/src/volume.rs`: `seal`, `writers_in_flight`) | `compact_oldest_volume` in `server/src/s3_api.rs` | compaction tests, `server/tests/compaction_inflight_test.rs` | `ObjectVersions.tla` (CSeal, CScan, CCopy, CDrop; `fixed_compact.cfg`, `compact_no_wait.cfg`) | #167 modelled it; `volume_step` still to write |
| Lifecycle of an object under a rule | live, expired, removed | implicit | `server/src/lifecycle.rs` | lifecycle tests | none | #118 |
| Encrypted multipart upload | key and nonce state per part | not built | | | | #121 |
| A scheduled background task | idle, running, stopping, stopped | wired | `server/src/fsm.rs::task_step`, called by `Scheduler` in `server/src/scheduler.rs` | `task_every_pair_is_decided`, server/tests/scheduler_test.rs | none | #158 |
| A streamed response | opening, streaming, done, aborted (read error, checksum mismatch, client gone) | wired | `server/src/fsm.rs::stream_step`, called by the reader in `server/src/stream_read.rs`, which sends one segment (a large object) or one per part (a multipart object above 4 MiB, `multipart_body`) | `stream_every_pair_is_decided`, `server/tests/stream_read_test.rs`, `server/tests/stream_read_disconnect_test.rs`, `server/tests/multipart_get_reads_test.rs`, `server/tests/multipart_get_disconnect_test.rs` | none: one reader per response | #155 steps two and three |
| Scrub of a needle | unchecked, good, bad | wired | `server/src/fsm.rs::scrub_step`, called by `ScrubManager` in `server/src/scrub.rs` and `S3ServiceState` in `server/src/s3_api.rs` | `scrub_every_pair_is_decided`, `server/tests/scrub_test.rs` | none | #155 |
| POST object form parser | preamble, after a boundary, part headers, field body, file body, epilogue; the file seen | wired | `server/src/fsm.rs::form_step`, called by `FormParser` in `server/src/form_upload.rs` (which POST object in `server/src/ops/post_object.rs` reads the form with) | `form_every_pair_is_decided`, `form_nothing_follows_the_file_and_the_body_needs_one`, `server/tests/post_object_suite_test.rs` | none: one parser per request | #119 |
| Streamed PUT pipeline | filling, draining, failed, done | wired | `server/src/fsm.rs::pipeline_step`, called by `put_object` in `server/src/ops/object.rs` | `pipeline_every_pair_is_decided`, `server/tests/streaming_put_test.rs` | none: one pipeline per streamed PUT | #170 |
| Streamed UploadPart pipeline | filling, draining, failed, done | wired | `server/src/fsm.rs::part_pipeline_step`, called by `upload_part` in `server/src/ops/multipart.rs` | `part_pipeline_every_pair_is_decided`, `server/tests/streaming_part_test.rs` | none: one pipeline per streamed UploadPart | #57 |

Not state machines, and tested as tables instead: request classification (#148) and the
authorization decision (#151).
