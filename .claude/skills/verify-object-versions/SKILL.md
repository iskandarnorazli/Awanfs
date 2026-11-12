---
name: verify-object-versions
description: Check that one object key's live state equals what restart recovery rebuilds, with the TLA+ model in verification/tla and its Rust replays. Use before merging any change to PUT, DELETE, CopyObject or CompleteMultipartUpload in s3_api.rs, to write_needle or recovery in volume.rs, or to the per-key lock.
---

# Verify object versions (live view = recovered view)

**Rule (AckedStateSurvivesRestart):** when no request is in flight, what a GET shows for a key now is what it shows after a restart.
**Harm when it breaks:** a client reads version A, the server restarts, and it reads version B, or a deleted object comes back, or an acknowledged object is gone.

## Does my change touch it?

Yes if the diff changes any of these (coverage record):

| Model step | Code |
|---|---|
| `Acquire` / `Release` | `S3ServiceState::lock_key` (s3_api.rs) and every caller: PUT, DELETE, CopyObject, CompleteMultipartUpload |
| `Reserve` | `VolumeManager::write_needle`: `current_offset.fetch_add` (volume.rs) |
| `Write` / `Fail` | `VolumeManager::write_needle`: `write_aligned_at` and its error path |
| `Index` | the `engine.put_object` / `engine.delete_object` calls in those handlers, and their order against the needle write |
| `Recover` | `S3ServiceState::with_data_dir` replay loop; `VolumeManager::open_mode` scan and `recover_needles` (the `next_indexed` gap jump, the `.idx` read) |

Last confirmed to describe the code at commit `ba4ffd6` (branch `verify/object-versions`, PR #60). Move this forward only after you have checked the table above against the code at the new commit.

## Run it

```bash
bash verification/run_tlc.sh                                                   # model: needs Java 11+, downloads TLC 1.7.4 once
cargo test --locked --manifest-path server/Cargo.toml --test key_order_test --test volume_gap_test   # replays on the real server
```

CI runs both (`model-check` job and the test jobs) on every PR.

## Read the result

- `run_tlc.sh` must print `ok` for all five configs. `fixed.cfg` is the design the code implements and must hold. Each other config switches one fix off (`UseKeyLock`, `RecoverSkipsGaps`, `DeleteLogsFirst`) and must be violated: that proves the model can still catch the bug that fix prevents.
- If `fixed.cfg` is violated, TLC prints the shortest path. Read it with the `/statechart` skill, which tells the path as steps in plain words. Then decide: a real bug in the design, or a model step that no longer matches the code (compare with the table above).
- If a broken config passes, the model lost the ability to catch that bug: the model changed, not the code. Fix the model.
- A replay test failing is a real bug in the code: `key_order_test` prints the live view (left) and the recovered view (right). Do not change the expected values.

## When the code changes on purpose

1. Change `verification/tla/ObjectVersions.tla` so each step still matches the code (keep the comment block that names the code for each step).
2. Run `bash verification/run_tlc.sh`; all five configs must still print `ok`. A new fix gets a new switch and a config that turns it off.
3. If the change adds a new way to change a key (a new handler), add it to the table above and give it a replay in `server/tests/key_order_test.rs` using `S3ServiceState::test_hook` (points `put_stored`, `delete_start`, `key_lock_contended`).
4. Update the commit above.

## What this does not cover

- Power loss: the model treats a written needle as kept. Syncing is #29.
- More than one key at a time, bucket create and delete, compaction (#14) and volume rollover (#37).
- The model has 2 writers and 1 deleter on one key; TLC explores every order of their steps, nothing larger.
