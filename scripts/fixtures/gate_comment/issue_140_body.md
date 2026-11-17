Part of #123. Design: `CONFORMANCE_DESIGN.md` section 6.7. Wave 1 (parallel, after the foundations). Map area: `bucket-basics`, `object-basics` (45 test functions pass, 14 do not, nightly run 43537724429).

## Goal
The plain bucket and object operations answer as S3 does in the cases the suite checks: bucket naming rules, limits, status codes for missing things, and `Expect: 100-continue`.

## Depends on
- #148 (operation table).
- #158 owns real Last-Modified, which this issue used to carry.

## Requirements
1. **Bucket names** at CreateBucket: 3 to 63 characters; lowercase letters, digits, hyphens and dots; start and end with a letter or digit; no `..`, `.-` or `-.`; not an IP address; no underscore. Otherwise 400 `InvalidBucketName`. Existing buckets with other names stay usable. (Twice in this repo a test used a two-character bucket: add the rule to the test helpers too.)
2. **GetBucketLocation** returns the configured region. `tests/s3tests/s3tests.conf` has `api_name = default` while the server answers `us-east-1`: make them agree (a `--region` flag, default `us-east-1`, and the conf set to it).
3. **DeleteObjects:** more than 1000 keys is 400 `MalformedXML`; `Quiet` mode; per-key errors.
4. **Missing things:** a missing bucket is 404 `NoSuchBucket` for every operation, before authorization (`test_object_delete_key_bucket_gone` gets 403 today); a missing key is 404 `NoSuchKey`.
5. **`Expect: 100-continue`:** when the request will be refused (authentication, authorization, missing bucket, bad headers), answer the error without sending `100 Continue` and without reading the body (`test_100_continue` expects 403 first).
6. A key whose percent-decoded bytes are not valid UTF-8 is 400 with the message `Couldn't parse the specified URI.` on every operation (`test_object_read_unreadable`; today GET answers 404).
7. `test_atomic_dual_write_8mb` is owned by #154.
8. Keys up to 1024 bytes; longer is 400 `KeyTooLongError`.

Where a requirement and a suite test disagree, the test's expectation is the one to meet; note the difference from AWS in the PR and in `tests/s3tests/PROFILE.md` if it needs a setting.

## State machines (`STATE_MACHINES.md`)
- **Bucket** (implicit: absent, active, deleted): #149 makes it explicit. Bucket naming is a pure function `valid_bucket_name(&str) -> Result<(), Refusal>` with a table test built from the suite's cases plus boundary lengths.

Rule for this and every issue: no rule about states lives in a handler. If the work touches a machine the registry marks `implicit`, it becomes a pure function in `server/src/fsm.rs` with a test over every (state, event) pair first, and its row is updated.

## Invariants, each with its test
- A request refused before the body is read never has its body buffered (work counter: zero body bytes).
- Name validation runs at create only: a bucket that exists is always addressable.

## Verification beyond unit tests
- Property test: every name the validator accepts is a valid DNS label sequence, and every name of the suite's `naming_bad_*` tests is refused.

## Gate
- **Conformance, checked on this PR (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` 6.1):** area `bucket-basics`, `object-basics`. On this PR's `s3tests` check the Ratchet line reads `0 of them no longer pass`, and every test under **Must pass** below passes there and is added to `tests/s3tests/passing.txt` in this PR. 54 tests of the area are already listed. A test under Must pass that cannot pass is a stop (`needs-decision`, with its failure), never a removal. Tests under **Owned elsewhere** are not this issue's.

<details><summary>Must pass: 3 tests, each with its failure in run 43913171442 (PR #222, 2026-11-17)</summary>

| test | first line of the failure |
|---|---|
| `test_s3::test_object_delete_key_bucket_gone` | assert 403 == 404 |
| `test_s3::test_bucket_get_location` | botocore.exceptions.ClientError: An error occurred (InvalidLocationConstraint) when calling the CreateBucket operation: The specified location-constra |
| `test_s3::test_object_read_unreadable` | assert 404 == 400 |

</details>

<details><summary>Owned elsewhere: 2 tests, each with its owner</summary>

| test | owner and reason |
|---|---|
| `test_s3::test_bucket_create_special_key_names` | #122 (ACLs, policy, public access block): botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutOb |
| `test_s3::test_100_continue` | #122 (ACLs, policy, public access block): botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBu |

</details>

- **Checks:** one PR for this issue alone; `gh pr checks <pr>` all green (Test on both platforms, Drive, Model check, Dashboard, `s3tests`), its output pasted in the evidence comment.
- **Tests first:** each requirement has a Rust test in `server/tests/` (or a unit test for pure functions) that failed before the change. The commit message names it and its negative control (the fix reverted, the test fails).
- **Both platforms:** `cargo test --locked --no-fail-fast --manifest-path server/Cargo.toml` green on ubuntu-latest and windows-latest.
- **State machines:** `python3 scripts/check_state_machines.py` passes, and every machine named above is `wired` in `STATE_MACHINES.md`.
- **Model:** `bash verification/run_tlc.sh` green, including any model change named above and its broken variant that TLC must reject.
- **Driven end to end:** `bash scripts/verify_drive.sh` has a journey for the behaviour and passes in the `Drive the server (aws CLI)` job; `.claude/skills/verify-awanfs/features/` describes it.
- **Speed and memory:** not run per PR. The reviewer runs `bench-ratio.yml` on `main` after a batch of PRs merges (`AGENTS.md`); if a row leaves tolerance and the cause is this change, the issue is reopened.
- **Docs in step:** `README.md` (the "Not yet" line and feature list), `ARCHITECTURE.md` for any decision, `FORMAT.md` for any stored bytes, `tests/s3tests/PROFILE.md` for any setting the suite needs.
- **Evidence:** a comment here lists each gate line with its command and result. The builder labels the issue `needs-review` and does not close it.

## Files
In scope: `server/src/ops/bucket.rs`, `server/src/ops/object.rs`, `server/src/main.rs` (`--region`), `tests/s3tests/s3tests.conf`, `server/tests/`, the bucket-name helper in `server/tests/common`.
Must not change: the on-disk format beyond what `FORMAT.md` already describes, the meaning of existing flags, any test's expectation.

## Stop and ask if
- A test expects behaviour that contradicts a recorded decision in `ARCHITECTURE.md` (name the decision and the test).
- The work needs a new on-disk record, a new side file, or an access check inside a handler: that is a change to `CONFORMANCE_DESIGN.md`.
- An existing test's expectation, or a default a user can see, would have to change.
- A row of the ratio check leaves tolerance and the cause is this change.

## Tests this issue owns that do not pass today
<details><summary>14 test functions, with each failure's first line</summary>

- `test_multi_object_delete_key_limit` (fail): AssertionError: ClientError not raised
- `test_multi_objectv2_delete_key_limit` (fail): AssertionError: ClientError not raised
- `test_object_delete_key_bucket_gone` (fail): assert 403 == 404
- `test_bucket_create_naming_bad_starts_nonalpha` (fail): AssertionError: ClientError not raised
- `test_bucket_create_naming_bad_ip` (fail): AssertionError: ClientError not raised
- `test_bucket_create_naming_dns_underscore` (fail): AssertionError: ClientError not raised
- `test_bucket_create_naming_dns_dash_at_end` (fail): AssertionError: ClientError not raised
- `test_bucket_create_naming_dns_dot_dot` (fail): AssertionError: ClientError not raised
- `test_bucket_create_naming_dns_dot_dash` (fail): AssertionError: ClientError not raised
- `test_bucket_create_naming_dns_dash_dot` (fail): AssertionError: ClientError not raised
- `test_bucket_get_location` (fail): AssertionError: assert 'us-east-1' == 'default'
- `test_100_continue` (fail): AssertionError: assert '403' == '100'
- `test_atomic_dual_write_8mb` (fail): Failed: Timeout (>60.0s) from pytest-timeout.
- `test_object_read_unreadable` (fail): assert 404 == 400
</details>


---
_Generated by [Claude Code](https://claude.ai/code)_
