Part of #123. Wave 1.

## Goal
The nightly conformance job also runs `s3tests/functional/test_headers.py` (48 test functions: malformed and missing request headers, date and authorization edge cases under SigV2 and SigV4). It is plain S3 behaviour, on the authentication path, and is not run today.

## Requirements
1. `.github/workflows/s3-tests.yml` runs `test_headers.py` after `test_s3.py`, with its own junit file.
2. `tests/s3tests/ratchet.py` and `tests/s3tests/passing.txt` handle both files: names are already `test_s3::name`; headers tests are `test_headers::name`.
3. `tests/s3tests/conformance_map.py` reads both files; an `AREAS` rule assigns the headers tests to a new area `request-headers`, owned by this issue. The "unassigned test fails the run" rule covers both files.
4. First run: record what passes in `passing.txt` (ratchet from the first measurement), list what fails here with each failure line, grouped by cause.
5. Then fix by cause. Expected groups, to confirm from the first run: missing or malformed `Content-Length`, bad `Authorization` (empty, garbage, unknown scheme), bad or skewed `Date` / `X-Amz-Date` (403 `RequestTimeTooSkewed`), missing `Host`, `Expect` handling, non-printable characters in headers, SigV2 support where the suite signs with it.

## State machines
None. Request authentication is a decision: `authenticate(headers, query, now) -> Principal | Refusal` is one function with a table test; each fixed case adds a row.

## Invariants, each with its test
- A malformed header never causes a panic, a hang or a 500: fuzz the header-to-`authenticate` path (extends the existing SigV4 fuzz target).
- A request refused for a header problem never has its body read.
- Clock-skew checks use the injected `Clock` (#158).

## Verification
- Review by someone who did not write it for any change to signature verification.

## Gate
- **Checks:** one PR for this issue alone; `gh pr checks <pr>` all green (Test on both platforms, Drive, Model check, Dashboard, `s3tests`), its output pasted in the evidence comment.
- On this PR's `s3tests` check the Ratchet line covers both files and reads `0 of them no longer pass` (owner, 2026-11-17: gates are checked on the PR, not the nightly run).

<details><summary>request-headers, from run 43913171442: Must pass (1) and Owned elsewhere (29)</summary>

| test | Must pass: failure / Owned elsewhere: owner |
|---|---|
| `test_headers::test_object_create_bad_contentlength_none` | **must pass**: AssertionError: ClientError not raised |
| `test_headers::test_object_create_bad_authorization_empty` | authentication: the suite's profile runs the server anonymous (reserved: the owner decides auth in the profile) |
| `test_headers::test_object_create_date_and_amz_date` | SigV4 request handling (reserved for Claude or the owner): botocore.exceptions.ClientError: An error occurred (AuthorizationHeaderMalformed |
| `test_headers::test_object_create_amz_date_and_no_date` | SigV4 request handling (reserved for Claude or the owner): botocore.exceptions.ClientError: An error occurred (AuthorizationHeaderMalformed |
| `test_headers::test_object_create_bad_authorization_none` | authentication: the suite's profile runs the server anonymous (reserved: the owner decides auth in the profile) |
| `test_headers::test_object_acl_create_contentlength_none` | #122 (ACLs, policy, public access block): botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutOb |
| `test_headers::test_bucket_put_bad_canned_acl` | #122 (ACLs) |
| `test_headers::test_bucket_create_bad_authorization_empty` | authentication: the suite's profile runs the server anonymous (reserved: the owner decides auth in the profile) |
| `test_headers::test_bucket_create_bad_authorization_none` | authentication: the suite's profile runs the server anonymous (reserved: the owner decides auth in the profile) |
| `test_headers::test_object_create_bad_md5_invalid_garbage_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_contentlength_mismatch_below_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_authorization_incorrect_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_authorization_invalid_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_ua_empty_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_ua_none_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_date_invalid_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_date_empty_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_date_none_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_date_before_today_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_date_before_epoch_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_object_create_bad_date_after_end_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_authorization_invalid_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_ua_empty_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_ua_none_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_date_invalid_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_date_empty_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_date_none_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_date_before_today_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_date_after_today_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |
| `test_headers::test_bucket_create_bad_date_before_epoch_aws2` | out of scope: SigV2 (owner, 2026-11-17; `CONFORMANCE_DESIGN.md` section 9) |

</details>

- `python3 tests/s3tests/test_conformance_map.py` and the ratchet's self-tests pass, with new cases for the second file.
- Every `request-headers` test in `tests/s3tests/CONFORMANCE_MAP.md` is in `passing.txt`, or is listed here with the decision that excludes it.
- `cargo test` green on both platforms; negative controls in commit messages.

## Files
In scope: `.github/workflows/s3-tests.yml`, `tests/s3tests/`, `server/src/auth.rs`, `server/src/sigv4.rs`, `server/fuzz/`, `server/tests/`. Must not change: which signatures are accepted for well-formed requests.

## Stop and ask if
Passing a test needs SigV2 support the server does not have (a security decision: SigV2 is deprecated), or weakening a signature check.

---
_Generated by [Claude Code](https://claude.ai/code)_