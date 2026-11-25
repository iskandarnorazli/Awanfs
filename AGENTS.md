# AwanFS: rules for agents

How to work (goal first, evidence, tests first, the owner decides, handoffs) is the
`iskandar-mode` skill. This file is what is specific to this repository; where the two
disagree, this file wins. `CLAUDE.md` points here.

AwanFS is an S3-compatible object store in Rust (`server/`). The plan of record is the
GitHub milestones and [`ROADMAP_AND_DEPENDENCIES.md`](ROADMAP_AND_DEPENDENCIES.md);
decisions are numbered in [`ARCHITECTURE.md`](ARCHITECTURE.md); stored bytes are in
[`FORMAT.md`](FORMAT.md); the conformance plan is
[`CONFORMANCE_DESIGN.md`](CONFORMANCE_DESIGN.md); the verification map is
[`VERIFICATION.md`](VERIFICATION.md). `AUTONOMOUS_EXECUTION_RUNBOOK.md` and
`HYBRID_POC_SPECIFICATION.md` describe the finished PoC and are not instructions.

## Which issues

- Work on any unblocked issue labelled `ready:antigravity` or `ready:claude` with no `wip:` label (owner, 2026-11-24). An empty queue means stop and say so.
- An issue blocked by another (GitHub's "blocked by" link, or a first line "Blocked by #N") waits until #N is closed.
- Never start an issue labelled `outline` or `needs-decision`.
- **An owner decision is a comment the owner wrote on the issue** (owner, 2026-11-19). Cite it by its link. A decision given anywhere else (a chat, a task file) is not one until the owner posts it there: stop with `needs-decision` and the options instead. On 2026-11-18 three evidence comments cited an "owner decision" that was on no issue, and two of them set aside a failing gate test.
- **A gate line is checked on the PR** (owner, 2026-11-17). Whoever writes an issue writes each gate line as something the PR's checks or a command run on its branch can show, before merge: never "the nightly run shows". A conformance gate names its tests (must pass, and owned elsewhere with the owning issue), as `CONFORMANCE_DESIGN.md` 6.1 says. A gate line the builder cannot check on the PR is a reason to stop and ask, not to skip it.
- These need the owner's decision before changing, even when an issue seems to ask for them. Stop with `needs-decision` and say which (owner, 2026-11-24):
  - stored bytes in volumes: the needle header, the journal, the volume format (`FORMAT.md`). A bucket manifest record is allowed when the issue gives its exact text and its gate has restart and replay tests;
  - any decision in `ARCHITECTURE.md`. A test never reverses one.

  Moving code is not changing it: a refactor may move such code when the issue says so, if what it decides and stores is unchanged and the existing tests pass untouched.

## How an issue is worked

1. Add `wip:antigravity` (or `wip:claude`). Branch from `main`: `issue/<n>-<slug>`. **One issue, one branch, one PR**, even when several issues are worked in parallel (owner, 2026-11-17). Never combine issues into a batch PR: in #216 ten issues in one PR hid a regression, skipped each issue's checks and could not be reverted one at a time.
2. Read the Goal, the Gate and the Stop list. Write the tests first and show them failing.
3. Commit as `Iskandar Norazli <111472977+iskandarnorazli@users.noreply.github.com>`, with no AI or Co-Authored-By lines. The message states the tests added, the commands run with their results, and the negative control: the fix turned off, the new test failing.
4. Push the branch and open a **draft** PR. Never push `main` directly and never force-push.
5. **Wait for the PR's checks and read them:** `gh pr checks <pr>`. Every check passes, including `s3tests`; it runs the ceph suite when the PR changes server code and passes in seconds otherwise. A failing or pending check means the work is not done: fix it, or stop and say why. `main` requires these checks, so a red PR cannot merge. Check the evidence comment with `python3 scripts/check_gate_comment.py <issue>` so every gate line is answered.
6. Comment on the issue: each gate line, its command and its result, and the `gh pr checks <pr>` output pasted as it printed. A suite-wide line ("cargo test passed") does not answer a line that names something else (a journey, a test list, a run id). Remove `wip:`. Once every required check in `gh pr checks <pr>` is green and `python3 scripts/check_gate_comment.py <issue>` exits `0`, mark the PR ready and merge it with `gh pr ready <pr> && gh pr merge <pr> --squash --delete-branch`, then close the linked issue (owner, 2026-11-24). If any check fails or a stop condition triggers, leave the PR as draft, add `needs-review` or `needs-decision`, and stop.
7. Stop and leave a comment when the work needs a file the issue does not list, a design decision, or a change to an existing test's expectation. Removing a test from `tests/s3tests/passing.txt` is a change to a test's expectation.

## Gate commands

| What | Command |
|---|---|
| Server tests, both platforms (CI runs both) | `cargo test --locked --no-fail-fast --manifest-path server/Cargo.toml` |
| State machines | `python3 scripts/test_check_state_machines.py && python3 scripts/check_state_machines.py` |
| Model | `bash verification/run_tlc.sh` (Java; CI's `Model check (TLA+)` job runs it) |
| Driven end to end | `bash scripts/verify_drive.sh` (CI's `Drive the server (aws CLI)` job) |
| Conformance map | `python3 tests/s3tests/test_conformance_map.py` |
| Benchmark tooling | `python3 benchmarks/test_run_warp_benchmark.py` and the other `benchmarks/test_*.py` |
| Conformance on the PR | the PR's `s3tests` check: its Ratchet line reads `0 of them no longer pass`, and every test the issue lists as its own is in its "Passing but not listed" list or in `passing.txt` (add them in the same PR) |
| Speed and memory | `gh workflow run bench-ratio.yml --ref <branch>` when the issue asks for it per PR; otherwise the reviewer runs it once on `main` after a batch of PRs merges (one self-hosted runner: ten runs would queue for hours) |

A change to PUT, DELETE, CopyObject, multipart completion, `write_needle` or recovery also
runs the `verify-object-versions` skill (`.claude/skills/verify-object-versions`). A
behaviour a user can see gets a journey in `scripts/verify_drive.sh` (the `verify-awanfs`
skill).

## The rule for state

No indirect state machines (owner, 2026-11-14). Anything with states and rules for moving
between them is a pure function in `server/src/fsm.rs` with a test over every
(state, event) pair and a row in [`STATE_MACHINES.md`](STATE_MACHINES.md). Handlers call the
function; they do not decide transitions.

## Benchmarks run defaults only

Every benchmark runs AwanFS, MinIO and RustFS once each, at their defaults (owner, 2026-11-20):
no extra durability modes, environment settings, forks or second builds of a competitor in a
scheduled or PR-triggered run. A comparison of modes or builds is an experiment. It runs once,
on a branch that is not merged, and its result goes in `measurements/`. The nightly runs had grown
too long.

## Traps this repository has hit

- **Bucket names are 3 to 63 characters.** Tests with a two-character bucket fail for the wrong reason (it happened twice).
- **A spawned server's stderr is never a pipe nobody reads.** It writes a line per request; a full pipe stops it (#135). Send it to a file. Kill a child before reading its stderr.
- **A measurement file starts with the command that produced it**, and its numbers are that command's output. A number nobody measured is written "not measured". Benchmark jobs write to their own paths, never over a file in `measurements/` (`scripts/check_workflow_outputs.py`).
- **Windows runners have two disks.** Python's temp directory is on C:, where a flush takes 3.9 ms; the runner's temp directory is on D:, where one takes 0.09 ms. Benchmarks put data under `runner.temp`.
- **In `scripts/verify_drive.sh`, `A && local x; x=... && B` is two lists:** a failure in A is masked. The script refuses that pattern at startup.
- **MinIO does not flush PUTs on Windows** (its fdatasync is a no-op there). Its Windows PUT rows are not durable writes. They are never a target: a Windows gate "at or above MinIO" compares with servers that sync their writes (RustFS) and notes MinIO's row (owner, 2026-11-19, #57).
- **The laptop's checkout has the self-hosted runner inside it** (`actions-runner/`, with its credentials and private key). It is in `.gitignore`; still stage files by name, never with `git add -A` or `git add .`.
- **This repository's workflows run under PowerShell on the self-hosted runner.** Use python for file work in steps without `shell: bash`.
