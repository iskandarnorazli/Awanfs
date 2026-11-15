# AwanFS: rules for agents

How to work (goal first, evidence, tests first, the owner decides, handoffs) is the
`iskandar-mode` skill. This file is what is specific to this repository; where the two
disagree, this file wins. `CLAUDE.md` points here.

AwanFS is an S3-compatible object store in Rust (`server/`). The plan of record is the
GitHub milestones and [`ROADMAP_AND_DEPENDENCIES.md`](ROADMAP_AND_DEPENDENCIES.md);
decisions are numbered in [`ARCHITECTURE.md`](ARCHITECTURE.md); stored bytes are in
[`FORMAT.md`](FORMAT.md); the conformance plan is
[`CONFORMANCE_DESIGN.md`](CONFORMANCE_DESIGN.md). `AUTONOMOUS_EXECUTION_RUNBOOK.md` and
`HYBRID_POC_SPECIFICATION.md` describe the finished PoC and are not instructions.

## Which issues

- Work only on issues labelled `ready:antigravity` with no `wip:` label. An empty queue means stop and say so.
- An issue blocked by another (GitHub's "blocked by" link, or a first line "Blocked by #N") waits until #N is closed.
- Never start an issue labelled `outline` or `needs-decision`.
- These need Claude or the owner, even when an issue seems to ask for them. Stop with `needs-decision` and say which:
  - stored bytes: the needle header, the journal, the bucket manifest's record kinds, the volume format (`FORMAT.md`);
  - recovery and replay, the key lock, the commit path, group commit, compaction;
  - authentication, authorization, SigV4, IAM, encryption;
  - the meaning of a TLA+ model in `verification/tla` (adding the configuration an issue names is fine);
  - the rules of an existing machine in `server/src/fsm.rs` (wiring a handler to it is fine);
  - any decision in `ARCHITECTURE.md`. A test never reverses one.

## How an issue is worked

1. Add `wip:antigravity` (or `wip:claude`). Branch from `main`: `issue/<n>-<slug>`.
2. Read the Goal, the Gate and the Stop list. Write the tests first and show them failing.
3. Commit as `Iskandar Norazli <111472977+iskandarnorazli@users.noreply.github.com>`, with no AI or Co-Authored-By lines. The message states the tests added, the commands run with their results, and the negative control: the fix turned off, the new test failing.
4. Push the branch and open a **draft** PR. Never push `main`, never force-push, never merge.
5. Comment on the issue: each gate line, its command and its result. Remove `wip:`, add `needs-review`. Never close an issue; the owner or a reviewer who did not build it does.
6. Stop and leave a comment when the work needs a file the issue does not list, a design decision, or a change to an existing test's expectation.

## Gate commands

| What | Command |
|---|---|
| Server tests, both platforms (CI runs both) | `cargo test --locked --no-fail-fast --manifest-path server/Cargo.toml` |
| State machines | `python3 scripts/test_check_state_machines.py && python3 scripts/check_state_machines.py` |
| Model | `bash verification/run_tlc.sh` (Java; CI's `Model check (TLA+)` job runs it) |
| Driven end to end | `bash scripts/verify_drive.sh` (CI's `Drive the server (aws CLI)` job) |
| Conformance map | `python3 tests/s3tests/test_conformance_map.py` |
| Benchmark tooling | `python3 benchmarks/test_run_warp_benchmark.py` and the other `benchmarks/test_*.py` |
| Speed and memory | `gh workflow run bench-ratio.yml --ref <branch>`: six rows in tolerance, budget gate under its ceiling |

A change to PUT, DELETE, CopyObject, multipart completion, `write_needle` or recovery also
runs the `verify-object-versions` skill (`.claude/skills/verify-object-versions`). A
behaviour a user can see gets a journey in `scripts/verify_drive.sh` (the `verify-awanfs`
skill).

## The rule for state

No indirect state machines (owner, 2026-11-14). Anything with states and rules for moving
between them is a pure function in `server/src/fsm.rs` with a test over every
(state, event) pair and a row in [`STATE_MACHINES.md`](STATE_MACHINES.md). Handlers call the
function; they do not decide transitions.

## Traps this repository has hit

- **Bucket names are 3 to 63 characters.** Tests with a two-character bucket fail for the wrong reason (it happened twice).
- **A spawned server's stderr is never a pipe nobody reads.** It writes a line per request; a full pipe stops it (#135). Send it to a file. Kill a child before reading its stderr.
- **A measurement file starts with the command that produced it**, and its numbers are that command's output. A number nobody measured is written "not measured". Benchmark jobs write to their own paths, never over a file in `measurements/` (`scripts/check_workflow_outputs.py`).
- **Windows runners have two disks.** Python's temp directory is on C:, where a flush takes 3.9 ms; the runner's temp directory is on D:, where one takes 0.09 ms. Benchmarks put data under `runner.temp`.
- **In `scripts/verify_drive.sh`, `A && local x; x=... && B` is two lists:** a failure in A is masked. The script refuses that pattern at startup.
- **MinIO does not flush PUTs on Windows** (its fdatasync is a no-op there). Its Windows PUT rows are not durable writes.
- **This repository's workflows run under PowerShell on the self-hosted runner.** Use python for file work in steps without `shell: bash`.
