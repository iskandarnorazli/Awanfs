---
name: verify-awanfs
description: Drive a fresh AwanFS s3-server the way a user does (aws CLI against its HTTP S3 API) and keep the evidence. Use to prove a change to buckets, objects, ranges, multipart, copy, versioning, listing or restart behaviour works end to end, beyond the Rust tests.
---

# Verify AwanFS by driving it

The surface is the S3 HTTP API of `s3-server` (`server/src/main.rs`). A user reaches it with an S3 client; this skill uses the `aws` CLI.

## Launch

```bash
cargo build --locked --release --manifest-path server/Cargo.toml
bash scripts/verify_drive.sh                 # all journeys
bash scripts/verify_drive.sh multipart copy  # some journeys (buckets always runs first)
```

The script starts its own server on a free port with an empty temporary data directory (`--port`, `--data-dir`), so it never touches another instance. Ready means `GET /` answers 200.

## Doctor

The first check, `doctor`, prints the port, the PID the script started and the data directory. When anything looks off, read `server.log` in the evidence folder.

## Drive

Journeys, one per feature (see `features/`): `buckets`, `objects`, `ranges`, `multipart`, `copy`, `versioning`, `listing`, `restart`. Each one uses the aws CLI the way a user would and compares bytes with `cmp`, ETags with an MD5 computed locally, and error codes by name, not just status codes. Signatures are checked: the script gives the server `AWANFS_ACCESS_KEY` and `AWANFS_SECRET_KEY` from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (defaults `verifykey` / `verifysecret`), and `doctor` proves an unsigned request is refused.

## Evidence

`verification/evidence/<UTC timestamp>/`: one `<journey>.txt` per check with the CLI output, the inputs and outputs it compared, `server.log`, and `data-dir.txt` (the data directory listing at the end). Cleanup keeps this folder. CI uploads it as the `drive-evidence` artifact.

## Cleanup

The script kills only the PID it started and deletes only the temporary data directory it created, on exit and on failure.

## Helpers

`scripts/verify_drive.sh` is the whole harness: `start`, `stop`, `doctor`, `check <name> <command>`, the comparison helpers `same` (bytes), `md5q` (single-part ETag), `mpetag` (multipart ETag) and `words` (a CLI text list, sorted), and one `j_<feature>` function per journey. A journey depends only on `j_buckets` having run; one that needs its own state makes its own bucket. Add a journey by adding a function, a file under `features/`, a row in its README, and its name in the default list.

A journey declares its locals before its `&&` chain. `A && local x; x=... && B` is two lists, and a failure in `A` is lost whenever `B` passes: `j_listing` passed live with a wrong delimiter listing that way. The script refuses to run a journey written like that (exit 2). Before changing a journey, run `bash scripts/test_verify_drive.sh`: it needs no server, drives `j_listing` against a stubbed `S3` that answers one early listing wrong, and checks the startup refusal.
