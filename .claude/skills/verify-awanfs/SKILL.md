---
name: verify-awanfs
description: Drive a fresh AwanFS s3-server the way a user does (aws CLI against its HTTP S3 API) and keep the evidence. Use to prove a change to buckets, objects, ranges, multipart, copy or restart behaviour works end to end, beyond the Rust tests.
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

Journeys, one per feature (see `features/`): `buckets`, `objects`, `ranges`, `multipart`, `copy`, `restart`. Each one uses the aws CLI the way a user would and compares bytes with `cmp`, not just status codes. Credentials come from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`; set them to the server's keys once signatures are checked (#35).

## Evidence

`verification/evidence/<UTC timestamp>/`: one `<journey>.txt` per check with the CLI output, the inputs and outputs it compared, `server.log`, and `data-dir.txt` (the data directory listing at the end). Cleanup keeps this folder. CI uploads it as the `drive-evidence` artifact.

## Cleanup

The script kills only the PID it started and deletes only the temporary data directory it created, on exit and on failure.

## Helpers

`scripts/verify_drive.sh` is the whole harness: `start`, `stop`, `doctor`, `check <name> <command>`, and one `j_<feature>` function per journey. Add a journey by adding a function and a file under `features/`.
