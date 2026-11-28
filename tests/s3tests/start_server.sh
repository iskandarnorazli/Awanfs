#!/usr/bin/env bash
set -euo pipefail

# Starts awanfs under the conformance profile documented in tests/s3tests/PROFILE.md.
# Used by .github/workflows/s3-tests.yml so the suite's server flags live in one place.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${AWANFS_BIN:-$ROOT/server/target/release/awanfs}"
DATA_DIR="${AWANFS_DATA_DIR:-/tmp/awanfs-data}"
USERS_FILE="${AWANFS_USERS_FILE:-/tmp/awanfs-users.toml}"
LOG_FILE="${AWANFS_LOG_FILE:-/tmp/awanfs.log}"

mkdir -p "$DATA_DIR"
install -m 600 "$ROOT/tests/s3tests/users.toml" "$USERS_FILE"

AWANFS_ACCESS_KEY=AWANFSMAINKEY0000000 \
AWANFS_SECRET_KEY=awanfs-main-secret-for-s3-tests-0000000000 \
AWANFS_LOG=off \
  nohup "$BIN" \
    --port 9000 \
    --data-dir "$DATA_DIR" \
    --auto-compaction off \
    --region default \
    --config "$USERS_FILE" \
    --lifecycle-day-seconds 2 \
    </dev/null >"$LOG_FILE" 2>&1 &

for _ in $(seq 1 50); do
  if curl -sf http://127.0.0.1:9000/_awanfs/health/ready >/dev/null; then
    exit 0
  fi
  sleep 0.2
done

echo "awanfs did not become ready at http://127.0.0.1:9000/_awanfs/health/ready" >&2
exit 1
