#!/usr/bin/env bash
set -euo pipefail

# Starts awanfs for the MinIO Mint conformance suite (#278).
# Redirects stdout and stderr to a log file (never an unread pipe; AGENTS.md #135).

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${AWANFS_BIN:-$ROOT/server/target/release/awanfs}"
DATA_DIR="${AWANFS_DATA_DIR:-/tmp/awanfs-mint-data}"
LOG_FILE="${AWANFS_LOG_FILE:-/tmp/awanfs-mint.log}"
PID_FILE="${AWANFS_PID_FILE:-/tmp/awanfs-mint.pid}"
PORT="${AWANFS_PORT:-9000}"

mkdir -p "$DATA_DIR"

AWANFS_ACCESS_KEY="${AWANFS_ACCESS_KEY:-AWANFSMAINKEY0000000}" \
AWANFS_SECRET_KEY="${AWANFS_SECRET_KEY:-awanfs-main-secret-for-mint-tests-0000000}" \
AWANFS_LOG="${AWANFS_LOG:-off}" \
  nohup "$BIN" \
    --port "$PORT" \
    --data-dir "$DATA_DIR" \
    --auto-compaction off \
    --region us-east-1 \
    --default-bucket-ownership ObjectWriter \
    </dev/null >"$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

for _ in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:${PORT}/_awanfs/health/ready" >/dev/null; then
    exit 0
  fi
  sleep 0.2
done

echo "awanfs did not become ready at http://127.0.0.1:${PORT}/_awanfs/health/ready" >&2
cat "$LOG_FILE" >&2 || true
exit 1
