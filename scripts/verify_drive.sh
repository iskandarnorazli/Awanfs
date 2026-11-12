#!/usr/bin/env bash
# Drives a fresh s3-server the way a user does (aws CLI) and keeps the evidence.
#   bash scripts/verify_drive.sh [feature ...]     features: buckets objects ranges multipart copy restart (default: all)
# Needs: a built server (cargo build --release --manifest-path server/Cargo.toml), the aws CLI, curl.
# Evidence: verification/evidence/<UTC timestamp>/ (kept after cleanup). Exit 0 only if every journey passed.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${AWANFS_BIN:-$ROOT/server/target/release/s3-server}"
[ -x "$BIN" ] || [ -x "$BIN.exe" ] || { echo "no server binary at $BIN: run cargo build --release --manifest-path server/Cargo.toml"; exit 2; }
command -v aws >/dev/null || { echo "the aws CLI is not installed"; exit 2; }
EVID="$ROOT/verification/evidence/$(date -u +%Y%m%dT%H%M%SZ)"
DATA="$(mktemp -d)"
mkdir -p "$EVID"
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')
URL="http://127.0.0.1:$PORT"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-verifykey}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-verifysecret}" AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
S3() { aws --endpoint-url "$URL" s3api "$@"; }
PID=""
start() { "$BIN" --port "$PORT" --data-dir "$DATA" >>"$EVID/server.log" 2>&1 & PID=$!;
  for _ in $(seq 1 100); do curl -s -o /dev/null "$URL/" && return 0; sleep 0.1; done; echo "server did not start"; return 1; }
stop() { [ -n "$PID" ] && kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""; }
cleanup() { stop; rm -rf "$DATA"; }
trap cleanup EXIT
pass=0; fail=0
check() { # name, command...
  local name="$1"; shift
  if "$@" >>"$EVID/$name.txt" 2>&1; then echo "ok    $name"; pass=$((pass+1)); else echo "FAIL  $name (see $EVID/$name.txt)"; tail -n 15 "$EVID/$name.txt" | sed "s/^/      /"; fail=$((fail+1)); fi; }
same() { cmp -s "$1" "$2"; }

doctor() { curl -s -o /dev/null -w '%{http_code}\n' "$URL/" | grep -q 200 && echo "server up on $PORT, pid $PID, data $DATA"; }

j_buckets() { S3 create-bucket --bucket verify-bkt && S3 list-buckets | grep -q '"verify-bkt"' && S3 head-bucket --bucket verify-bkt; }
j_objects() { head -c 70000 /dev/urandom >"$EVID/obj.bin"; S3 put-object --bucket verify-bkt --key a/obj --body "$EVID/obj.bin" >/dev/null &&
  S3 get-object --bucket verify-bkt --key a/obj "$EVID/obj.out" >/dev/null && same "$EVID/obj.bin" "$EVID/obj.out" &&
  S3 head-object --bucket verify-bkt --key a/obj | grep -q '"ContentLength": 70000' &&
  S3 delete-object --bucket verify-bkt --key a/obj && ! S3 head-object --bucket verify-bkt --key a/obj 2>/dev/null; }
j_ranges() { head -c 20000 /dev/urandom >"$EVID/r.bin"; S3 put-object --bucket verify-bkt --key r --body "$EVID/r.bin" >/dev/null &&
  S3 get-object --bucket verify-bkt --key r --range bytes=100-4195 "$EVID/r.out" >/dev/null &&
  dd if="$EVID/r.bin" bs=1 skip=100 count=4096 status=none >"$EVID/r.expect" && same "$EVID/r.expect" "$EVID/r.out"; }
j_multipart() { head -c 6000000 /dev/urandom >"$EVID/p1"; head -c 1000 /dev/urandom >"$EVID/p2";
  local id; id=$(S3 create-multipart-upload --bucket verify-bkt --key mp --query UploadId --output text) &&
  local e1 e2; e1=$(S3 upload-part --bucket verify-bkt --key mp --part-number 1 --upload-id "$id" --body "$EVID/p1" --query ETag --output text) &&
  e2=$(S3 upload-part --bucket verify-bkt --key mp --part-number 2 --upload-id "$id" --body "$EVID/p2" --query ETag --output text) &&
  S3 complete-multipart-upload --bucket verify-bkt --key mp --upload-id "$id" --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$e1},{\"PartNumber\":2,\"ETag\":$e2}]}" >/dev/null &&
  cat "$EVID/p1" "$EVID/p2" >"$EVID/mp.expect" && S3 get-object --bucket verify-bkt --key mp "$EVID/mp.out" >/dev/null && same "$EVID/mp.expect" "$EVID/mp.out"; }
j_copy() { echo "copy me" >"$EVID/c.bin"; S3 put-object --bucket verify-bkt --key c-src --body "$EVID/c.bin" >/dev/null &&
  S3 copy-object --bucket verify-bkt --key c-dst --copy-source verify-bkt/c-src >/dev/null &&
  S3 get-object --bucket verify-bkt --key c-dst "$EVID/c.out" >/dev/null && same "$EVID/c.bin" "$EVID/c.out"; }
j_restart() { echo "survives" >"$EVID/s.bin"; S3 put-object --bucket verify-bkt --key s --body "$EVID/s.bin" >/dev/null && stop && start &&
  S3 get-object --bucket verify-bkt --key s "$EVID/s.out" >/dev/null && same "$EVID/s.bin" "$EVID/s.out" &&
  S3 head-bucket --bucket verify-bkt; }

start || exit 1
check doctor doctor
features=("$@"); [ ${#features[@]} -eq 0 ] && features=(buckets objects ranges multipart copy restart)
[[ " ${features[*]} " == *" buckets "* ]] || check buckets j_buckets
for f in "${features[@]}"; do check "$f" "j_$f"; done
ls -la "$DATA" >"$EVID/data-dir.txt" 2>&1
echo "evidence: $EVID  ($pass passed, $fail failed)"
[ "$fail" -eq 0 ]
