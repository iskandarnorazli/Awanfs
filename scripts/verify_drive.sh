#!/usr/bin/env bash
# Drives a fresh s3-server the way a user does (aws CLI) and keeps the evidence.
#   bash scripts/verify_drive.sh [feature ...]     features: buckets objects ranges multipart copy versioning listing restart (default: all)
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
start() { AWANFS_ACCESS_KEY="$AWS_ACCESS_KEY_ID" AWANFS_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" "$BIN" --port "$PORT" --data-dir "$DATA" >>"$EVID/server.log" 2>&1 & PID=$!;
  for _ in $(seq 1 100); do curl -s -o /dev/null "$URL/" && return 0; sleep 0.1; done; echo "server did not start"; return 1; }
stop() { [ -n "$PID" ] && kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""; }
cleanup() { stop; rm -rf "$DATA"; }
trap cleanup EXIT
pass=0; fail=0
check() { # name, command...
  local name="$1"; shift
  if "$@" >>"$EVID/$name.txt" 2>&1; then echo "ok    $name"; pass=$((pass+1)); else echo "FAIL  $name (see $EVID/$name.txt)"; tail -n 15 "$EVID/$name.txt" | sed "s/^/      /"; fail=$((fail+1)); fi; }
same() { cmp -s "$1" "$2"; }

# Up, refusing unsigned requests (403), and answering the aws CLI's signed ones.
doctor() { curl -s -o /dev/null -w '%{http_code}\n' "$URL/" | grep -q 403 && S3 list-buckets >/dev/null &&
  echo "server up on $PORT, pid $PID, data $DATA; unsigned 403, signed 200"; }

# A journey declares its locals before its && chain: `A && local x; x=... && B` is two lists, and
# a failure in A is lost when B passes. j_listing passed live with a wrong delimiter listing that way.
md5q() { echo "\"$(md5sum "$1" | cut -d' ' -f1)\""; }  # the quoted MD5 S3 returns as a single-part ETag
words() { tr '\t' '\n' | sort | tr '\n' ' '; }          # aws --output text list -> sorted, space-separated
mpetag() { python3 -c 'import hashlib,sys;print("\"%s-%d\"" % (hashlib.md5(b"".join(hashlib.md5(open(f,"rb").read()).digest() for f in sys.argv[1:])).hexdigest(), len(sys.argv)-1))' "$@"; }

j_buckets() { S3 create-bucket --bucket verify-bkt && S3 list-buckets | grep -q '"verify-bkt"' && S3 head-bucket --bucket verify-bkt &&
  ! S3 create-bucket --bucket ab 2>"$EVID/b.err" && grep -q InvalidBucketName "$EVID/b.err" &&
  S3 create-bucket --bucket verify-gone && echo x >"$EVID/b.bin" && S3 put-object --bucket verify-gone --key k --body "$EVID/b.bin" >/dev/null &&
  ! S3 delete-bucket --bucket verify-gone 2>"$EVID/b.err" && grep -q BucketNotEmpty "$EVID/b.err" &&
  S3 delete-object --bucket verify-gone --key k && S3 delete-bucket --bucket verify-gone && ! S3 head-bucket --bucket verify-gone 2>/dev/null; }
j_objects() { local et; head -c 70000 /dev/urandom >"$EVID/obj.bin" &&
  et=$(S3 put-object --bucket verify-bkt --key a/obj --body "$EVID/obj.bin" --query ETag --output text) && [ "$et" = "$(md5q "$EVID/obj.bin")" ] &&
  S3 get-object --bucket verify-bkt --key a/obj "$EVID/obj.out" >/dev/null && same "$EVID/obj.bin" "$EVID/obj.out" &&
  S3 head-object --bucket verify-bkt --key a/obj | grep -q '"ContentLength": 70000' &&
  : >"$EVID/z.bin" && S3 put-object --bucket verify-bkt --key z --body "$EVID/z.bin" --query ETag --output text | grep -qx '"d41d8cd98f00b204e9800998ecf8427e"' &&
  S3 get-object --bucket verify-bkt --key z "$EVID/z.out" >/dev/null && [ ! -s "$EVID/z.out" ] &&
  S3 delete-object --bucket verify-bkt --key a/obj && ! S3 head-object --bucket verify-bkt --key a/obj 2>/dev/null &&
  S3 delete-object --bucket verify-bkt --key never-existed &&
  S3 put-object --bucket verify-bkt --key d1 --body "$EVID/obj.bin" >/dev/null && S3 put-object --bucket verify-bkt --key d2 --body "$EVID/obj.bin" >/dev/null &&
  S3 delete-objects --bucket verify-bkt --delete '{"Objects":[{"Key":"d1"},{"Key":"d2"}]}' --query 'length(Deleted)' --output text | grep -qx 2 &&
  ! S3 head-object --bucket verify-bkt --key d1 2>/dev/null && ! S3 head-object --bucket verify-bkt --key d2 2>/dev/null &&
  S3 delete-object --bucket verify-bkt --key z; }
j_ranges() { head -c 20000 /dev/urandom >"$EVID/r.bin" && S3 put-object --bucket verify-bkt --key r --body "$EVID/r.bin" >/dev/null &&
  S3 get-object --bucket verify-bkt --key r --range bytes=100-4195 "$EVID/r.out" >"$EVID/r.json" && grep -q '"ContentRange": "bytes 100-4195/20000"' "$EVID/r.json" &&
  dd if="$EVID/r.bin" bs=1 skip=100 count=4096 status=none >"$EVID/r.expect" && same "$EVID/r.expect" "$EVID/r.out" &&
  S3 get-object --bucket verify-bkt --key r --range bytes=-500 "$EVID/r2.out" >"$EVID/r2.json" && grep -q '"ContentRange": "bytes 19500-19999/20000"' "$EVID/r2.json" &&
  tail -c 500 "$EVID/r.bin" >"$EVID/r2.expect" && same "$EVID/r2.expect" "$EVID/r2.out" &&
  S3 get-object --bucket verify-bkt --key r --range bytes=19990- "$EVID/r3.out" >"$EVID/r3.json" && grep -q '"ContentRange": "bytes 19990-19999/20000"' "$EVID/r3.json" &&
  tail -c 10 "$EVID/r.bin" >"$EVID/r3.expect" && same "$EVID/r3.expect" "$EVID/r3.out" &&
  ! S3 get-object --bucket verify-bkt --key r --range bytes=20000-20010 "$EVID/r4.out" 2>"$EVID/r4.err" && grep -q InvalidRange "$EVID/r4.err"; }
j_multipart() { local id e1 e2 et id2; head -c 6000000 /dev/urandom >"$EVID/p1" && head -c 1000 /dev/urandom >"$EVID/p2" &&
  id=$(S3 create-multipart-upload --bucket verify-bkt --key mp --query UploadId --output text) &&
  e1=$(S3 upload-part --bucket verify-bkt --key mp --part-number 1 --upload-id "$id" --body "$EVID/p1" --query ETag --output text) &&
  e2=$(S3 upload-part --bucket verify-bkt --key mp --part-number 2 --upload-id "$id" --body "$EVID/p2" --query ETag --output text) &&
  [ "$e1" = "$(md5q "$EVID/p1")" ] && [ "$e2" = "$(md5q "$EVID/p2")" ] &&
  S3 list-parts --bucket verify-bkt --key mp --upload-id "$id" --query 'Parts[].Size' --output text | grep -qE '^6000000[[:space:]]+1000$' &&
  et=$(S3 complete-multipart-upload --bucket verify-bkt --key mp --upload-id "$id" --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$e1},{\"PartNumber\":2,\"ETag\":$e2}]}" --query ETag --output text) &&
  [ "$et" = "$(mpetag "$EVID/p1" "$EVID/p2")" ] &&
  cat "$EVID/p1" "$EVID/p2" >"$EVID/mp.expect" && S3 get-object --bucket verify-bkt --key mp "$EVID/mp.out" >/dev/null && same "$EVID/mp.expect" "$EVID/mp.out" &&
  id2=$(S3 create-multipart-upload --bucket verify-bkt --key mp-gone --query UploadId --output text) &&
  S3 upload-part --bucket verify-bkt --key mp-gone --part-number 1 --upload-id "$id2" --body "$EVID/p2" >/dev/null &&
  ! S3 upload-part-copy --bucket verify-bkt --key mp-gone --part-number 2 --upload-id "$id2" --copy-source verify-bkt/mp 2>"$EVID/upc.err" &&
  grep -q NotImplemented "$EVID/upc.err" &&
  S3 abort-multipart-upload --bucket verify-bkt --key mp-gone --upload-id "$id2" &&
  ! S3 list-parts --bucket verify-bkt --key mp-gone --upload-id "$id2" 2>"$EVID/mp.err" && grep -q NoSuchUpload "$EVID/mp.err"; }
j_copy() { local v1; echo "copy me" >"$EVID/c.bin" && S3 put-object --bucket verify-bkt --key c-src --body "$EVID/c.bin" >/dev/null &&
  S3 copy-object --bucket verify-bkt --key c-dst --copy-source verify-bkt/c-src >/dev/null &&
  S3 get-object --bucket verify-bkt --key c-dst "$EVID/c.out" >/dev/null && same "$EVID/c.bin" "$EVID/c.out" &&
  S3 create-bucket --bucket verify-cp && S3 copy-object --bucket verify-cp --key c --copy-source verify-bkt/c-src >/dev/null &&
  S3 delete-object --bucket verify-bkt --key c-src &&
  S3 get-object --bucket verify-cp --key c "$EVID/c2.out" >/dev/null && same "$EVID/c.bin" "$EVID/c2.out" &&
  ! S3 copy-object --bucket verify-bkt --key c-x --copy-source verify-bkt/c-missing 2>"$EVID/c.err" && grep -q NoSuchKey "$EVID/c.err" &&
  S3 create-bucket --bucket verify-cv && S3 put-bucket-versioning --bucket verify-cv --versioning-configuration Status=Enabled &&
  echo "old" >"$EVID/cv1.bin" && echo "new" >"$EVID/cv2.bin" &&
  v1=$(S3 put-object --bucket verify-cv --key s --body "$EVID/cv1.bin" --query VersionId --output text) &&
  S3 put-object --bucket verify-cv --key s --body "$EVID/cv2.bin" >/dev/null &&
  S3 copy-object --bucket verify-cv --key from-v1 --copy-source "verify-cv/s?versionId=$v1" | grep -q "\"CopySourceVersionId\": \"$v1\"" &&
  S3 get-object --bucket verify-cv --key from-v1 "$EVID/cv.out" >/dev/null && same "$EVID/cv1.bin" "$EVID/cv.out"; }
j_versioning() { local a b; S3 create-bucket --bucket verify-ver &&
  S3 put-bucket-versioning --bucket verify-ver --versioning-configuration Status=Enabled &&
  S3 get-bucket-versioning --bucket verify-ver --query Status --output text | grep -qx Enabled &&
  echo one >"$EVID/v1.bin" && echo two >"$EVID/v2.bin" &&
  a=$(S3 put-object --bucket verify-ver --key k --body "$EVID/v1.bin" --query VersionId --output text) &&
  b=$(S3 put-object --bucket verify-ver --key k --body "$EVID/v2.bin" --query VersionId --output text) &&
  [ -n "$a" ] && [ "$a" != None ] && [ "$a" != "$b" ] &&
  S3 get-object --bucket verify-ver --key k "$EVID/v.out" >/dev/null && same "$EVID/v2.bin" "$EVID/v.out" &&
  S3 get-object --bucket verify-ver --key k --version-id "$a" "$EVID/va.out" >/dev/null && same "$EVID/v1.bin" "$EVID/va.out" &&
  S3 list-object-versions --bucket verify-ver --query 'length(Versions)' --output text | grep -qx 2 &&
  S3 delete-object --bucket verify-ver --key k >/dev/null && ! S3 head-object --bucket verify-ver --key k 2>/dev/null &&
  S3 list-object-versions --bucket verify-ver --query 'length(DeleteMarkers)' --output text | grep -qx 1 &&
  S3 get-object --bucket verify-ver --key k --version-id "$b" "$EVID/vb.out" >/dev/null && same "$EVID/v2.bin" "$EVID/vb.out"; }
j_listing() { local tok; S3 create-bucket --bucket verify-list && echo x >"$EVID/l.bin" &&
  for k in a/1 a/2 b/1 top; do S3 put-object --bucket verify-list --key "$k" --body "$EVID/l.bin" >/dev/null || return 1; done &&
  [ "$(S3 list-objects-v2 --bucket verify-list --delimiter / --query 'CommonPrefixes[].Prefix' --output text | words)" = "a/ b/ " ] &&
  [ "$(S3 list-objects-v2 --bucket verify-list --delimiter / --query 'Contents[].Key' --output text | words)" = "top " ] &&
  [ "$(S3 list-objects-v2 --bucket verify-list --prefix a/ --query 'Contents[].Key' --output text | words)" = "a/1 a/2 " ] &&
  tok=$(S3 list-objects-v2 --bucket verify-list --max-keys 1 --no-paginate --query NextContinuationToken --output text) &&
  [ -n "$tok" ] && [ "$tok" != None ] &&
  [ "$(S3 list-objects-v2 --bucket verify-list --max-keys 3 --no-paginate --continuation-token "$tok" --query 'Contents[].Key' --output text | words)" = "a/2 b/1 top " ]; }
j_restart() { local id e1 e2 a; echo "survives" >"$EVID/s.bin" && S3 put-object --bucket verify-bkt --key s --body "$EVID/s.bin" >/dev/null &&
  S3 put-object --bucket verify-bkt --key s-gone --body "$EVID/s.bin" >/dev/null && S3 delete-object --bucket verify-bkt --key s-gone &&
  head -c 5300000 /dev/urandom >"$EVID/sp1" && head -c 700 /dev/urandom >"$EVID/sp2" &&
  id=$(S3 create-multipart-upload --bucket verify-bkt --key s-mp --query UploadId --output text) &&
  e1=$(S3 upload-part --bucket verify-bkt --key s-mp --part-number 1 --upload-id "$id" --body "$EVID/sp1" --query ETag --output text) &&
  e2=$(S3 upload-part --bucket verify-bkt --key s-mp --part-number 2 --upload-id "$id" --body "$EVID/sp2" --query ETag --output text) &&
  S3 complete-multipart-upload --bucket verify-bkt --key s-mp --upload-id "$id" --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$e1},{\"PartNumber\":2,\"ETag\":$e2}]}" >/dev/null &&
  S3 create-bucket --bucket verify-rv && S3 put-bucket-versioning --bucket verify-rv --versioning-configuration Status=Enabled &&
  a=$(S3 put-object --bucket verify-rv --key k --body "$EVID/s.bin" --query VersionId --output text) &&
  S3 put-object --bucket verify-rv --key k --body "$EVID/sp2" >/dev/null &&
  stop && start &&
  S3 get-object --bucket verify-bkt --key s "$EVID/s.out" >/dev/null && same "$EVID/s.bin" "$EVID/s.out" &&
  ! S3 head-object --bucket verify-bkt --key s-gone 2>/dev/null &&
  cat "$EVID/sp1" "$EVID/sp2" >"$EVID/smp.expect" && S3 get-object --bucket verify-bkt --key s-mp "$EVID/smp.out" >/dev/null && same "$EVID/smp.expect" "$EVID/smp.out" &&
  S3 get-bucket-versioning --bucket verify-rv --query Status --output text | grep -qx Enabled &&
  S3 get-object --bucket verify-rv --key k --version-id "$a" "$EVID/srv.out" >/dev/null && same "$EVID/s.bin" "$EVID/srv.out" &&
  S3 head-bucket --bucket verify-bkt; }

# Refuse to run a journey whose && chain is split by a `local x;` (see the note above md5q).
masked=$(sed -n '/^j_buckets()/,/^start || exit 1/p' "$0" | grep -nE '^[[:space:]]+local [^;]*;|&& *local ' || true)
[ -z "$masked" ] || { echo "a journey splits its && chain with local:"; echo "$masked"; exit 2; }

start || exit 1
check doctor doctor
features=("$@"); [ ${#features[@]} -eq 0 ] && features=(buckets objects ranges multipart copy versioning listing restart)
[[ " ${features[*]} " == *" buckets "* ]] || check buckets j_buckets
for f in "${features[@]}"; do check "$f" "j_$f"; done
ls -la "$DATA" >"$EVID/data-dir.txt" 2>&1
echo "evidence: $EVID  ($pass passed, $fail failed)"
[ "$fail" -eq 0 ]
