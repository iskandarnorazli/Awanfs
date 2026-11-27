#!/usr/bin/env bash
# Drives a fresh s3-server the way a user does (aws CLI) and keeps the evidence.
#   bash scripts/verify_drive.sh [feature ...]     features: buckets objects ranges multipart copy versioning listing tagging cors checksums metadata bucket_tagging list_buckets v1_prefix checksums_extra suspended restart quickstart conditional lock post lifecycle access (default: all)
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

# Up, refusing unsigned write requests (403), and answering the aws CLI's signed ones.
doctor() { curl -s -o /dev/null -w '%{http_code}\n' -X PUT "$URL/doctor-unsigned" | grep -q 403 && S3 list-buckets >/dev/null &&
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
  S3 get-bucket-location --bucket verify-bkt --query LocationConstraint --output text | grep -qx None &&
  ! S3 create-bucket --bucket verify-loc-bad --create-bucket-configuration LocationConstraint=eu-west-1 2>"$EVID/bl.err" && grep -q InvalidLocationConstraint "$EVID/bl.err" &&
  S3 delete-object --bucket verify-gone --key k && S3 delete-bucket --bucket verify-gone && ! S3 head-bucket --bucket verify-gone 2>/dev/null; }
j_objects() { local et; head -c 70000 /dev/urandom >"$EVID/obj.bin" &&
  et=$(S3 put-object --bucket verify-bkt --key a/obj --body "$EVID/obj.bin" --query ETag --output text) && [ "$et" = "$(md5q "$EVID/obj.bin")" ] &&
  S3 get-object --bucket verify-bkt --key a/obj "$EVID/obj.out" >/dev/null && same "$EVID/obj.bin" "$EVID/obj.out" &&
  S3 head-object --bucket verify-bkt --key a/obj | grep -q '"ContentLength": 70000' &&
  ! S3 get-object --bucket verify-bkt --key a/obj --if-none-match "$et" "$EVID/o304.out" 2>"$EVID/o304.err" && grep -qE '304|Not ?Modified' "$EVID/o304.err" &&
  ! S3 get-object --bucket verify-bkt --key a/obj --if-match '"x"' "$EVID/o412.out" 2>"$EVID/o412.err" && grep -qE '412|PreconditionFailed' "$EVID/o412.err" &&
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
  S3 get-object --bucket verify-bkt --key mp --part-number 2 "$EVID/mp-p2.out" >/dev/null && same "$EVID/p2" "$EVID/mp-p2.out" &&
  id2=$(S3 create-multipart-upload --bucket verify-bkt --key mp-gone --query UploadId --output text) &&
  S3 upload-part --bucket verify-bkt --key mp-gone --part-number 1 --upload-id "$id2" --body "$EVID/p2" >/dev/null &&
  S3 upload-part-copy --bucket verify-bkt --key mp-gone --part-number 2 --upload-id "$id2" --copy-source verify-bkt/mp >/dev/null &&
  ! S3 upload-part-copy --bucket verify-bkt --key mp-gone --part-number 3 --upload-id "$id2" --copy-source verify-bkt/missing 2>"$EVID/upc.err" &&
  grep -q NoSuchKey "$EVID/upc.err" &&
  S3 list-parts --bucket verify-bkt --key mp-gone --upload-id "$id2" --max-parts 1 --no-paginate --query NextPartNumberMarker --output text | grep -qx 1 &&
  S3 list-parts --bucket verify-bkt --key mp-gone --upload-id "$id2" --max-parts 1 --part-number-marker 1 --no-paginate --query 'Parts[].PartNumber' --output text | grep -qx 2 &&
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
  S3 get-object --bucket verify-ver --key k --version-id "$b" "$EVID/vb.out" >/dev/null && same "$EVID/v2.bin" "$EVID/vb.out" &&
  S3 put-bucket-versioning --bucket verify-ver --versioning-configuration Status=Suspended &&
  S3 delete-object --bucket verify-ver --key k >/dev/null &&
  S3 list-object-versions --bucket verify-ver --query 'length(DeleteMarkers)' --output text | grep -qx 2 &&
  S3 delete-objects --bucket verify-ver --delete "{\"Objects\":[{\"Key\":\"k\",\"VersionId\":\"$a\"}]}" --query 'length(Deleted)' --output text | grep -qx 1; }
j_listing() { local tok; S3 create-bucket --bucket verify-list && echo x >"$EVID/l.bin" &&
  for k in a/1 a/2 b/1 top; do S3 put-object --bucket verify-list --key "$k" --body "$EVID/l.bin" >/dev/null || return 1; done &&
  [ "$(S3 list-objects-v2 --bucket verify-list --delimiter / --query 'CommonPrefixes[].Prefix' --output text | words)" = "a/ b/ " ] &&
  [ "$(S3 list-objects-v2 --bucket verify-list --delimiter / --query 'Contents[].Key' --output text | words)" = "top " ] &&
  [ "$(S3 list-objects-v2 --bucket verify-list --prefix a/ --query 'Contents[].Key' --output text | words)" = "a/1 a/2 " ] &&
  tok=$(S3 list-objects-v2 --bucket verify-list --max-keys 1 --no-paginate --query NextContinuationToken --output text) &&
  [ -n "$tok" ] && [ "$tok" != None ] &&
  [ "$(S3 list-objects-v2 --bucket verify-list --max-keys 3 --no-paginate --continuation-token "$tok" --query 'Contents[].Key' --output text | words)" = "a/2 b/1 top " ] &&
  S3 list-objects-v2 --bucket verify-list --max-keys 0 --no-paginate --query KeyCount --output text | grep -qx 0 &&
  S3 put-object --bucket verify-list --key "sp key" --body "$EVID/l.bin" >/dev/null &&
  S3 list-objects-v2 --bucket verify-list --prefix "sp " --encoding-type url --query 'Contents[0].Key' --output text | grep -qx 'sp%20key' &&
  S3 list-objects --bucket verify-list --max-keys 1 --no-paginate --query 'Contents[0].Owner.ID' --output text | grep -q .; }
j_tagging() { echo "tagged" >"$EVID/t.bin" &&
  S3 put-object --bucket verify-bkt --key tag-obj --body "$EVID/t.bin" --tagging "a=1&b=2" >/dev/null &&
  S3 head-object --bucket verify-bkt --key tag-obj --query TagCount --output text | grep -qx 2 &&
  S3 get-object-tagging --bucket verify-bkt --key tag-obj --query 'length(TagSet)' --output text | grep -qx 2 &&
  S3 put-object-tagging --bucket verify-bkt --key tag-obj --tagging 'TagSet=[{Key=c,Value=3}]' &&
  S3 get-object-tagging --bucket verify-bkt --key tag-obj --query 'TagSet[0].Key' --output text | grep -qx c &&
  S3 delete-object-tagging --bucket verify-bkt --key tag-obj &&
  S3 get-object-tagging --bucket verify-bkt --key tag-obj --query 'length(TagSet)' --output text | grep -qx 0; }
j_cors() { S3 create-bucket --bucket verify-cors && echo "cors" >"$EVID/co.bin" &&
  S3 put-object --bucket verify-cors --key c --body "$EVID/co.bin" >/dev/null &&
  S3 put-bucket-cors --bucket verify-cors --cors-configuration '{"CORSRules":[{"AllowedOrigins":["https://example.com"],"AllowedMethods":["GET"]}]}' &&
  curl -si -X OPTIONS -H "Origin: https://example.com" -H "Access-Control-Request-Method: GET" "$URL/verify-cors/c" | grep -qi "Access-Control-Allow-Origin: https://example.com" &&
  curl -si -H "Origin: https://example.com" "$URL/verify-cors/c" | grep -qi "Access-Control-Allow-Origin: https://example.com" &&
  S3 delete-bucket-cors --bucket verify-cors; }
j_checksums() { local cs; echo "checksum-data" >"$EVID/ck.bin" &&
  cs=$(S3 put-object --bucket verify-bkt --key ck --body "$EVID/ck.bin" --checksum-algorithm SHA256 --query ChecksumSHA256 --output text) &&
  [ -n "$cs" ] && [ "$cs" != None ] &&
  S3 head-object --bucket verify-bkt --key ck --checksum-mode ENABLED --query ChecksumSHA256 --output text | grep -qx "$cs" &&
  S3 get-object-attributes --bucket verify-bkt --key ck --object-attributes Checksum ObjectSize --query Checksum.ChecksumSHA256 --output text | grep -qx "$cs"; }
j_metadata() { echo "meta-data" >"$EVID/md.bin" &&
  S3 put-object --bucket verify-bkt --key md --body "$EVID/md.bin" --metadata owner=alice --cache-control max-age=60 >/dev/null &&
  S3 head-object --bucket verify-bkt --key md --query 'Metadata.owner' --output text | grep -qx alice &&
  S3 head-object --bucket verify-bkt --key md --query CacheControl --output text | grep -qx max-age=60 &&
  S3 copy-object --bucket verify-bkt --key md --copy-source verify-bkt/md --metadata-directive REPLACE --metadata owner=bob >/dev/null &&
  S3 head-object --bucket verify-bkt --key md --query 'Metadata.owner' --output text | grep -qx bob; }
j_bucket_tagging() { local id e1; S3 create-bucket --bucket verify-btag &&
  ! S3 get-bucket-tagging --bucket verify-btag 2>"$EVID/bt0.err" && grep -q NoSuchTagSet "$EVID/bt0.err" &&
  S3 put-bucket-tagging --bucket verify-btag --tagging 'TagSet=[{Key=z,Value=2},{Key=a,Value=1}]' &&
  S3 get-bucket-tagging --bucket verify-btag --query 'TagSet[].Key' --output text | grep -qxE 'a[[:space:]]+z' &&
  S3 get-bucket-tagging --bucket verify-btag --query 'TagSet[0].Value' --output text | grep -qx 1 &&
  S3 get-bucket-tagging --bucket verify-btag --query 'TagSet[1].Value' --output text | grep -qx 2 &&
  S3 delete-bucket-tagging --bucket verify-btag &&
  ! S3 get-bucket-tagging --bucket verify-btag 2>"$EVID/bt1.err" && grep -q NoSuchTagSet "$EVID/bt1.err" &&
  echo "mp-tag" >"$EVID/btmp.bin" &&
  id=$(S3 create-multipart-upload --bucket verify-btag --key mp-tagged --tagging "mk1=mv1&mk2=mv2" --query UploadId --output text) &&
  e1=$(S3 upload-part --bucket verify-btag --key mp-tagged --part-number 1 --upload-id "$id" --body "$EVID/btmp.bin" --query ETag --output text) &&
  S3 complete-multipart-upload --bucket verify-btag --key mp-tagged --upload-id "$id" --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$e1}]}" >/dev/null &&
  S3 head-object --bucket verify-btag --key mp-tagged --query TagCount --output text | grep -qx 2 &&
  S3 get-object-tagging --bucket verify-btag --key mp-tagged --query 'TagSet[].Key' --output text | grep -qxE 'mk1[[:space:]]+mk2' &&
  S3 copy-object --bucket verify-btag --key cp-keep --copy-source verify-btag/mp-tagged >/dev/null &&
  S3 head-object --bucket verify-btag --key cp-keep --query TagCount --output text | grep -qx 2 &&
  S3 copy-object --bucket verify-btag --key cp-rep --copy-source verify-btag/mp-tagged --tagging-directive REPLACE --tagging "cpk=cpv" >/dev/null &&
  S3 get-object-tagging --bucket verify-btag --key cp-rep --query 'TagSet[].Key' --output text | grep -qx cpk &&
  S3 copy-object --bucket verify-btag --key cp-clr --copy-source verify-btag/mp-tagged --tagging-directive REPLACE >/dev/null &&
  S3 get-object-tagging --bucket verify-btag --key cp-clr --query 'length(TagSet)' --output text | grep -qx 0; }
j_list_buckets() { local tok1 tok2;
  S3 create-bucket --bucket verify-lb-a && S3 create-bucket --bucket verify-lb-b && S3 create-bucket --bucket verify-lb-c &&
  tok1=$(S3 list-buckets --prefix verify-lb- --max-buckets 1 --no-paginate --query ContinuationToken --output text) &&
  [ -n "$tok1" ] && [ "$tok1" != None ] &&
  S3 list-buckets --prefix verify-lb- --max-buckets 1 --no-paginate --query '[Prefix, Buckets[0].Name]' --output text | grep -qxE 'verify-lb-[[:space:]]+verify-lb-a' &&
  tok2=$(S3 list-buckets --prefix verify-lb- --max-buckets 1 --continuation-token "$tok1" --no-paginate --query ContinuationToken --output text) &&
  [ -n "$tok2" ] && [ "$tok2" != None ] &&
  S3 list-buckets --prefix verify-lb- --max-buckets 1 --continuation-token "$tok1" --no-paginate --query 'Buckets[0].Name' --output text | grep -qx verify-lb-b &&
  S3 list-buckets --prefix verify-lb- --max-buckets 1 --continuation-token "$tok2" --no-paginate --query '[Buckets[0].Name, ContinuationToken]' --output text | grep -qxE 'verify-lb-c[[:space:]]+None' &&
  [ "$(S3 list-buckets --prefix verify-lb- --page-size 1 --query 'Buckets[].Name' --output text | words)" = "verify-lb-a verify-lb-b verify-lb-c " ] &&
  ! S3 list-buckets --continuation-token zzz --no-paginate 2>"$EVID/lb-bad.err" && grep -q InvalidArgument "$EVID/lb-bad.err"; }
j_v1_prefix() { local p1 p2; S3 create-bucket --bucket verify-v1p && echo "v1" >"$EVID/v1p.bin" &&
  S3 put-object --bucket verify-v1p --key "sp dir/item 1" --body "$EVID/v1p.bin" >/dev/null &&
  p1=$(S3 list-objects --bucket verify-v1p --prefix "sp " --query '[Prefix, Contents[0].Key]' --output text) &&
  [ "$p1" = $'sp \tsp dir/item 1' ] &&
  p2=$(S3 list-objects --bucket verify-v1p --prefix "sp " --encoding-type url --query '[Prefix, Contents[0].Key]' --output text) &&
  [ "$p2" = $'sp \tsp%20dir/item%201' ] &&
  S3 list-objects-v2 --bucket verify-v1p --prefix "sp " --encoding-type url --query '[Prefix, Contents[0].Key]' --output text | grep -qxE 'sp%20[[:space:]]+sp%20dir/item%201'; }
j_checksums_extra() { local c64 cs256 id64 out e64_1 k64_1 e64_2 k64_2 full64 p64 ids es_1 ks_1 es_2 ks_2 comps ps;
  echo "checksum-extra-data" >"$EVID/cke.bin" &&
  c64=$(S3 put-object --bucket verify-bkt --key ck64 --body "$EVID/cke.bin" --checksum-algorithm CRC64NVME --query ChecksumCRC64NVME --output text) &&
  [ -n "$c64" ] && [ "$c64" != None ] &&
  S3 head-object --bucket verify-bkt --key ck64 --checksum-mode ENABLED --query ChecksumCRC64NVME --output text | grep -qxF "$c64" &&
  S3 get-object --bucket verify-bkt --key ck64 --checksum-mode ENABLED "$EVID/ck64.out" --query ChecksumCRC64NVME --output text | grep -qxF "$c64" &&
  same "$EVID/cke.bin" "$EVID/ck64.out" &&
  S3 get-object-attributes --bucket verify-bkt --key ck64 --object-attributes Checksum ObjectSize --query Checksum.ChecksumCRC64NVME --output text | grep -qxF "$c64" &&
  cs256=$(S3 put-object --bucket verify-bkt --key ck256 --body "$EVID/cke.bin" --checksum-algorithm SHA256 --query ChecksumSHA256 --output text) &&
  [ -n "$cs256" ] && [ "$cs256" != None ] &&
  S3 get-object --bucket verify-bkt --key ck256 --checksum-mode ENABLED "$EVID/ck256.out" --query ChecksumSHA256 --output text | grep -qxF "$cs256" &&
  same "$EVID/cke.bin" "$EVID/ck256.out" &&
  head -c 5300000 /dev/urandom >"$EVID/ckp1.bin" && head -c 1000 /dev/urandom >"$EVID/ckp2.bin" &&
  id64=$(S3 create-multipart-upload --bucket verify-bkt --key mp-ck64 --checksum-algorithm CRC64NVME --checksum-type FULL_OBJECT --query UploadId --output text) &&
  out=$(S3 upload-part --bucket verify-bkt --key mp-ck64 --part-number 1 --upload-id "$id64" --body "$EVID/ckp1.bin" --checksum-algorithm CRC64NVME --query '[ETag, ChecksumCRC64NVME]' --output text) &&
  e64_1=${out%%$'\t'*} && k64_1=${out#*$'\t'} && [ -n "$k64_1" ] && [ "$k64_1" != None ] &&
  out=$(S3 upload-part --bucket verify-bkt --key mp-ck64 --part-number 2 --upload-id "$id64" --body "$EVID/ckp2.bin" --checksum-algorithm CRC64NVME --query '[ETag, ChecksumCRC64NVME]' --output text) &&
  e64_2=${out%%$'\t'*} && k64_2=${out#*$'\t'} && [ -n "$k64_2" ] && [ "$k64_2" != None ] &&
  full64=$(S3 complete-multipart-upload --bucket verify-bkt --key mp-ck64 --upload-id "$id64" --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$e64_1,\"ChecksumCRC64NVME\":\"$k64_1\"},{\"PartNumber\":2,\"ETag\":$e64_2,\"ChecksumCRC64NVME\":\"$k64_2\"}]}" --query ChecksumCRC64NVME --output text) &&
  [ -n "$full64" ] && [ "$full64" != None ] &&
  p64=$(S3 get-object --bucket verify-bkt --key mp-ck64 --part-number 1 "$EVID/mpck64-p1.out" --query '[ChecksumType, ChecksumCRC64NVME]' --output text) &&
  [ "$p64" = $'FULL_OBJECT\t'"$k64_1" ] && same "$EVID/ckp1.bin" "$EVID/mpck64-p1.out" &&
  ids=$(S3 create-multipart-upload --bucket verify-bkt --key mp-ck256 --checksum-algorithm SHA256 --checksum-type COMPOSITE --query UploadId --output text) &&
  out=$(S3 upload-part --bucket verify-bkt --key mp-ck256 --part-number 1 --upload-id "$ids" --body "$EVID/ckp1.bin" --checksum-algorithm SHA256 --query '[ETag, ChecksumSHA256]' --output text) &&
  es_1=${out%%$'\t'*} && ks_1=${out#*$'\t'} && [ -n "$ks_1" ] && [ "$ks_1" != None ] &&
  out=$(S3 upload-part --bucket verify-bkt --key mp-ck256 --part-number 2 --upload-id "$ids" --body "$EVID/ckp2.bin" --checksum-algorithm SHA256 --query '[ETag, ChecksumSHA256]' --output text) &&
  es_2=${out%%$'\t'*} && ks_2=${out#*$'\t'} && [ -n "$ks_2" ] && [ "$ks_2" != None ] &&
  comps=$(S3 complete-multipart-upload --bucket verify-bkt --key mp-ck256 --upload-id "$ids" --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$es_1,\"ChecksumSHA256\":\"$ks_1\"},{\"PartNumber\":2,\"ETag\":$es_2,\"ChecksumSHA256\":\"$ks_2\"}]}" --query ChecksumSHA256 --output text) &&
  [[ "$comps" == *-2 ]] &&
  ps=$(S3 get-object --bucket verify-bkt --key mp-ck256 --part-number 1 "$EVID/mpck256-p1.out" --query '[ChecksumType, ChecksumSHA256]' --output text) &&
  [ "$ps" = $'COMPOSITE\t'"$ks_1" ] && same "$EVID/ckp1.bin" "$EVID/mpck256-p1.out" &&
  ! S3 put-object --bucket verify-bkt --key ck-bad64 --body "$EVID/cke.bin" --checksum-crc64-nvme AAAA 2>"$EVID/ckbad1.err" && grep -q BadDigest "$EVID/ckbad1.err" &&
  ! S3 put-object --bucket verify-bkt --key ck-bad256 --body "$EVID/cke.bin" --checksum-sha256 bad! 2>"$EVID/ckbad2.err" && grep -q BadDigest "$EVID/ckbad2.err"; }
j_suspended() { local v1 vid_buf vid_strm; S3 create-bucket --bucket verify-susp &&
  S3 put-bucket-versioning --bucket verify-susp --versioning-configuration Status=Enabled &&
  echo "v1-data" >"$EVID/susp-v1.bin" && echo "v2-data" >"$EVID/susp-v2.bin" &&
  head -c 4300000 /dev/urandom >"$EVID/susp-big.bin" &&
  v1=$(S3 put-object --bucket verify-susp --key k --body "$EVID/susp-v1.bin" --query VersionId --output text) &&
  [ -n "$v1" ] && [ "$v1" != None ] && [ "$v1" != null ] &&
  S3 put-bucket-versioning --bucket verify-susp --versioning-configuration Status=Suspended &&
  S3 get-bucket-versioning --bucket verify-susp --query Status --output text | grep -qx Suspended &&
  vid_buf=$(S3 put-object --bucket verify-susp --key k --body "$EVID/susp-v2.bin" --query VersionId --output text) &&
  [ "$vid_buf" = None ] &&
  vid_strm=$(S3 put-object --bucket verify-susp --key k-big --body "$EVID/susp-big.bin" --query VersionId --output text) &&
  [ "$vid_strm" = None ] &&
  S3 head-object --bucket verify-susp --key k --query VersionId --output text | grep -qx null &&
  S3 get-object --bucket verify-susp --key k "$EVID/susp-k.out" --query VersionId --output text | grep -qx null &&
  same "$EVID/susp-v2.bin" "$EVID/susp-k.out" &&
  S3 get-object --bucket verify-susp --key k --version-id "$v1" "$EVID/susp-v1.out" --query VersionId --output text | grep -qxF "$v1" &&
  same "$EVID/susp-v1.bin" "$EVID/susp-v1.out" &&
  S3 list-object-versions --bucket verify-susp --query 'length(Versions)' --output text | grep -qx 3 &&
  S3 list-object-versions --bucket verify-susp --query 'Versions[].VersionId' --output text | tr '\t' '\n' | grep -cx null | grep -qx 2; }
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
j_quickstart() { local qd qpid; qd="$EVID/qs" && mkdir -p "$qd/server/target/release" && ln -sfn "$BIN" "$qd/server/target/release/s3-server" && (
  cd "$qd" || exit 1
  qpid=""
  cleanup_qs() { [ -n "${qpid:-}" ] && kill "$qpid" 2>/dev/null || true; [ -n "${qpid:-}" ] && wait "$qpid" 2>/dev/null || true; }
  trap cleanup_qs EXIT
  AWANFS_ACCESS_KEY=s3admin AWANFS_SECRET_KEY=s3secret \
    ./server/target/release/s3-server --port 9000 --data-dir ./s3-data >"$EVID/quickstart-server.log" 2>&1 &
  qpid=$!
  for _ in $(seq 1 100); do curl -sf http://127.0.0.1:9000/_awanfs/health/ready >/dev/null && break; sleep 0.1; done
  curl -sf http://127.0.0.1:9000/_awanfs/health/ready >/dev/null || exit 1
  export AWS_ACCESS_KEY_ID=s3admin
  export AWS_SECRET_ACCESS_KEY=s3secret
  export AWS_DEFAULT_REGION=us-east-1
  echo "hello awanfs" > hello.txt &&
    aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://quickstart-bkt &&
    aws --endpoint-url http://127.0.0.1:9000 s3 cp hello.txt s3://quickstart-bkt/hello.txt &&
    aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://quickstart-bkt/ | grep -q "hello.txt" &&
    aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://quickstart-bkt/hello.txt hello.out &&
    cmp -s hello.txt hello.out &&
    aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://quickstart-bkt/hello.txt &&
    aws --endpoint-url http://127.0.0.1:9000 s3 rb s3://quickstart-bkt
); }
j_conditional() { local et1 et2 et_k2 id p1;
  S3 create-bucket --bucket verify-cond &&
  echo "cond-v1" >"$EVID/cond1.bin" && echo "cond-v2" >"$EVID/cond2.bin" &&
  ! S3 put-object --bucket verify-cond --key missing --body "$EVID/cond1.bin" --if-match "*" 2>"$EVID/cond-404.err" && grep -q NoSuchKey "$EVID/cond-404.err" &&
  et1=$(S3 put-object --bucket verify-cond --key k --body "$EVID/cond1.bin" --if-none-match "*" --query ETag --output text) &&
  [ "$et1" = "$(md5q "$EVID/cond1.bin")" ] &&
  ! S3 put-object --bucket verify-cond --key k --body "$EVID/cond2.bin" --if-none-match "*" 2>"$EVID/cond-inm.err" && grep -q PreconditionFailed "$EVID/cond-inm.err" &&
  ! S3 put-object --bucket verify-cond --key k --body "$EVID/cond2.bin" --if-match '"0000"' 2>"$EVID/cond-im.err" && grep -q PreconditionFailed "$EVID/cond-im.err" &&
  et2=$(S3 put-object --bucket verify-cond --key k --body "$EVID/cond2.bin" --if-match "$et1" --query ETag --output text) &&
  [ "$et2" = "$(md5q "$EVID/cond2.bin")" ] &&
  ! S3 delete-object --bucket verify-cond --key k --if-match '"0000"' 2>"$EVID/cond-del.err" && grep -q PreconditionFailed "$EVID/cond-del.err" &&
  S3 delete-object --bucket verify-cond --key k --if-match "$et2" &&
  ! S3 head-object --bucket verify-cond --key k 2>/dev/null &&
  et_k2=$(S3 put-object --bucket verify-cond --key k2 --body "$EVID/cond1.bin" --query ETag --output text) &&
  S3 delete-objects --bucket verify-cond --delete '{"Objects":[{"Key":"k2","ETag":"\"0000\""}]}' --query 'Errors[0].Code' --output text | grep -qx PreconditionFailed &&
  S3 delete-objects --bucket verify-cond --delete "{\"Objects\":[{\"Key\":\"k2\",\"ETag\":$et_k2}]}" --query 'Deleted[0].Key' --output text | grep -qx k2 &&
  id=$(S3 create-multipart-upload --bucket verify-cond --key mp-cond --query UploadId --output text) &&
  p1=$(S3 upload-part --bucket verify-cond --key mp-cond --part-number 1 --upload-id "$id" --body "$EVID/cond1.bin" --query ETag --output text) &&
  S3 complete-multipart-upload --bucket verify-cond --key mp-cond --upload-id "$id" --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$p1}]}" --if-none-match "*" >/dev/null &&
  S3 head-object --bucket verify-cond --key mp-cond >/dev/null; }
j_lock() { local vid;
  S3 create-bucket --bucket verify-lock --object-lock-enabled-for-bucket &&
  ! S3 put-bucket-versioning --bucket verify-lock --versioning-configuration Status=Suspended 2>"$EVID/lock-suspend.err" && grep -q InvalidBucketState "$EVID/lock-suspend.err" &&
  S3 put-object-lock-configuration --bucket verify-lock --object-lock-configuration '{"ObjectLockEnabled":"Enabled","Rule":{"DefaultRetention":{"Mode":"GOVERNANCE","Days":1}}}' &&
  S3 get-object-lock-configuration --bucket verify-lock --query 'ObjectLockConfiguration.Rule.DefaultRetention.Mode' --output text | grep -qx GOVERNANCE &&
  echo "locked-data" >"$EVID/lock.bin" &&
  vid=$(S3 put-object --bucket verify-lock --key locked --body "$EVID/lock.bin" --checksum-algorithm SHA256 --query VersionId --output text) &&
  S3 get-object-retention --bucket verify-lock --key locked --query 'Retention.Mode' --output text | grep -qx GOVERNANCE &&
  S3 put-object-legal-hold --bucket verify-lock --key locked --legal-hold Status=ON &&
  S3 head-object --bucket verify-lock --key locked --query ObjectLockLegalHoldStatus --output text | grep -qx ON &&
  ! S3 delete-object --bucket verify-lock --key locked --version-id "$vid" 2>"$EVID/lock-del.err" && grep -q AccessDenied "$EVID/lock-del.err" &&
  S3 put-object-legal-hold --bucket verify-lock --key locked --legal-hold Status=OFF &&
  S3 delete-object --bucket verify-lock --key locked --version-id "$vid" --bypass-governance-retention; }

# A browser form upload (#119), signed with SigV4 the way the AWS SDKs sign presigned POSTs.
# postform BUCKET KEY FILE EXPIRY_SECONDS [success_action_status]: prints the HTTP status, then the body.
postform() { python3 - "$URL" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" "$@" <<'PY'
import base64, datetime, hashlib, hmac, json, sys, urllib.error, urllib.request
url, ak, sk, bucket, key, path, expiry = sys.argv[1:8]
status = sys.argv[8] if len(sys.argv) > 8 else ""
now = datetime.datetime.now(datetime.timezone.utc)
date = now.strftime("%Y%m%dT%H%M%SZ")
cred = f"{ak}/{date[:8]}/us-east-1/s3/aws4_request"
fields = [("key", key), ("Content-Type", "text/plain"), ("x-amz-meta-journey", "post"),
          ("x-amz-algorithm", "AWS4-HMAC-SHA256"), ("x-amz-credential", cred), ("x-amz-date", date)]
if status:
    fields.append(("success_action_status", status))
conds = [{"bucket": bucket}] + [{k: v} for k, v in fields] + [["content-length-range", 0, 10485760]]
exp = (now + datetime.timedelta(seconds=int(expiry))).strftime("%Y-%m-%dT%H:%M:%SZ")
policy = base64.b64encode(json.dumps({"expiration": exp, "conditions": conds}).encode()).decode()
h = lambda k, m: hmac.new(k, m.encode(), hashlib.sha256).digest()
skey = h(h(h(h(("AWS4" + sk).encode(), date[:8]), "us-east-1"), "s3"), "aws4_request")
fields += [("policy", policy), ("x-amz-signature", hmac.new(skey, policy.encode(), hashlib.sha256).hexdigest())]
b = "journeyboundary"
body = b"".join(f'--{b}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode() for k, v in fields)
body += f'--{b}\r\nContent-Disposition: form-data; name="file"; filename="f"\r\n\r\n'.encode() + open(path, "rb").read() + f"\r\n--{b}--\r\n".encode()
req = urllib.request.Request(f"{url}/{bucket}", data=body, method="POST", headers={"Content-Type": f"multipart/form-data; boundary={b}"})
try:
    with urllib.request.urlopen(req) as r:
        print(r.status); print(r.read().decode())
except urllib.error.HTTPError as e:
    print(e.code); print(e.read().decode())
PY
}
j_post() { S3 create-bucket --bucket verify-post && head -c 6000000 /dev/urandom >"$EVID/post-big.bin" && echo "posted by a form" >"$EVID/post.bin" &&
  postform verify-post up/hello.txt "$EVID/post.bin" 600 | head -1 | grep -qx 204 &&
  S3 get-object --bucket verify-post --key up/hello.txt "$EVID/post.out" >"$EVID/post-head.json" && same "$EVID/post.bin" "$EVID/post.out" &&
  grep -q '"journey": "post"' "$EVID/post-head.json" && grep -q '"ContentType": "text/plain"' "$EVID/post-head.json" &&
  postform verify-post up/big.bin "$EVID/post-big.bin" 600 201 >"$EVID/post-201.txt" && head -1 "$EVID/post-201.txt" | grep -qx 201 &&
  grep -q '<Key>up/big.bin</Key>' "$EVID/post-201.txt" &&
  S3 get-object --bucket verify-post --key up/big.bin "$EVID/post-big.out" >/dev/null && same "$EVID/post-big.bin" "$EVID/post-big.out" &&
  postform verify-post up/late.txt "$EVID/post.bin" -60 >"$EVID/post-expired.txt" && head -1 "$EVID/post-expired.txt" | grep -qx 403 &&
  grep -q AccessDenied "$EVID/post-expired.txt" && ! S3 head-object --bucket verify-post --key up/late.txt 2>/dev/null; }
j_lifecycle() { local lc_data lc_port lc_url lc_pid="";
  S3 create-bucket --bucket verify-lc &&
  S3 put-bucket-lifecycle-configuration --bucket verify-lc --lifecycle-configuration '{"Rules":[{"ID":"r1","Filter":{"Prefix":"exp/"},"Status":"Enabled","Expiration":{"Days":5}}]}' &&
  S3 get-bucket-lifecycle-configuration --bucket verify-lc --query 'Rules[0].ID' --output text | grep -qx r1 &&
  echo "lc-body" >"$EVID/lc.bin" &&
  S3 put-object --bucket verify-lc --key exp/a --body "$EVID/lc.bin" >"$EVID/lc-put.json" && grep -q 'rule-id=\\"r1\\"' "$EVID/lc-put.json" &&
  S3 head-object --bucket verify-lc --key exp/a >"$EVID/lc-head.json" && grep -q 'rule-id=\\"r1\\"' "$EVID/lc-head.json" &&
  ! S3 put-bucket-lifecycle-configuration --bucket verify-lc --lifecycle-configuration '{"Rules":[{"ID":"bad","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"Days":0}}]}' 2>"$EVID/lc-bad.err" &&
  grep -q InvalidArgument "$EVID/lc-bad.err" &&
  S3 delete-bucket-lifecycle --bucket verify-lc &&
  ! S3 get-bucket-lifecycle-configuration --bucket verify-lc 2>"$EVID/lc-gone.err" && grep -q NoSuchLifecycleConfiguration "$EVID/lc-gone.err" &&
  lc_data="$(mktemp -d)" &&
  lc_port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])') &&
  lc_url="http://127.0.0.1:$lc_port" &&
  (
    trap '[ -n "${lc_pid:-}" ] && kill "$lc_pid" 2>/dev/null; wait "${lc_pid:-}" 2>/dev/null; rm -rf "$lc_data"' EXIT
    AWANFS_ACCESS_KEY="$AWS_ACCESS_KEY_ID" AWANFS_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" "$BIN" --port "$lc_port" --data-dir "$lc_data" --lifecycle-day-seconds 1 >>"$EVID/lc-server.log" 2>&1 & lc_pid=$!
    ok=0; for _ in $(seq 1 100); do if curl -sf "$lc_url/_awanfs/health/ready" >/dev/null; then ok=1; break; fi; sleep 0.1; done; [ "$ok" -eq 1 ]
    aws --endpoint-url "$lc_url" s3api create-bucket --bucket verify-lc-exp >/dev/null
    aws --endpoint-url "$lc_url" s3api put-bucket-versioning --bucket verify-lc-exp --versioning-configuration Status=Enabled >/dev/null
    aws --endpoint-url "$lc_url" s3api put-object --bucket verify-lc-exp --key k --body "$EVID/lc.bin" >/dev/null
    aws --endpoint-url "$lc_url" s3api put-object --bucket verify-lc-exp --key k --body "$EVID/lc.bin" >/dev/null
    aws --endpoint-url "$lc_url" s3api delete-object --bucket verify-lc-exp --key k >/dev/null
    aws --endpoint-url "$lc_url" s3api put-bucket-lifecycle-configuration --bucket verify-lc-exp --lifecycle-configuration '{"Rules":[{"ID":"nc-dm","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"Days":1},"NoncurrentVersionExpiration":{"NoncurrentDays":1}}]}' >/dev/null
    expired=0; for _ in $(seq 1 40); do
      aws --endpoint-url "$lc_url" s3api list-object-versions --bucket verify-lc-exp >"$EVID/lc-vers.json"
      if ! grep -q '"Key": "k"' "$EVID/lc-vers.json"; then expired=1; break; fi
      sleep 0.2
    done
    [ "$expired" -eq 1 ]
  ); }
j_access() { local code;
  S3 create-bucket --bucket verify-access &&
  echo "public-via-policy" >"$EVID/access.bin" &&
  S3 put-object --bucket verify-access --key pub.txt --body "$EVID/access.bin" >/dev/null &&
  code=$(curl -s -o /dev/null -w '%{http_code}' "$URL/verify-access/pub.txt") && [ "$code" = "403" ] &&
  S3 put-bucket-policy --bucket verify-access --policy '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::verify-access/*"}]}' &&
  S3 get-bucket-policy-status --bucket verify-access --query 'PolicyStatus.IsPublic' --output text | grep -qi true &&
  curl -sf "$URL/verify-access/pub.txt" | grep -qx "public-via-policy" &&
  S3 put-public-access-block --bucket verify-access --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true &&
  code=$(curl -s -o /dev/null -w '%{http_code}' "$URL/verify-access/pub.txt") && [ "$code" = "403" ] &&
  S3 delete-public-access-block --bucket verify-access &&
  S3 delete-bucket-policy --bucket verify-access &&
  S3 create-bucket --bucket verify-acl-own --object-ownership ObjectWriter >/dev/null &&
  S3 put-bucket-ownership-controls --bucket verify-acl-own --ownership-controls 'Rules=[{ObjectOwnership=ObjectWriter}]' &&
  S3 get-bucket-ownership-controls --bucket verify-acl-own --query 'OwnershipControls.Rules[0].ObjectOwnership' --output text | grep -qx ObjectWriter &&
  S3 put-bucket-acl --bucket verify-acl-own --acl public-read &&
  S3 get-bucket-acl --bucket verify-acl-own --query 'Grants[?Grantee.URI!=null].Permission' --output text | grep -q READ &&
  S3 put-bucket-acl --bucket verify-acl-own --acl private &&
  S3 delete-bucket-ownership-controls --bucket verify-acl-own; }

# Refuse to run a journey whose && chain is split by a `local x;` (see the note above md5q).
masked=$(sed -n '/^j_buckets()/,/^start || exit 1/p' "$0" | grep -nE '^[[:space:]]+local [^;]*;|&& *local ' || true)
[ -z "$masked" ] || { echo "a journey splits its && chain with local:"; echo "$masked"; exit 2; }

start || exit 1
check doctor doctor
features=("$@"); [ ${#features[@]} -eq 0 ] && features=(buckets objects ranges multipart copy versioning listing tagging cors checksums metadata bucket_tagging list_buckets v1_prefix checksums_extra suspended restart quickstart conditional lock post lifecycle access)
[[ " ${features[*]} " == *" buckets "* ]] || check buckets j_buckets
for f in "${features[@]}"; do check "$f" "j_$f"; done
ls -la "$DATA" >"$EVID/data-dir.txt" 2>&1
echo "evidence: $EVID  ($pass passed, $fail failed)"
[ "$fail" -eq 0 ]
