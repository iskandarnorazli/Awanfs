#!/usr/bin/env bash
# Self-tests for scripts/verify_drive.sh that need no server: a journey must fail when one of its
# early checks fails, and the harness must refuse a journey whose && chain a `local x;` splits.
#   bash scripts/test_verify_drive.sh        exit 0 when every case behaves
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
H="$ROOT/scripts/verify_drive.sh"
fails=0
t() { if "$@"; then echo "ok    $1"; else echo "FAIL  $1"; fails=$((fails+1)); fi; }

# j_listing passed live with a wrong delimiter listing before the locals moved (masked by `local tok;`).
listing_fails_on_a_wrong_early_listing() {
  ( EVID=$(mktemp -d)
    same() { cmp -s "$1" "$2"; }
    eval "$(sed -n '/^words()/p' "$H"; sed -n '/^j_listing()/,/; }$/p' "$H")"
    S3() { case "$*" in
        *"--delimiter / --query CommonPrefixes"*) printf 'WRONG\n' ;;
        *"--delimiter / --query Contents"*)      printf 'top\n' ;;
        *"--prefix a/"*)                          printf 'a/1\ta/2\n' ;;
        *"--max-keys 1"*)                         printf 'tok123\n' ;;
        *"--continuation-token"*)                 printf 'a/2\tb/1\ttop\n' ;;
        *) return 0 ;; esac; }
    ! j_listing ); }
t listing_fails_on_a_wrong_early_listing

# The same stub answering right must pass, or the case above proves nothing.
listing_passes_on_right_answers() {
  ( EVID=$(mktemp -d)
    eval "$(sed -n '/^words()/p' "$H"; sed -n '/^j_listing()/,/; }$/p' "$H")"
    S3() { case "$*" in
        *"--delimiter / --query CommonPrefixes"*) printf 'a/\tb/\n' ;;
        *"--delimiter / --query Contents"*)      printf 'top\n' ;;
        *"--prefix a/"*)                          printf 'a/1\ta/2\n' ;;
        *"--max-keys 1"*)                         printf 'tok123\n' ;;
        *"--continuation-token"*)                 printf 'a/2\tb/1\ttop\n' ;;
        *) return 0 ;; esac; }
    j_listing ); }
t listing_passes_on_right_answers

# The startup self-check refuses a harness with the split chain.
startup_refuses_a_split_chain() {
  local d; d=$(mktemp -d); mkdir -p "$d/scripts" "$d/server/target/release"
  sed 's/^j_listing() { local tok; S3 create-bucket/j_listing() { S3 create-bucket/; s/^  tok=\$(S3 list-objects-v2/  local tok; tok=$(S3 list-objects-v2/' "$H" >"$d/scripts/verify_drive.sh"
  grep -q '^  local tok; tok=' "$d/scripts/verify_drive.sh" || return 1   # the mutation took
  printf '#!/bin/sh\nexit 0\n' >"$d/server/target/release/s3-server"; chmod +x "$d/server/target/release/s3-server"
  local out; out=$(PATH="$d:$PATH" bash -c 'aws() { :; }; export -f aws; bash "$0"' "$d/scripts/verify_drive.sh" 2>&1); local code=$?
  [ "$code" -eq 2 ] && grep -q "splits its && chain" <<<"$out"; }
t startup_refuses_a_split_chain

[ "$fails" -eq 0 ] && echo "all self-tests passed" || { echo "$fails self-test(s) failed"; exit 1; }
