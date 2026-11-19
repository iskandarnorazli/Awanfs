#!/usr/bin/env bash
# Model-checks verification/tla/ObjectVersions.tla with TLC.
#   fixed*.cfg must pass: the design the server implements (fixed_stream*.cfg: with
#   streamed PUTs that take the key lock only to commit, #154).
#   Every other .cfg turns one fix off and must FAIL with AckedStateSurvivesRestart
#   violated (or the invariant its first line names after "Must fail:"): proof that the
#   check can catch the bug that fix prevents.
# Usage: bash verification/run_tlc.sh            (needs Java 11+ and network on first run)
set -euo pipefail
cd "$(dirname "$0")/tla"
JAR="${TLA2TOOLS_JAR:-../../.tmp/tla2tools-1.7.4.jar}"
SHA=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
if [ ! -f "$JAR" ]; then
  mkdir -p "$(dirname "$JAR")"
  curl -fsSL -o "$JAR" https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
fi
got=$(sha256sum "$JAR" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "$JAR" | cut -d' ' -f1)
[ "$got" = "$SHA" ] || { echo "tla2tools.jar checksum $got, expected $SHA (TLC 1.7.4)"; exit 2; }
meta="$(mktemp -d)"
trap 'rm -rf "$meta"' EXIT
status=0
for cfg in *.cfg; do
  name="${cfg%.cfg}"
  # A configuration named manifest_* checks Manifest.tla (#26 stage 4); the others ObjectVersions.tla.
  module=ObjectVersions; base="$name"
  if [[ "$name" == manifest_* ]]; then module=Manifest; base="${name#manifest_}"; fi
  out="$(java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -config "$cfg" -workers 1 -metadir "$meta/$name" "$module.tla" 2>&1 || true)"
  if [[ "$base" == fixed* ]]; then
    if grep -q "No error has been found" <<<"$out"; then echo "ok    $name: the rule holds"; else echo "FAIL  $name: expected no error"; echo "$out" | tail -40; status=1; fi
  else
    inv="$(sed -n '1s/.*Must fail: \([A-Za-z]*\).*/\1/p' "$cfg")"
    inv="${inv:-AckedStateSurvivesRestart}"
    if grep -q "Invariant $inv is violated" <<<"$out"; then echo "ok    $name: the check catches the bug this fix prevents ($inv)"; else echo "FAIL  $name: expected $inv to be violated"; echo "$out" | tail -40; status=1; fi
  fi
done
exit $status
