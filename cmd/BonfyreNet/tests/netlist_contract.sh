#!/bin/sh
# What this proves: a netlist that names an organ the circuit runtime cannot
# actually run does not compile. The components that stood in for organs with
# arithmetic of their own are gone, so nothing runs in their name.
set -eu

base=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
bin=${BIN:-$base/bonfyre-net}
fixture="$base/tests/frontier_gym_minimal.bfnet"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-net-contract.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

# The text still parses, and the check says which components are unknown.
"$bin" check "$fixture" >"$tmp/check.out" 2>&1 || true
grep -q 'components : 2' "$tmp/check.out"
grep -q 'unknown' "$tmp/check.out"

# A meter and a ledger are organs; the runtime has no component that is one.
if "$bin" compile "$fixture" --out "$tmp/frontier.bfcircuit" >"$tmp/compile.out" 2>&1; then
  echo 'compiled a circuit of components that do not exist' >&2; exit 1
fi
[ ! -s "$tmp/frontier.bfcircuit" ]
grep -q "unknown component type 'BonfyreMeter'" "$tmp/compile.out"

# A netlist of organs is run by running the organs. Speech is made and heard
# back by two real processes wired by one net.
printf 'The estate keeps a receipt for every piece of work it does.\n' > "$tmp/say.txt"
cat > "$tmp/speak.bfnet" <<NET
.WORLD speak-and-hear
.COMPONENT say BonfyreNarrate $tmp/say.txt \${out}
.COMPONENT hear BonfyreTranscribe \${say}/artifact.wav \${out}
.NET say.audio hear.input audio
NET
"$bin" exec "$tmp/speak.bfnet" --out "$tmp/run" >"$tmp/exec.out"
grep -q 'status=OK' "$tmp/exec.out"
test -s "$tmp/run/say/artifact.wav"
tr 'A-Z' 'a-z' < "$tmp/run/hear/transcript.txt" | grep -q 'receipt'
[ "$(grep -c '"exit":0' "$tmp/run/trace.jsonl")" = 2 ]

# A component that fails stops what depends on it, and the run says so.
cat > "$tmp/broken.bfnet" <<NET
.WORLD broken
.COMPONENT say BonfyreNarrate $tmp/absent.txt \${out}
.COMPONENT hear BonfyreTranscribe \${say}/artifact.wav \${out}
.NET say.audio hear.input audio
NET
if "$bin" exec "$tmp/broken.bfnet" --out "$tmp/broken" >"$tmp/broken.out" 2>&1; then echo 'a failed component was reported as a run' >&2; exit 1; fi
grep -q 'not run: an earlier component failed' "$tmp/broken.out"
[ ! -e "$tmp/broken/hear/transcript.txt" ]

# Reading another component's output needs a net from it.
cat > "$tmp/unwired.bfnet" <<NET
.WORLD unwired
.COMPONENT say BonfyreNarrate $tmp/say.txt \${out}
.COMPONENT hear BonfyreTranscribe \${say}/artifact.wav \${out}
NET
if "$bin" exec "$tmp/unwired.bfnet" --out "$tmp/unwired" >/dev/null 2>&1; then echo 'an unwired reference was resolved' >&2; exit 1; fi

echo "netlist contract: PASS"
