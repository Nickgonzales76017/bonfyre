#!/bin/sh
set -eu

ROOT="${1:-/tmp/bonfyre_watch_test}"
BIN="${BIN:-cmd/BonfyreWatch/bonfyre-watch}"

rm -rf "$ROOT"
mkdir -p "$ROOT/inbox"
printf 'hello bonfyre watch\n' > "$ROOT/inbox/sample.txt"

$BIN "$ROOT/inbox" --pipeline pipeline --root "$ROOT/state" --out "$ROOT/out" --once --dry-run >/tmp/bonfyre-watch-dryrun.json
grep -q '"event":"planned"' /tmp/bonfyre-watch-dryrun.json

$BIN "$ROOT/inbox" --pipeline pipeline --root "$ROOT/state" --out "$ROOT/out" --once >/tmp/bonfyre-watch-live.json
grep -Eq '"event":"(triggered|failed)"' /tmp/bonfyre-watch-live.json

echo "watch smoke ok"
