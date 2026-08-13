#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-race}/race-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"
export BONFYRE_STATE_DIR="$state"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
"$fabric" fabric init >/dev/null
"$fabric" mission create race-proof >/dev/null
"$fabric" work add race-proof task core.identity >/dev/null
("$fabric" work claim-next racer-a --lease-ms 30000 >"$state/a.out" 2>"$state/a.err"; echo $? >"$state/a.rc") &
pid_a=$!
("$fabric" work claim-next racer-b --lease-ms 30000 >"$state/b.out" 2>"$state/b.err"; echo $? >"$state/b.rc") &
pid_b=$!
wait "$pid_a" || true
wait "$pid_b" || true
successes=0
for side in a b; do
    if grep -q '^result=ok$' "$state/$side.out"; then successes=$((successes + 1)); fi
done
test "$successes" -eq 1
winner=a
grep -q '^result=ok$' "$state/b.out" && winner=b
token=$(sed -n 's/^claim_token=//p' "$state/$winner.out")
worker="racer-$winner"
"$fabric" work complete race-proof task "$worker" "$token" | grep -q '^status=complete$'
