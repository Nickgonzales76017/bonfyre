#!/bin/sh
set -eu
stage="$1"
case "$stage" in /*) ;; *) stage="$(pwd)/$stage";; esac
queue="../BonfyreQueue/bonfyre-queue"
[ -x "$queue" ] || { echo "native Queue checkout binary unavailable; cannot witness review-only WorkGraph integration" >&2; exit 1; }
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
"$stage" portfolio queue-plan --contracts experience-maturities.yaff --root ../.. > "$tmp/candidates.tsv"
[ "$(wc -l < "$tmp/candidates.tsv" | tr -d ' ')" = 2 ]
grep -q '^maturity-21-verification-network[[:space:]]review-experience[[:space:]]bonfyre://experience/maturity/21[[:space:]]experience-review' "$tmp/candidates.tsv"
grep -q '^maturity-22-foreign-twin-import-clip[[:space:]]review-experience[[:space:]]bonfyre://experience/maturity/22[[:space:]]experience-review' "$tmp/candidates.tsv"
! grep -q '^maturity-17-' "$tmp/candidates.tsv"
db="$tmp/graph.db"
"$queue" enqueue-batch "$tmp/candidates.tsv" --db "$db" > "$tmp/first"
grep -q '"accepted":2' "$tmp/first"
grep -q '"failed":0' "$tmp/first"
"$queue" enqueue-batch "$tmp/candidates.tsv" --db "$db" > "$tmp/repeat"
grep -q '"accepted":2' "$tmp/repeat"
grep -q '"failed":0' "$tmp/repeat"
"$queue" mission maturity-21-verification-network --db "$db" > "$tmp/mission"
grep -q 'maturity-21-verification-network' "$tmp/mission"
grep -q 'bonfyre://experience/maturity/21' "$tmp/mission"
"$queue" claim-next bounded-review-worker --family experience-review --db "$db" > "$tmp/claim"
grep -q 'maturity-21-verification-network' "$tmp/claim"
if "$queue" claim-next wrong-pool --family unrelated-pool --db "$db" >"$tmp/mismatch" 2>&1;then
  grep -q 'not_eligible' "$tmp/mismatch" || grep -q 'no_eligible' "$tmp/mismatch" || {
    echo "An unrelated pool unexpectedly claimed the review work" >&2
    exit 1
  }
fi
echo 'experience maturity WorkGraph handoff: PASS (2 prerequisite roots, typed queue family, idempotent batch, claim scope, descendants not scheduled)'
