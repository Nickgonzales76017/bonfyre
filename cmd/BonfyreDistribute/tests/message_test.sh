#!/bin/sh
# What this proves: a message is the offer's own promise, and an offer that
# was not quoted, or states no promise, gets no message at all.
# What this does not prove: the quoted offer below is the real unquoted one
# with a promise typed in by this test, because no organ on this estate
# quotes an offer. It shows the organ reads those fields, not that a quote
# was ever agreed.
set -eu
bin=${1:?bonfyre-distribute required}
offer=${2:?an offer.json written by bonfyre-offer is required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin";; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-distribute-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

name=$(sed -n 's/.*"offerName":"\([^"]*\)".*/\1/p' "$offer" | head -1)
test -n "$name"

set +e
"$bin" message "$offer" "$name" dm > "$tmp/unquoted.out" 2> "$tmp/unquoted.err"
rc=$?
set -e
test "$rc" -eq 2
test ! -s "$tmp/unquoted.out"
grep -q 'not sendable' "$tmp/unquoted.err"

sed -e 's/"sendable":false/"sendable":true/' \
    -e 's/"promise":null/"promise":"One recorded meeting returned as a checked brief."/' \
    "$offer" > "$tmp/quoted.json"
"$bin" message "$tmp/quoted.json" "$name" dm > "$tmp/quoted.out"
grep -q '^Message: One recorded meeting returned as a checked brief\.$' "$tmp/quoted.out"
! grep -q 'messy recording' "$tmp/quoted.out"

sed -e 's/"sendable":false/"sendable":true/' "$offer" > "$tmp/no-promise.json"
set +e
"$bin" message "$tmp/no-promise.json" "$name" dm > "$tmp/np.out" 2> "$tmp/np.err"
rc=$?
set -e
test "$rc" -eq 2
test ! -s "$tmp/np.out"

echo "distribute message: PASS"
