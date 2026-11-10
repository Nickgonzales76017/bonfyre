#!/bin/sh
# Witness: only an open offered release is approved, once, with the approval
# file the release watcher reads; stray, closed and malformed releases are refused.
set -eu
bin=$(CDPATH= cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
t=$(mktemp -d "${TMPDIR:-/tmp}/release-approval.XXXXXX"); trap 'rm -rf "$t"' EXIT INT TERM
fail() { echo "release approval: FAIL: $1" >&2; exit 1; }
op="$t/op.db"; A="$t/approvals"
sqlite3 "$op" "CREATE TABLE goals(id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT,details TEXT,priority INTEGER,state TEXT,source TEXT,last_error TEXT DEFAULT '',created_at REAL,updated_at REAL);
INSERT INTO goals VALUES('g1','release-approval:rel-ok','Approve','Ready',90,'blocked','release-approval','',1,1);
INSERT INTO goals VALUES('g2','release-approval:rel-done','Approve','Old',90,'resolved','release-approval','',1,1);
INSERT INTO goals VALUES('g3','provider-estate:checklist','x','x',40,'blocked','provider-estate-checklist','',1,1);"
r() { "$bin" "$@" --operator-db "$op"; }
r list | grep -q '"release":"rel-ok","goal":"g1"' || fail "the open release was not listed"
r list | grep -q 'rel-done' && fail "a resolved release was listed"
r approve --release rel-ok --actor user:19 --approvals "$A" | grep -q '"approved":"rel-ok"' || fail "an open release was not approved"
[ "$(cat "$A/rel-ok.approved" | head -2)" = "$(printf 'release\trel-ok\ngoal\tg1')" ] || fail "the approval file is not what the watcher reads"
sqlite3 "$op" "select last_error from goals where id='g1'" | grep -q "^Approved by user:19 through MCP" || fail "the goal does not say who approved"
if r approve --release rel-ok --actor user:19 --approvals "$A" 2>/dev/null; then fail "a release was approved twice"; fi
if r approve --release rel-done --actor user:19 --approvals "$A" 2>/dev/null; then fail "a resolved release was approved"; fi
if r approve --release checklist --actor user:19 --approvals "$A" 2>/dev/null; then fail "a non-release goal was approved"; fi
if r approve --release '../x' --actor user:19 --approvals "$A" 2>/dev/null; then fail "a path was accepted as a release"; fi
ln -s /dev/null "$A/rel-link.approved"; sqlite3 "$op" "INSERT INTO goals VALUES('g4','release-approval:rel-link','A','R',90,'blocked','release-approval','',1,1)"
if r approve --release rel-link --actor user:19 --approvals "$A" 2>/dev/null; then fail "an approval followed a symlink"; fi
echo "release approval: PASS"
