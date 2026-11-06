#!/bin/sh
# Witness for approved_activation.sh: only accepted releases are offered; an
# approval for an open goal activates once and reports back; a failed switch
# leaves the task open with the reason; a stray approval does nothing.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
t=$(mktemp -d "${TMPDIR:-/tmp}/approved-activation.XXXXXX"); trap 'rm -rf "$t"' EXIT INT TERM
fail() { echo "approved activation: FAIL: $1" >&2; exit 1; }
R="$t/.bonfyre/releases"; A="$t/.bonfyre/state/approvals"; op="$t/op.db"
mkdir -p "$R/rel-ok" "$R/rel-no" "$A"; : > "$R/rel-ok/accepted.mpk"
sqlite3 "$op" "CREATE TABLE goals(id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL)"
run() { HOME="$t" BONFYRE_OPERATOR_DB="$op" BONFYRE_BUN_BINARY="$t/bun" sh "$here/approved_activation.sh" "$@"; }
printf '#!/bin/sh\necho "Error: readback failed"\nexit 1\n' > "$t/bun"; chmod +x "$t/bun"

if run offer "$R/rel-no" "x" 2>/dev/null; then fail "a release without acceptance was offered"; fi
run offer "$R/rel-ok" "Work board actions" >/dev/null
[ "$(sqlite3 "$op" "select state||'|'||source from goals")" = "blocked|release-approval" ] || fail "offer did not create one approval task waiting on a person"
g=$(sqlite3 "$op" "select id from goals")

printf 'release\trel-ok\ngoal\t%s\n' "$g" > "$A/rel-ok.approved"; run run
[ "$(sqlite3 "$op" "select state from goals")" = blocked ] || fail "a failed switch closed the task"
sqlite3 "$op" "select last_error from goals" | grep -q "previous release kept serving. Error: readback failed" || fail "a failed switch did not say why"
[ -f "$A/failed/rel-ok.approved" ] || fail "a failed approval was not set aside"

printf '#!/bin/sh\nexit 0\n' > "$t/bun"
printf 'release\trel-ok\ngoal\t%s\n' "$g" > "$A/rel-ok.approved"; run run
[ "$(sqlite3 "$op" "select state from goals")" = resolved ] || fail "an approved release did not resolve its task"
sqlite3 "$op" "select last_error from goals" | grep -q "^Live since" || fail "the task does not say when it went live"
[ -f "$A/done/rel-ok.approved" ] || fail "a done approval was not kept as a receipt"

printf 'release\tother\ngoal\t%s\n' "$g" > "$A/other.approved"; run run
[ -f "$A/failed/other.approved" ] || fail "an approval for a different release was acted on"
printf 'release\trel-ok;rm\ngoal\tx\n' > "$A/bad.approved"; run run
[ -f "$A/failed/bad.approved" ] || fail "a malformed approval was not refused"
echo "approved activation: PASS"
