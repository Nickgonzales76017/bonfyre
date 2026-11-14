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

# Simulate a computer crash that left a stale watcher lock and dead pid.
mkdir "$A/.running"; printf '99999999\n' > "$A/.running/pid"
printf 'release\trel-ok\ngoal\t%s\n' "$g" > "$A/rel-ok.approved"; run run
[ ! -e "$A/.running" ] || fail "stale watcher lock prevented release processing"
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
# A temporary network outage must not consume the approval. Retry requires no
# second operator action; non-transient verification failures still stop.
mkdir -p "$R/rel-retry"; : > "$R/rel-retry/accepted.mpk"
run offer "$R/rel-retry" "retry transport" >/dev/null
r=$(sqlite3 "$op" "select id from goals where dedupe_key='release-approval:rel-retry'")
printf 'release\trel-retry\ngoal\t%s\n' "$r" > "$A/rel-retry.approved"
printf '#!/bin/sh\nprintf "Error: transport unavailable temporarily\\n" >&2\nexit 1\n' > "$t/bun"; chmod +x "$t/bun"
run run
[ -f "$A/rel-retry.approved" ] || fail "transient failure consumed the person's approval"
[ -f "$A/retry/rel-retry.next" ] || fail "transient failure has no bounded retry"
[ "$(sqlite3 "$op" "select state from goals where id='$r'")" = blocked ] || fail "temporary outage resolved an unactivated release"
# A second immediate run is skipped, without calling the failing release command.
printf '#!/bin/sh\nprintf "unexpected immediate invocation\\n" >&2\nexit 1\n' > "$t/bun"; chmod +x "$t/bun"
run run
[ ! -f "$A/rel-retry.log" ] || grep -q 'transport unavailable' "$A/rel-retry.log" || fail "retry ignored its backoff"
printf '0\n' > "$A/retry/rel-retry.next"
printf '#!/bin/sh\nexit 0\n' > "$t/bun"; chmod +x "$t/bun"
run run
[ -f "$A/done/rel-retry.approved" ] || fail "transport recovery did not activate existing approval"
[ ! -f "$A/retry/rel-retry.next" ] || fail "successful activation retained retry schedule"
[ "$(sqlite3 "$op" "select state from goals where id='$r'")" = resolved ] || fail "transport recovery did not resolve release"
# Mixed transport and rollback failures are not safe to retry automatically.
mkdir -p "$R/rel-mixed"; : > "$R/rel-mixed/accepted.mpk"
run offer "$R/rel-mixed" "unsafe rollback" >/dev/null
mixed=$(sqlite3 "$op" "select id from goals where dedupe_key='release-approval:rel-mixed'")
printf 'release\trel-mixed\ngoal\t%s\n' "$mixed" > "$A/rel-mixed.approved"
printf '#!/bin/sh\nprintf "Error: transport unavailable\\nError: rollback failed\\n" >&2\nexit 1\n' > "$t/bun"; chmod +x "$t/bun"
run run
[ -f "$A/failed/rel-mixed.approved" ] || fail "rollback failure should stop automatic retry"
[ ! -f "$A/retry/rel-mixed.next" ] || fail "rollback failure retained retry state"
echo "approved activation: PASS"
