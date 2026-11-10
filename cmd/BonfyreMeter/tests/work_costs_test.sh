#!/bin/sh
# One evidenced physical attempt may be tied to a known account work/run;
# the same receipt cannot change owner, date, model cost, or scope on replay.
set -eu
BIN=${1:?bonfyre-meter binary required}
case "$BIN" in /*) ;; *) BIN="$(CDPATH= cd -- "$(dirname "$BIN")" && pwd)/$(basename "$BIN")";; esac
TMP=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-work-costs.XXXXXX")
trap 'rm -rf "$TMP"' EXIT HUP INT TERM
DB="$TMP/meter.db"
WORK="$TMP/work.db"
sqlite3 "$WORK" "CREATE TABLE account_work(id TEXT PRIMARY KEY,subject TEXT,scope TEXT);
CREATE TABLE account_work_run(work_id TEXT,run_id TEXT);
INSERT INTO account_work VALUES('account-operator-g1','user:19','self');
INSERT INTO account_work_run VALUES('account-operator-g1','r1');
INSERT INTO account_work_run VALUES('account-operator-g1','r2');"
"$BIN" admit-rate --db "$DB" --provider fixture --op read --unit query --amount 0.25 \
  --effective-at 2026-09-01T00:00:00Z --evidence receipt:rate-september >/dev/null
"$BIN" admit-rate --db "$DB" --provider fixture --op read --unit query --amount 0.90 \
  --effective-at 2026-10-01T00:00:00Z --evidence receipt:rate-october >/dev/null
record() {
 "$BIN" record --db "$DB" --key fixture --provider fixture --bytes 55 \
   --receipt receipt:source-one --realization-ref realization:one \
   --work-db "$WORK" --subject "$1" --scope self \
   --work-ref "$2" --run-ref "$3" --attempt-ref "$4" --at "$5" --op "$6" >/dev/null
}
record user:19 account-operator-g1 r1 attempt:one 2026-09-24T00:00:00Z read
record user:19 account-operator-g1 r1 attempt:one 2026-09-24T00:00:00Z read
[ "$(sqlite3 "$DB" "SELECT count(*) FROM events")" = 1 ] || { echo "replayed attempt duplicated" >&2; exit 1; }
if record user:20 account-operator-g1 r1 attempt:two 2026-09-24T00:00:00Z read 2>/dev/null; then echo "cross-account write admitted" >&2; exit 1; fi
if record user:19 account-operator-g1 other attempt:two 2026-09-24T00:00:00Z read 2>/dev/null; then echo "nonexistent run admitted" >&2; exit 1; fi
if record user:19 account-operator-g1 r2 attempt:one 2026-09-24T00:00:00Z read 2>/dev/null; then echo "attempt replay changed run" >&2; exit 1; fi
record user:19 account-operator-g1 r2 attempt:two 2026-09-24T00:00:00Z send
record user:19 account-operator-g1 r2 attempt:three 2026-10-02T00:00:00Z read
"$BIN" work-costs --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-operator-g1 >"$TMP/costs.tsv"
get() { awk -F '\t' -v key="$1" '$1==key{print $2}' "$TMP/costs.tsv"; }
[ "$(get linked_attempts)" = 3 ] || { echo "linked attempts lost" >&2; exit 1; }
[ "$(get priced_attempts)" = 2 ] || { echo "effective rates lost" >&2; exit 1; }
[ "$(get unpriced_attempts)" = 1 ] || { echo "unknown rates hidden" >&2; exit 1; }
[ "$(get cost_coverage)" = partial ] || { echo "partial coverage hidden" >&2; exit 1; }
[ "$(get meter_accrual_usd)" = 1.150000 ] || { echo "historical rate repriced incorrectly" >&2; exit 1; }
[ "$(get allocation_status)" = not_evaluated ] || { echo "allocation incorrectly inferred" >&2; exit 1; }
[ "$(get money_status)" = unknown_not_inferred ] || { echo "payment status inferred" >&2; exit 1; }
if "$BIN" work-costs --db "$DB" --work-db "$WORK" --subject user:20 --scope self --mission account-operator-g1 >/dev/null 2>&1; then echo "other account read admitted" >&2; exit 1; fi
[ "$(sqlite3 "$DB" "SELECT count(*),count(DISTINCT physical_attempt_ref),count(rate_id),sum(cost) FROM events")" = "3|3|2|1.15" ] || { echo "meter receipt state mismatch" >&2; exit 1; }
echo "meter scoped work-costs: PASS"
