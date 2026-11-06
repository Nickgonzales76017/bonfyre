#!/bin/sh
# Model-token evidence: a reconciled agent-usage export becomes physical-attempt
# events under Meter's replay law, joins account Work only through exact
# provider session keys, and is valued only by admitted, dated per-token rates.
# Fixture rows are synthetic; the archive witness replays the real export.
set -eu
BIN=${1:?bonfyre-meter binary required}
case "$BIN" in /*) ;; *) BIN="$(CDPATH= cd -- "$(dirname "$BIN")" && pwd)/$(basename "$BIN")";; esac
TMP=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-token-evidence.XXXXXX")
trap 'rm -rf "$TMP"' EXIT HUP INT TERM
DB="$TMP/meter.db"; WORK="$TMP/work.db"
fail() { echo "meter token evidence: $1" >&2; exit 1; }
H='provider,session_id,execution_id,run_id,message_id,request_id,event_uuid,parent_uuid,agent_id,timestamp,model,effort,speed,uncached_input_tokens,cached_input_tokens,cache_write_input_tokens,input_tokens_including_cache,output_tokens,reasoning_output_tokens,total_tokens,source_file,accounting_note'
DEDUPE='deduplicated_message_id_and_request_id;no_separate_reasoning_count'
SNAP='first_cumulative_snapshot;may_include_inherited_parent_history'
cat >"$TMP/usage.csv" <<EOF
$H
claude,sess-one,ex1,,msg1,req1,u1,,,2026-08-15T10:00:00.000Z,claude-opus-5,xhigh,standard,10,1000,0,1010,100,,1110,private/transcript-one.jsonl,$DEDUPE
claude,sess-one,ex2,,msg2,req2,u2,u1,,2026-09-15T10:00:00.000Z,claude-opus-5,xhigh,standard,20,2000,0,2020,200,,2220,private/transcript-one.jsonl,$DEDUPE
claude,sess-one,ex3,,msg3,req3,u3,u2,,2026-09-16T10:00:00.000Z,claude-opus-5,xhigh,standard,5,500,50,555,10,,565,private/transcript-one.jsonl,$DEDUPE
codex,sess-two,e1,e1,,,,,,2026-09-20T00:00:00.000Z,gpt-5.4,high,,100,900,0,1000,50,10,1050,private/rollout-two.jsonl,$SNAP
codex,sess-two,e2,e2,,,,,,2026-09-20T00:01:00.000Z,gpt-5.4,high,,30,70,0,100,5,1,105,private/rollout-two.jsonl,cumulative_counter_delta
codex,sess-two,e2,e2,,,,,,2026-09-20T00:01:00.000Z,gpt-5.4,high,,30,70,0,100,5,1,105,private/rollout-two.jsonl,cumulative_counter_delta
codex,sess-three,e3,e3,,,,,,2026-09-21T00:00:00.000Z,gpt-5.4,high,,1,1,0,2,1,0,3,private/rollout-three.jsonl,cumulative_counter_delta
claude,sess-four,ex4,,msg4,req4,u4,,,2026-09-22T00:00:00.000Z,claude-opus-5,xhigh,standard,1,1,0,2,1,,3,private/transcript-four.jsonl,$DEDUPE
claude,sess-one,ex5,,,,u5,,,2026-09-23T00:00:00.000Z,<synthetic>,,,0,0,0,0,0,,0,private/transcript-one.jsonl,$DEDUPE
codex,sess-six,e6,e6,,,,,,2026-09-24T00:00:00.000Z,gpt-5.4,high,,2,2,0,4,2,0,6,private/rollout-six.jsonl,cumulative_counter_delta
EOF
get() { awk -F '\t' -v key="$1" '$1==key{print $2}' "$2"; }

"$BIN" token-import --db "$DB" --subject user:19 --source "$TMP/usage.csv" >"$TMP/import.tsv"
[ "$(get imported "$TMP/import.tsv")" = 10 ] || fail "rows not recorded"
[ "$(get nonadditive_snapshots "$TMP/import.tsv")" = 1 ] || fail "inherited snapshot not separated"
[ "$(get raw_source "$TMP/import.tsv")" = not_retained ] || fail "raw source retention not stated"
[ "$(sqlite3 "$DB" "select count(*),count(DISTINCT physical_attempt_ref),count(cost),count(reasoning_tokens) from events")" = "10|10|0|5" ] || fail "identity, cost or unknown reasoning wrong"
# Two byte-identical Codex deltas are two observations, not one replay.
[ "$(sqlite3 "$DB" "select count(*) from events where session_ref='sess-two' and counter_basis='counter_delta'")" = 2 ] || fail "identical deltas collapsed"
sqlite3 "$DB" .dump | grep -q 'private/' && fail "transcript path retained"

# Replay is idempotent.
"$BIN" token-import --db "$DB" --subject user:19 --source "$TMP/usage.csv" >"$TMP/replay.tsv"
[ "$(get imported "$TMP/replay.tsv"):$(get replayed "$TMP/replay.tsv")" = "0:10" ] || fail "replay was not idempotent"

# A repeated request/message id with different counters is refused, and the
# whole export with it: the new valid row beside it is not recorded either.
{ echo "$H"
  echo "claude,sess-one,ex1,,msg1,req1,u1,,,2026-08-15T10:00:00.000Z,claude-opus-5,xhigh,standard,10,1000,0,1010,999,,2009,x,$DEDUPE"
  echo "claude,sess-one,ex9,,msg9,req9,u9,,,2026-09-25T00:00:00.000Z,claude-opus-5,xhigh,standard,1,1,0,2,1,,3,x,$DEDUPE"; } >"$TMP/conflict.csv"
if "$BIN" token-import --db "$DB" --subject user:19 --source "$TMP/conflict.csv" >"$TMP/conflict.tsv" 2>/dev/null; then fail "changed request payload admitted"; fi
[ "$(get conflicts "$TMP/conflict.tsv")" = 1 ] || fail "conflict not counted"
[ "$(sqlite3 "$DB" "select count(*) from events")" = 10 ] || fail "partial import after conflict"
# The same receipts claimed by another account are refused.
if "$BIN" token-import --db "$DB" --subject user:20 --source "$TMP/usage.csv" >/dev/null 2>&1; then fail "cross-account replay admitted"; fi
# Counters that do not add up, unknown notes and foreign headers are refused.
{ echo "$H"; echo "codex,s,e,e,,,,,,2026-09-20T00:00:00.000Z,gpt-5.4,high,,1,1,0,3,1,0,4,x,cumulative_counter_delta"; } >"$TMP/bad.csv"
if "$BIN" token-import --db "$DB" --subject user:19 --source "$TMP/bad.csv" >/dev/null 2>&1; then fail "inconsistent counters admitted"; fi
{ echo "$H"; echo "codex,s,e,e,,,,,,2026-09-20T00:00:00.000Z,gpt-5.4,high,,1,1,0,2,1,0,3,x,guessed"; } >"$TMP/note.csv"
if "$BIN" token-import --db "$DB" --subject user:19 --source "$TMP/note.csv" >/dev/null 2>&1; then fail "unknown counter basis admitted"; fi
printf 'provider,session_id\nclaude,s\n' >"$TMP/foreign.csv"
if "$BIN" token-import --db "$DB" --subject user:19 --source "$TMP/foreign.csv" >/dev/null 2>&1; then fail "foreign export admitted"; fi
[ "$(sqlite3 "$DB" "select count(*) from events")" = 10 ] || fail "refused import wrote rows"

# Admitted, dated per-token rates. No cache-write rate is admitted.
for pair in uncached_input:15 cached_input:1.5 output:75; do
  "$BIN" admit-rate --db "$DB" --provider claude --op "claude-opus-5:${pair%%:*}" --unit megatoken --amount "${pair#*:}" \
    --effective-at 2026-09-01T00:00:00Z --evidence receipt:fixture-rate-card >/dev/null 2>&1
done

# Account Work index: task t1 has two workers (claude failed, codex passed),
# a session another task also names, and a session with no evidence.
sqlite3 "$WORK" "CREATE TABLE account_work(id TEXT PRIMARY KEY,subject TEXT,scope TEXT);
CREATE TABLE account_work_run(work_id TEXT,run_id TEXT,agent TEXT,state TEXT,provider_session_ref TEXT NOT NULL DEFAULT '');
INSERT INTO account_work VALUES('account-fabric-t1','user:19','self'),('account-fabric-t2','user:19','self'),('account-fabric-t3','user:20','self');
INSERT INTO account_work_run VALUES('account-fabric-t1','r1','claude','failed','sess-one'),('account-fabric-t1','r2','codex','completed','sess-two'),
('account-fabric-t1','r3','codex','completed','sess-three'),('account-fabric-t1','r6','claude','completed','sess-five'),
('account-fabric-t2','r4','codex','completed','sess-three'),('account-fabric-t3','r5','claude','completed','sess-four');"
"$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t1 >"$TMP/t1.tsv"
for want in runs:4 runs_without_session_key:0 sessions_exact:3 sessions_shared_refused:1 sessions_with_token_evidence:2 \
            sessions_without_token_evidence:1 failed_runs_with_token_evidence:1 records:7 additive_tokens:4105 \
            nonadditive_snapshot_tokens:1050 source_reported_tokens:5155 valued_records:1 unvalued_records:5 \
            nonadditive_records_not_valued:1 valuation_coverage:partial provisional_valuation_usd:0.018300 \
            charged_usd:unknown_no_authoritative_ledger money_status:unknown_not_inferred allocation_status:not_evaluated; do
  [ "$(get "${want%%:*}" "$TMP/t1.tsv")" = "${want#*:}" ] || fail "task t1 ${want%%:*} is $(get "${want%%:*}" "$TMP/t1.tsv"), want ${want#*:}"
done
awk -F '\t' '$1=="model"&&$3=="gpt-5.4"{print $4":"$10":"$11}' "$TMP/t1.tsv" | grep -qx '3:12:0' || fail "Codex reasoning breakdown wrong"
awk -F '\t' '$1=="model"&&$3=="claude-opus-5"{print $4":"$10":"$11}' "$TMP/t1.tsv" | grep -qx '3:unknown:3' || fail "unreported reasoning became zero"
# Preserve the complete partial-valuation report, including row order and unknowns.
cat >"$TMP/priced-expected.tsv" <<'EXPECTED'
task	account-fabric-t1
runs	4
runs_without_session_key	0
sessions_exact	3
sessions_shared_refused	1
sessions_with_token_evidence	2
sessions_without_token_evidence	1
failed_runs_with_token_evidence	1
#model	provider	model	records	sessions	uncached_input	cached_input	cache_write	output	reasoning	reasoning_unreported_records	additive_tokens	nonadditive_snapshot_tokens
model	claude	<synthetic>	1	1	0	0	0	0	unknown	1	0	0
model	claude	claude-opus-5	3	1	35	3500	50	310	unknown	3	3895	0
model	codex	gpt-5.4	3	1	160	1040	0	60	12	0	210	1050
records	7
additive_tokens	4105
nonadditive_snapshot_tokens	1050
source_reported_tokens	5155
valuation_basis	provisional_admitted_rate_not_charged
valued_records	1
unvalued_records	5
nonadditive_records_not_valued	1
valuation_coverage	partial
provisional_valuation_usd	0.018300
charged_usd	unknown_no_authoritative_ledger
money_status	unknown_not_inferred
allocation_status	not_evaluated
EXPECTED
cmp "$TMP/priced-expected.tsv" "$TMP/t1.tsv" || fail "priced report bytes changed"
# The shared session and the other account's session never reach t1.
grep -q 'sess-three\|sess-four' "$TMP/t1.tsv" && fail "session ids leaked into readback"
if "$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:20 --scope self --mission account-fabric-t1 >/dev/null 2>&1; then fail "other account read t1"; fi
if "$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t3 >/dev/null 2>&1; then fail "t3 read across accounts"; fi
# user:20's task names sess-four, but the evidence belongs to user:19's key.
"$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:20 --scope self --mission account-fabric-t3 >"$TMP/t3.tsv"
[ "$(get records "$TMP/t3.tsv"):$(get sessions_with_token_evidence "$TMP/t3.tsv")" = "0:0" ] || fail "evidence crossed account keys"

"$BIN" token-usage --db "$DB" --subject user:19 --work-db "$WORK" --scope self >"$TMP/acct.tsv"
for want in records:10 source_reported_tokens:5167 nonadditive_snapshot_tokens:1050 valued_records:2 sessions:5 \
            sessions_exact_task:2 sessions_named_not_exact_refused:2 sessions_unmapped:1 tasks_with_token_evidence:1; do
  [ "$(get "${want%%:*}" "$TMP/acct.tsv")" = "${want#*:}" ] || fail "account ${want%%:*} is $(get "${want%%:*}" "$TMP/acct.tsv"), want ${want#*:}"
done
# A separately keyed unpriced session preserves the complete report bytes.
sqlite3 "$WORK" "INSERT INTO account_work VALUES('account-fabric-t4','user:19','self'); INSERT INTO account_work_run VALUES('account-fabric-t4','r7','codex','completed','sess-six');"
"$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t4 >"$TMP/t4.tsv"
cat >"$TMP/unpriced-expected.tsv" <<'EXPECTED'
task	account-fabric-t4
runs	1
runs_without_session_key	0
sessions_exact	1
sessions_shared_refused	0
sessions_with_token_evidence	1
sessions_without_token_evidence	0
failed_runs_with_token_evidence	0
#model	provider	model	records	sessions	uncached_input	cached_input	cache_write	output	reasoning	reasoning_unreported_records	additive_tokens	nonadditive_snapshot_tokens
model	codex	gpt-5.4	1	1	2	2	0	2	0	0	6	0
records	1
additive_tokens	6
nonadditive_snapshot_tokens	0
source_reported_tokens	6
valuation_basis	provisional_admitted_rate_not_charged
valued_records	0
unvalued_records	1
nonadditive_records_not_valued	0
valuation_coverage	none
provisional_valuation_usd	unknown
charged_usd	unknown_no_authoritative_ledger
money_status	unknown_not_inferred
allocation_status	not_evaluated
EXPECTED
cmp "$TMP/unpriced-expected.tsv" "$TMP/t4.tsv" || fail "unpriced report bytes changed"
# The same session on multiple runs of one task is still counted once.
sqlite3 "$WORK" "INSERT INTO account_work_run VALUES('account-fabric-t4','r8','codex','failed','sess-six');"
"$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t4 >"$TMP/reused.tsv"
[ "$(get runs "$TMP/reused.tsv"):$(get sessions_exact "$TMP/reused.tsv"):$(get records "$TMP/reused.tsv")" = '2:1:1' ] || fail "same-task session duplicated events"
# A second task in another account, or a second provider on the same task,
# makes the session ambiguous across the complete index.
sqlite3 "$WORK" "INSERT INTO account_work_run VALUES('account-fabric-t3','r9','codex','completed','sess-six');"
"$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t4 >"$TMP/shared-account.tsv"
[ "$(get sessions_shared_refused "$TMP/shared-account.tsv"):$(get records "$TMP/shared-account.tsv")" = '1:0' ] || fail "cross-account shared session admitted"
sqlite3 "$WORK" "DELETE FROM account_work_run WHERE run_id='r9'; INSERT INTO account_work_run VALUES('account-fabric-t4','r10','claude','completed','sess-six');"
"$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t4 >"$TMP/shared-agent.tsv"
[ "$(get sessions_shared_refused "$TMP/shared-agent.tsv"):$(get records "$TMP/shared-agent.tsv")" = '1:0' ] || fail "mixed-provider session admitted"
# Evidence from a different provider under the same session is not selected.
sqlite3 "$WORK" "DELETE FROM account_work_run WHERE run_id IN ('r8','r10');"
sqlite3 "$DB" "UPDATE events SET session_ref='sess-six' WHERE provider='claude' AND session_ref='sess-one';"
"$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t4 >"$TMP/wrong-provider.tsv"
cmp "$TMP/unpriced-expected.tsv" "$TMP/wrong-provider.tsv" || fail "other provider session evidence admitted"
# Older stores without the optional session index still produce the same output.
sqlite3 "$WORK" "DELETE FROM account_work_run WHERE run_id IN ('r8','r10');"
sqlite3 "$DB" 'DROP INDEX idx_events_session;'
"$BIN" work-tokens --db "$DB" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t4 >"$TMP/no-index.tsv"
cmp "$TMP/unpriced-expected.tsv" "$TMP/no-index.tsv" || fail "unindexed store changed report"
# An index from before session keys cannot attribute anything.
sqlite3 "$TMP/old.db" "CREATE TABLE account_work(id TEXT,subject TEXT,scope TEXT);CREATE TABLE account_work_run(work_id TEXT,run_id TEXT,agent TEXT,state TEXT);
INSERT INTO account_work VALUES('account-fabric-t1','user:19','self');"
if "$BIN" work-tokens --db "$DB" --work-db "$TMP/old.db" --subject user:19 --scope self --mission account-fabric-t1 >/dev/null 2>&1; then fail "keyless index attributed tokens"; fi
# A store with no token evidence says so instead of reporting zero.
"$BIN" admit-rate --db "$TMP/empty.db" --provider p --op o --unit call --amount 1 --evidence e >/dev/null 2>&1
if "$BIN" token-usage --db "$TMP/empty.db" --subject user:19 >/dev/null 2>&1; then fail "empty store reported token usage"; fi
echo "meter token evidence: PASS"
