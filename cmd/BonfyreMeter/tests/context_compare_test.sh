#!/bin/sh
# Context handoff comparison: two runs of one Work item, baseline context and
# a verified compact handoff, compared in the provider's own counter classes.
# Fixture rows and the handoff receipt are synthetic; the receipt has the shape
# `bonfyre-kvcache context-verify` prints (its own contract test produces it).
set -eu
BIN=${1:?bonfyre-meter binary required}
case "$BIN" in /*) ;; *) BIN="$(CDPATH= cd -- "$(dirname "$BIN")" && pwd)/$(basename "$BIN")";; esac
QUEUE=${2:-}   # bonfyre-queue: the governed WorkGraph section runs only when given
TMP=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-context-compare.XXXXXX")
trap 'rm -rf "$TMP"' EXIT HUP INT TERM
DB="$TMP/meter.db"; WORK="$TMP/work.db"
fail() { echo "meter context compare: $1" >&2; exit 1; }
get() { awk -F '\t' -v key="$1" '$1==key{print $2}' "$2"; }
H='provider,session_id,execution_id,run_id,message_id,request_id,event_uuid,parent_uuid,agent_id,timestamp,model,effort,speed,uncached_input_tokens,cached_input_tokens,cache_write_input_tokens,input_tokens_including_cache,output_tokens,reasoning_output_tokens,total_tokens,source_file,accounting_note'
D='deduplicated_message_id_and_request_id;no_separate_reasoning_count'
cat >"$TMP/usage.csv" <<EOF
$H
claude,sess-base-0001,x1,,m1,q1,u1,,,2026-10-01T10:00:00.000Z,claude-opus-5,xhigh,standard,1000,50000,2000,53000,500,,53500,private/a.jsonl,$D
claude,sess-base-0001,x2,,m2,q2,u2,u1,,2026-10-01T10:05:00.000Z,claude-opus-5,xhigh,standard,800,60000,0,60800,300,,61100,private/a.jsonl,$D
claude,sess-compact-01,x3,,m3,q3,u3,,,2026-10-02T10:00:00.000Z,claude-opus-5,xhigh,standard,200,40000,500,40700,400,,41100,private/b.jsonl,$D
claude,sess-sonnet-001,x4,,m4,q4,u4,,,2026-10-02T11:00:00.000Z,claude-sonnet-5,high,standard,10,10,0,20,10,,30,private/c.jsonl,$D
codex,sess-codex-0001,e1,e1,,,,,,2026-10-03T00:00:00.000Z,gpt-5.4,high,,100,900,0,1000,50,10,1050,private/d.jsonl,cumulative_counter_delta
codex,sess-codex-0002,e2,e2,,,,,,2026-10-03T01:00:00.000Z,gpt-5.4,high,,30,70,0,100,5,1,105,private/e.jsonl,first_cumulative_snapshot;may_include_inherited_parent_history
claude,sess-shared-001,x5,,m5,q5,u5,,,2026-10-02T12:00:00.000Z,claude-opus-5,xhigh,standard,1,1,0,2,1,,3,private/f.jsonl,$D
EOF
"$BIN" token-import --db "$DB" --subject user:19 --source "$TMP/usage.csv" >/dev/null
cp "$DB" "$TMP/norate.db"
# Admitted, dated rates; one admitted after the runs, which must not apply.
for pair in uncached_input:15 cached_input:1.5 cache_write:18.75 output:75; do
  "$BIN" admit-rate --db "$DB" --provider claude --op "claude-opus-5:${pair%%:*}" --unit megatoken --amount "${pair#*:}" \
    --effective-at 2026-09-01T00:00:00Z --evidence receipt:fixture-rate-card >/dev/null 2>&1
done
"$BIN" admit-rate --db "$DB" --provider claude --op claude-opus-5:uncached_input --unit megatoken --amount 99 \
  --effective-at 2026-12-01T00:00:00Z --evidence receipt:fixture-later-card >/dev/null 2>&1
for pair in uncached_input:15 cached_input:1.5 output:75; do   # no cache-write rate
  "$BIN" admit-rate --db "$TMP/norate.db" --provider claude --op "claude-opus-5:${pair%%:*}" --unit megatoken --amount "${pair#*:}" \
    --effective-at 2026-09-01T00:00:00Z --evidence receipt:fixture-rate-card >/dev/null 2>&1
done

sqlite3 "$WORK" "CREATE TABLE account_work(id TEXT PRIMARY KEY,subject TEXT,scope TEXT);
CREATE TABLE account_work_run(work_id TEXT,run_id TEXT,agent TEXT,state TEXT,provider_session_ref TEXT NOT NULL DEFAULT '');
INSERT INTO account_work VALUES('account-fabric-t1','user:19','self'),('account-fabric-t2','user:19','self');
INSERT INTO account_work_run VALUES
 ('account-fabric-t1','r1','claude','completed','sess-base-0001'),('account-fabric-t1','r2','claude','completed','sess-compact-01'),
 ('account-fabric-t1','r3','codex','completed','sess-codex-0001'),('account-fabric-t1','r4','claude','completed','sess-shared-001'),
 ('account-fabric-t2','r9','claude','completed','sess-shared-001'),('account-fabric-t1','r5','claude','failed',''),
 ('account-fabric-t1','r6','claude','completed','sess-no-usage-1'),('account-fabric-t1','r7','codex','completed','sess-codex-0002'),
 ('account-fabric-t1','r8','claude','failed','sess-sonnet-001'),('account-fabric-t2','r10','claude','completed','sess-other-001');"

handoff() { cat >"$TMP/h.json" <<EOF
{"schema_version":"bonfyre.context.handoff.v1","verdict":"$1","receipt_sha256":"$(printf a | shasum -a 256 | cut -c1-64)",
 "context_sha256":"$(printf b | shasum -a 256 | cut -c1-64)","task":"$2","revision":"rev-abc123","bodies_checked":4,
 "bodies_in_context":2,"witnesses_preserved":1,"source_tokens":9000,"context_tokens":1200,"token_budget":1500,
 "token_basis":"tokenizer_count_not_provider_usage"}
EOF
}
cmp() { "$BIN" context-compare --db "$1" --work-db "$WORK" --subject user:19 --scope self --mission account-fabric-t1 \
          --baseline-run "$2" --compact-run "$3" --handoff "$TMP/h.json"; }
handoff PASS account-fabric-t1

cmp "$DB" r1 r2 >"$TMP/c.tsv" || fail "comparable pair refused"
for want in task_equivalence:same_work_item handoff_revision:rev-abc123 handoff_binding:declared_by_caller_not_launcher_observed \
            handoff_context_tokens:1200 handoff_token_basis:tokenizer_count_not_provider_usage comparable:yes \
            delta_uncached_input:-1600 delta_cache_write:-1500 delta_expensive_new_input:-3100 delta_cached_input:-70000 \
            delta_output:-400 delta_reasoning:unknown_not_reported delta_provisional_valuation_usd:-0.187125 \
            outcome:same_run_state quality_acceptance:not_measured charged_usd:unknown_no_authoritative_ledger \
            money_status:unknown_not_inferred allocation_status:not_evaluated; do
  [ "$(get "${want%%:*}" "$TMP/c.tsv")" = "${want#*:}" ] || fail "${want%%:*} is $(get "${want%%:*}" "$TMP/c.tsv"), want ${want#*:}"
done
awk -F '\t' '$1=="side"&&$2=="baseline"{print $5":"$6":"$7":"$8":"$9":"$10":"$11":"$12":"$15":"$18}' "$TMP/c.tsv" | \
  grep -qx 'claude:claude-opus-5:2:1800:110000:2000:800:unknown:3800:0.289500' || fail "baseline side wrong: $(grep baseline "$TMP/c.tsv")"
awk -F '\t' '$1=="side"&&$2=="compact"{print $7":"$15":"$18}' "$TMP/c.tsv" | grep -qx '1:700:0.102375' || fail "compact side wrong"
grep -q 'sess-' "$TMP/c.tsv" && fail "provider session ids leaked into readback"

# Without an admitted cache-write rate the runs stay unvalued; counters still compare.
cmp "$TMP/norate.db" r1 r2 >"$TMP/n.tsv"
[ "$(get delta_provisional_valuation_usd "$TMP/n.tsv")" = unknown_no_admitted_rate_for_every_record ] || fail "missing rate became a value"
[ "$(get delta_expensive_new_input "$TMP/n.tsv")" = -3100 ] || fail "counters lost without a rate"
awk -F '\t' '$1=="side"&&$2=="baseline"{print $18}' "$TMP/n.tsv" | grep -qx unknown || fail "partly valued side printed a sum"

# Different provider, different model, inherited snapshot: shown, never subtracted.
cmp "$DB" r1 r3 >"$TMP/p.tsv"; [ "$(get comparable "$TMP/p.tsv")" = no_provider_differs ] || fail "cross-provider compared"
grep -q '^delta_' "$TMP/p.tsv" && fail "cross-provider delta printed"
cmp "$DB" r1 r8 >"$TMP/m.tsv"; [ "$(get comparable "$TMP/m.tsv")" = no_model_differs ] || fail "cross-model compared"
[ "$(get outcome "$TMP/m.tsv")" = run_state_differs ] || fail "failed compact run not reported"
cmp "$DB" r3 r7 >"$TMP/s.tsv"; [ "$(get comparable "$TMP/s.tsv")" = no_inherited_snapshot_present ] || fail "snapshot compared"
# The snapshot is shown apart, never inside the side's additive counters.
awk -F '\t' '$1=="side"&&$2=="compact"{print $7":"$8":"$13":"$14":"$15}' "$TMP/s.tsv" | grep -qx '1:0:1:105:0' || fail "snapshot entered side counters"
grep -q '^delta_' "$TMP/s.tsv" && fail "snapshot delta printed"

# Refusals: each leaves no output and a non-zero exit.
refuse() { why=$1; shift; if "$@" >"$TMP/x.tsv" 2>"$TMP/x.err"; then fail "admitted: $why"; fi
           [ ! -s "$TMP/x.tsv" ] || fail "partial output on refusal: $why"; }
refuse "same run" cmp "$DB" r1 r1
refuse "shared session" cmp "$DB" r1 r4
refuse "keyless run" cmp "$DB" r1 r5
refuse "run without usage" cmp "$DB" r1 r6
refuse "run of another task" cmp "$DB" r1 r10
refuse "unknown run" cmp "$DB" r1 nope
handoff REFUSED account-fabric-t1; refuse "unverified handoff" cmp "$DB" r1 r2
handoff PASS account-fabric-t2;    refuse "handoff for another task" cmp "$DB" r1 r2
printf '{"schema_version":"bonfyre.context.compile.v2","verdict":"PASS"}' >"$TMP/h.json"; refuse "compile receipt is not a verified handoff" cmp "$DB" r1 r2
handoff PASS account-fabric-t1
if "$BIN" context-compare --db "$DB" --work-db "$WORK" --subject user:20 --scope self --mission account-fabric-t1 \
     --baseline-run r1 --compact-run r2 --handoff "$TMP/h.json" >/dev/null 2>&1; then fail "another account compared t1"; fi
sqlite3 "$TMP/old.db" "CREATE TABLE account_work(id TEXT,subject TEXT,scope TEXT);CREATE TABLE account_work_run(work_id TEXT,run_id TEXT,agent TEXT,state TEXT);"
if "$BIN" context-compare --db "$DB" --work-db "$TMP/old.db" --subject user:19 --scope self --mission account-fabric-t1 \
     --baseline-run r1 --compact-run r2 --handoff "$TMP/h.json" >/dev/null 2>&1; then fail "keyless index compared"; fi
# The read writes nothing.
[ "$(sqlite3 "$DB" "select count(*) from events")" = 7 ] || fail "compare wrote events"
# Governed WorkGraph execution: the same two sessions run as two attempts of one
# native WorkGraph node, each attested under its live claim with `run-attest`.
# The account Work index projects the attempts as runs, Meter joins their usage
# by the attested session, and the compact attempt's recorded handoff decides
# the binding instead of the caller's word. No trace store is created.
if [ -n "$QUEUE" ]; then
  case "$QUEUE" in /*) ;; *) QUEUE="$(CDPATH= cd -- "$(dirname "$QUEUE")" && pwd)/$(basename "$QUEUE")";; esac
  WG="$TMP/workgraph.db"; GW="$TMP/governed-work.db"
  tok() { sed -E 's/.*"claim_token":"([0-9a-f]{64})".*/\1/'; }
  printf 't1\tdeveloper.task\tfile:///task\tqueue\t100\t2\t0\n' >"$TMP/batch.tsv"
  "$QUEUE" enqueue-batch "$TMP/batch.tsv" --db "$WG" >/dev/null
  T1=$("$QUEUE" claim t1 job dev --db "$WG" | tok)
  "$QUEUE" run-attest t1 job dev "$T1" --agent claude --session sess-base-0001 --model claude-opus-5 --db "$WG" >/dev/null || fail "baseline attempt not attested"
  "$QUEUE" fail t1 job dev "$T1" --class transient --message "baseline stopped" --db "$WG" >/dev/null
  T2=; for i in 1 2 3 4 5 6; do T2=$("$QUEUE" claim t1 job dev --db "$WG" | tok) && [ ${#T2} = 64 ] && break; sleep 1; done
  [ ${#T2} = 64 ] || fail "retry attempt never became claimable"
  RECEIPT=$(printf a | shasum -a 256 | cut -c1-64)
  if "$QUEUE" run-attest t1 job dev "$T2" --agent claude --session sess-base-0001 --db "$WG" >/dev/null; then fail "one session attested for two attempts"; fi
  "$QUEUE" run-attest t1 job dev "$T2" --agent claude --session sess-compact-01 --model claude-opus-5 --handoff "sha256:$RECEIPT" --db "$WG" >/dev/null || fail "compact attempt not attested"
  "$QUEUE" ack t1 job dev "$T2" --output-uri file:///result --db "$WG" >/dev/null
  "$QUEUE" account-work index --db "$GW" --subject user:19 --scope self --owner user:19 --source "workgraph=$WG" >/dev/null || fail "governed index failed"
  [ "$(sqlite3 "$GW" "select group_concat(run_id||':'||state||':'||agent||':'||provider_session_ref||':'||(handoff_ref<>''),'|') from (select * from account_work_run order by run_id)")" = \
    "job#1:failed:claude:sess-base-0001:0|job#2:completed:claude:sess-compact-01:1" ] || fail "attested attempts were not projected as runs"
  [ "$(sqlite3 "$TMP/workgraph.db" "select count(*) from sqlite_master where name like '%trace%'")" = 0 ] || fail "a trace table appeared"
  gcmp() { "$BIN" context-compare --db "$DB" --work-db "$GW" --subject user:19 --scope self --mission account-workgraph-t1 \
             --baseline-run "$1" --compact-run "$2" --handoff "$TMP/h.json"; }
  handoff PASS account-workgraph-t1
  gcmp 'job#1' 'job#2' >"$TMP/g.tsv" || fail "governed pair refused"
  for want in handoff_binding:recorded_by_claimed_workgraph_attempt_not_consumption_observed comparable:yes \
              delta_expensive_new_input:-3100 delta_provisional_valuation_usd:-0.187125 outcome:run_state_differs \
              quality_acceptance:not_measured; do
    [ "$(get "${want%%:*}" "$TMP/g.tsv")" = "${want#*:}" ] || fail "governed ${want%%:*} is $(get "${want%%:*}" "$TMP/g.tsv"), want ${want#*:}"
  done
  refuse "baseline recorded the handoff" gcmp 'job#2' 'job#1'
  grep -q 'baseline run recorded this handoff' "$TMP/x.err" || fail "baseline refusal gave another reason: $(cat "$TMP/x.err")"
  sed "s/$RECEIPT/$(printf c | shasum -a 256 | cut -c1-64)/" "$TMP/h.json" >"$TMP/h2.json"; mv "$TMP/h2.json" "$TMP/h.json"
  refuse "compact recorded another handoff" gcmp 'job#1' 'job#2'
  grep -q 'compact run recorded another handoff' "$TMP/x.err" || fail "compact refusal gave another reason: $(cat "$TMP/x.err")"
  echo "meter context compare: governed WorkGraph section PASS"
else
  echo "meter context compare: governed WorkGraph section SKIP (no bonfyre-queue given)"
fi
echo "meter context compare: PASS"
