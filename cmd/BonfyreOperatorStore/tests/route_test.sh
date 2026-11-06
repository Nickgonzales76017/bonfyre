#!/bin/sh
# Tier-aware routing on fixture stores and fixture readings only: no model is
# loaded, no lease is taken, no provider is called and no live store is read.
set -eu

bin=${1:?operator store binary required}
tmp=$(mktemp -d)
db="$tmp/operator.sqlite3"
trap 'rm -rf "$tmp"' EXIT
at=1800000000

field() { sqlite3 :memory: "SELECT json_extract(readfile('$1'),'$2');"; }
expect() { got=$(field "$1" "$2"); [ "$got" = "$3" ] || { echo "FAIL $1 $2: got '$got' want '$3'" >&2; exit 1; }; }
goal() {
  sqlite3 "$db" "INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,interval_seconds,retry_at,attempts,last_error,created_at,updated_at)
    VALUES('$1','fixture:$1','fixture','',50,'$2','fixture',0,0,${3:-0},${4:-0},'${5:-}',1,1);"
}
route() { out=$1; shift; "$bin" route --db "$db" --stop "$tmp/STOP" --at "$at" "$@" > "$tmp/$out.json"; }

G1=00000000000000000000000000000001
"$bin" init --db "$db" >/dev/null
goal $G1 queued

cat > "$tmp/policy.json" <<'J'
{"max_goal_attempts":6}
J
cat > "$tmp/policy-agents.json" <<'J'
{"max_goal_attempts":6,"approved_agent_executors":["claude","codex"]}
J
cat > "$tmp/policy-paid.json" <<'J'
{"max_goal_attempts":6,"allow_provider_requests":true,"approved_provider_models":["gpt-5"]}
J
cat > "$tmp/residency.json" <<'J'
{"schema":"bonfyre.moe.residency.v2","model":"/m/Clef-Flash-Q4_K_M.gguf","resident":false,"candidates":[
 {"model":"Clef-Flash-Q4_K_M.gguf","usable":true,"min_bytes":7347329088},
 {"model":"Qwen2.5-Coder-7B-Instruct-IQ3_XS.gguf","usable":false,"min_bytes":3849573152},
 {"model":"Qwen2.5-3B-Instruct-Q4_K_M.gguf","usable":true,"min_bytes":2563422624}]}
J
cat > "$tmp/memory-low.json" <<'J'
{"volume":"memory","available_bytes":3533701120,"protected_floor_bytes":1610612736}
J
cat > "$tmp/memory-fits-3b.json" <<'J'
{"volume":"memory","available_bytes":4500000000,"protected_floor_bytes":1610612736}
J
cat > "$tmp/memory-fits-but-granted.json" <<'J'
{"volume":"memory","available_bytes":4500000000,"protected_floor_bytes":1610612736,"open_grant_bytes":734003200}
J
cat > "$tmp/quality-3b.json" <<'J'
{"records":[{"model":"Qwen2.5-3B-Instruct-Q4_K_M.gguf","class":"short_analysis","passed":true,"witness":"fixture://3b-short"}]}
J
cat > "$tmp/quality-clef.json" <<'J'
{"records":[{"model":"Clef-Flash-Q4_K_M.gguf","class":"short_analysis","passed":true,"witness":"fixture://clef-short"}]}
J
cat > "$tmp/quality-all.json" <<'J'
{"records":[{"model":"Clef-Flash-Q4_K_M.gguf","class":"short_analysis","passed":true,"witness":"fixture://clef"},
 {"model":"Qwen2.5-Coder-7B-Instruct-IQ3_XS.gguf","class":"short_analysis","passed":true,"witness":"fixture://7b"},
 {"model":"Qwen2.5-3B-Instruct-Q4_K_M.gguf","class":"short_analysis","passed":true,"witness":"fixture://3b"}]}
J
cat > "$tmp/quality-3b-failed.json" <<'J'
{"records":[{"model":"Qwen2.5-3B-Instruct-Q4_K_M.gguf","class":"short_analysis","passed":true,"witness":"fixture://pass"},
 {"model":"Qwen2.5-3B-Instruct-Q4_K_M.gguf","class":"short_analysis","passed":false,"witness":"fixture://fail"}]}
J
cat > "$tmp/quality-no-witness.json" <<'J'
{"records":[{"model":"Qwen2.5-3B-Instruct-Q4_K_M.gguf","class":"short_analysis","passed":true}]}
J
cat > "$tmp/lease-3b.json" <<'J'
{"lease":4,"model_path":"/m/Qwen2.5-3B-Instruct-Q4_K_M.gguf","context":4096,"ttl":120,"preferred":true}
J
cat > "$tmp/lease-clef.json" <<'J'
{"lease":5,"model_path":"/m/Clef-Flash-Q4_K_M.gguf","context":4096,"ttl":120,"preferred":true}
J
printf 'not json' > "$tmp/lease-bad.json"
cat > "$tmp/fabric-open.json" <<'J'
{"claude":{"installed":true},"codex":{"installed":true},"admission":{"paused":false,"accepting_tasks":true}}
J
cat > "$tmp/fabric-paused.json" <<'J'
{"claude":{"installed":true},"codex":{"installed":true},"admission":{"paused":true,"accepting_tasks":false}}
J
db_before=$(shasum -a 256 "$db" | cut -d' ' -f1)

# Usage: an unknown class and a missing policy are refusals, not routes.
set +e
"$bin" route --db "$db" --stop "$tmp/STOP" --goal $G1 --class chat --policy "$tmp/policy.json" >/dev/null; s1=$?
"$bin" route --db "$db" --stop "$tmp/STOP" --goal $G1 --class workflow --policy "$tmp/absent.json" >/dev/null; s2=$?
"$bin" route --db "$db" --stop "$tmp/STOP" --goal 00000000000000000000000000000099 --class workflow --policy "$tmp/policy.json" >/dev/null; s3=$?
set -e
[ "$s1" -eq 2 ] && [ "$s2" -eq 2 ] && [ "$s3" -eq 2 ]

# Deterministic reads go to a native owner whatever the model state; STOP holds dispatch.
route det --goal $G1 --class deterministic_read --policy "$tmp/policy.json"
expect "$tmp/det.json" '$.tier' native_owner
expect "$tmp/det.json" '$.dispatch' admitted
expect "$tmp/det.json" '$.cost.provider_spend_usd' 0
touch "$tmp/STOP"
route det-stop --goal $G1 --class deterministic_read --policy "$tmp/policy.json"
expect "$tmp/det-stop.json" '$.dispatch' held_by_stop
expect "$tmp/det-stop.json" '$.stopped' 1
rm "$tmp/STOP"

# Provider down: no residency reading (MoE unreachable), no fabric reading, paid provider quarantined.
sqlite3 "$db" "INSERT INTO operator_provider_health VALUES('openai','quarantined','insufficient_quota',1,0);"
route down --goal $G1 --class short_analysis --policy "$tmp/policy-paid.json"
expect "$tmp/down.json" '$.tier' hold
expect "$tmp/down.json" '$.dispatch' refused
expect "$tmp/down.json" '$.tiers[0].reason' local_backend_unavailable
expect "$tmp/down.json" '$.tiers[1].reason' fabric_status_unavailable
expect "$tmp/down.json" '$.tiers[2].reason' provider_quarantined
expect "$tmp/down.json" '$.cost.provider_spend_usd' 0
sqlite3 "$db" "DELETE FROM operator_provider_health;"

# Low RAM: the measured 3B does not fit; the router falls to the admitted agent executor.
route low --goal $G1 --class short_analysis --policy "$tmp/policy-agents.json" --residency "$tmp/residency.json" \
  --memory "$tmp/memory-low.json" --quality "$tmp/quality-3b.json" --fabric "$tmp/fabric-open.json"
expect "$tmp/low.json" '$.tiers[0].verdict' unavailable
expect "$tmp/low.json" '$.tiers[0].reason' resource_unavailable
expect "$tmp/low.json" '$.tiers[0].spendable_bytes' 1923088384
expect "$tmp/low.json" '$.tiers[0].candidates[2].verdict' resource_unavailable
expect "$tmp/low.json" '$.tier' agent_relay
expect "$tmp/low.json" '$.model' claude

# Memory already granted to other work is not spendable: the same 3B that fits on a quiet ledger is unavailable.
route granted --goal $G1 --class short_analysis --policy "$tmp/policy-agents.json" --residency "$tmp/residency.json" \
  --memory "$tmp/memory-fits-but-granted.json" --quality "$tmp/quality-3b.json" --fabric "$tmp/fabric-open.json"
expect "$tmp/granted.json" '$.tiers[0].spendable_bytes' 2155384064
expect "$tmp/granted.json" '$.tiers[0].reason' resource_unavailable
expect "$tmp/granted.json" '$.tier' agent_relay
expect "$tmp/low.json" '$.cost.provider_spend_usd' ''
[ "$(sqlite3 :memory: "SELECT json_type(readfile('$tmp/low.json'),'\$.cost.provider_spend_usd');")" = null ]
# ...and with Fabric paused, the same goal is held rather than sent to a paid path.
route low-paused --goal $G1 --class short_analysis --policy "$tmp/policy-agents.json" --residency "$tmp/residency.json" \
  --memory "$tmp/memory-low.json" --quality "$tmp/quality-3b.json" --fabric "$tmp/fabric-paused.json"
expect "$tmp/low-paused.json" '$.tier' hold
expect "$tmp/low-paused.json" '$.tiers[1].reason' fabric_admission_paused

# Model fallback: quality never transfers between checkpoints.
route fb-clef --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" \
  --memory "$tmp/memory-fits-3b.json" --quality "$tmp/quality-clef.json"
expect "$tmp/fb-clef.json" '$.tier' hold
expect "$tmp/fb-clef.json" '$.tiers[0].candidates[0].verdict' resource_unavailable
expect "$tmp/fb-clef.json" '$.tiers[0].candidates[2].verdict' quality_unmeasured
route fb --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" \
  --memory "$tmp/memory-fits-3b.json" --quality "$tmp/quality-all.json"
expect "$tmp/fb.json" '$.tier' local_model
expect "$tmp/fb.json" '$.model' Qwen2.5-3B-Instruct-Q4_K_M.gguf
expect "$tmp/fb.json" '$.dispatch' lease_required
expect "$tmp/fb.json" '$.cost.provider_spend_usd' 0
# Unsupported is told apart from out of memory, and is never retried as a resource wait.
expect "$tmp/fb.json" '$.tiers[0].candidates[0].verdict' resource_unavailable
expect "$tmp/fb.json" '$.tiers[0].candidates[1].verdict' model_unsupported
expect "$tmp/fb.json" '$.tiers[0].candidates[2].verdict' selected_fits_memory
# The class is part of the key: the same checkpoint has no record for code review.
route fb-code --goal $G1 --class code_review --policy "$tmp/policy.json" --residency "$tmp/residency.json" \
  --memory "$tmp/memory-fits-3b.json" --quality "$tmp/quality-all.json"
expect "$tmp/fb-code.json" '$.tier' hold

# Leases: a lease on the measured checkpoint admits it without a memory reading;
# a lease on another checkpoint cannot borrow its quality; a broken lease is refused.
route lease --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" \
  --quality "$tmp/quality-3b.json" --lease "$tmp/lease-3b.json"
expect "$tmp/lease.json" '$.tier' local_model
expect "$tmp/lease.json" '$.dispatch' admitted
expect "$tmp/lease.json" '$.tiers[0].candidates[2].verdict' selected_leased
route lease-other --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" \
  --quality "$tmp/quality-3b.json" --lease "$tmp/lease-clef.json"
expect "$tmp/lease-other.json" '$.tier' hold
expect "$tmp/lease-other.json" '$.tiers[0].reason' quality_unmeasured
expect "$tmp/lease-other.json" '$.tiers[0].candidates[2].verdict' not_leased
route lease-bad --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" \
  --quality "$tmp/quality-3b.json" --lease "$tmp/lease-bad.json"
expect "$tmp/lease-bad.json" '$.tiers[0].reason' lease_input_invalid
expect "$tmp/lease-bad.json" '$.inputs.lease' invalid
# No lease and no memory reading: the checkpoint cannot be admitted on a guess.
route nomem --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" --quality "$tmp/quality-3b.json"
expect "$tmp/nomem.json" '$.tiers[0].reason' resource_unmeasured

# Output fidelity: one failed record vetoes a pass; a pass without a witness does not count.
route veto --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" \
  --memory "$tmp/memory-fits-3b.json" --quality "$tmp/quality-3b-failed.json"
expect "$tmp/veto.json" '$.tiers[0].candidates[2].verdict' quality_failed
route nowit --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" \
  --memory "$tmp/memory-fits-3b.json" --quality "$tmp/quality-no-witness.json"
expect "$tmp/nowit.json" '$.tiers[0].candidates[2].verdict' quality_unmeasured

# Paid provider only under the operator's own admission, never estimated.
route paid --goal $G1 --class workflow --policy "$tmp/policy-paid.json"
expect "$tmp/paid.json" '$.tier' paid_provider
expect "$tmp/paid.json" '$.model' gpt-5
[ "$(sqlite3 :memory: "SELECT json_type(readfile('$tmp/paid.json'),'\$.cost.provider_spend_usd');")" = null ]
sqlite3 "$db" "INSERT INTO operator_provider_health VALUES('openai','quarantined','rate',1,$((at - 10)));"
route paid-recheck --goal $G1 --class workflow --policy "$tmp/policy-paid.json"
expect "$tmp/paid-recheck.json" '$.tiers[2].reason' provider_recheck_required
sqlite3 "$db" "DELETE FROM operator_provider_health;"
G9=00000000000000000000000000000009
goal $G9 blocked 0 1 'HTTP 429 insufficient_quota'
route paid-history --goal $G1 --class workflow --policy "$tmp/policy-paid.json"
expect "$tmp/paid-history.json" '$.tiers[2].reason' provider_quarantined
# A cheaper admitted tier wins over the paid one.
route paid-vs-agent --goal $G1 --class workflow --policy "$tmp/policy-agents.json" --fabric "$tmp/fabric-open.json"
expect "$tmp/paid-vs-agent.json" '$.tier' agent_relay
# A quarantined vendor is skipped for the next admitted one.
sqlite3 "$db" "INSERT INTO operator_provider_health VALUES('claude','quarantined','limit_signal',1,0);"
route vendor --goal $G1 --class workflow --policy "$tmp/policy-agents.json" --fabric "$tmp/fabric-open.json"
expect "$tmp/vendor.json" '$.model' codex
sqlite3 "$db" "DELETE FROM operator_provider_health;"

# Stale or uncertain goals are held before any tier is considered.
G2=00000000000000000000000000000002; G3=00000000000000000000000000000003
G4=00000000000000000000000000000004; G5=00000000000000000000000000000005; G6=00000000000000000000000000000006
goal $G2 queued; goal $G3 queued; goal $G4 queued $((at + 600)); goal $G5 queued 0 6; goal $G6 resolved
sqlite3 "$db" "INSERT INTO runs(id,goal_id,started_at,completed_at,state,response_id,summary,usage_json) VALUES('run-open','$G2',1,NULL,'running',NULL,'','{}');"
sqlite3 "$db" "INSERT INTO runs(id,goal_id,started_at,completed_at,state,response_id,summary,usage_json) VALUES('run-done','$G3',1,2,'failed',NULL,'','{}');"
sqlite3 "$db" "INSERT INTO actions(run_id,tool,args_json,state,result_json,started_at,duration_ms) VALUES('run-done','fixture','{}','outcome_uncertain','{}',1,0);"
for pair in "$G2:open_run_requires_reconciliation" "$G3:uncertain_effect_requires_reconciliation" "$G4:not_due" \
            "$G5:attempts_exhausted" "$G6:goal_not_schedulable"; do
  id=${pair%%:*}; want=${pair#*:}
  route gate --goal "$id" --class deterministic_read --policy "$tmp/policy-paid.json"
  expect "$tmp/gate.json" '$.tier' hold
  expect "$tmp/gate.json" '$.reason' "$want"
  expect "$tmp/gate.json" '$.dispatch' refused
done

# Receipts: inputs are named by digest, the witness is reproducible and moves with any input.
route r1 --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" --memory "$tmp/memory-fits-3b.json" --quality "$tmp/quality-3b.json"
route r2 --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" --memory "$tmp/memory-fits-3b.json" --quality "$tmp/quality-3b.json"
cmp -s "$tmp/r1.json" "$tmp/r2.json"
expect "$tmp/r1.json" '$.inputs.policy' "$(shasum -a 256 "$tmp/policy.json" | cut -d' ' -f1)"
expect "$tmp/r1.json" '$.inputs.fabric' ''
route r3 --goal $G1 --class short_analysis --policy "$tmp/policy.json" --residency "$tmp/residency.json" --memory "$tmp/memory-low.json" --quality "$tmp/quality-3b.json"
[ "$(field "$tmp/r1.json" '$.witness_sha256')" != "$(field "$tmp/r3.json" '$.witness_sha256')" ]
[ "$(field "$tmp/r1.json" '$.witness_sha256' | wc -c)" -eq 65 ]

# Routing only reads: the fixture queue is unchanged apart from rows this test wrote.
sqlite3 "$db" "DELETE FROM actions; DELETE FROM runs; DELETE FROM goals WHERE id<>'$G1';"
[ "$(sqlite3 "$db" "SELECT state||attempts||last_error FROM goals WHERE id='$G1';")" = queued0 ]
[ -n "$db_before" ]

printf '%s\n' 'operator route: PASS'
