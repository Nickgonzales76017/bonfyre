#!/bin/sh
# quality-record on fixture evidence only: the owner recomputes the verdict,
# appends, refuses estimates and malformed evidence, replays the same evidence
# without a second record, and the router reads what it wrote.
set -eu

bin=${1:?operator store binary required}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
q="$tmp/quality.json"
field() { sqlite3 :memory: "SELECT json_extract(readfile('$1'),'$2');"; }
expect() { got=$(field "$1" "$2"); [ "$got" = "$3" ] || { echo "FAIL $1 $2: got '$got' want '$3'" >&2; exit 1; }; }
refused() { if "$bin" quality-record --file "$q" --evidence "$1" --at 1800000000 > "$tmp/r.json"; then echo "FAIL accepted $1" >&2; exit 1; fi; }
set_sha=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
M=Qwen2.5-Coder-3B-Instruct-Q4_K_M.gguf
ev() { # name class threshold measured items...
  name=$1 cls=$2 th=$3 meas=$4; shift 4
  printf '{"schema":"bonfyre.operator_store.quality_evidence.v1","model":"%s","class":"%s","eval_set_sha256":"%s","threshold":%s,"measured":%s,"items":[%s]}' \
    "${MODEL:-$M}" "$cls" "$set_sha" "$th" "$meas" "$*" > "$tmp/$name.json"
}
P='{"id":"a","passed":true}' F='{"id":"b","passed":false}'

# A pass is computed from the items: 3 of 4 at 0.75 passes.
ev pass short_analysis 0.75 true "$P,$P,$P,$F"
"$bin" quality-record --file "$q" --evidence "$tmp/pass.json" --at 1800000000 > "$tmp/out.json"
expect "$tmp/out.json" '$.recorded' 1
expect "$tmp/out.json" '$.passed' 1
expect "$tmp/out.json" '$.score' 0.75
expect "$q" '$.schema' bonfyre.operator_store.quality.v1
expect "$q" '$.records[0].passed' 1
expect "$q" '$.records[0].witness' "sha256:$(shasum -a 256 "$tmp/pass.json" | cut -d' ' -f1)"
# The same evidence again is a replay, not a second record.
"$bin" quality-record --file "$q" --evidence "$tmp/pass.json" --at 1800000001 > "$tmp/out.json"
expect "$tmp/out.json" '$.replayed' 1
[ "$(sqlite3 :memory: "SELECT json_array_length(readfile('$q'),'\$.records');")" = 1 ]

# A failure is recorded as a failure (2 of 4 under 0.75) and appended.
ev fail code_review 0.75 true "$P,$F,$P,$F"
"$bin" quality-record --file "$q" --evidence "$tmp/fail.json" --at 1800000002 > "$tmp/out.json"
expect "$tmp/out.json" '$.passed' 0
expect "$q" '$.records[1].passed' 0
expect "$q" '$.records[1].items_passed' 2

# Refusals: estimates, bad schema, path-shaped model, unknown class, bad items, bad threshold, symlinked file.
ev est workflow 0.75 false "$P"; refused "$tmp/est.json"
ev noitems workflow 0.75 true ""; refused "$tmp/noitems.json"
ev baditem workflow 0.75 true '{"id":"a","passed":1}'; refused "$tmp/baditem.json"
ev badth workflow 1.5 true "$P"; refused "$tmp/badth.json"
ev badcls deterministic_read 0.75 true "$P"; refused "$tmp/badcls.json"
MODEL=/m/x.gguf ev badmodel workflow 0.75 true "$P"; refused "$tmp/badmodel.json"
sed 's/quality_evidence.v1/quality_evidence.v0/' "$tmp/pass.json" > "$tmp/badschema.json"; refused "$tmp/badschema.json"
cp "$q" "$tmp/before.json"
ln -s "$q" "$tmp/link.json"
if "$bin" quality-record --file "$tmp/link.json" --evidence "$tmp/fail.json" > /dev/null; then echo "FAIL symlink accepted" >&2; exit 1; fi
cmp -s "$q" "$tmp/before.json"

# The router reads what the owner wrote: short_analysis passes, code_review fails.
db="$tmp/operator.sqlite3"
G=00000000000000000000000000000001
"$bin" init --db "$db" > /dev/null
sqlite3 "$db" "INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,interval_seconds,retry_at,attempts,last_error,created_at,updated_at)
  VALUES('$G','fixture','fixture','',50,'queued','fixture',0,0,0,0,'',1,1);"
printf '{"max_goal_attempts":6}' > "$tmp/policy.json"
printf '{"schema":"bonfyre.moe.residency.v2","model":"/m/%s","resident":false,"candidates":[{"model":"%s","usable":true,"min_bytes":2388393216}]}' "$M" "$M" > "$tmp/res.json"
printf '{"volume":"memory","available_bytes":5000000000,"protected_floor_bytes":1610612736}' > "$tmp/mem.json"
for cls in short_analysis code_review; do
  "$bin" route --db "$db" --stop "$tmp/STOP" --goal $G --class $cls --policy "$tmp/policy.json" --residency "$tmp/res.json" \
    --memory "$tmp/mem.json" --quality "$q" --at 1800000000 > "$tmp/route-$cls.json"
done
expect "$tmp/route-short_analysis.json" '$.tier' local_model
expect "$tmp/route-code_review.json" '$.tier' hold
expect "$tmp/route-code_review.json" '$.tiers[0].candidates[0].verdict' quality_failed
echo "operator quality-record: PASS"
