#!/bin/sh
# Measure one local checkpoint on the fixed evaluation sets in this directory
# and write one evidence file per task class for `bonfyre-operator-store
# quality-record`. The harness never loads a model itself: it asks the memory
# ledger first, then takes a lease from the residency owner (bonfyre-moe serve)
# and releases it on every exit path. Only a localhost model endpoint is used.
#
# usage: run_quality_eval.sh --out DIR [--moe http://127.0.0.1:7000]
#          [--prefer Qwen2.5-Coder-3B-Instruct-Q4_K_M] [--classes "short_analysis code_review workflow"]
#          [--admission BIN]
set -eu

here=$(cd "$(dirname "$0")" && pwd)
out=""; moe="http://127.0.0.1:7000"; prefer="Qwen2.5-Coder-3B-Instruct-Q4_K_M"
classes="short_analysis code_review workflow"; admission="$HOME/.bonfyre/bin/bonfyre-admission"
while [ $# -gt 1 ]; do
  case "$1" in
    --out) out=$2 ;; --moe) moe=$2 ;; --prefer) prefer=$2 ;; --classes) classes=$2 ;; --admission) admission=$2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
  shift 2
done
[ -n "$out" ] || { echo "--out DIR required" >&2; exit 2; }
case "$moe" in http://127.0.0.1:*|http://localhost:*) ;; *) echo "only a localhost residency owner is allowed" >&2; exit 2 ;; esac
mkdir -p "$out"
db="$out/run.sqlite3"
rm -f "$db"
sqlite3 "$db" "CREATE TABLE items(cls TEXT, ord INTEGER, obj TEXT); CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT);"
q() { sqlite3 "$db" "$1"; }
put() { sqlite3 "$db" "INSERT OR REPLACE INTO kv VALUES('$1', CAST(readfile('$2') AS TEXT));"; }
now() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }
sha() { shasum -a 256 "$1" | cut -d' ' -f1; }

lease_id=""
release() {
  if [ -n "$lease_id" ]; then
    curl -s -m 10 -X POST "$moe/bonfyre/lease?release=$lease_id" > "$out/lease-release.json" || true
    lease_id=""
  fi
}
trap release EXIT INT TERM

# 1. Ask the memory ledger before anything can load.
"$admission" memory > "$out/admission-before.json"
curl -s -m 10 "$moe/bonfyre/residency" > "$out/residency-before.json"
put admission_before "$out/admission-before.json"
put residency_before "$out/residency-before.json"
need=$(q "SELECT json_extract(value,'\$.min_bytes') FROM kv, json_each(kv.v,'\$.candidates') WHERE k='residency_before'
          AND instr(lower(json_extract(value,'\$.model')), lower('$prefer'))>0 LIMIT 1;")
# Spendable as the residency owner computes it: available, less the floor,
# less what other owners hold open on the ledger.
ledger=${BONFYRE_ADMISSION_DB:-$HOME/.bonfyre/admission.db}
"$admission" status --db "$ledger" --volume memory > "$out/admission-ledger-before.json"
put ledger_before "$out/admission-ledger-before.json"
spendable=$(q "SELECT json_extract(a.v,'\$.available_bytes')-json_extract(a.v,'\$.protected_floor_bytes')-MAX(COALESCE(json_extract(l.v,'\$.committed_bytes'),0),0)
               FROM kv a, kv l WHERE a.k='admission_before' AND l.k='ledger_before';")
resident=$(q "SELECT json_extract(v,'\$.resident') FROM kv WHERE k='residency_before';")
[ -n "$need" ] || { echo "preferred checkpoint is not a residency candidate" >&2; exit 3; }
if [ "$resident" != 1 ] && [ "$spendable" -lt "$need" ]; then
  q "SELECT json_object('schema','bonfyre.operator_store.quality_refusal.v1','refused_by','bonfyre-admission memory',
     'prefer','$prefer','need_bytes',$need,'spendable_bytes',$spendable,'at',$(now),'ledger',json(CAST(readfile('$out/admission-ledger-before.json') AS TEXT)),
     'admission',json(v)) FROM kv WHERE k='admission_before';" > "$out/refusal.json"
  echo "refused: $prefer needs $need bytes, $spendable spendable above the floor" >&2
  exit 4
fi

# 2. Lease through the residency owner; it takes the memory grant itself.
t0=$(now)
code=$(curl -s -m 600 -o "$out/lease.json" -w '%{http_code}' -X POST "$moe/bonfyre/lease?ttl=1800&prefer=$prefer")
t1=$(now)
if [ "$code" != 200 ]; then
  cp "$out/lease.json" "$out/refusal.json"
  echo "lease refused by the residency owner (HTTP $code)" >&2
  exit 5
fi
put lease "$out/lease.json"
lease_id=$(q "SELECT json_extract(v,'\$.lease') FROM kv WHERE k='lease';")
model_path=$(q "SELECT json_extract(v,'\$.model_path') FROM kv WHERE k='lease';")
model=$(basename "$model_path")
curl -s -m 10 "$moe/bonfyre/residency" > "$out/residency-leased.json"
put residency_leased "$out/residency-leased.json"
pid=$(q "SELECT json_extract(v,'\$.pid') FROM kv WHERE k='residency_leased';")
rss_max=0
sample_rss() {
  r=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || true)
  [ -n "$r" ] && [ "$r" -gt "$rss_max" ] && rss_max=$r
  return 0
}
sample_rss

# One generation: writes $out/resp.json, sets latency.
generate() { # $1 eval file, $2 json path of prompt, $3 max_tokens, $4 seed, $5 stage1 file or ""
  s1=${5:-/dev/null}
  sqlite3 :memory: "SELECT json_object('model','local','temperature',0,'seed',$4,'max_tokens',$3,'cache_prompt',json('false'),
     'messages',json_array(json_object('role','system','content','Answer exactly as asked. No explanation.'),
     json_object('role','user','content',replace(json_extract(readfile('$1'),'$2'),'{stage1}',COALESCE(CAST(readfile('$s1') AS TEXT),'')))));" > "$out/req.json"
  latency=$(curl -s -m 600 -o "$out/resp.json" -w '%{time_total}' -H 'Content-Type: application/json' \
            --data-binary @"$out/req.json" "$moe/v1/chat/completions" || echo -1)
  sample_rss
}
answer() { sqlite3 :memory: "SELECT COALESCE(json_extract(readfile('$out/resp.json'),'\$.choices[0].message.content'),'');"; }
usage() { sqlite3 :memory: "SELECT COALESCE(json_extract(readfile('$out/resp.json'),'\$.usage.$1'),-1);"; }
norm() { sqlite3 :memory: "SELECT lower(trim(CAST(readfile('$1') AS TEXT),' .'||char(10,9,13)||'\`\"'''));"; }

for cls in $classes; do
  f="$here/$cls.json"
  set_sha=$(sha "$f")
  n=$(sqlite3 :memory: "SELECT json_array_length(readfile('$f'),'\$.items');")
  maxtok=$(sqlite3 :memory: "SELECT json_extract(readfile('$f'),'\$.generation.max_tokens');")
  seed=$(sqlite3 :memory: "SELECT json_extract(readfile('$f'),'\$.generation.seed');")
  started=$(now)
  i=0
  while [ "$i" -lt "$n" ]; do
    id=$(sqlite3 :memory: "SELECT json_extract(readfile('$f'),'\$.items[$i].id');")
    stages=$(sqlite3 :memory: "SELECT COALESCE(json_array_length(readfile('$f'),'\$.items[$i].stages'),0);")
    if [ "$stages" -eq 0 ]; then
      generate "$f" "\$.items[$i].prompt" "$maxtok" "$seed" ""
      answer > "$out/ans.txt"
      kind=$(sqlite3 :memory: "SELECT COALESCE(json_extract(readfile('$f'),'\$.items[$i].kind'),'exact');")
      sqlite3 :memory: "SELECT json_extract(readfile('$f'),'\$.items[$i].expected');" | tr -d '\n' > "$out/exp.txt"
      if [ "$cls" = code_review ]; then
        got=$(grep -oE '[0-9]+' "$out/ans.txt" | head -1 || true); want=$(cat "$out/exp.txt")
      elif [ "$kind" = set ]; then
        got=$(norm "$out/ans.txt" | tr -d ' ' | tr ',' '\n' | sed '/^$/d' | sort | paste -sd, -)
        want=$(norm "$out/exp.txt" | tr -d ' ' | tr ',' '\n' | sort | paste -sd, -)
      else
        got=$(norm "$out/ans.txt"); want=$(norm "$out/exp.txt")
      fi
      ok=false; [ -n "$got" ] && [ "$got" = "$want" ] && ok=true
      q "INSERT INTO items VALUES('$cls',$i,json_object('id','$id','passed',json('$ok'),'expected',CAST(readfile('$out/exp.txt') AS TEXT),
         'answer',CAST(readfile('$out/ans.txt') AS TEXT),'latency_s',$latency,'prompt_tokens',$(usage prompt_tokens),
         'completion_tokens',$(usage completion_tokens),'answer_sha256','$(sha "$out/ans.txt")'));"
    else
      # Stage 1: extraction to JSON. Stage 2 receives the model's own stage-1 JSON.
      generate "$f" "\$.items[$i].stages[0].prompt" "$maxtok" "$seed" ""
      answer > "$out/ans1.txt"; l1=$latency; p1=$(usage prompt_tokens); c1=$(usage completion_tokens)
      # The JSON object is the text from the first '{' to the last '}'.
      sqlite3 :memory: "SELECT CASE WHEN first>0 AND last>first THEN substr(a,first,last-first+1) ELSE '' END FROM
                        (SELECT a, instr(a,'{') first, length(rtrim(a,replace(a,'}',''))) last FROM (SELECT CAST(readfile('$out/ans1.txt') AS TEXT) a));" \
        | tr -d '\n' > "$out/stage1.txt"
      ok1=$(sqlite3 :memory: "SELECT CASE WHEN json_valid(a) AND NOT EXISTS(SELECT 1 FROM json_each(json_extract(readfile('$f'),'\$.items[$i].stages[0].expected')) x
                        WHERE json_extract(a,'\$.'||x.key) IS NOT x.atom OR json_type(a,'\$.'||x.key) IS NOT x.type) THEN 'true' ELSE 'false' END
                        FROM (SELECT CAST(readfile('$out/stage1.txt') AS TEXT) a);")
      generate "$f" "\$.items[$i].stages[1].prompt" "$maxtok" "$seed" "$out/stage1.txt"
      answer > "$out/ans.txt"; l2=$latency; p2=$(usage prompt_tokens); c2=$(usage completion_tokens)
      sqlite3 :memory: "SELECT json_extract(readfile('$f'),'\$.items[$i].stages[1].expected');" | tr -d '\n' > "$out/exp.txt"
      got=$(norm "$out/ans.txt"); want=$(norm "$out/exp.txt")
      ok2=false; [ -n "$got" ] && [ "$got" = "$want" ] && ok2=true
      ok=false; [ "$ok1" = true ] && [ "$ok2" = true ] && ok=true
      q "INSERT INTO items VALUES('$cls',$i,json_object('id','$id','passed',json('$ok'),
         'stages',json_array(
           json_object('passed',json('$ok1'),'answer',CAST(readfile('$out/ans1.txt') AS TEXT),'latency_s',$l1,'prompt_tokens',$p1,'completion_tokens',$c1),
           json_object('passed',json('$ok2'),'expected',CAST(readfile('$out/exp.txt') AS TEXT),'answer',CAST(readfile('$out/ans.txt') AS TEXT),'latency_s',$l2,'prompt_tokens',$p2,'completion_tokens',$c2)),
         'latency_s',$l1+$l2,'prompt_tokens',$p1+$p2,'completion_tokens',$c1+$c2));"
    fi
    i=$((i + 1))
  done
  finished=$(now)
  "$admission" memory > "$out/admission-$cls.json"
  q "SELECT json_object(
      'schema','bonfyre.operator_store.quality_evidence.v1','measured',json('true'),
      'model','$model','model_path','$model_path','class','$cls',
      'eval_set','$f','eval_set_sha256','$set_sha',
      'threshold',json_extract(CAST(readfile('$f') AS TEXT),'\$.threshold'),
      'generation',json(json_extract(CAST(readfile('$f') AS TEXT),'\$.generation')),
      'endpoint','$moe/v1/chat/completions',
      'harness','$here/run_quality_eval.sh','harness_sha256','$(sha "$here/run_quality_eval.sh")',
      'lease',json_object('id',$lease_id,'context',json_extract(l.v,'\$.context'),'preferred',json_extract(l.v,'\$.preferred'),
                          'acquire_seconds',round($t1-$t0,3)),
      'memory',json_object('admission_before',json(a.v),'admission_after',json(CAST(readfile('$out/admission-$cls.json') AS TEXT)),
                           'llama_server_pid',$pid,'llama_server_rss_max_bytes',$rss_max*1024,
                           'residency_grant_id',json_extract(r.v,'\$.grant_id'),'residency_context',json_extract(r.v,'\$.context')),
      'summary',(SELECT json_object('items',COUNT(*),'items_passed',SUM(json_extract(obj,'\$.passed')),
                 'latency_total_s',round(SUM(json_extract(obj,'\$.latency_s')),3),'latency_max_s',MAX(json_extract(obj,'\$.latency_s')),
                 'prompt_tokens',SUM(json_extract(obj,'\$.prompt_tokens')),'completion_tokens',SUM(json_extract(obj,'\$.completion_tokens')))
                 FROM items WHERE cls='$cls'),
      'items',(SELECT json_group_array(json(obj)) FROM (SELECT obj FROM items WHERE cls='$cls' ORDER BY ord)),
      'started_at',$started,'finished_at',$finished)
     FROM kv a, kv l, kv r WHERE a.k='admission_before' AND l.k='lease' AND r.k='residency_leased';" > "$out/evidence-$cls.json"
  echo "$cls: $(q "SELECT SUM(json_extract(obj,'\$.passed'))||'/'||COUNT(*) FROM items WHERE cls='$cls';")"
done

# 3. Recovery: a preference cannot evict a leased model, an unknown lease is
# refused, and our lease releases.
curl -s -m 30 -X POST "$moe/bonfyre/lease?ttl=5&prefer=Clef-Flash" > "$out/recovery-pin.json" || true
pin_id=$(sqlite3 :memory: "SELECT json_extract(readfile('$out/recovery-pin.json'),'\$.lease');")
[ -n "$pin_id" ] && curl -s -m 10 -X POST "$moe/bonfyre/lease?release=$pin_id" > "$out/recovery-pin-release.json"
unknown_code=$(curl -s -m 10 -o "$out/recovery-unknown.json" -w '%{http_code}' -X POST "$moe/bonfyre/lease?release=999999999")
release
curl -s -m 10 "$moe/bonfyre/residency" > "$out/residency-after.json"
"$admission" memory > "$out/admission-after.json"
sqlite3 :memory: "SELECT json_object('schema','bonfyre.operator_store.quality_recovery.v1','at',$(now),
  'pin_under_lease',json(readfile('$out/recovery-pin.json')),
  'pin_release',json(COALESCE(readfile('$out/recovery-pin-release.json'),'null')),
  'unknown_release_http',$unknown_code,'unknown_release',json(readfile('$out/recovery-unknown.json')),
  'lease_release',json(readfile('$out/lease-release.json')),
  'residency_after',json(readfile('$out/residency-after.json')),
  'admission_after',json(readfile('$out/admission-after.json')));" > "$out/recovery.json"
rm -f "$out/req.json" "$out/resp.json" "$out/ans.txt" "$out/ans1.txt" "$out/exp.txt" "$out/stage1.txt"
echo "evidence in $out"
