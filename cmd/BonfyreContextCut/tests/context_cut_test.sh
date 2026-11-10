#!/bin/sh
set -eu

tool=${1:?context-cut tool required}
: "${GIGATOKEN_BIN:?set to the native GigaToken batch encoder}"
: "${GIGATOKEN_TOKENIZER:?set to the matching tokenizer.json}"
runner=${RUNNER:-bun}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-context-cut.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
root=$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)
make -C "$root/cmd/BonfyreAdmission" all >/dev/null
admission="$root/cmd/BonfyreAdmission/bonfyre-admission"

# Exact BPE equivalence across a native cohort boundary. Two 5 MiB
# synthetic bodies must cross the 8 MiB batching threshold without moving
# token order, changing UTF-8 replacement, or altering u32le framing.
python3 - "$tmp/gigatoken-cohort-a.body" "$tmp/gigatoken-cohort-b.body" <<'PY'
import pathlib,sys
size = 5 * 1024 * 1024
parts = (b"Bonfyre public synthetic context proof.\n",
         b"Second synthetic context with invalid UTF-8 \xff\xfe and text.\n")
for path,part in zip(sys.argv[1:],parts):
    pathlib.Path(path).write_bytes((part * (size // len(part) + 1))[:size])
PY
"$GIGATOKEN_BIN" "$GIGATOKEN_TOKENIZER" "$tmp/gigatoken-cohort-a.body" "$tmp/gigatoken-cohort-b.body" > "$tmp/gigatoken-combined.bin"
"$GIGATOKEN_BIN" "$GIGATOKEN_TOKENIZER" "$tmp/gigatoken-cohort-a.body" > "$tmp/gigatoken-a.bin"
"$GIGATOKEN_BIN" "$GIGATOKEN_TOKENIZER" "$tmp/gigatoken-cohort-b.body" > "$tmp/gigatoken-b.bin"
python3 - "$tmp/gigatoken-combined.bin" "$tmp/gigatoken-a.bin" "$tmp/gigatoken-b.bin" <<'PY'
from pathlib import Path
import struct,sys
combined,a,b=(Path(path).read_bytes() for path in sys.argv[1:])
assert len(combined)>=4 and len(a)>=4 and len(b)>=4
assert struct.unpack_from("<I",combined)[0]==2
assert struct.unpack_from("<I",a)[0]==1
assert struct.unpack_from("<I",b)[0]==1
assert combined[4:]==a[4:]+b[4:], "cross-cohort token frames diverged"
PY

sqlite3 "$tmp/rank.db" "
CREATE TABLE bitnet_rank_run(run_id TEXT PRIMARY KEY,gate_state TEXT,candidate_count INTEGER);
CREATE TABLE ranked_candidate_envelope(run_id TEXT,rank INTEGER,candidate_identity TEXT,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT);
INSERT INTO bitnet_rank_run VALUES('bitnet:test','ADMIT',3);
INSERT INTO ranked_candidate_envelope VALUES
 ('bitnet:test',0,'source:a','external_occurrence','ExternalEstate:a','ForeignTwin:admission','MaterialCompiler:external'),
 ('bitnet:test',1,'source:b','external_occurrence','ExternalEstate:b','ForeignTwin:admission','MaterialCompiler:external'),
 ('bitnet:test',2,'source:c','external_occurrence','ExternalEstate:c','ForeignTwin:admission','MaterialCompiler:external');"
sqlite3 "$tmp/address.db" "
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT);
INSERT INTO candidate_advertisement VALUES
 ('source:a','external_occurrence','ExternalEstate:a','ForeignTwin:admission','MaterialCompiler:external'),
 ('source:b','external_occurrence','ExternalEstate:b','ForeignTwin:admission','MaterialCompiler:external'),
 ('source:c','external_occurrence','ExternalEstate:c','ForeignTwin:admission','MaterialCompiler:external');"
printf '%s\n' 'The first exact retained body is context.' > "$tmp/a.body"
cp "$tmp/a.body" "$tmp/b.body"
printf '%s\n' 'A lower ranked body must remain recoverable after the budget cut.' > "$tmp/c.body"
ha=$(shasum -a 256 "$tmp/a.body" | awk '{print $1}')
hc=$(shasum -a 256 "$tmp/c.body" | awk '{print $1}')
printf '%s\n' \
 "{\"candidate_identity\":\"source:a\",\"body_path\":\"$tmp/a.body\",\"content_sha256\":\"sha256:$ha\",\"receipt_digest\":\"receipt:a\",\"evidence_ref\":\"ForeignTwin:a\",\"information_policy_class\":\"public\",\"temporal_frontier\":\"t:1\"}" \
 "{\"candidate_identity\":\"source:b\",\"body_path\":\"$tmp/b.body\",\"content_sha256\":\"sha256:$ha\",\"receipt_digest\":\"receipt:b\",\"evidence_ref\":\"ForeignTwin:b\",\"information_policy_class\":\"public\",\"temporal_frontier\":\"t:1\"}" \
 "{\"candidate_identity\":\"source:c\",\"body_path\":\"$tmp/c.body\",\"content_sha256\":\"sha256:$hc\",\"receipt_digest\":\"receipt:c\",\"evidence_ref\":\"ForeignTwin:c\",\"information_policy_class\":\"public\",\"temporal_frontier\":\"t:1\"}" > "$tmp/material.jsonl"
"$runner" "$tool" compile --rank-db "$tmp/rank.db" --run-id bitnet:test --address-db "$tmp/address.db" --material-manifest "$tmp/material.jsonl" --tokenizer "$GIGATOKEN_TOKENIZER" --tokenizer-passport qwen2.5-coder-tokenizer --gigatoken-bin "$GIGATOKEN_BIN" --token-cache-db "$tmp/tokens.db" --information-policy-class public --purpose "Explain the retained public body" --token-budget 10000 --max-candidates 2 --out "$tmp/context.json" --admission-bin "$admission" --admission-db "$tmp/admission.db" --admission-plane context --admission-volume "$tmp" --admission-estimated-bytes 65536 --admission-free-bytes 20000000000 | grep -q '"state":"compiled"'
"$runner" "$tool" compile --rank-db "$tmp/rank.db" --run-id bitnet:test --address-db "$tmp/address.db" --material-manifest "$tmp/material.jsonl" --tokenizer "$GIGATOKEN_TOKENIZER" --tokenizer-passport qwen2.5-coder-tokenizer --gigatoken-bin "$GIGATOKEN_BIN" --token-cache-db "$tmp/tokens.db" --information-policy-class public --purpose "Explain the retained public body" --token-budget 10000 --max-candidates 2 --out "$tmp/context-reused.json" --admission-bin "$admission" --admission-db "$tmp/admission.db" --admission-plane context --admission-volume "$tmp" --admission-estimated-bytes 65536 --admission-free-bytes 20000000000 | grep -q '"state":"compiled"'
bun -e 'const d=await Bun.file(process.argv[1]).json();if(d.selected_candidates[0].identity!=="source:a"||d.selected_candidates[1].identity!=="source:b"||d.unique_bodies!==1||d.deduplicated_body_references!==1||d.token_encoding!=="u32le"||d.token_spans.length!==1||d.token_spans[0].offset_tokens!==0||d.token_cache.hits!==0||d.token_cache.misses!==1||d.token_cache.native_batches!==1||d.token_cache.native_body_bytes!==(await Bun.file(process.argv[2]).arrayBuffer()).byteLength||d.token_cache.exact_pruned_bodies!==1||d.token_cache.fallback_scans!==0||!d.resource_admission.grant_released||!(await Bun.file(d.token_body).arrayBuffer()).byteLength||d.omitted_not_materialized_by_exact_owner!==0||d.truth!=="purpose_bound_execution_context_only"||d.forbidden_inference.length!==5)process.exit(1)' "$tmp/context.json" "$tmp/a.body"
bun "$root/components/BonfyreDecision/decision-plane.mjs" plan-context "$tmp/context.json" "$tmp/decision-plan.jsonl" >/dev/null
bun -e 'const d=await Bun.file(process.argv[1]).json();const rows=(await Bun.file(process.argv[2]).text()).trim().split("\n").map(JSON.parse);if(d.purpose!=="Explain the retained public body"||d.decision_frontier.state!=="reconstructable"||d.decision_frontier.resident_case_pack_bytes!==0||rows.length!==2||rows.some(r=>r.state!=="planned"||r.contract.gate_state!=="ADMIT"||r.contract.owner!=="ContextCut"))process.exit(1)' "$tmp/context.json" "$tmp/decision-plan.jsonl"
bun -e 'const d=await Bun.file(process.argv[1]).json();if(d.token_cache.hits!==1||d.token_cache.misses!==0||d.token_cache.native_batches!==0||d.token_cache.native_body_bytes!==0||d.token_cache.exact_pruned_bodies!==1)process.exit(1)' "$tmp/context-reused.json"
if "$runner" "$tool" compile --rank-db "$tmp/rank.db" --run-id bitnet:test --address-db "$tmp/address.db" --material-manifest "$tmp/material.jsonl" --tokenizer "$GIGATOKEN_TOKENIZER" --tokenizer-passport qwen2.5-coder-tokenizer --gigatoken-bin "$tmp/missing-encoder" --token-cache-db "$tmp/uncached.db" --information-policy-class public --purpose "Explain the retained public body" --token-budget 10000 --max-candidates 2 --out "$tmp/context-encoder-reject.json" --admission-bin "$admission" --admission-db "$tmp/admission.db" --admission-plane context --admission-volume "$tmp" --admission-estimated-bytes 65536 --admission-free-bytes 20000000000 >/dev/null 2>&1; then
  echo 'missing native encoder was accepted' >&2; exit 1
fi
"$admission" status --db "$tmp/admission.db" --volume "$tmp" --plane context | grep -q '"committed_bytes":0'
# A measured-but-DEFER representation can yield a physically useful token
# cache receipt, but never executable model context.  This is the bridge that
# lets real BitNet shadow frontiers measure GigaToken reuse without laundering
# a learned rank into selection or a model invocation.
sqlite3 "$tmp/rank.db" "UPDATE bitnet_rank_run SET gate_state='SHADOW' WHERE run_id='bitnet:test';"
"$runner" "$tool" compile --shadow-diagnostic --rank-db "$tmp/rank.db" --run-id bitnet:test --address-db "$tmp/address.db" --material-manifest "$tmp/material.jsonl" --tokenizer "$GIGATOKEN_TOKENIZER" --tokenizer-passport qwen2.5-coder-tokenizer --gigatoken-bin "$GIGATOKEN_BIN" --token-cache-db "$tmp/tokens.db" --information-policy-class public --purpose "Explain the retained public body" --token-budget 10000 --max-candidates 2 --out "$tmp/context-shadow.json" --admission-bin "$admission" --admission-db "$tmp/admission.db" --admission-plane context --admission-volume "$tmp" --admission-estimated-bytes 65536 --admission-free-bytes 20000000000 | grep -q '"state":"diagnostic"'
bun -e 'const d=await Bun.file(process.argv[1]).json();if(d.rank_gate_state!=="SHADOW"||d.truth!=="diagnostic_receipted_context_only"||d.token_cache.hits!==1||d.token_cache.misses!==0||d.token_cache.exact_pruned_bodies!==1||!d.forbidden_inference.includes("a SHADOW diagnostic context may be projected to an executing model, lowerer, or carrier"))process.exit(1)' "$tmp/context-shadow.json"
# Provider ranks are a separate representation family.  They can reuse the
# same receipt-bound material contract only in explicit diagnostic mode; the
# context compiler must never rename them as BitNet or make them executable.
sqlite3 "$tmp/provider.db" "
CREATE TABLE provider_embedding_run(run_id TEXT PRIMARY KEY,representation_id TEXT,requested_count INTEGER,stored_count INTEGER,failed_count INTEGER,dimensions INTEGER,status TEXT);
CREATE TABLE provider_embedding_query(query_id TEXT PRIMARY KEY,run_id TEXT,candidate_count INTEGER,top_k INTEGER);
CREATE TABLE provider_embedding_rank(query_id TEXT,rank INTEGER,subject_id TEXT,source_digest TEXT,PRIMARY KEY(query_id,rank));
INSERT INTO provider_embedding_run VALUES('provider:test','mistral:test',3,3,0,4,'complete');
INSERT INTO provider_embedding_query VALUES('provider:query','provider:test',3,3);
INSERT INTO provider_embedding_rank VALUES('provider:query',1,'source:a','source-a'),('provider:query',2,'source:b','source-b'),('provider:query',3,'source:c','source-c');"
bun -e 'import{createHash}from"node:crypto";const [source,out,ledger]=process.argv.slice(1);const materials=(await Bun.file(source).text()).trim().split("\n").map(JSON.parse);const evidence=[],output=[];for(const [index,material]of materials.entries()){const suffix=material.candidate_identity.split(":").at(-1);const row={schema:"bonfyre-provider-exact-owner-evidence.v1",run_id:"provider:test",query_id:"provider:query",representation_id:"mistral:test",rank:index+1,candidate_identity:material.candidate_identity,candidate_kind:"external_occurrence",owner_ref:"ExternalEstate:"+suffix,exact_verifier_ref:"ForeignTwin:admission",lowerer_ref:"MaterialCompiler:external",source_digest:"source-"+suffix,query_state:"queried",outcome:"observed",receipt_id:"foreign:"+suffix};const raw=JSON.stringify(row);evidence.push(raw);material.receipt_digest=row.receipt_id;material.evidence_ref="sha256:"+createHash("sha256").update(raw).digest("hex");output.push(JSON.stringify(material))}await Bun.write(ledger,evidence.join("\n")+"\n");await Bun.write(out,output.join("\n")+"\n")' "$tmp/material.jsonl" "$tmp/provider-material.jsonl" "$tmp/provider-exact.jsonl"
# A full rank ledger retains exact negatives.  The context cut must preserve
# their refusal boundary while allowing the observed rows above to bind bytes.
printf '%s\n' '{"schema":"bonfyre-provider-exact-owner-evidence.v1","run_id":"provider:test","query_id":"provider:query","representation_id":"mistral:test","rank":3,"candidate_identity":"source:c","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:c","exact_verifier_ref":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","source_digest":"source-c","query_state":"queried","outcome":"unobserved","receipt_id":null}' >> "$tmp/provider-exact.jsonl"
if "$runner" "$tool" compile --provider-db "$tmp/provider.db" --provider-run-id provider:test --provider-query-id provider:query --provider-exact-owner-evidence "$tmp/provider-exact.jsonl" --address-db "$tmp/address.db" --material-manifest "$tmp/provider-material.jsonl" --tokenizer "$GIGATOKEN_TOKENIZER" --tokenizer-passport qwen2.5-coder-tokenizer --gigatoken-bin "$GIGATOKEN_BIN" --token-cache-db "$tmp/tokens.db" --information-policy-class public --purpose "Explain the retained public body" --token-budget 10000 --max-candidates 2 --out "$tmp/context-provider-reject.json" --admission-bin "$admission" --admission-db "$tmp/admission.db" --admission-plane context --admission-volume "$tmp" --admission-estimated-bytes 65536 --admission-free-bytes 20000000000 >/dev/null 2>&1; then
  echo 'provider rank context was accepted without its mandatory diagnostic boundary' >&2; exit 1
fi
"$runner" "$tool" compile --shadow-diagnostic --provider-db "$tmp/provider.db" --provider-run-id provider:test --provider-query-id provider:query --provider-exact-owner-evidence "$tmp/provider-exact.jsonl" --address-db "$tmp/address.db" --material-manifest "$tmp/provider-material.jsonl" --tokenizer "$GIGATOKEN_TOKENIZER" --tokenizer-passport qwen2.5-coder-tokenizer --gigatoken-bin "$GIGATOKEN_BIN" --token-cache-db "$tmp/tokens.db" --information-policy-class public --purpose "Explain the retained public body" --token-budget 10000 --max-candidates 2 --out "$tmp/context-provider.json" --admission-bin "$admission" --admission-db "$tmp/admission.db" --admission-plane context --admission-volume "$tmp" --admission-estimated-bytes 65536 --admission-free-bytes 20000000000 | grep -q '"state":"diagnostic"' 
bun -e 'const d=await Bun.file(process.argv[1]).json();if(d.frontier.kind!=="provider_embedding_rank"||d.frontier.run_id!=="provider:test"||d.frontier.query_id!=="provider:query"||d.rank_gate_state!=="SHADOW"||d.truth!=="diagnostic_receipted_context_only"||d.selected_candidates[0].identity!=="source:a")process.exit(1)' "$tmp/context-provider.json"
# If the exact ranked prefix cannot fit, the suffix becomes reachable. The
# lazy batch must then produce the same context and token bytes as an eager
# four-candidate pass, including the later short body.
printf 'x' > "$tmp/d.body"
hd=$(shasum -a 256 "$tmp/d.body" | awk '{print $1}')
sqlite3 "$tmp/rank.db" "UPDATE bitnet_rank_run SET gate_state='ADMIT',candidate_count=4 WHERE run_id='bitnet:test'; UPDATE ranked_candidate_envelope SET rank=3 WHERE candidate_identity='source:c'; INSERT INTO ranked_candidate_envelope VALUES('bitnet:test',2,'source:d','external_occurrence','ExternalEstate:d','ForeignTwin:admission','MaterialCompiler:external');"
sqlite3 "$tmp/address.db" "INSERT INTO candidate_advertisement VALUES('source:d','external_occurrence','ExternalEstate:d','ForeignTwin:admission','MaterialCompiler:external');"
printf '%s\n' "{\"candidate_identity\":\"source:d\",\"body_path\":\"$tmp/d.body\",\"content_sha256\":\"sha256:$hd\",\"receipt_digest\":\"receipt:d\",\"evidence_ref\":\"ForeignTwin:d\",\"information_policy_class\":\"public\",\"temporal_frontier\":\"t:1\"}" >> "$tmp/material.jsonl"
first_count=$(sqlite3 "$tmp/tokens.db" "SELECT token_count FROM gigatoken_body WHERE content_sha256='$ha' LIMIT 1")
if [ "$first_count" -le 1 ]; then echo 'fixture first body must exceed one token' >&2; exit 1; fi
small_budget=$((first_count - 1))
"$runner" "$tool" compile --rank-db "$tmp/rank.db" --run-id bitnet:test --address-db "$tmp/address.db" --material-manifest "$tmp/material.jsonl" --tokenizer "$GIGATOKEN_TOKENIZER" --tokenizer-passport qwen2.5-coder-tokenizer --gigatoken-bin "$GIGATOKEN_BIN" --token-cache-db "$tmp/tokens-lazy.db" --information-policy-class public --purpose "Explain the retained public body" --token-budget "$small_budget" --max-candidates 2 --out "$tmp/context-lazy.json" --admission-bin "$admission" --admission-db "$tmp/admission.db" --admission-plane context --admission-volume "$tmp" --admission-estimated-bytes 65536 --admission-free-bytes 20000000000 | grep -q '"state":"compiled"'
"$runner" "$tool" compile --rank-db "$tmp/rank.db" --run-id bitnet:test --address-db "$tmp/address.db" --material-manifest "$tmp/material.jsonl" --tokenizer "$GIGATOKEN_TOKENIZER" --tokenizer-passport qwen2.5-coder-tokenizer --gigatoken-bin "$GIGATOKEN_BIN" --token-cache-db "$tmp/tokens-eager.db" --information-policy-class public --purpose "Explain the retained public body" --token-budget "$small_budget" --max-candidates 4 --out "$tmp/context-eager.json" --admission-bin "$admission" --admission-db "$tmp/admission.db" --admission-plane context --admission-volume "$tmp" --admission-estimated-bytes 65536 --admission-free-bytes 20000000000 | grep -q '"state":"compiled"'
bun -e 'const [lazy,eager]=await Promise.all(process.argv.slice(1,3).map(async path=>Bun.file(path).json()));const bytes=(await Promise.all(process.argv.slice(3).map(async path=>(await Bun.file(path).arrayBuffer()).byteLength))).reduce((a,b)=>a+b,0);if(lazy.selected_candidates[0].identity!=="source:d"||lazy.context_sha256!==eager.context_sha256||lazy.token_sha256!==eager.token_sha256||JSON.stringify(lazy.selected_candidates)!==JSON.stringify(eager.selected_candidates)||lazy.token_cache.fallback_scans!==1||lazy.token_cache.native_batches!==2||lazy.token_cache.native_body_bytes!==bytes||lazy.token_cache.misses!==3||lazy.token_cache.exact_pruned_bodies!==0||eager.token_cache.native_batches!==1||eager.token_cache.native_body_bytes!==bytes)process.exit(1)' "$tmp/context-lazy.json" "$tmp/context-eager.json" "$tmp/a.body" "$tmp/c.body" "$tmp/d.body"
printf '%s\n' 'context cut: PASS'
