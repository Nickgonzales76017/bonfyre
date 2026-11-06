#!/bin/sh
set -eu
bin=${1:?binary}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
sqlite3 "$tmp/address.db" "CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT);CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);INSERT INTO candidate_advertisement VALUES('source:a','external_occurrence','ExternalEstate:a','ForeignTwin:admission','MaterialCompiler:external');INSERT INTO candidate_advertisement_term VALUES('source:a','domain','weather');"
printf '%s\n' '{"survivors":[{"identity":"source:a"}]}' > "$tmp/forward.json"
sqlite3 "$tmp/model.db" "CREATE TABLE model_representation_passport(representation_id TEXT,measurement_state TEXT,admission_state TEXT,admission_scope TEXT);INSERT INTO model_representation_passport VALUES('bitnet','measured','DEFER','candidate_reorder');"
# No endpoint may be contacted when the exact representation gate is not ADMIT.
if "$bin" reorder --address-db "$tmp/address.db" --forward-result "$tmp/forward.json" --model-db "$tmp/model.db" --representation bitnet --scope candidate_reorder --endpoint http://127.0.0.1:1 --query weather --out "$tmp/out.db"; then
  echo 'unadmitted BitNet representation ran' >&2; exit 1
fi
# Passport admission is not current physical admission.  Even an ADMIT
# representation must be stopped before HTTP when the native grant ledger
# says the declared rerank workload cannot fit.
sqlite3 "$tmp/model.db" "UPDATE model_representation_passport SET admission_state='ADMIT';"
if "$bin" reorder --address-db "$tmp/address.db" --forward-result "$tmp/forward.json" --model-db "$tmp/model.db" --representation bitnet --scope candidate_reorder --endpoint http://127.0.0.1:1 --query weather --out "$tmp/out.db" --admission-db "$tmp/admission.db" --admission-volume test --admission-plane model-bitnet --admission-estimated-bytes 1024 --admission-free-bytes 1 > "$tmp/admission.out" 2>&1; then
  echo 'resource-unadmitted BitNet representation ran' >&2; exit 1
fi
grep -q 'BitNet ResourceAdmission' "$tmp/admission.out"
# Widening consumes the persisted full frontier; it must not need an endpoint
# or issue another embedding call.  The still-ADMIT passport is rechecked
# before it can publish the next declared lowerer envelope.
sqlite3 "$tmp/rank.db" "CREATE TABLE bitnet_rank_run(run_id TEXT PRIMARY KEY,representation_id TEXT,scope TEXT,query_digest TEXT,gate_state TEXT,dimensions INTEGER,candidate_count INTEGER,created_at INTEGER);CREATE TABLE ranked_candidate_envelope(run_id TEXT,rank INTEGER,candidate_identity TEXT,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,PRIMARY KEY(run_id,rank));INSERT INTO bitnet_rank_run VALUES('run-1','bitnet','candidate_reorder','q','ADMIT',4,2,0);INSERT INTO ranked_candidate_envelope VALUES('run-1',0,'protocol:x402','protocol_chain','ProtocolCommons:x402','ProtocolContract:conformance','ProtocolCompiler:late_carrier');INSERT INTO ranked_candidate_envelope VALUES('run-1',1,'protocol:beckn','protocol_chain','ProtocolCommons:beckn','ProtocolContract:conformance','ProtocolCompiler:late_carrier');"
sqlite3 "$tmp/rank.db" "CREATE TABLE embedding_projection(run_id TEXT,candidate_identity TEXT,representation_id TEXT,source_digest TEXT,vector_digest TEXT,vector BLOB,score REAL,exact_verifier TEXT,lowerer_ref TEXT,causal_frontier TEXT);INSERT INTO embedding_projection VALUES('run-1','protocol:x402','bitnet','src-a','vec-a',X'00',0.9,'ProtocolContract:conformance','ProtocolCompiler:late_carrier','AddressPlane:forward-result');INSERT INTO embedding_projection VALUES('run-1','protocol:beckn','bitnet','src-b','vec-b',X'01',0.8,'ProtocolContract:conformance','ProtocolCompiler:late_carrier','AddressPlane:forward-result');"
"$bin" evidence --rank-db "$tmp/rank.db" --run-id run-1 --out "$tmp/evidence.jsonl" > "$tmp/evidence-output.json"
[ "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["records"])' "$tmp/evidence-output.json")" = 2 ]
[ "$(grep -c 'bonfyre-representation-use-evidence.v1' "$tmp/evidence.jsonl")" = 2 ]
[ "$(grep -c '"exact_owner_queried":false' "$tmp/evidence.jsonl")" = 2 ]
# Retention quality is measured only against a complete independently emitted
# ForeignTwin owner-query ledger. One missing candidate must fail closed; a
# good retained-body concentration still leaves the representation's gate
# untouched and cannot hand the ranked slice to a lowerer.
printf '%s\n' \
 '{"schema":"bonfyre-exact-owner-query-evidence.v1","run_id":"run-1","representation_id":"bitnet","gate_state":"ADMIT","candidate_identity":"protocol:x402","exact_verifier_ref":"ForeignTwin:admission","query_state":"queried","outcome":"observed"}' \
 '{"schema":"bonfyre-exact-owner-query-evidence.v1","run_id":"run-1","representation_id":"bitnet","gate_state":"ADMIT","candidate_identity":"protocol:beckn","exact_verifier_ref":"ForeignTwin:admission","query_state":"queried","outcome":"unobserved"}' > "$tmp/exact-owner.jsonl"
head -1 "$tmp/exact-owner.jsonl" > "$tmp/incomplete-owner.jsonl"
if "$bin" evaluate-retention --rank-db "$tmp/rank.db" --run-id run-1 --exact-owner-evidence "$tmp/incomplete-owner.jsonl" --out "$tmp/incomplete-evaluation.json"; then
  echo 'incomplete exact owner ledger produced a retention evaluation' >&2; exit 1
fi
"$bin" evaluate-retention --rank-db "$tmp/rank.db" --run-id run-1 --exact-owner-evidence "$tmp/exact-owner.jsonl" --out "$tmp/retention.json" > "$tmp/retention-output.json"
python3 - "$tmp/retention.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d['frontier_candidates'] == 2
assert d['retained_observations'] == 1
assert d['baseline_retained_precision'] == 0.5
assert d['top_k'][0]['k'] == 1 and d['top_k'][0]['retained_precision'] == 1.0
assert d['admission_decision'] == 'unchanged'
assert d['truth'] == 'exact_retained_observation_coverage_measurement_only'
PY
"$bin" expand --rank-db "$tmp/rank.db" --run-id run-1 --model-db "$tmp/model.db" --representation bitnet --scope candidate_reorder --handoff-dir "$tmp/widen" --offset 1 --top-k 1 > "$tmp/widen.out"
grep -q '"written":1' "$tmp/widen.out"
[ -f "$tmp/widen/ProtocolCompiler-late_carrier/0001-protocol_beckn.json" ]
grep -q '"identity":"protocol:beckn"' "$tmp/widen/ProtocolCompiler-late_carrier/0001-protocol_beckn.json"
echo 'BonfyreBitNet contract: PASS'
