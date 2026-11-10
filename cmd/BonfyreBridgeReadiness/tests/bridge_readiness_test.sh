#!/bin/sh
set -eu

bin="${1:-./bonfyre-bridge-readiness}"
here="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
repo="$(CDPATH= cd -- "$here/../.." && pwd)"
experience="$repo/cmd/BonfyreExperienceMap"
evidence="$experience/generated/participation_evidence.jsonl"
witness="$experience/generated/protocol_derivation_bridge_receipt.json"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

run_case() {
  evidence_path="$1"
  witness_path="$2"
  output_path="$3"
  "$bin" audit \
    --evidence "$evidence_path" \
    --protocol-witness "$witness_path" \
    --output "$output_path" >/dev/null
}

run_case "$evidence" "$witness" "$tmp/live.json"
python3 - "$tmp/live.json" <<'PY'
import json, sys
row = json.load(open(sys.argv[1], encoding="utf-8"))
assert row["schema"] == "bonfyre.bridge-readiness.v2"
assert row["truth_level"] == "receipt_backed_projection"
assert row["overall_ready"] is True
domain = row["bridges"]["ForeignTwinGraph->DomainProfile"]
protocol = row["bridges"]["ProtocolContract->SemanticDerivationGraph"]
assert domain["ready"] is True and domain["validated_bindings"] == 1
assert protocol["ready"] is True and protocol["validated_protocol_bindings"] == 1
assert protocol["joined_protocol_bindings"] == 1
assert protocol["registration_witness_valid"] is True
assert domain["source_text_used"] is False and protocol["source_text_used"] is False
PY

python3 - "$evidence" "$tmp/forged-hash.jsonl" "$tmp/detached.jsonl" <<'PY'
import json, sys
source, forged_path, detached_path = sys.argv[1:]
rows = [json.loads(line) for line in open(source, encoding="utf-8") if line.strip()]

forged = []
for row in rows:
    copy = dict(row)
    if copy.get("schema") == "bonfyre-domain-binding.v1":
        copy["semantic_record_sha256"] = "not-a-sha"
    forged.append(copy)
with open(forged_path, "w", encoding="utf-8") as out:
    for row in forged:
        out.write(json.dumps(row, sort_keys=True) + "\n")

detached = []
for row in rows:
    copy = dict(row)
    if copy.get("schema") == "bonfyre-protocol-binding.v1":
        copy["occurrence_id"] = "external:detached-fixture"
    detached.append(copy)
with open(detached_path, "w", encoding="utf-8") as out:
    for row in detached:
        out.write(json.dumps(row, sort_keys=True) + "\n")
PY

run_case "$tmp/forged-hash.jsonl" "$witness" "$tmp/forged-hash.json"
run_case "$tmp/detached.jsonl" "$witness" "$tmp/detached.json"

python3 - "$witness" "$tmp/non-atomic.json" <<'PY'
import json, sys
row = json.load(open(sys.argv[1], encoding="utf-8"))
row["atomic"] = False
json.dump(row, open(sys.argv[2], "w", encoding="utf-8"), sort_keys=True)
PY
run_case "$evidence" "$tmp/non-atomic.json" "$tmp/non-atomic-output.json"

python3 - "$tmp/forged-hash.json" "$tmp/detached.json" "$tmp/non-atomic-output.json" <<'PY'
import json, sys
forged, detached, non_atomic = [json.load(open(path, encoding="utf-8")) for path in sys.argv[1:]]

domain = forged["bridges"]["ForeignTwinGraph->DomainProfile"]
protocol = forged["bridges"]["ProtocolContract->SemanticDerivationGraph"]
assert forged["overall_ready"] is False
assert domain["ready"] is False and domain["rejected_bindings"] == 1
assert protocol["joined_protocol_bindings"] == 0

domain = detached["bridges"]["ForeignTwinGraph->DomainProfile"]
protocol = detached["bridges"]["ProtocolContract->SemanticDerivationGraph"]
assert detached["overall_ready"] is False
assert domain["ready"] is True
assert protocol["validated_protocol_bindings"] == 1
assert protocol["joined_protocol_bindings"] == 0
assert protocol["ready"] is False

protocol = non_atomic["bridges"]["ProtocolContract->SemanticDerivationGraph"]
assert non_atomic["overall_ready"] is False
assert protocol["registration_witness_valid"] is False
assert protocol["ready"] is False
PY

printf '%s\n' \
  "bridge readiness: PASS (receipt-only live join; forged hash/detached occurrence/non-atomic witness fail closed)"
