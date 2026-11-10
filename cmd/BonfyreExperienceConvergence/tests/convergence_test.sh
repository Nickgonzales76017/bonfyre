#!/bin/sh
set -eu

binding="$1"
participation="$2"
disturbance="$3"
boundary="$4"
readiness="$5"
stage="$6"

here="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
repo="$(CDPATH= cd -- "$here/../.." && pwd)"
map="$repo/cmd/BonfyreExperienceMap"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

grep -v -e 'bonfyre-domain-binding.v1' -e 'bonfyre-protocol-binding.v1' \
  "$map/generated/participation_evidence.jsonl" >"$tmp/evidence.jsonl"

"$binding" domain external:protocol-0304 \
  --observation-receipts "$tmp/evidence.jsonl" \
  --ingest-dir "$map/generated/ingest_protocol-0304" \
  --output "$tmp/evidence.jsonl" >"$tmp/domain.stdout"

"$binding" protocol external:protocol-0304 \
  --evidence "$tmp/evidence.jsonl" \
  --native-receipt "$map/generated/protocol_observation_bridge_receipt.json" \
  --output "$tmp/evidence.jsonl" >"$tmp/protocol.stdout"

"$participation" compile \
  --external-bindings "$map/generated/external_bindings.jsonl" \
  --evidence "$tmp/evidence.jsonl" \
  --manifest "$tmp/participation.json"

"$participation" activate \
  --external-bindings "$map/generated/external_bindings.jsonl" \
  --evidence "$tmp/evidence.jsonl" \
  --manifest "$tmp/activation.json" \
  --frontier "$tmp/frontier.jsonl" \
  --limit 250 >"$tmp/activation.stdout"

"$disturbance" audit \
  --trace "$map/generated/disturbance_trace.jsonl" \
  --output "$tmp/disturbance.json" >"$tmp/disturbance.stdout"

"$boundary" project \
  --receipts "$map/generated/disturbance_sensor_receipt.jsonl" \
  --output "$tmp/boundary.json"

"$readiness" audit \
  --evidence "$tmp/evidence.jsonl" \
  --protocol-witness "$map/generated/protocol_derivation_bridge_receipt.json" \
  --output "$tmp/readiness.json" >"$tmp/readiness.stdout"

"$stage" compile \
  --chains "$map/data/experience_chains.tsv" \
  --external-bindings "$map/generated/external_bindings.jsonl" \
  --external-raw "$repo/estate/generated/external-estate-v4/observed-external-estate.jsonl" \
  --ghost-bindings "$map/generated/ghost_bindings.jsonl" \
  --app-bindings "$map/generated/app_bindings.jsonl" \
  --output "$tmp/stage.jsonl" \
  --manifest "$tmp/stage.json"

python3 - "$tmp" "$map" <<'PY'
import json, pathlib, sys
tmp = pathlib.Path(sys.argv[1])
emap = pathlib.Path(sys.argv[2])

def load(path):
    return json.loads(path.read_text(encoding="utf-8"))

evidence = [json.loads(line) for line in (tmp / "evidence.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
domains = [r for r in evidence if r.get("schema") == "bonfyre-domain-binding.v1"]
protocols = [r for r in evidence if r.get("schema") == "bonfyre-protocol-binding.v1"]
assert len(domains) == 1 and domains[0]["binding_validated"] is True
assert len(protocols) == 1 and protocols[0]["binding_validated"] is True
assert domains[0]["occurrence_id"] == protocols[0]["occurrence_id"] == "external:protocol-0304"
assert domains[0]["semantic_record_sha256"] == protocols[0]["semantic_record_sha256"]

participation = load(tmp / "participation.json")
assert participation["total_occurrences"] == 4891
assert participation["measured_axis_counts"]["semantic"] == 1
assert participation["measured_axis_counts"]["protocol"] == 1

actual_frontier = [json.loads(line) for line in (tmp / "frontier.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
expected_frontier = [json.loads(line) for line in (emap / "generated/participation_activation_frontier.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
assert actual_frontier == expected_frontier
assert len(actual_frontier) == 250
assert actual_frontier[0]["id"] == "external:ext-02715"
assert actual_frontier[0]["activation_score"] == 171

readiness = load(tmp / "readiness.json")
assert readiness["overall_ready"] is True
assert readiness["bridges"]["ForeignTwinGraph->DomainProfile"]["validated_bindings"] == 1
assert readiness["bridges"]["ProtocolContract->SemanticDerivationGraph"]["joined_protocol_bindings"] == 1

boundary = load(tmp / "boundary.json")
assert boundary["attempts"] == 2
assert boundary["valid_receipts"] == 2
assert boundary["body_observed"] is False
assert boundary["states"] == {"policy_excluded": 1, "policy_indeterminate": 1}

disturbance = load(tmp / "disturbance.json")
assert disturbance["accepted"] is False
assert "no disturbance trace supplied" in disturbance["failures"]

stage = load(tmp / "stage.json")
expected_stage = load(emap / "generated/stage_manifest.json")
for key in ("binding_records", "chain_stage_edges", "human_chain_count", "records_with_stage_bindings", "unbound_experience_references", "digest"):
    assert stage[key] == expected_stage[key], (key, stage[key], expected_stage[key])
assert stage["digest"] == "4d9815879606b013dfd4baf92b3eafd1c4aab9db3a1372b627d2660a1abbf348"
PY

echo "experience native convergence: PASS (binding regenerated -> 4891 participation -> 250/250 activation -> boundary/readiness/disturbance -> exact stage digest)"
