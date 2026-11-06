#!/bin/sh
set -eu

bin="${1:-./bonfyre-disturbance-audit}"
here="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
repo="$(CDPATH= cd -- "$here/../.." && pwd)"
expected_manifest="$repo/cmd/BonfyreExperienceMap/generated/participation_activation_manifest.json"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

"$bin" audit --trace "$tmp/missing.jsonl" --output "$tmp/empty.json" >/dev/null

cat >"$tmp/emergent.jsonl" <<'EOF'
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e01","owner":"ForeignTwinGraph","kind":"disturbance_observed","subject":"festival:fixture","causal_parents":[],"evidence_ref":"receipt:fixture:01"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e02","owner":"DomainProfile","kind":"interpreted","subject":"festival:fixture","causal_parents":["e01"],"evidence_ref":"receipt:fixture:02"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e03","owner":"SemanticDerivationGraph","kind":"assumption_retracted","subject":"festival:fixture","causal_parents":["e02"],"evidence_ref":"receipt:fixture:03"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e04","owner":"EvidenceGraph","kind":"evidence_changed","subject":"festival:fixture","causal_parents":["e03"],"evidence_ref":"receipt:fixture:04"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e05","owner":"BlockerGraph","kind":"blocker_opened","subject":"festival:fixture","causal_parents":["e04"],"evidence_ref":"receipt:fixture:05"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e06","owner":"UnlockGraph","kind":"join_admitted","subject":"festival:fixture","causal_parents":["e05"],"evidence_ref":"receipt:fixture:06"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e07","owner":"ActorGraph","kind":"human_decision","subject":"festival:fixture","causal_parents":["e06"],"evidence_ref":"receipt:fixture:07"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e08","owner":"WorkGraph","kind":"work_opened","subject":"festival:fixture","causal_parents":["e07"],"evidence_ref":"receipt:fixture:08"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e09","owner":"CommitmentGraph","kind":"commitment_opened","subject":"festival:fixture","causal_parents":["e08"],"evidence_ref":"receipt:fixture:09"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e10","owner":"MoneyGraph","kind":"quoted","subject":"festival:fixture","causal_parents":["e09"],"evidence_ref":"receipt:fixture:10"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e11","owner":"PublicationProgram","kind":"publication_projected","subject":"festival:fixture","causal_parents":["e10"],"evidence_ref":"receipt:fixture:11"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e12","owner":"RoundTripTwin","kind":"roundtrip_checked","subject":"festival:fixture","causal_parents":["e11"],"evidence_ref":"receipt:fixture:12","verdict":"drifted"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e13","owner":"WorkGraph","kind":"correction_opened","subject":"festival:fixture","causal_parents":["e12"],"evidence_ref":"receipt:fixture:13"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e14","owner":"CommitmentGraph","kind":"commitment_compensated","subject":"festival:fixture","causal_parents":["e13"],"evidence_ref":"receipt:fixture:14"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e15","owner":"MoneyGraph","kind":"settled","subject":"festival:fixture","causal_parents":["e14"],"evidence_ref":"receipt:fixture:15"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:fixture","event_id":"e16","owner":"BlockerGraph","kind":"blocker_resolved","subject":"festival:fixture","causal_parents":["e15"],"evidence_ref":"receipt:fixture:16"}
EOF
"$bin" audit --trace "$tmp/emergent.jsonl" --output "$tmp/emergent.json" >/dev/null

: >"$tmp/scripted.jsonl"
i=1
while [ "$i" -le 47 ]; do
  printf '{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:scripted","event_id":"step-%s","owner":"WorkGraph","kind":"scripted_step","subject":"festival:fixture","causal_parents":[],"step":%s}\n' "$i" "$i" >>"$tmp/scripted.jsonl"
  i=$((i + 1))
done
"$bin" audit --trace "$tmp/scripted.jsonl" --output "$tmp/scripted.json" >/dev/null

cat >"$tmp/cycle.jsonl" <<'EOF'
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:cycle","event_id":"a","owner":"WorkGraph","kind":"work_opened","subject":"fixture","causal_parents":["b"],"evidence_ref":"receipt:a"}
{"schema":"bonfyre.disturbance-event.v1","disturbance_id":"storm:cycle","event_id":"b","owner":"BlockerGraph","kind":"blocker_opened","subject":"fixture","causal_parents":["a"],"evidence_ref":"receipt:b"}
EOF
"$bin" audit --trace "$tmp/cycle.jsonl" --output "$tmp/cycle.json" >/dev/null

python3 - "$expected_manifest" "$tmp/empty.json" "$tmp/emergent.json" "$tmp/scripted.json" "$tmp/cycle.json" <<'PY'
import json
import sys

expected_path, empty_path, emergent_path, scripted_path, cycle_path = sys.argv[1:]
with open(expected_path, encoding="utf-8") as handle:
    historical = json.load(handle)["disturbance_acceptance"]
with open(empty_path, encoding="utf-8") as handle:
    empty = json.load(handle)
with open(emergent_path, encoding="utf-8") as handle:
    emergent = json.load(handle)
with open(scripted_path, encoding="utf-8") as handle:
    scripted = json.load(handle)
with open(cycle_path, encoding="utf-8") as handle:
    cycle = json.load(handle)

for key in ("schema", "accepted", "metrics", "failures", "cannot_infer"):
    if empty.get(key) != historical.get(key):
        raise SystemExit(f"historical empty-trace mismatch for {key}: expected={historical.get(key)!r} actual={empty.get(key)!r}")

if not emergent["accepted"] or emergent["failures"]:
    raise SystemExit(f"emergent fixture rejected: {emergent['failures']}")
if emergent["metrics"]["max_owner_braid_depth"] < 8:
    raise SystemExit(f"owner braid depth too shallow: {emergent['metrics']['max_owner_braid_depth']}")
if emergent["metrics"]["round_trip"] != {"drifted": 1}:
    raise SystemExit(f"round-trip verdict mismatch: {emergent['metrics']['round_trip']}")

if scripted["accepted"]:
    raise SystemExit("scripted 47-step trace was incorrectly accepted")
if scripted["metrics"]["max_event_depth"] != 1 or scripted["metrics"]["max_owner_braid_depth"] != 0:
    raise SystemExit(f"step numbers leaked into causal depth: {scripted['metrics']}")
if not any("owner evidence" in failure for failure in scripted["failures"]):
    raise SystemExit("scripted trace did not fail missing owner evidence")
if not any("one root disturbance" in failure for failure in scripted["failures"]):
    raise SystemExit("scripted trace did not fail root cardinality")

if cycle["accepted"] or not any("cycle" in failure for failure in cycle["failures"]):
    raise SystemExit(f"causal cycle was not rejected: {cycle['failures']}")

print("disturbance audit: PASS (historical empty exact; emergent accepted; scripted/cycle fail closed)")
PY
