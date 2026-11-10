#!/bin/sh
set -eu

bin="${1:-./bonfyre-observation-boundary}"
here="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
repo="$(CDPATH= cd -- "$here/../.." && pwd)"
live="$repo/cmd/BonfyreExperienceMap/generated/disturbance_sensor_receipt.jsonl"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

"$bin" project --receipts "$live" --output "$tmp/live.json"
python3 - "$tmp/live.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as handle:
    row = json.load(handle)
assert row["schema"] == "bonfyre.observation-boundary.v1"
assert row["attempts"] == 2
assert row["valid_receipts"] == 2
assert row["rejected_receipts"] == 0
assert row["states"] == {"policy_excluded": 1, "policy_indeterminate": 1}
assert row["policy_decisions"] == {"robots_disallow": 1, "robots_preflight_unavailable": 1}
assert row["body_observed"] is False
assert "documented public feed -> lawful observation by this agent" in row["cannot_infer"]
PY

cat >"$tmp/adversarial.jsonl" <<'EOF'
{"schema":"bonfyre-foreign-observation.v1","twin_id":"external:fixture-good","source_url":"https://example.test/good","observed_at":"2026-09-15T00:00:00Z","observe_right":"public","robots_url":"https://example.test/robots.txt","robots_transport_exit":0,"robots_http_status":200,"policy_decision":"allow","fetch_attempted":true,"transport_exit":0,"http_status":200,"body_bytes":12,"content_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","availability_claim":false,"observation_kind":"source_observed"}
{"schema":"bonfyre-foreign-observation.v1","twin_id":"external:fixture-excluded-forged","source_url":"https://example.test/excluded","observed_at":"2026-09-15T00:00:00Z","observe_right":"public","robots_url":"https://example.test/robots.txt","robots_transport_exit":0,"robots_http_status":200,"policy_decision":"robots_disallow","fetch_attempted":true,"transport_exit":0,"http_status":200,"body_bytes":10,"content_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","availability_claim":false,"observation_kind":"policy_excluded"}
{"schema":"bonfyre-foreign-observation.v1","twin_id":"external:fixture-observed-forged","source_url":"https://example.test/forged","observed_at":"2026-09-15T00:00:00Z","observe_right":"public","robots_url":"https://example.test/robots.txt","robots_transport_exit":0,"robots_http_status":200,"policy_decision":"allow","fetch_attempted":true,"transport_exit":0,"http_status":200,"body_bytes":10,"content_sha256":"deadbeef","availability_claim":false,"observation_kind":"source_observed"}
EOF

"$bin" project --receipts "$tmp/adversarial.jsonl" --output "$tmp/adversarial.json"
python3 - "$tmp/adversarial.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as handle:
    row = json.load(handle)
assert row["attempts"] == 3
assert row["valid_receipts"] == 1
assert row["rejected_receipts"] == 2
assert row["states"] == {"source_observed": 1}
assert row["policy_decisions"] == {"allow": 1}
assert row["body_observed"] is True
PY

cat >"$tmp/no-body.jsonl" <<'EOF'
{"schema":"bonfyre-foreign-observation.v1","twin_id":"external:fixture-no-body","source_url":"https://example.test/no-body","observed_at":"2026-09-15T00:00:00Z","observe_right":"public","robots_url":"https://example.test/robots.txt","robots_transport_exit":0,"robots_http_status":200,"policy_decision":"allow","fetch_attempted":true,"transport_exit":0,"http_status":200,"body_bytes":0,"content_sha256":null,"availability_claim":false,"observation_kind":"source_observed"}
EOF

"$bin" project --receipts "$tmp/no-body.jsonl" --output "$tmp/no-body.json"
python3 - "$tmp/no-body.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as handle:
    row = json.load(handle)
assert row["attempts"] == 1 and row["valid_receipts"] == 0 and row["rejected_receipts"] == 1
assert row["states"] == {}
assert row["body_observed"] is False
PY

echo "observation boundary: PASS (historical sensor projection preserved; contradictory/forged body claims fail closed)"
