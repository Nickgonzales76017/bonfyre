#!/bin/sh
set -eu

bin="${1:-./bonfyre-participation}"
here="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
repo="$(CDPATH= cd -- "$here/../.." && pwd)"
experience="$repo/cmd/BonfyreExperienceMap"
expected_frontier="$experience/generated/participation_activation_frontier.jsonl"
expected_manifest="$experience/generated/participation_activation_manifest.json"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

"$bin" activate \
  --external-bindings "$experience/generated/external_bindings.jsonl" \
  --evidence "$experience/generated/participation_evidence.jsonl" \
  --manifest "$tmp/manifest.json" \
  --frontier "$tmp/frontier.jsonl" \
  --limit 250 >"$tmp/run.json"

python3 - "$expected_frontier" "$tmp/frontier.jsonl" "$expected_manifest" "$tmp/manifest.json" <<'PY'
import json
import sys

expected_frontier, actual_frontier, expected_manifest, actual_manifest = sys.argv[1:]

def jsonl(path):
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]

with open(expected_manifest, encoding="utf-8") as handle:
    expected = json.load(handle)
with open(actual_manifest, encoding="utf-8") as handle:
    actual = json.load(handle)

want_rows = jsonl(expected_frontier)
got_rows = jsonl(actual_frontier)
if got_rows != want_rows:
    for index, (want, got) in enumerate(zip(want_rows, got_rows), start=1):
        if want != got:
            raise SystemExit(
                f"activation frontier mismatch at row {index}:\n"
                f"expected={json.dumps(want, sort_keys=True)}\n"
                f"actual={json.dumps(got, sort_keys=True)}"
            )
    raise SystemExit(f"activation frontier length mismatch: expected={len(want_rows)} actual={len(got_rows)}")

for key in (
    "total_occurrences",
    "frontier_rows",
    "leverage_mode_counts",
    "next_owner_counts",
    "cohort_heads",
):
    if actual.get(key) != expected.get(key):
        raise SystemExit(
            f"activation manifest mismatch for {key}:\n"
            f"expected={json.dumps(expected.get(key), sort_keys=True)}\n"
            f"actual={json.dumps(actual.get(key), sort_keys=True)}"
        )

if len(got_rows) != 250:
    raise SystemExit(f"activation frontier expected 250 rows, got {len(got_rows)}")
if got_rows[0]["id"] != "external:ext-02715" or got_rows[0]["activation_score"] != 171:
    raise SystemExit(f"unexpected activation head: {got_rows[0]}")

print("activation native compatibility: PASS (250/250 ranked rows exact; Deepnets=171)")
PY
