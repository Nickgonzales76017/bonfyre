#!/bin/sh
set -eu

bin="${1:-./bonfyre-participation}"
root="$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)"
experience="$root/cmd/BonfyreExperienceMap"
expected="$experience/generated/participation_manifest.json"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

run() {
  evidence="$1"
  manifest="$2"
  "$bin" compile \
    --external-bindings "$experience/generated/external_bindings.jsonl" \
    --evidence "$evidence" \
    --manifest "$manifest"
}

compact() { tr -d '[:space:]' < "$1"; }
object() {
  key="$1"
  file="$2"
  compact "$file" | sed -n "s/.*\"$key\":{\([^}]*\)}.*/\1/p"
}
scalar() {
  key="$1"
  file="$2"
  compact "$file" | sed -n "s/.*\"$key\":\([0-9][0-9]*\).*/\1/p"
}

run "$experience/generated/participation_evidence.jsonl" "$tmp/manifest.json" >"$tmp/run.json"

for key in candidate_axis_counts measured_axis_counts join_power_distribution braid_depth_distribution; do
  want="$(object "$key" "$expected")"
  got="$(object "$key" "$tmp/manifest.json")"
  [ -n "$want" ] && [ "$got" = "$want" ] || {
    echo "participation mismatch: $key expected={$want} got={$got}" >&2
    exit 1
  }
done

for key in total_occurrences owner_evidence_rows frontier_limit; do
  want="$(scalar "$key" "$expected")"
  got="$(scalar "$key" "$tmp/manifest.json")"
  [ -n "$want" ] && [ "$got" = "$want" ] || {
    echo "participation mismatch: $key expected=$want got=$got" >&2
    exit 1
  }
done

cp "$experience/generated/participation_evidence.jsonl" "$tmp/evidence.jsonl"
cat >>"$tmp/evidence.jsonl" <<'EOF'
{"schema":"bonfyre-owner-participation.v1","occurrence_id":"external:ext-00001","axis":"economic","state":"settled","owner":"MoneyGraph","from_owner":"Quote","to_owner":"MoneyGraph","produced_by":"forged","basis_sha256":"no","evidence_sha256":"no"}
EOF
run "$tmp/evidence.jsonl" "$tmp/forged.json" >"$tmp/forged-run.json"

base_measured="$(object measured_axis_counts "$tmp/manifest.json")"
forged_measured="$(object measured_axis_counts "$tmp/forged.json")"
[ "$base_measured" = "$forged_measured" ] || {
  echo "invalid owner receipt created realized progress" >&2
  exit 1
}
[ "$(scalar owner_evidence_rows "$tmp/forged.json")" = 17 ] || {
  echo "raw evidence row accounting lost invalid row" >&2
  exit 1
}

echo "participation native compatibility: PASS ($(scalar total_occurrences "$tmp/manifest.json") occurrences; fail-closed forged receipt)"
