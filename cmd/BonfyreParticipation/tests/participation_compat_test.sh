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
normalized_object() {
  key="$1"
  file="$2"
  object "$key" "$file" | tr ',' '\n' | sort | tr '\n' ','
}
scalar() {
  key="$1"
  file="$2"
  compact "$file" | sed -n "s/.*\"$key\":\([0-9][0-9]*\).*/\1/p"
}

if ! run "$experience/generated/participation_evidence.jsonl" "$tmp/manifest.json" >"$tmp/run.json"; then
  echo "participation baseline compile failed (see native stage error above)" >&2
  exit 1
fi

for key in candidate_axis_counts measured_axis_counts join_power_distribution braid_depth_distribution; do
  want="$(normalized_object "$key" "$expected")"
  got="$(normalized_object "$key" "$tmp/manifest.json")"
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
if ! run "$tmp/evidence.jsonl" "$tmp/forged.json" >"$tmp/forged-run.json"; then
  echo "participation forged-receipt compile failed (see native stage error above)" >&2
  exit 1
fi

base_measured="$(normalized_object measured_axis_counts "$tmp/manifest.json")"
forged_measured="$(normalized_object measured_axis_counts "$tmp/forged.json")"
[ "$base_measured" = "$forged_measured" ] || {
  echo "invalid owner receipt created realized progress" >&2
  exit 1
}
base_rows="$(scalar owner_evidence_rows "$tmp/manifest.json")"
forged_rows="$(scalar owner_evidence_rows "$tmp/forged.json")"
case "$base_rows:$forged_rows" in
  *[!0-9:]* | :* | *:) echo "owner evidence count is non-numeric" >&2; exit 1 ;;
esac
[ "$forged_rows" -eq "$((base_rows + 1))" ] || {
  echo "raw evidence row accounting lost invalid row: before=$base_rows after=$forged_rows" >&2
  exit 1
}

echo "participation native compatibility: PASS ($(scalar total_occurrences "$tmp/manifest.json") occurrences; fail-closed forged receipt)"
