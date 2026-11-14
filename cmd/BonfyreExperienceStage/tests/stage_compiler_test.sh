#!/bin/sh
set -eu

bin="${1:-./bonfyre-experience-stage}"
root="$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)"
experience="$root/cmd/BonfyreExperienceMap"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

# Validate the native compiler against a small source-backed contract before
# attempting the separately retained production-size corpus.
printf 'id\tterritories\tentry_intents\tstages\nproof\tlife\tinspect\tfield-or-herd|weather|harvest-or-output|market|sale\n' > "$tmp/fixture-chains.tsv"
printf '%s\n' '{"id":"external:weather","projection":{"candidate_affordances":["weather"],"estate_roles":[],"direct_domains":["agriculture"]},"truth_level":"observed_candidate"}' > "$tmp/fixture-raw.jsonl"
printf '%s\n' '{"record_kind":"external","id":"external:weather","name":"Weather","human_chains":["proof"]}' > "$tmp/fixture-external.jsonl"
printf '%s\n' '{"record_kind":"ghost","id":"ghost:one","name":"Ghost","human_chains":["proof"],"categories":[],"presentation_families":[],"renderer_mixes":[]}' > "$tmp/fixture-ghost.jsonl"
printf '%s\n' '{"record_kind":"app","id":"app:one","name":"Weather App","human_chains":["proof"],"reference_only":true,"source_ref":"donor"}' > "$tmp/fixture-app.jsonl"
if ! "$bin" compile --chains "$tmp/fixture-chains.tsv" --external-bindings "$tmp/fixture-external.jsonl" --external-raw "$tmp/fixture-raw.jsonl" \
  --ghost-bindings "$tmp/fixture-ghost.jsonl" --app-bindings "$tmp/fixture-app.jsonl" \
  --output "$tmp/fixture-stage.jsonl" --manifest "$tmp/fixture-manifest.json" >"$tmp/fixture-run.json"; then
  echo "experience stage native fixture compile failed" >&2; exit 1
fi
[ "$(wc -l < "$tmp/fixture-stage.jsonl" | tr -d ' ')" = 3 ] || { echo "experience stage fixture row count failed" >&2; exit 1; }
grep -q '"weather"' "$tmp/fixture-stage.jsonl" || { echo "experience stage source-bound weather stage missing" >&2; exit 1; }
echo "experience stage native compiler fixture: PASS (3 source records)"
for required in "$experience/generated/external_bindings.jsonl" "$experience/generated/ghost_bindings.jsonl" \
  "$experience/generated/app_bindings.jsonl" "$root/estate/generated/external-estate-v4/observed-external-estate.jsonl"; do
  [ -r "$required" ] || { echo "experience stage corpus unavailable: $required (regenerate from canonical source, do not synthesize)" >&2; exit 2; }
done
"$bin" compile \
  --chains "$experience/data/experience_chains.tsv" \
  --external-bindings "$experience/generated/external_bindings.jsonl" \
  --external-raw "$root/estate/generated/external-estate-v4/observed-external-estate.jsonl" \
  --ghost-bindings "$experience/generated/ghost_bindings.jsonl" \
  --app-bindings "$experience/generated/app_bindings.jsonl" \
  --output "$tmp/stage_bindings.jsonl" \
  --manifest "$tmp/stage_manifest.json" >"$tmp/run.json"

value() {
  key="$1"
  file="$2"
  sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\\([0-9][0-9]*\\).*/\\1/p" "$file" | head -n 1
}
digest() {
  sed -n 's/.*"digest"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$1" | head -n 1
}

expected="$experience/generated/stage_manifest.json"
for key in binding_records chain_stage_edges records_with_stage_bindings unbound_experience_references human_chain_count; do
  want="$(value "$key" "$expected")"
  got="$(value "$key" "$tmp/stage_manifest.json")"
  [ -n "$want" ] && [ "$got" = "$want" ] || {
    echo "stage compiler mismatch: $key expected=$want got=$got" >&2
    exit 1
  }
done

want_digest="$(digest "$expected")"
got_digest="$(digest "$tmp/stage_manifest.json")"
[ -n "$want_digest" ] && [ "$got_digest" = "$want_digest" ] || {
  echo "stage compiler digest mismatch: expected=$want_digest got=$got_digest" >&2
  exit 1
}

records="$(value binding_records "$tmp/stage_manifest.json")"
lines="$(wc -l < "$tmp/stage_bindings.jsonl" | tr -d ' ')"
[ "$lines" = "$records" ] || {
  echo "stage compiler row mismatch: manifest=$records jsonl=$lines" >&2
  exit 1
}

echo "experience stage native compatibility: PASS ($records records; $got_digest)"
