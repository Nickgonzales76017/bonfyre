#!/bin/sh
set -eu

bin="${1:-./bonfyre-experience-stage}"
root="$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)"
experience="$root/cmd/BonfyreExperienceMap"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

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
