#!/bin/sh
# Carry retained observations the rest of the way down the ladder.
#
# A receipt with a body is observation rung 3 or 4 and nothing more. The rungs
# past it are owned by other architectures: Ingest normalizes the bytes,
# DomainProfile binds them to a semantic record, ProtocolContract compiles a
# contract over that binding. Each hop is a real owner leaving occurrence-keyed
# evidence, which is what turns a set of separate reports into a chain.
#
# Most sources will not make it past DomainProfile and that is the design. The
# binding requires a name, an authority, a description and a source url out of
# the retained body; a page that publishes none of those has not told this
# estate enough to be bound, and inventing the fields would be the laundering
# the ladder exists to prevent. A refusal here is a real answer about a source.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
sweep=${1:-$HOME/.bonfyre/foreign-sweep}
out=${2:-$HOME/.bonfyre/estate/chain-evidence.jsonl}
work=${WORK_DIR:-$HOME/.bonfyre/estate/chain}
ingest="$root/cmd/BonfyreIngest/bonfyre-ingest"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
[ -x "$ingest" ] || { echo "build cmd/BonfyreIngest first" >&2; exit 1; }
mkdir -p "$work"
: > "$out"

bound=0 refused=0 no_body=0 considered=0
for receipt in "$sweep"/receipts/*.json; do
  [ -f "$receipt" ] || continue
  oid=$("$lex" field --kind json_string --key twin_id --line last --file "$receipt" 2>/dev/null) || continue
  case "$oid" in external:*) ;; *) continue ;; esac
  body=$("$lex" field --kind json_string --key body_path --line last --file "$receipt" 2>/dev/null) || { no_body=$((no_body+1)); continue; }
  [ -n "$body" ] && [ -f "$body" ] || { no_body=$((no_body+1)); continue; }
  considered=$((considered + 1))

  slug=$(printf '%s' "$oid" | tr -c 'A-Za-z0-9._-' '_')
  dir="$work/$slug"
  rm -rf "$dir"; mkdir -p "$dir"
  # Ingest owns normalization. Its manifest is what DomainProfile checks the
  # retained bytes against, so a body that changed under us fails here rather
  # than being bound to something nobody observed.
  "$ingest" "$body" "$dir" >/dev/null 2>&1 || { refused=$((refused+1)); continue; }

  if bun cmd/BonfyreExperienceMap/tools/domain_observation_bridge.mjs "$oid" \
       --observation-receipts "$receipt" --ingest-dir "$dir" --output "$out" >/dev/null 2>&1; then
    bound=$((bound + 1))
  else
    refused=$((refused + 1))
  fi
done

printf '{"schema":"bonfyre-observation-chain.v1","considered":%d,"profile_bound":%d,"refused":%d,"no_retained_body":%d,' \
  "$considered" "$bound" "$refused" "$no_body"
printf '"truth":"bindings_over_retained_bodies_not_a_claim_any_source_is_correct",'
printf '"cannot_infer":["a refusal -> a bad source; it is a source that published too little to bind",'
printf '"profile_bound -> the record being true; it is this estate reading what the source said about itself"]}\n'
