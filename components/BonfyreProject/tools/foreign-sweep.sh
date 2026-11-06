#!/bin/sh
# Drive ForeignTwin across a plan of occurrences.
#
# The estate has 4,891 occurrences and had observed ten. Everything above the
# observation axis -- a bound DomainProfile, a compiled contract, a realized
# braid of any depth -- was blocked on this one thing, and every receipt in the
# tree before now was a fixture written by hand.
#
# This is a driver and owns no semantics. ForeignTwin decides robots, retains
# bytes, computes digests and writes receipts; the plan decides what is worth a
# request; this decides only the pacing.
#
# Politeness is not optional and is not a setting to turn off:
#   one request per occurrence, never a crawl of a host
#   a delay between requests, defaulting to a quarter second
#   a conditional request whenever a prior receipt carries a validator
#   robots evaluated per request by the observer, which may refuse
#
# Cannot infer: a completed sweep is not a licence to anything it read. Each
# receipt records what the source said about reuse, and mostly sources say
# nothing, which is not permission.
set -eu

plan=${1:?usage: foreign-sweep.sh <plan.tsv> <out-dir> [limit] [delay-seconds]}
out=${2:?usage: foreign-sweep.sh <plan.tsv> <out-dir> [limit] [delay-seconds]}
limit=${3:-50}
delay=${4:-0.25}
observer=$(cd "$(dirname -- "$0")/.." && pwd)/bonfyre-project
lex=$(cd "$(dirname -- "$0")/../../.." && pwd)/cmd/BonfyreLex/bonfyre-lex
[ -x "$observer" ]
mkdir -p "$out/receipts" "$out/bodies"

observed=0 excluded=0 boundary=0 repeat=0 failed=0 attempted=0
while IFS="$(printf '\t')" read -r twin url host facets phenotype; do
  case "$twin" in ''|'#'*) continue ;; esac
  [ "$attempted" -lt "$limit" ] || break
  attempted=$((attempted + 1))
  slug=$(printf '%s' "$twin" | tr -c 'A-Za-z0-9._-' '_')
  receipt="$out/receipts/$slug.json"
  body="$out/bodies/$slug.body"
  # The observer exits non-zero for a refusal it has already recorded, so the
  # receipt is read for the outcome rather than the exit status.
  "$observer" foreign observe "$twin" "$url" --receipt "$receipt" --body "$body" \
    > /dev/null 2>&1 || true
  if [ -s "$receipt" ]; then
    kind=$("$lex" field --kind json_string --key observation_kind --line last --file "$receipt")
    case "$kind" in
      source_observed)      observed=$((observed + 1)) ;;
      repeat_observed)      repeat=$((repeat + 1)) ;;
      policy_excluded)      excluded=$((excluded + 1)) ;;
      boundary_response)    boundary=$((boundary + 1)) ;;
      *)                    failed=$((failed + 1)) ;;
    esac
  else
    failed=$((failed + 1))
  fi
  sleep "$delay"
done < "$plan"

# One receipt per line, which is what every downstream owner reads.
cat "$out"/receipts/*.json > "$out/observations.jsonl" 2>/dev/null || true

printf '{"schema":"bonfyre-foreign-sweep.v1","attempted":%d,"source_observed":%d,' \
  "$attempted" "$observed"
printf '"repeat_observed":%d,"policy_excluded":%d,"boundary_response":%d,"failed":%d,' \
  "$repeat" "$excluded" "$boundary" "$failed"
printf '"receipts":"%s/observations.jsonl",' "$out"
printf '"truth":"observations_with_receipts_not_a_licence_to_anything_read",'
printf '"cannot_infer":["a completed sweep -> permission to republish",'
printf '"a boundary response -> a source that does not exist",'
printf '"policy_excluded -> a source refusing us specifically; robots is a standing rule"]}\n'
