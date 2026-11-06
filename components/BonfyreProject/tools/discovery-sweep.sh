#!/bin/sh
# Ask the services what they want, not what they are.
#
# The estate swept 3,316 primary URLs and got landing pages: what a service says
# about itself to a human. Only 16 of those carried a machine-readable
# declaration of required inputs, which read as "the internet is not asking for
# anything" when what it actually meant was that nobody had looked where the
# asking happens.
#
# The asking happens at the discovery paths. A service that wants to be called
# by an agent publishes /openapi.json, /.well-known/agent.json, /.well-known/x402
# or /.well-known/ai-plugin.json, and those documents declare, field by field,
# exactly what must be supplied to call it. That is demand in machine-readable
# form, standing, published, and free to read.
#
# Every request goes through the same observer as the first sweep, so robots is
# preflighted per path and a receipt with a digest is written for each. The twin
# id stays the occurrence's own: this is another observation of the same thing,
# at a path it publishes for exactly this purpose.
#
# Cannot infer: a declared input -> a need this estate can meet, or a service
# anyone wants called. It is what the service says it requires.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
plan=${1:?usage: discovery-sweep.sh <plan.tsv> <out-dir> [limit] [delay]}
out=${2:?out dir}
limit=${3:-4000}
delay=${4:-0.3}
observer=$(cd "$(dirname -- "$0")/.." && pwd)/bonfyre-project
lex=$(cd "$(dirname -- "$0")/../../.." && pwd)/cmd/BonfyreLex/bonfyre-lex
[ -x "$observer" ]
mkdir -p "$out/receipts" "$out/bodies"

found=0 empty=0 excluded=0 attempted=0
while IFS="$(printf '\t')" read -r twin url label; do
  case "$twin" in ''|'#'*) continue ;; esac
  [ "$attempted" -lt "$limit" ] || break
  attempted=$((attempted + 1))
  slug=$(printf '%s__%s' "$twin" "$label" | tr -c 'A-Za-z0-9._-' '_')
  receipt="$out/receipts/$slug.json"
  body="$out/bodies/$slug.body"
  "$observer" foreign observe "$twin" "$url" --receipt "$receipt" --body "$body" >/dev/null 2>&1 || true
  if [ -s "$receipt" ]; then
    kind=$("$lex" field --kind json_string --key observation_kind --line last --file "$receipt" 2>/dev/null || echo '')
    case "$kind" in
      source_observed|repeat_observed) found=$((found + 1)) ;;
      policy_excluded)                 excluded=$((excluded + 1)) ;;
      *)                               empty=$((empty + 1)) ;;
    esac
  else
    empty=$((empty + 1))
  fi
  sleep "$delay"
done < "$plan"

printf '{"schema":"bonfyre-discovery-sweep.v1","attempted":%d,"declarations_retained":%d,' \
  "$attempted" "$found"
printf '"no_declaration":%d,"policy_excluded":%d,' "$empty" "$excluded"
printf '"truth":"documents_services_publish_so_agents_can_call_them",'
printf '"cannot_infer":["a declared input -> a need this estate can meet",'
printf '"a discovery document -> a service anyone wants called"]}\n'
