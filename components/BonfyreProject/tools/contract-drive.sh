#!/bin/sh
# Compile a protocol contract for every occurrence a real observation supports.
#
# This is the rung the estate could describe and had reached exactly once. The
# facet compiler already proposes candidate facets for all 4,880 typed
# occurrences; a candidate becomes a compiled contract only where a receipt
# observes that specific occurrence, the retained bytes still match the digest
# the receipt recorded, and the relation is admitted by a phenotype the source
# itself evidenced rather than by the neighbourhood it sits in.
#
# Everything here is local. Nothing is fetched: the observation already happened
# and this reads its receipt.
#
# Cannot infer: compiled -> enacted. A compiled contract is an interaction
# shape for this source, not a legal finding. It is not a request anyone made, an authority anyone
# granted, or an effect anyone authorized.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
sweep=${1:-$HOME/.bonfyre/foreign-sweep}
out=${2:-$HOME/.bonfyre/estate/contract-evidence.jsonl}
corpus=${CORPUS:-$root/estate/generated/external-estate-v4/observed-external-estate.jsonl}
facet="$root/cmd/BonfyreFacet/bonfyre-facet"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
if [ ! -x "$facet" ] || [ ! -x "$lex" ]; then
  echo 'read contract drive: native facet and lex executables required' >&2
  exit 1
fi
command -v bun >/dev/null
helper="$root/components/BonfyreProject/tools/read-protocol-contract.mjs"
bun "$helper" owner-values "$corpus" "$out"
work=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-read-drive.XXXXXX")
trap 'rm -f "$work/compiled.json"; rmdir "$work"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
: > "$out"
compile_failed=0 invalid_compiler=0

compiled=0 refused=0 considered=0
for receipt in "$sweep"/receipts/*.json; do
  [ -f "$receipt" ] || continue
  oid=$("$lex" field --kind json_string --key twin_id --line last --file "$receipt" 2>/dev/null) || continue
  case "$oid" in external:*) ;; *) continue ;; esac
  bun "$helper" owner-values "$oid" >/dev/null 2>&1 || { refused=$((refused + 1)); continue; }
  body=$("$lex" field --kind json_string --key body_path --line last --file "$receipt" 2>/dev/null) || continue
  [ -n "$body" ] && [ -f "$body" ] || continue
  considered=$((considered + 1))

  # The compiler proposes many facets; this carries only the one the estate
  # actually performed. Taking the first shape-derived facet compiled
  # AUTHENTICATE against a thousand sources nobody authenticated to, and PUBLISH
  # against sources whose own receipt says in as many words that robots allowing
  # a read is not a licence to republish. A contract for an interaction that
  # never happened and could not lawfully happen is worse than no contract.
  #
  # What happened is a read: a GET of the source's own representation, permitted
  # by its robots and recorded with a digest. DESCRIBE is that relation. QUERY
  # and DISCOVER are the weaker readings kept as fallbacks; nothing else is
  # eligible here.
  #
  # A facet resting on domain evidence is skipped rather than forced through
  # with an override, because a contract asserts a shape for this source and not
  # for sources like it.
  # Do not let a parseable stdout hide a failed native compilation.
  if ! "$facet" compile --corpus "$corpus" --occurrence "$oid" >"$work/compiled.json" 2>/dev/null; then
    compile_failed=$((compile_failed + 1)); refused=$((refused + 1)); continue
  fi
  pick_status=0
  pick=$(bun "$helper" pick "$work/compiled.json" 2>/dev/null) || pick_status=$?
  if [ "$pick_status" -ne 0 ]; then
    [ "$pick_status" -eq 4 ] || invalid_compiler=$((invalid_compiler + 1))
    refused=$((refused + 1)); continue
  fi
  [ -n "$pick" ] || { refused=$((refused + 1)); continue; }
  proto=${pick%% *}
  rel=${pick##* }

  if "$facet" contract --corpus "$corpus" --occurrence "$oid" --protocol "$proto" \
       --relation "$rel" --receipt "$receipt" --out "$out" >/dev/null 2>&1; then
    compiled=$((compiled + 1))
  else
    refused=$((refused + 1))
  fi
done

bun "$helper" contract-summary "$considered" "$compiled" "$refused" "$compile_failed" "$invalid_compiler"
