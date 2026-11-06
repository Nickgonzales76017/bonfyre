#!/bin/sh
set -eu
here=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
repo=$(CDPATH= cd -- "$here/../.." && pwd)
tmp=$(mktemp -d)
pid=
trap 'if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi; rm -rf "$tmp"' EXIT HUP INT TERM
cp -R "$here/tests/contracts/input/." "$tmp/"
export BONFYRE_ATLAS_DIR="$tmp/architecture"
compile() { "$here/bonfyre-atlas" compile-contracts "$@"; }
for mode in app-lineages carriers settlement institutions; do compile "$mode" >/dev/null; compile "$mode" --check-generated; done
for path in integrations/mcp/app_lineages_contract.json engine/core/include/bf_app_lineages.h integrations/mcp/carriers_contract.json cmd/BonfyreWebAddress/src/bf_settlement.h integrations/mcp/institution_contract.json engine/core/include/bf_institutions.h; do cmp "$tmp/$path" "$here/tests/contracts/expected/$path"; done
refuse() { if compile "$@" >"$tmp/out" 2>"$tmp/error"; then echo "unexpected contract acceptance: $*" >&2; exit 1; fi; }
cp "$tmp/integrations/mcp/app_lineages_contract.json" "$tmp/retained"
printf '\n  lineage invalid dir:x stage Doc - owns:first owns:second\n' >> "$tmp/architecture/apps/app-lineages.contract.yaff"
refuse app-lineages
cmp "$tmp/retained" "$tmp/integrations/mcp/app_lineages_contract.json"
cp "$here/tests/contracts/input/architecture/apps/app-lineages.contract.yaff" "$tmp/architecture/apps/app-lineages.contract.yaff"
printf 'stale\n' >> "$tmp/integrations/mcp/app_lineages_contract.json"
refuse app-lineages --check-generated
compile app-lineages >/dev/null
printf '\n  rail other\n' >> "$tmp/architecture/economic/settlement.contract.yaff"
refuse settlement
cp "$here/tests/contracts/input/architecture/economic/settlement.contract.yaff" "$tmp/architecture/economic/settlement.contract.yaff"
sed '/leadership_object/d' "$here/tests/contracts/input/architecture/institutional/namespaces.yaff" > "$tmp/architecture/institutional/namespaces.yaff"
refuse institutions
grep -q 'missing leadership_object' "$tmp/error"
printf '\n  role fractional 1.5\n' >> "$tmp/architecture/institutional/namespaces.yaff"
refuse institutions
grep -q 'invalid role rank' "$tmp/error"
cp "$here/tests/contracts/input/architecture/institutional/namespaces.yaff" "$tmp/architecture/institutional/namespaces.yaff"
refuse unknown
refuse carriers --unknown
# A missing JSON projection refuses without touching the existing C header.
cp "$tmp/engine/core/include/bf_institutions.h" "$tmp/retained"
rm "$tmp/integrations/mcp/institution_contract.json"
refuse institutions --check-generated
cmp "$tmp/retained" "$tmp/engine/core/include/bf_institutions.h"
compile institutions >/dev/null
cue vet -c "$tmp/integrations/mcp/app_lineages_contract.json" "$repo/architecture/contract-projections.cue" -d '#AppLineages'
cue vet -c "$tmp/integrations/mcp/carriers_contract.json" "$repo/architecture/contract-projections.cue" -d '#Carriers'
cue vet -c "$tmp/integrations/mcp/institution_contract.json" "$repo/architecture/contract-projections.cue" -d '#Institutions'
${CC:-cc} -std=c11 -Wall -Wextra -Werror "$here/tests/contracts-http.c" -o "$tmp/http"
"$tmp/http" > "$tmp/port" & pid=$!
i=0; while [ ! -s "$tmp/port" ]; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep 0.05; done
origin="http://127.0.0.1:$(cat "$tmp/port")"
compile carriers --check --origin="$origin" > "$tmp/probed"
grep -q 'okay *200' "$tmp/probed"; grep -q 'priced *402' "$tmp/probed"
printf 'carrier_contract fixture\n  carrier redirect /redirect\n' > "$tmp/architecture/external/carriers.contract.yaff"
compile carriers >/dev/null
compile carriers --check --origin="$origin" > "$tmp/probed"
grep -q 'redirect *200' "$tmp/probed"
printf 'carrier_contract fixture\n  carrier missing /missing\n' > "$tmp/architecture/external/carriers.contract.yaff"
compile carriers >/dev/null
refuse carriers --check --origin="$origin"
grep -q '1 declared carrier(s) not serving' "$tmp/error"
kill "$pid"; wait "$pid" 2>/dev/null || :; pid=
# --check-generated never probes an unreachable origin.
compile carriers --check-generated --origin="$origin" >/dev/null
refuse carriers --check --origin="$origin"
grep -q 'unreachable' "$tmp/error"
printf '%s\n' 'contracts: six JS-oracle projections, CUE shapes, refusal/preservation and local HTTP probes pass'
