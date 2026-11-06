#!/bin/sh
# Public Bonfyre CLI -> semantic shell -> existing owner conformance.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-semantic-shell.XXXXXX")
trap 'rm -rf "$state"' EXIT
mkdir -p "$state/control" "$state/household/food" "$state/household/housing" \
  "$state/household/cross-domain" "$state/receipts"
printf '%s\n' '{"observations":[{"provider":"codex","state":"ACTIVE"}]}' \
  >"$state/control/continuous-provider-weather.json"
printf '%s\n' '{"schema":"food.v1"}' >"$state/household/food/basket-options.json"
printf '%s\n' '{"schema":"housing.v1"}' >"$state/household/housing/effective-costs.json"
printf '%s\n' '{"schema":"circuit.v1","cross_domain_join":{"state":"no_explicit_residence_identity_join"},"constraints":[]}' \
  >"$state/household/cross-domain/economic-circuit.json"
printf '%s\n' '{"work_item":"food-state","summary":"food realization"}' >"$state/receipts/food.json"

make -C "$root/cmd/BonfyreCLI" >/dev/null
make -C "$root/cmd/BonfyreTime" >/dev/null
make -C "$root/cmd/BonfyreHash" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
cli="$root/cmd/BonfyreCLI/bonfyre"
fabric="$root/programs/bonfyre/bonfyre"

"$cli" shell --state "$state" '@provider/codex |> ?usage' | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["result"]["owner"] == "ProviderState"
assert result["result"]["value"]["metered_usage"] is None
'
"$cli" shell --state "$state" '@work/household |> ?economics |> ?WHY join |> ?PROVE total' | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["result"]["value"]["proof_state"] == "not_proven"
'
"$cli" shell --state "$state" '@power/BonfyreTime |> !--help' | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["result"]["value"]["operator"] == "ProcessOccurrence"
assert result["result"]["value"]["native_power"] == "BonfyreTime"
assert result["result"]["value"]["exit_code"] == 0
'

# Non-probe Power execution reaches the existing Fabric operator-contract ABI.
# An explicit ACT edge is required for the exact actor, operator, and purpose;
# the shell never turns an address or advertised binary into that grant.
export BONFYRE_STATE_DIR="$state"
"$fabric" fabric compile "$root/bonfyre.workspace.yaff" >/dev/null
artifact_uri=$("$fabric" artifact ingest "$root/estate/catalog.yaff" text/yaff | sed -n 's/^uri=//p')
if "$fabric" operator invoke command.hash --actor person:test --purpose semantic-power --input "$artifact_uri" >/dev/null 2>&1; then
  echo 'ungoverned native Power invocation was admitted' >&2
  exit 1
fi
sqlite3 "$state/fabric.db" 'INSERT INTO authority_edges(edge_id,actor,permission,subject,purpose,scope,delegable,revoked,evidence,originating_authority) VALUES("semantic-shell-grant","person:test","act","command.hash","semantic-power","",0,0,"test"," ");'
"$cli" shell --state "$state" "@power/BonfyreHash |> !--actor person:test --purpose semantic-power --input $artifact_uri" | python3 -c '
import json, sys
result = json.load(sys.stdin)
occurrence = result["result"]["value"]
assert occurrence["native_power"] == "BonfyreHash"
assert occurrence["fabric_operator"] == "command.hash"
assert occurrence["authority"]["decision"] == "admitted"
assert occurrence["process_receipt"].startswith("bonfyre://receipt/")
assert occurrence["authority_receipt"].startswith("bonfyre://receipt/")
'
