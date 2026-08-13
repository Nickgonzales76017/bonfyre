#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
: "${BONFYRE_STATE_DIR:=/tmp/bonfyre-operator-contracts}"
export BONFYRE_STATE_DIR
rg -q 'operator_contracts' "$root/engine/core/src/fabric.c"
rg -q 'operator_contract_bindings' "$root/engine/core/src/fabric.c"
rg -q 'bf_operator_contract_expand' "$root/engine/core/src/operator_contract.c"
if rg -q '#define execute_hash execute_operator|execute_hash|command\.hash|artifact-only|hash-only' "$root/engine/core"; then
  echo 'legacy hash-specific execution compatibility remains' >&2
  exit 1
fi
if rg -q 'const char \*arguments\[\] = \{ binary, "file", locator, NULL \}' "$root/engine/core/src/fabric_exec.c"; then
  echo 'fixed universal file argv construction remains' >&2
  exit 1
fi
fabric="$root/programs/bonfyre/bonfyre"
"$fabric" fabric compile "$root/bonfyre.workspace.yaff" >/dev/null
sqlite3 "$BONFYRE_STATE_DIR/fabric.db" 'SELECT count(*) >= 3 FROM operator_contracts;' | grep -qx '1' || { echo 'compiled operator contracts are absent' >&2; exit 1; }
sqlite3 "$BONFYRE_STATE_DIR/fabric.db" 'SELECT count(*) = 93 FROM operator_contract_bindings WHERE operator_id LIKE "command.%";' | grep -qx '1' || { echo '93 public contract bindings are absent' >&2; exit 1; }
sqlite3 "$BONFYRE_STATE_DIR/fabric.db" 'SELECT count(DISTINCT operator_id) = 93 FROM operator_contract_bindings WHERE operator_id LIKE "command.%";' | grep -qx '1' || { echo 'public contract bindings are not unique' >&2; exit 1; }
sqlite3 "$BONFYRE_STATE_DIR/fabric.db" 'SELECT count(*) = 0 FROM operator_contract_bindings WHERE generation != (SELECT value FROM fabric_meta WHERE key="catalog_generation");' | grep -qx '1' || { echo 'stale contract binding generation' >&2; exit 1; }
sqlite3 "$BONFYRE_STATE_DIR/fabric.db" 'SELECT count(*) = 2 FROM declarations WHERE path LIKE "%operator-contract%";' | grep -qx '1' || { echo 'contract declarations are missing provenance' >&2; exit 1; }

# Execute the checked-in hash workload through catalog -> contract -> argv ->
# bounded process -> artifact -> probe -> event -> receipt.  This is a runtime
# assertion, not a declaration-row assertion.
runtime_state="$BONFYRE_STATE_DIR/runtime"
BONFYRE_STATE_DIR="$runtime_state" sh "$root/tests/fabric_smoke.sh" >/dev/null
sqlite3 "$runtime_state/state/fabric.db" "SELECT payload FROM receipts WHERE subject_kind='operator-execution';" | grep -q '"contract":"artifact_file_v1"' || { echo 'contract runtime receipt evidence is absent' >&2; exit 1; }
sqlite3 "$runtime_state/state/fabric.db" "SELECT payload FROM receipts WHERE subject_kind='operator-execution';" | grep -q '"workload_result":"passed"' || { echo 'workload probe result was not persisted' >&2; exit 1; }
sqlite3 "$runtime_state/state/fabric.db" "SELECT payload FROM receipts WHERE subject_kind='operator-execution';" | grep -q '"quality_result":"passed"' || { echo 'quality probe result was not persisted' >&2; exit 1; }
sqlite3 "$runtime_state/state/fabric.db" "SELECT count(*) > 0 FROM execution_metrics WHERE quality_result='passed';" | grep -qx '1' || { echo 'contract quality result was not persisted' >&2; exit 1; }
