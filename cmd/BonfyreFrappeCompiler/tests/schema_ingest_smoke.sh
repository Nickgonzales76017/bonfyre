#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
COMPILER_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
REPO_ROOT=$(CDPATH= cd -- "$COMPILER_DIR/../.." && pwd)
COMPILER="$COMPILER_DIR/bonfyre-frappe-compiler"

test -x "$COMPILER"
"$COMPILER" --help >/dev/null

# Run the compiler's real inventory phase against every checked-in Frappe app.
# Full schema phases require a live bench fixture and belong to integration CI.
for app_path in "$REPO_ROOT"/integrations/frappe-bench/apps/*; do
  [ -d "$app_path" ] || continue
  log_path="/tmp/bonfyre_frappe_inventory_$(basename "$app_path").log"
  "$COMPILER" --phase 0 -r "$REPO_ROOT" "$app_path" >"$log_path" 2>&1
  grep -q "Inventory complete" "$log_path"
done

echo "schema ingestion smoke tests passed"
