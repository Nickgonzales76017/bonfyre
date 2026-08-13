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
app_count=0
for app_path in "$REPO_ROOT"/integrations/frappe-bench/apps/*; do
  [ -d "$app_path" ] || continue
  app_count=$((app_count + 1))
  log_path="/tmp/bonfyre_frappe_inventory_$(basename "$app_path").log"
  "$COMPILER" --phase 0 -r "$REPO_ROOT" "$app_path" >"$log_path" 2>&1
  grep -q "Inventory complete" "$log_path"
done

test "$app_count" -gt 0

emit_dir=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-frappe-emit.XXXXXX")
trap 'rm -rf "$emit_dir"' EXIT
"$COMPILER" --phase 0 -r "$REPO_ROOT" \
  --emit-schema-ir "$emit_dir/schema.json" \
  --emit-bindings "$emit_dir/bindings.json" \
  --emit-rule-universes "$emit_dir/rules.json" \
  --emit-pack "$emit_dir/pack.json" \
  "$REPO_ROOT/integrations/frappe-bench/apps/frappe" >/dev/null
for emitted in "$emit_dir/schema.json" "$emit_dir/bindings.json" "$emit_dir/rules.json" "$emit_dir/pack.json"; do
  test -s "$emitted"
  grep -q '"kind"' "$emitted"
done
"$COMPILER" --phase 1 -r "$REPO_ROOT" \
  --emit-schema-ir "$emit_dir/phase1-schema.json" \
  --emit-pack "$emit_dir/phase1-pack.json" \
  "$REPO_ROOT/integrations/frappe-bench/apps/frappe" >"$emit_dir/phase1.log"
grep -q 'Schema graph built: [1-9][0-9]* doctypes' "$emit_dir/phase1.log"
grep -q '"doctypes"' "$emit_dir/phase1-schema.json"
grep -q '"AppPack"' "$emit_dir/phase1-pack.json"
for family in frappe erpnext crm hrms helpdesk lms wiki drive insights; do
  "$COMPILER" --phase 1 -r "$REPO_ROOT" \
    --emit-pack "$emit_dir/$family.apppack.json" \
    "$REPO_ROOT/integrations/frappe-bench/apps/$family" >"$emit_dir/$family.log"
  grep -q 'Schema graph built: [1-9][0-9]* doctypes' "$emit_dir/$family.log"
  grep -q '"AppPack"' "$emit_dir/$family.apppack.json"
done
if "$COMPILER" --dry-run --emit-pack "$emit_dir/forbidden.json" "$REPO_ROOT/integrations/frappe-bench/apps/frappe" >/dev/null 2>&1; then
  echo "dry run unexpectedly emitted output" >&2
  exit 1
fi

echo "schema ingestion smoke tests passed"
