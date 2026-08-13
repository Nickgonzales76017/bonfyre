#!/bin/sh
# Closes the remaining piece of the "one authoritative compiled catalog"
# requirement: the discovery-index catalog (catalog.db -- models/surfaces/
# transports/families/recipes, no generation concept of its own) and the
# governed executable-dispatch catalog (fabric.db's catalog_generation,
# hash-verified across catalog_bindings/operator_contract_bindings) are
# real, explicitly cross-referenced via `catalog stamp-generation`, not
# just "the same source files, coincidentally."
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

discovery_db="$requirement_runtime/discovery-catalog.db"
rm -f "$discovery_db"

echo "== stamp-generation writes the fabric's real catalog_generation into the discovery db =="
stamp_out=$("$requirement_fabric" catalog stamp-generation "$discovery_db")
fabric_generation=$(sqlite3 "$requirement_db" "SELECT value FROM fabric_meta WHERE key='catalog_generation';")
echo "$stamp_out" | grep -q "fabric_catalog_generation=$fabric_generation" \
  || { echo "FAIL: stamp-generation output did not report the real generation: $stamp_out" >&2; exit 1; }

echo "== discovery db actually has the matching row =="
stamped=$(sqlite3 "$discovery_db" "SELECT value FROM catalog_meta WHERE key='fabric_catalog_generation';")
[ "$stamped" = "$fabric_generation" ] || { echo "FAIL: discovery db has $stamped, fabric has $fabric_generation" >&2; exit 1; }

echo "== a real stamped_at timestamp was recorded =="
stamped_at=$(sqlite3 "$discovery_db" "SELECT value FROM catalog_meta WHERE key='fabric_catalog_generation_stamped_at';")
[ "$stamped_at" -gt 0 ] 2>/dev/null || { echo "FAIL: no real stamped_at timestamp: $stamped_at" >&2; exit 1; }

echo "== stamping is idempotent: no duplicate catalog_meta rows =="
"$requirement_fabric" catalog stamp-generation "$discovery_db" >/dev/null
row_count=$(sqlite3 "$discovery_db" "SELECT count(*) FROM catalog_meta WHERE key='fabric_catalog_generation';")
[ "$row_count" -eq 1 ] || { echo "FAIL: expected exactly 1 catalog_meta row, got $row_count" >&2; exit 1; }

echo 'catalog generation bridge: passed'
