#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

mission=operator-surface
"$requirement_fabric" mission create "$mission" >/dev/null
"$requirement_fabric" work add "$mission" ready-node core.intake --family operator >/dev/null
"$requirement_fabric" work add "$mission" running-node core.intake --family operator >/dev/null
claim=$("$requirement_fabric" work claim "$mission" running-node operator-worker --lease-ms 30000)
printf '%s\n' "$claim" | grep -q '^status=running$'

generation=$(sqlite3 "$requirement_db" "SELECT value FROM fabric_meta WHERE key='catalog_generation';")
trap 'fabric_test_stop_daemon' EXIT HUP INT TERM
fabric_test_start_daemon
curl --silent --fail "http://127.0.0.1:$requirement_port/operator" >"$requirement_runtime/operator.html"
grep -q '<h1>Bonfyre Operator</h1>' "$requirement_runtime/operator.html"
grep -q "data-catalog-generation=\"$generation\"" "$requirement_runtime/operator.html"
grep -q '<dd id="ready-count">1</dd>' "$requirement_runtime/operator.html"
grep -q '<dd id="running-count">1</dd>' "$requirement_runtime/operator.html"

fabric_test_stop_daemon
trap - EXIT HUP INT TERM
echo 'operator surface: passed'
