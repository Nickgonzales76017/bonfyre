#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap
make -C "$requirement_root/cmd/BonfyreHash" >/dev/null

input="$requirement_runtime/metrics-input.txt"
printf 'factual metrics are derived from this exact payload\n' >"$input"
input_uri=$(fabric_test_ingest "$input" text/plain)
run_output=$(fabric_test_run_node factual-metrics hash command.hash "$input_uri")
output_uri=$(printf '%s\n' "$run_output" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ -n "$output_uri" ]
output_path=$("$requirement_fabric" namespace show "$output_uri" | sed -n 's/^locator=//p')
[ -f "$output_path" ]

event_id=$(sqlite3 "$requirement_db" "SELECT id FROM events WHERE mission_id='factual-metrics' AND task_id='hash' AND operator_id='command.hash' AND status='complete';")
[ -n "$event_id" ]
recorded_in=$(sqlite3 "$requirement_db" "SELECT bytes_in FROM execution_metrics WHERE event_id='$event_id';")
recorded_out=$(sqlite3 "$requirement_db" "SELECT bytes_out FROM execution_metrics WHERE event_id='$event_id';")
[ "$recorded_in" -eq "$(wc -c <"$input" | tr -d ' ')" ]
stdout_path="$BONFYRE_STATE_DIR/missions/factual-metrics/hash-1/stdout.txt"
stderr_path="$BONFYRE_STATE_DIR/missions/factual-metrics/hash-1/stderr.txt"
captured_bytes=$(( $(wc -c <"$stdout_path" | tr -d ' ') + $(wc -c <"$stderr_path" | tr -d ' ') ))
[ "$recorded_out" -eq "$captured_bytes" ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM usage_ledger WHERE event_id='$event_id' AND bytes_in=$recorded_in AND bytes_out=$recorded_out AND duration_ms>=0;")" -eq 1 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM execution_metrics WHERE event_id='$event_id' AND catalog_generation=(SELECT value FROM fabric_meta WHERE key='catalog_generation') AND runtime_generation=(SELECT value FROM fabric_meta WHERE key='runtime_generation') AND quality_result='passed';")" -eq 1 ]

receipt_id=$(sqlite3 "$requirement_db" "SELECT receipt_id FROM events WHERE id='$event_id';")
"$requirement_fabric" receipt show "$receipt_id" | grep -q '"workload_result":"passed"'
echo 'factual metrics: passed'
