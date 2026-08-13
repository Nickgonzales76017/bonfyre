#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
make -C "$root/cmd/BonfyreMeter" >/dev/null
fabric_test_bootstrap

input="$requirement_runtime/economy-input.txt"
printf 'local execution has measured usage and zero external provider charge\n' >"$input"
input_uri=$(fabric_test_ingest "$input" text/plain)
run_output=$(fabric_test_run_node economy-meter measure command.meter "$input_uri")
printf '%s\n' "$run_output" | grep -q 'status=complete output=bonfyre://artifact/'

[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM events e JOIN execution_metrics m ON m.event_id=e.id JOIN usage_ledger u ON u.event_id=e.id JOIN economic_ledger c ON c.event_id=e.id JOIN value_ledger v ON v.event_id=e.id JOIN receipts r ON r.id=e.receipt_id WHERE e.mission_id='economy-meter' AND e.provider_id='native' AND e.status='complete' AND m.bytes_in>0 AND m.bytes_out>0 AND m.quality_result='passed' AND u.bytes_in=m.bytes_in AND u.bytes_out=m.bytes_out AND c.projected_cost=0.0 AND c.realized_cost=0.0 AND v.accepted IS NULL AND json_extract(r.payload,'$.workload_result')='passed';")" -eq 1 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(DISTINCT event_id) FROM economic_ledger;")" -eq "$(sqlite3 "$requirement_db" "SELECT count(*) FROM execution_metrics;")" ]
echo 'economic provenance: passed'
