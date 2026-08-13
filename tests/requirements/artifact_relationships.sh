#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap
make -C "$requirement_root/cmd/BonfyreIngest" >/dev/null
make -C "$requirement_root/cmd/BonfyreHash" >/dev/null

source_file="$requirement_runtime/relationship-source.txt"
printf 'alpha relates to beta through a governed transform\n' >"$source_file"
source_uri=$(fabric_test_ingest "$source_file" text/plain)
first_run=$(fabric_test_run_node relation-normalize normalize command.ingest "$source_uri")
normalized_uri=$(printf '%s\n' "$first_run" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ -n "$normalized_uri" ]
second_run=$(fabric_test_run_node relation-hash hash command.hash "$normalized_uri")
hash_uri=$(printf '%s\n' "$second_run" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ -n "$hash_uri" ]
[ "$source_uri" != "$normalized_uri" ]
[ "$normalized_uri" != "$hash_uri" ]

actual_chain=$(sqlite3 -separator '|' "$requirement_db" \
  "SELECT input_uri,output_uri FROM events WHERE mission_id IN ('relation-normalize','relation-hash') AND operator_id LIKE 'command.%' AND status='complete' ORDER BY rowid;")
expected_chain=$(printf '%s|%s\n%s|%s' "$source_uri" "$normalized_uri" "$normalized_uri" "$hash_uri")
[ "$actual_chain" = "$expected_chain" ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM events child JOIN events parent ON child.input_uri=parent.output_uri WHERE parent.mission_id='relation-normalize' AND child.mission_id='relation-hash';")" -eq 1 ]
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM receipts r JOIN events e ON e.receipt_id=r.id WHERE e.mission_id IN ('relation-normalize','relation-hash') AND e.operator_id LIKE 'command.%' AND e.status='complete' AND json_extract(r.payload,'$.output')=e.output_uri AND json_extract(r.payload,'$.workload_result')='passed';")" -eq 2 ]

"$requirement_fabric" namespace show "$source_uri" | grep -q '^kind=artifact$'
"$requirement_fabric" namespace show "$normalized_uri" | grep -q '^kind=artifact$'
"$requirement_fabric" namespace show "$hash_uri" | grep -q '^kind=artifact$'
echo 'artifact relationship graph: passed'
