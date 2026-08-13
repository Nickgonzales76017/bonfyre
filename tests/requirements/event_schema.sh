#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

fixture="$requirement_runtime/event-schema-fixture.txt"
printf 'event schema fixture\n' >"$fixture"
input_uri=$(fabric_test_ingest "$fixture" text/plain)
fabric_test_run_node "m-event-schema" "n-event-schema" "command.hash" "$input_uri" >/dev/null

sqlite3 "$requirement_db" "SELECT json_group_array(json_object('id',id,'mission_id',mission_id,'task_id',task_id,'attempt',attempt,'actor',actor,'operator_id',operator_id,'provider_id',provider_id,'model_id',model_id,'start_at',start_at,'end_at',end_at,'duration_ms',duration_ms,'input_uri',input_uri,'output_uri',output_uri,'effect_class',effect_class,'status',status,'error_code',error_code,'receipt_id',receipt_id)) FROM events;" \
  >"$requirement_runtime/events.json"

python3 - "$requirement_root/completion/event.schema.json" "$requirement_runtime/events.json" <<'PY'
import json
import sys

schema = json.load(open(sys.argv[1]))
events = json.load(open(sys.argv[2]))
assert events, "no rows in events table -- nothing to validate against the schema"

py_types = {"string": str, "integer": int, "null": type(None)}

def check_type(value, type_spec):
    allowed = type_spec if isinstance(type_spec, list) else [type_spec]
    return any(isinstance(value, py_types[t]) for t in allowed)

for row in events:
    for field in schema["required"]:
        assert row.get(field) is not None, f"missing required field {field!r} in row {row!r}"
    for field, value in row.items():
        prop = schema["properties"].get(field)
        assert prop is not None, f"field {field!r} not declared in event.schema.json"
        assert check_type(value, prop["type"]), f"field {field!r}={value!r} does not match {prop['type']!r}"

print(f"event schema: validated {len(events)} row(s) against completion/event.schema.json")
PY

echo 'event schema: passed'
