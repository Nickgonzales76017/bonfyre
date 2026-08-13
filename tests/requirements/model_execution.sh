#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
make -C "$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)/cmd/BonfyreQwenFPQ" >/dev/null
fabric_test_bootstrap

prompt="$requirement_runtime/model-prompt.txt"
printf 'Write exactly one valid Blender Python line that creates a cube.\n' >"$prompt"
prompt_uri=$(fabric_test_ingest "$prompt" text/plain)
run_output=$(fabric_test_run_node local-model generate command.model "$prompt_uri")
model_uri=$(printf '%s\n' "$run_output" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ -n "$model_uri" ]
model_path=$("$requirement_fabric" namespace show "$model_uri" | sed -n 's/^locator=//p')
[ -s "$model_path" ]

python3 - "$model_path" <<'PY'
import json
import pathlib
import sys

normalized = pathlib.Path(sys.argv[1])
assert normalized.read_text().strip().startswith("bpy.ops.mesh.primitive_cube_add")
manifest = json.loads((normalized.parent / "model-inference.json").read_text())
assert manifest["representation"] == "native-fp16"
assert manifest["model_id"] == "qwen2.5-coder-0.5b-instruct-native-fp16"
assert manifest["generated_tokens"] > 0
raw = pathlib.Path(manifest["raw_output_path"])
assert raw.is_file() and raw.stat().st_size > 0
assert len(manifest["raw_output_digest"]) == 64
assert normalized.read_text().strip() in raw.read_text()
PY

[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM events e JOIN execution_metrics m ON m.event_id=e.id JOIN usage_ledger u ON u.event_id=e.id JOIN economic_ledger c ON c.event_id=e.id WHERE e.mission_id='local-model' AND e.operator_id='command.model' AND e.status='complete' AND m.quality_result='passed' AND u.duration_ms>0 AND c.realized_cost=0.0;")" -eq 1 ]
receipt_id=$(sqlite3 "$requirement_db" "SELECT receipt_id FROM events WHERE mission_id='local-model' AND operator_id='command.model' AND status='complete';")
"$requirement_fabric" receipt show "$receipt_id" | grep -q '"quality_result":"passed"'
echo 'local model execution: passed'
