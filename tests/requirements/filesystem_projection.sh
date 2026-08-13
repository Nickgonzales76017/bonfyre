#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap
make -C "$requirement_root/cmd/BonfyreHash" >/dev/null

input="$requirement_runtime/projection-input.txt"
printf 'BonfyreFS projects governed content without copying authority\n' >"$input"
input_uri=$(fabric_test_ingest "$input" text/plain)
run_output=$(fabric_test_run_node filesystem-projection hash command.hash "$input_uri")
output_uri=$(printf '%s\n' "$run_output" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ -n "$output_uri" ]

projection="$requirement_runtime/BonfyreFS"
project_output=$("$requirement_fabric" filesystem project "$projection")
printf '%s\n' "$project_output" | grep -q '^state=complete$'
[ -f "$projection/catalog.json" ]
[ -f "$projection/missions/filesystem-projection.json" ]

python3 - "$projection" "$input_uri" "$output_uri" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
catalog = json.loads((root / "catalog.json").read_text())
assert catalog["public_commands"] == 93
assert catalog["typed_command_contracts"] == 93
assert len(catalog["catalog_generation"]) == 64
mission = json.loads((root / "missions" / "filesystem-projection.json").read_text())
assert mission["status"] == "complete"
for uri in sys.argv[2:]:
    digest = uri.rsplit("/", 1)[1]
    link = root / "artifacts" / f"{digest}.artifact"
    metadata = json.loads((root / "artifacts" / f"{digest}.json").read_text())
    assert link.is_symlink() and link.resolve().is_file()
    assert metadata["uri"] == uri and metadata["digest"] == digest
    assert hashlib.sha256(link.read_bytes()).hexdigest() == digest
PY

if "$requirement_fabric" filesystem project "$projection" >/dev/null 2>&1; then
  echo 'filesystem projection overwrote an existing tree' >&2
  exit 1
fi
echo 'BonfyreFS projection: passed'
