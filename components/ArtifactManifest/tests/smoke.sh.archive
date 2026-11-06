#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
cli="$root/validate_artifact.py"
examples="$root/examples"

pass=0
fail=0

echo "=== Bonfyre ArtifactManifest smoke test ==="

for f in "$examples"/*.artifact.json; do
  name=$(basename "$f")
  echo ""
  echo "--- Validating: $name ---"
  if python3 "$cli" "$f"; then
    pass=$((pass + 1))
  else
    echo "  FAIL"
    fail=$((fail + 1))
  fi
done

echo ""
echo "--- Compute hashes: transcript-family ---"
python3 "$cli" --compute-hashes "$examples/transcript-family.artifact.json"

echo ""
echo "--- Update hashes: brief ---"
cp "$examples/brief.artifact.json" "/tmp/brief-test.artifact.json"
python3 "$cli" --update-hashes "/tmp/brief-test.artifact.json"
python3 "$cli" "/tmp/brief-test.artifact.json"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  exit 1
fi
