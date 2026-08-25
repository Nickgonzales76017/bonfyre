#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$here/.." && pwd)
emb="$repo_root/tests/fixtures/sample_embeddings.npy"
meta="$repo_root/tests/fixtures/sample_meta.json"
if [ ! -f "$emb" ]; then
  mkdir -p "$(dirname "$emb")"
  python3 - <<'PY'
import numpy as np
np.save('''$emb''', np.random.rand(10, 768))
PY
fi
if [ ! -f "$meta" ]; then
  mkdir -p "$(dirname "$meta")"
  cat > "$meta" <<JSON
[{"id": "doc1", "text": "hello world"}]
JSON
fi
cmd="$repo_root/bin/index_weaviate.py --emb $emb --meta $meta --dry-run"
echo "Running smoke: $cmd"
bash -c "$cmd"
