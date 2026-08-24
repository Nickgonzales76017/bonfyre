#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cli="$here/../bin/faiss_search.py"
echo "=== FaissLocalSearch smoke test ==="
echo "--- build-index --dry-run ---"
python3 "$cli" build-index /tmp/embeddings --dry-run
echo "--- search --dry-run ---"
python3 "$cli" search /tmp/index.faiss /tmp/query.json --dry-run
echo "--- build-index + search (real, native fallback) ---"
tmpdir=$(mktemp -d)
for i in 1 2 3; do
  python3 -c "import json,random; json.dump({'id':'vec-$i','embedding':[random.gauss(0,1) for _ in range(8)]}, open('$tmpdir/v$i.json','w'))"
done
python3 "$cli" build-index "$tmpdir" --out "$tmpdir/test.idx" --dim 8
python3 -c "import json,random; json.dump({'embedding':[random.gauss(0,1) for _ in range(8)]}, open('$tmpdir/q.json','w'))"
python3 "$cli" search "$tmpdir/test.idx" "$tmpdir/q.json" --k 2
rm -rf "$tmpdir"
echo "=== PASS ==="
