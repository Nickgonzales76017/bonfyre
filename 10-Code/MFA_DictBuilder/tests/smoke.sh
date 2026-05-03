#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$here/.." && pwd)
trans="$repo_root/tests/fixtures/sample_transcript.txt"
out="$repo_root/tests/fixtures/sample_lexicon.dict"
mkdir -p "$(dirname "$trans")"
cat > "$trans" <<'TXT'
Hello world. This is a sample transcript for MFA dict builder.
TXT
cmd="$repo_root/bin/build_mfa_dict.py --transcript $trans --out $out --dry-run"
echo "Running smoke: $cmd"
bash -c "$cmd"

echo "Now generate actual lexicon to $out"
bash -c "$repo_root/bin/build_mfa_dict.py --transcript $trans --out $out"
echo "Lexicon contents:"
sed -n '1,20p' "$out" || true
