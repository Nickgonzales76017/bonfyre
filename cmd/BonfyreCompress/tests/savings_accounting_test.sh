#!/usr/bin/env bash
# A saving is reported against the thing it was measured on.
#
# The defect this guards: file bytes were divided into compressed-archive bytes,
# so tar's headers and padding were counted as if they were compression. A
# directory of tiny files reported nonsense either way, and "bytes_saved": 0
# hid a delivered archive larger than the files it carried.
set -u
BIN="${1:-./bonfyre-compress}"
fail=0; ok() { printf '  ok   %s\n' "$1"; }; bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/small" "$tmp/big"
for i in 1 2 3 4 5; do printf 'x' > "$tmp/small/f$i.txt"; done
python3 -c "open('$tmp/big/a.txt','w').write('the quick brown fox jumps over the lazy dog\n'*40000)"
get() { "$BIN" savings "$1" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[sys.argv[1]])" "$2"; }

[ "$(get "$tmp/small" raw_bytes)" = 5 ] && ok "file bytes are the files' own" || bad "raw_bytes is not the files' bytes"
[ "$(get "$tmp/small" archive_stream_bytes)" -gt 5 ] && ok "the archive stream is measured separately" || bad "archive stream not measured"
[ "$(get "$tmp/small" delivered_larger_than_files)" = True ] && ok "a delivered archive larger than its files says so" || bad "an archive larger than its files was not reported"
[ "$(get "$tmp/small" bytes_added)" -gt 0 ] && ok "and by how much" || bad "bytes_added missing"
[ "$(get "$tmp/big" delivered_larger_than_files)" = False ] && ok "a compressible directory is not flagged" || bad "a compressible directory was flagged"
python3 - "$BIN" "$tmp/big" <<'PY' && ok "the two ratios are each compressed bytes over their own base" || bad "a ratio does not match its base"
import json,subprocess,sys
d=json.loads(subprocess.run([sys.argv[1],'savings',sys.argv[2]],capture_output=True,text=True).stdout)
assert abs(d['stream_ratio']-d['compressed_bytes']/d['archive_stream_bytes'])<1e-3
assert abs(d['ratio']-d['compressed_bytes']/d['raw_bytes'])<1e-3
assert d['archive_overhead_bytes']==d['archive_stream_bytes']-d['raw_bytes']
PY
[ "$fail" -eq 0 ] && echo "savings accounting: PASS" || echo "savings accounting: FAIL"
[ "$fail" -eq 0 ]
