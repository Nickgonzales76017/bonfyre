#!/bin/sh
# Read-only by default; --apply is the explicit local approval for protected files.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
case "${1:-review}" in review|--apply) ;; *) echo 'usage: retire_atlas_contracts.sh [review|--apply]' >&2; exit 2;; esac
atlas="$root/cmd/BonfyreAtlas/bonfyre-atlas"
old=architecture/compile-contracts.mjs
[ -x "$atlas" ] && [ -f "$old" ] || { echo 'Build Atlas and confirm legacy source first' >&2; exit 2; }
command -v python3 >/dev/null; command -v zstd >/dev/null
git diff --check
check_hash(){
 actual=$(shasum -a 256 "$1" | awk '{print $1}')
 [ "$actual" = "$2" ] || { echo "Changed source: $1 ($actual)" >&2; exit 1; }
}
check_hash "$old" 6b26461946af9d0d9f069d559075836173d4dc5e165a0c60e47fdfec42496c2f
check_hash engine/core/include/bf_app_lineages.h 8ec97382b3bed92abec3a232fe0c3585589d79c1b74718439138a25825b29a7f
check_hash engine/core/include/bf_institutions.h 592c58e46b78c7aa2f64977e85186b2ad066668809f1ada964e8eb36b4c79b81
make -C cmd/BonfyreAtlas test
tmp=$(mktemp -d "${TMPDIR:-/tmp}/atlas-retirement.XXXXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/architecture/apps" "$tmp/architecture/institutional"
cp architecture/apps/app-lineages.contract.yaff "$tmp/architecture/apps/"
cp architecture/institutional/namespaces.yaff "$tmp/architecture/institutional/"
env -u BONFYRE_ATLAS_DIR -u BONFYRE_ROOT BONFYRE_REPO_ROOT="$tmp" "$atlas" compile-contracts app-lineages
env -u BONFYRE_ATLAS_DIR -u BONFYRE_ROOT BONFYRE_REPO_ROOT="$tmp" "$atlas" compile-contracts institutions
python3 - "$root" "$tmp" <<'PY'
from pathlib import Path
import sys, difflib
root,temp=map(Path,sys.argv[1:])
for p in ['engine/core/include/bf_app_lineages.h','engine/core/include/bf_institutions.h']:
 before=(root/p).read_bytes()
 after=(temp/p).read_bytes()
 expected=before.replace(b'architecture/compile-contracts.mjs',b'cmd/BonfyreAtlas/bonfyre-atlas compile-contracts')
 if before==expected or expected!=after:
  print('REFUSED: header differs beyond generator provenance: '+p,file=sys.stderr)
  print(*difflib.unified_diff(before.decode().splitlines(),after.decode().splitlines(),fromfile='current',tofile='native'),sep='\n',file=sys.stderr)
  sys.exit(1)
 print('HEADER_PARITY '+p)
for p in ['integrations/mcp/app_lineages_contract.json','integrations/mcp/institution_contract.json']:
 if (root/p).read_bytes()!=(temp/p).read_bytes():sys.exit('REFUSED: contract data differs: '+p)
 print('JSON_PARITY '+p)
PY
[ "${1:-review}" = "--apply" ] || {
 echo 'REVIEW_ONLY: run sh scripts/release/retire_atlas_contracts.sh --apply after reviewing the native parity.'
 exit 0
}
# Restore-verified preimage: do not lose dirty generated headers and protected declarations.
archive="$HOME/.bonfyre/archive/retired/atlas-contracts-pre-migration-20261029.tar.zst"
[ ! -e "$archive" ] || { echo "Existing archive: $archive; review before retrying" >&2; exit 1; }
mkdir -p "$(dirname "$archive")" "$tmp/restore"
sources="$old engine/core/include/bf_app_lineages.h engine/core/include/bf_institutions.h architecture/tests/compile_contracts.test.mjs architecture/institutional/namespaces.yaff architecture/apps/frappe-apps.yaff architecture/external/carriers.contract.yaff architecture/economic/settlement.contract.yaff"
tar -cf - $sources | zstd -q -T0 -o "$archive"
zstd -dc "$archive" | tar -xf - -C "$tmp/restore"
for p in $sources; do cmp -s "$p" "$tmp/restore/$p" || { echo "Backup failed for $p" >&2; exit 1; }; done
echo "BACKUP_VERIFIED $archive"
# Guard ALL reference substitutions before writing any of them.
python3 - "$root" <<'PY'
from pathlib import Path
import sys
root=Path(sys.argv[1])
edits={
 'architecture/tests/compile_contracts.test.mjs':[
  ("'architecture/compile-contracts.mjs'","'cmd/BonfyreAtlas/bonfyre-atlas'"),
  ("[process.execPath, compiler, mode, '--check-generated']","[compiler, 'compile-contracts', mode, '--check-generated']")],
 'architecture/institutional/namespaces.yaff':[
  ('source_path architecture/compile-contracts.mjs','source_path cmd/BonfyreAtlas/src/contracts.h'),
  ('bun architecture/compile-contracts.mjs institutions','cmd/BonfyreAtlas/bonfyre-atlas compile-contracts institutions')],
 'architecture/apps/frappe-apps.yaff':[
  ('bun architecture/compile-contracts.mjs app-lineages','cmd/BonfyreAtlas/bonfyre-atlas compile-contracts app-lineages')],
 'architecture/external/carriers.contract.yaff':[
  ('bun architecture/compile-contracts.mjs carriers','cmd/BonfyreAtlas/bonfyre-atlas compile-contracts carriers')],
 'architecture/economic/settlement.contract.yaff':[
  ('bun architecture/compile-contracts.mjs settlement','cmd/BonfyreAtlas/bonfyre-atlas compile-contracts settlement')]
}
pending={}
for path,subs in edits.items():
 text=(root/path).read_text()
 for before,after in subs:
  if text.count(before)!=1:sys.exit(f'REFUSED: expected one {before!r} in {path}, found {text.count(before)}')
  text=text.replace(before,after)
 pending[path]=text
for path,text in pending.items():(root/path).write_text(text)
PY
cp "$tmp/engine/core/include/bf_app_lineages.h" engine/core/include/bf_app_lineages.h
cp "$tmp/engine/core/include/bf_institutions.h" engine/core/include/bf_institutions.h
"$atlas" compile-contracts app-lineages --check-generated
"$atlas" compile-contracts institutions --check-generated
bun test architecture/tests/compile_contracts.test.mjs
sh scripts/retire.sh atlas-contracts-native-20261029 'Native Atlas replaces JS compiler; header/data parity and four contract modes verified' "$old"
git diff --check
sh scripts/native-ratchet.sh check
echo 'ATLAS_RETIRED_AND_RATCHET_PASSED; run the governed release preparation separately.'
