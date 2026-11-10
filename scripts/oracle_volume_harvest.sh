#!/bin/sh
# Harvest the old estate volume instead of holding it.
#
# The x86 boot volume from the disabled Oracle instance (47 GB, plus a rescue
# clone and a full backup) has been kept whole because the real auth store and
# CMS are on it. Almost none of 47 GB is that: it is an operating system, build
# trees and caches. This reads the volume where it is mounted, read-only, and
# takes out only the estate's own state -- inventoried, checksummed, and packed
# -- so what matters can come home and be reconciled, and the volume stops
# being the only copy of anything.
#
# It never writes to the volume, never deletes, and never takes private keys
# or credential files: those are listed by path and size in the manifest and
# left where they are for a person to decide.
#
#   oracle_volume_harvest.sh <mounted-volume-root> <out-dir>
set -eu
root=${1:?where the old volume is mounted, read-only}
out=${2:?where to write the harvest}
[ -d "$root" ] || { echo "no volume mounted at $root" >&2; exit 2; }
mkdir -p "$out"
case "$out" in "$root"|"$root"/*) echo "refusing to write the harvest onto the volume it reads" >&2; exit 2 ;; esac
manifest="$out/manifest.jsonl"; : > "$manifest"
note() { printf '%s\n' "$*" >&2; }
sum() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
size() { wc -c < "$1" | tr -d ' '; }

# Where the estate kept its own state on that host.
STATE="var/lib/bonfyre opt/bonfyre/state etc/bonfyre home/*/.bonfyre root/.bonfyre"
# Never copied: named in the manifest instead.
secret() { case "$1" in *.pem|*.key|*id_rsa*|*id_ed25519*|*.p12|*secrets*|*.env|*credentials*|*token*) return 0 ;; esac; return 1; }

total=0; taken=0; held=0; dbs=0
for pattern in $STATE; do
  for dir in $root/$pattern; do
    [ -d "$dir" ] || continue
    rel=${dir#"$root"/}
    note "reading $rel"
    find "$dir" -type f 2>/dev/null | sort | while IFS= read -r file; do
      path=${file#"$root"/}; bytes=$(size "$file")
      if secret "$path"; then
        printf '{"path":"%s","bytes":%s,"taken":false,"why":"a key or credential is not copied; decide by hand"}\n' "$path" "$bytes" >> "$manifest"
        continue
      fi
      kind=file
      # A database is copied whole and checked, so a half-written page is caught here and not at home.
      case "$path" in *.db|*.sqlite|*.sqlite3) kind=database ;; esac
      dest="$out/tree/$path"; mkdir -p "$(dirname "$dest")"
      cp -p "$file" "$dest"
      digest=$(sum "$dest")
      [ "$digest" = "$(sum "$file")" ] || { echo "copy of $path does not match the volume" >&2; exit 1; }
      check=""
      if [ "$kind" = database ] && command -v sqlite3 >/dev/null 2>&1; then
        check=$(sqlite3 "file:$dest?mode=ro" "PRAGMA integrity_check" 2>&1 | head -1 | tr -d '"')
      fi
      printf '{"path":"%s","bytes":%s,"sha256":"%s","taken":true,"kind":"%s","integrity":"%s"}\n' "$path" "$bytes" "$digest" "$kind" "$check" >> "$manifest"
    done
  done
done
total=$(wc -l < "$manifest" | tr -d ' ')
taken=$(grep -c '"taken":true' "$manifest" || true)
held=$(grep -c '"taken":false' "$manifest" || true)
dbs=$(grep -c '"kind":"database"' "$manifest" || true)
bad=$(grep '"kind":"database"' "$manifest" | grep -vc '"integrity":"ok"' || true)
[ "$taken" -gt 0 ] || { echo "nothing of the estate's was found under $root" >&2; exit 3; }
# One ordered, compressed archive: like beside like, so what repeats is stored once.
( cd "$out/tree" && find . -type f | sort | tar -cf - --no-recursion -T - ) | zstd -19 --long=27 -T0 -q -o "$out/estate-state.tar.zst"
raw=$(du -sk "$out/tree" | cut -f1); packed=$(du -sk "$out/estate-state.tar.zst" | cut -f1)
printf '{"schema":"bonfyre-oracle-volume-harvest.v1","volume_root":"%s","files_seen":%s,"taken":%s,"left_for_a_person":%s,"databases":%s,"databases_failing_integrity":%s,"raw_kb":%s,"packed_kb":%s,"archive_sha256":"%s","volume_written":false,"cannot_infer":"a harvested file -> a current one; the volume stopped changing when its instance was disabled, and the estate at home has moved on"}\n' \
  "$root" "$total" "$taken" "$held" "$dbs" "$bad" "$raw" "$packed" "$(sum "$out/estate-state.tar.zst")" | tee "$out/receipt.json"
