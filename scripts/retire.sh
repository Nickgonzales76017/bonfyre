#!/bin/sh
# Retire source files that a native owner has replaced. The files go into a
# zstd archive under ~/.bonfyre/archive/retired/ with a sha256 manifest, are
# listed in the archive README, and leave the tree. Use it for every port in
# docs/engineering/mjs-retirement-20261022/PLAN.md so nothing is lost and
# nothing stays behind "just in case".
#
#   scripts/retire.sh NAME "why it was retired" PATH...
#
# RETIRE_FROM=DIR retires paths relative to DIR instead of the repo (for
# hand-off bundles in ~/Downloads, duplicate trees under ~/.bonfyre).
#
# Restore:  zstd -dc --long=27 ~/.bonfyre/archive/retired/NAME.tar.zst | tar -xf - -C DIR
set -eu
[ $# -ge 3 ] || { echo "usage: $0 NAME REASON PATH..." >&2; exit 2; }
name=$1; reason=$2; shift 2
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
dir=${BONFYRE_ARCHIVE:-"$HOME/.bonfyre/archive"}/retired
mkdir -p "$dir"
archive="$dir/$name.tar.zst"
[ ! -e "$archive" ] || { echo "retire: $archive already exists" >&2; exit 1; }
base=${RETIRE_FROM:-$root}
cd "$base"
for p in "$@"; do [ -e "$p" ] || { echo "retire: no such path: $p" >&2; exit 1; }; done
manifest=$(mktemp); entries=$(mktemp)
find "$@" -type f -exec shasum -a 256 {} + | sort -k2 > "$manifest"
# Every entry (symlinks and empty directories too), ordered by its path inside
# its top-level tree so copies of one file across trees sit side by side where
# zstd's window sees them (.claude/skills/density: the ordering is the lever).
find "$@" | awk -F/ '{rel=$0; if (index(rel, "/")) sub(/^[^\/]*\//, "", rel); print rel "\t" $0}' |
  LC_ALL=C sort | cut -f2- > "$entries"
tar -cf - --numeric-owner --no-recursion -T "$entries" | zstd -q -${RETIRE_LEVEL:-19} -T0 --long=27 -o "$archive"
# Prove the archive restores entry for entry before anything is removed.
check=$(mktemp -d)
zstd -dc --long=27 "$archive" | tar -xf - -C "$check"
ok=1
for p in "$@"; do diff -r --no-dereference "$p" "$check/$p" >/dev/null 2>&1 || ok=0; done
(cd "$check" && shasum -a 256 -c "$manifest" >/dev/null 2>&1) || ok=0
rm -rf "$check" "$entries"
[ "$ok" = 1 ] || { echo "retire: archive does not restore exactly; nothing removed" >&2; rm -f "$archive" "$manifest"; exit 1; }
cp "$manifest" "$dir/$name.sha256"; rm -f "$manifest"
rm -rf "$@"
printf '%s: %s. From %s: %s. Restore: zstd -dc --long=27 %s | tar -xf - -C %s\n' \
  "$name.tar.zst" "$reason" "$base" "$*" "$archive" "$base" >> "$dir/README.txt"
echo "retired $(wc -l < "$dir/$name.sha256" | tr -d ' ') files into $archive ($(wc -c < "$archive" | tr -d ' ') bytes)"
