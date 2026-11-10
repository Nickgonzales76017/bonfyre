#!/bin/sh
# Read-only witness of identical installed payloads at distinct inodes.
# This never changes a binary, updates a budget or grants deployment authority.
set -eu
bin=${BONFYRE_BIN:-"$HOME/.bonfyre/bin"}
[ -d "$bin" ] || { echo "native footprint: managed directory absent"; exit 0; }
command -v shasum >/dev/null 2>&1 || { echo "native footprint: shasum unavailable" >&2; exit 1; }
tmp=$(mktemp /tmp/bonfyre-native-inodes.XXXXXX)
trap 'rm -f "$tmp"' EXIT HUP INT TERM
for f in "$bin"/bonfyre*; do
  [ -f "$f" ] && [ ! -L "$f" ] || continue
  inode=$(ls -i "$f" | awk '{print $1}')
  bytes=$(wc -c < "$f" | tr -d ' ')
  hash=$(shasum -a 256 "$f" | awk '{print $1}')
  printf '%s\t%s\t%s\t%s\n' "$hash" "$inode" "$bytes" "$f" >> "$tmp"
done
awk -F '\t' '
  seen_inode[$2]++ {next}
  {
    if (first[$1] != "") {
      if (!shown[$1]++) printf "native footprint duplicate source: %s\n",first[$1]
      printf "native footprint duplicate copy: %s (%s bytes)\n",$4,$3
      recoverable += $3; duplicates++
    } else first[$1]=$4
  }
  END {
    printf "native footprint: distinct inode duplicate copies=%d, potential hardlink savings=%.0f bytes\n",duplicates,recoverable
    if (!duplicates) print "native footprint: no byte-identical independent copies found"
  }' "$tmp"
