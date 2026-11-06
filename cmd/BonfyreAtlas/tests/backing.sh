#!/bin/sh
# Exercise maturity backing through the native validation entry point.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
organ=$(dirname "$here")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir "$tmp/architecture"
cp "$here/../../../architecture/atlas-fields.cue" "$tmp/architecture/"
export BONFYRE_ATLAS_DIR="$tmp/architecture"
fixture() {
  cat > "$tmp/architecture/backing.yaff" <<EOF
atlas backing-test
architecture backing
  canonical_name Backing
  family evidence
  domain Test
  semantic_question Does the declared backing exist?
  maturity $1
  source_path $2
  forbidden_inference readable backing -> a reproducible proof
EOF
}
refuses() {
  if "$organ/bonfyre-atlas" validate > "$tmp/result" 2>&1; then
    echo "expected refusal: $1" >&2; exit 1
  fi
  grep -F "$1" "$tmp/result" >/dev/null || { cat "$tmp/result"; exit 1; }
}
accepts() { "$organ/bonfyre-atlas" validate > "$tmp/result" 2>&1 || { cat "$tmp/result"; exit 1; }; }
fixture implemented architecture/missing.c
refuses "source_path 'architecture/missing.c' names a file that does not open"
touch "$tmp/architecture/source.c.archive"
fixture implemented architecture/source.c.archive
refuses 'every source is archived and no known_gap says so'
printf '  known_gap source retired to its native owner\n' >> "$tmp/architecture/backing.yaff"
accepts
fixture measured architecture/backing.yaff
printf '  witness architecture/missing-proof.txt proves backing\n' >> "$tmp/architecture/backing.yaff"
refuses "maturity 'measured' with no witness that opens"
touch "$tmp/architecture/missing-proof.txt"
accepts
fixture measured architecture/backing.yaff
printf '  witness architecture/source.c.archive\n' >> "$tmp/architecture/backing.yaff"
refuses 'every witness is archived'
printf '  witness architecture/backing.yaff verifies the declaration\n' >> "$tmp/architecture/backing.yaff"
accepts
echo 'bonfyre-atlas backing: 7 cases PASS'
