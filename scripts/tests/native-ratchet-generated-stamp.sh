#!/bin/sh
# The ratchet excludes stamped build outputs automatically: a script whose
# first line carries `bonfyre:generated` is never counted, with no
# per-artifact ratchet edit. This holds that contract with two probes.
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
plain="$root/.ratchet-stamp-plain.mjs"
stamped="$root/.ratchet-stamp-stamped.mjs"
trap 'rm -f "$plain" "$stamped"' EXIT HUP INT TERM
field() { printf '%s' "$1" | grep -o "\"$2\":[0-9]*" | cut -d: -f2; }
before=$(sh "$root/scripts/native-ratchet.sh" measure)
printf 'export const plain = 1;\nexport const tail = 2;\n' > "$plain"
printf '/*!bonfyre:generated owner=ratchet-self-test*/\nexport const stamped = 1;\n' > "$stamped"
after=$(sh "$root/scripts/native-ratchet.sh" measure)
files_delta=$(($(field "$after" authored_mjs_files) - $(field "$before" authored_mjs_files)))
lines_delta=$(($(field "$after" authored_mjs_lines) - $(field "$before" authored_mjs_lines)))
[ "$files_delta" -eq 1 ] && [ "$lines_delta" -eq 2 ] || {
  echo "generated-stamp exclusion failed: files +$files_delta lines +$lines_delta" >&2; exit 1; }
echo 'native ratchet generated stamp: PASS'
