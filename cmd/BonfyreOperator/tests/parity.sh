#!/bin/sh
# bonfyre-operator golden vectors. Each line of tests/cases.txt runs against
# tests/fixture; stdout, stderr and exit status must equal tests/golden/<n>.
#   tests/parity.sh BINARY              check
#   tests/parity.sh BINARY --record     rewrite the vectors (review the diff)
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
bin=$1; mode=${2:-check}
case "$bin" in /*) ;; *) bin="$PWD/$bin" ;; esac
mkdir -p "$here/golden"
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
n=0; fail=0
while IFS= read -r line; do
  n=$((n + 1))
  # shellcheck disable=SC2086
  (cd "$here" && "$bin" --state fixture $line > "$tmp/out" 2> "$tmp/err"; echo "exit $?" > "$tmp/code") || true
  cat "$tmp/out" "$tmp/err" "$tmp/code" > "$tmp/got"
  if [ "$mode" = --record ]; then cp "$tmp/got" "$here/golden/$n"
  elif ! cmp -s "$tmp/got" "$here/golden/$n"; then
    echo "FAIL $n: $line"; diff "$here/golden/$n" "$tmp/got" | head -20; fail=1
  fi
done < "$here/cases.txt"
[ "$fail" = 0 ] && echo "operator parity: $n vectors PASS"
exit "$fail"
