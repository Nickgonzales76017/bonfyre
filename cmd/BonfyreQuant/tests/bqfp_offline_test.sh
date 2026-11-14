#!/bin/sh
# Fail-closed parser smoke independent of a model download. The full shared
# malformed/valid tensor contract remains required by `make test`.
set -eu
kind=${1:?reader kind required}; bin=${2:?reader binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd)/$(basename -- "$bin")
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-bqfp-offline.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
: > "$tmp/empty.bqfp"
printf "wrong-magic" > "$tmp/bad-magic.bqfp"
printf "BQFP" > "$tmp/short-header.bqfp"
for name in empty bad-magic short-header; do
  input="$tmp/$name.bqfp"
  case "$kind" in
    sli)
      if "$bin" inspect --model "$input" > /dev/null 2>&1; then echo "sli accepted $name" >&2; exit 1; fi
      if "$bin" decode --model "$input" --out "$tmp/decoded.f32" > /dev/null 2>&1; then echo "sli decoded $name" >&2; exit 1; fi
      test ! -e "$tmp/decoded.f32" || { echo "sli wrote output for $name" >&2; exit 1; }
      ;;
    fpqx)
      if "$bin" align "$input" "$input" --out "$tmp/out-$name" > /dev/null 2>&1; then echo "fpqx aligned $name" >&2; exit 1; fi
      test ! -e "$tmp/out-$name/fpqx_alignment.json" || { echo "fpqx wrote output for $name" >&2; exit 1; }
      ;;
    *) echo "unknown reader kind: $kind" >&2; exit 2 ;;
  esac
done
echo "$kind offline BQFP malformed inputs: PASS (3 invalid refused)"
