#!/bin/sh
set -eu

compress=${1:?bonfyre-compress binary required}
case "$compress" in /*) ;; *) compress="$(pwd)/$compress" ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-compress-family.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

family="$tmp/a';touch $tmp/injected;echo '"
mkdir -p "$family"
printf 'one subject, one retained body\n' > "$family/item.txt"
mkdir -p "$family/nested"
printf 'nested material\n' > "$family/nested/other.txt"
printf 'hidden material\n' > "$family/.hidden"
archive="$tmp/family.tar.zst"
"$compress" family "$family" --out "$archive"
test -s "$archive"
test ! -e "$tmp/injected"

mkdir -p "$tmp/restored"
zstd -q -d -c "$archive" | tar -xf - -C "$tmp/restored"
# Absolute tar paths are stripped by tar; the restored path is relative.
restored="$tmp/restored/${family#/}"
test -f "$restored/item.txt"
cmp "$family/item.txt" "$restored/item.txt"
cmp "$family/nested/other.txt" "$restored/nested/other.txt"
cmp "$family/.hidden" "$restored/.hidden"

savings=$("$compress" savings "$family")
expected_raw=$(wc -c < "$family/item.txt")
expected_raw=$((expected_raw + $(wc -c < "$family/nested/other.txt") + $(wc -c < "$family/.hidden")))
printf '%s' "$savings" | BONFYRE_EXPECTED_RAW="$expected_raw" bun -e 'let s="";for await(const c of Bun.stdin.stream())s+=new TextDecoder().decode(c);const x=JSON.parse(s);if(x.raw_bytes!==Number(process.env.BONFYRE_EXPECTED_RAW)||x.compressed_bytes<=0)process.exit(1)'
test ! -e "$tmp/injected"

if "$compress" family "$tmp/does-not-exist" --out "$tmp/missing.tar.zst" 2>/dev/null; then
  echo 'missing family was reported as packed' >&2
  exit 1
fi
test ! -e "$tmp/missing.tar.zst"

printf 'sentinel\n' > "$tmp/sentinel.tar.zst"
if "$compress" family "$family" --out "$tmp/sentinel.tar.zst" 2>/dev/null; then
  echo 'existing output was overwritten without authorization' >&2
  exit 1
fi
test "$(cat "$tmp/sentinel.tar.zst")" = sentinel
echo 'family compression pipeline: PASS'
