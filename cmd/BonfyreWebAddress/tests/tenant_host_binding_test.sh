#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

cat >"$tmp/valid.tsv" <<'EOF'
# Exact tenant hosts (not hardcoded in engine).
work demo.example.net login.demo.example.net
life people.example.net auth.people.example.net
live live.example.net auth.live.example.net
shop shop.example.net auth.shop.example.net
EOF
cat >"$tmp/duplicate.tsv" <<'EOF'
work demo.example.net login.demo.example.net
life demo.example.net auth.people.example.net
EOF
cat >"$tmp/malformed.tsv" <<'EOF'
work demo.example.net https://evil.example.net/path
EOF
cc -std=c11 -Wall -Wextra -Werror -pedantic \
  -I"$root/cmd/BonfyreWebAddress/src" \
  "$root/cmd/BonfyreWebAddress/tests/tenant_host_binding_test.c" -o "$tmp/tenant-test"
"$tmp/tenant-test" "$tmp/valid.tsv" "$tmp/duplicate.tsv" "$tmp/malformed.tsv"
