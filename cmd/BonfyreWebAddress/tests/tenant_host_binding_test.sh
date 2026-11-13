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
# Shipping release gate uses the identical C host-map parser.
cc -std=c11 -Wall -Wextra -Werror -pedantic \
  -I"$root/cmd/BonfyreWebAddress/src" \
  "$root/cmd/BonfyreWebAddress/tools/check_tenant_hosts.c" \
  -o "$tmp/tenant-check"
"$tmp/tenant-check" "$tmp/valid.tsv" >"$tmp/readback.tsv"
"$tmp/tenant-check" "$root/cmd/BonfyreWebAddress/examples/tenant-hosts.example.tsv" >"$tmp/example-readback"
grep -q 'TENANT_HOST_MAP_VALID entries=3' "$tmp/example-readback"
grep -q 'TENANT_HOST_MAP_VALID entries=4' "$tmp/readback.tsv"
grep -q 'work[[:space:]]demo.example.net[[:space:]]login.demo.example.net' "$tmp/readback.tsv"
if "$tmp/tenant-check" "$tmp/duplicate.tsv" >"$tmp/duplicate-readback" 2>&1; then
  echo "ERROR: duplicate tenant map was admitted" >&2; exit 1
fi
if "$tmp/tenant-check" "$tmp/malformed.tsv" >"$tmp/invalid-readback" 2>&1; then
  echo "ERROR: malformed host was admitted" >&2; exit 1
fi
echo "tenant_host_release_preflight: PASS"
