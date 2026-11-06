#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin="$root/cmd/BonfyreDomain/bonfyre-domain"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
secrets="$tmp/provider.refs.env"
printf '%s\n' 'AUREKAI_LIFE_DNS_PASSWORD=not-a-real-secret' > "$secrets"
"$bin" status --only aurekai.life --secrets "$secrets" | grep -q 'action=inspect'
"$bin" plan --only aurekai.life --ip 203.0.113.42 --secrets "$secrets" | grep -q 'action=would_set_203.0.113.42'
"$bin" plan --only aurekai.life --host '*' --ip 203.0.113.42 --secrets "$secrets" | grep -q 'action=would_set_203.0.113.42'
if "$bin" apply --only aurekai.life --ip 203.0.113.42 --secrets "$secrets" >/dev/null 2>&1; then
    echo 'unguarded DNS apply was accepted' >&2
    exit 1
fi
"$bin" plan --only aurekai.shop --ip 203.0.113.42 --secrets "$secrets" 2>&1 | grep -q 'blocked_missing_secret'
echo 'native domain control: PASS'
