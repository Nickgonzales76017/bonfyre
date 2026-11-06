#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
if ! command -v docker >/dev/null 2>&1; then
    echo 'web address compose service: SKIP (docker CLI unavailable)'
    exit 0
fi
docker compose -f "$root/docker-compose.yml" config >"${TMPDIR:-/tmp}/bonfyre-web-address-compose.$$"
trap 'rm -f "${TMPDIR:-/tmp}/bonfyre-web-address-compose.$$"' EXIT INT TERM
grep -q 'bonfyre-web-address' "${TMPDIR:-/tmp}/bonfyre-web-address-compose.$$"
grep -q 'bonfyre-web-address bootstrap-owned --db /data/matrix.db' "${TMPDIR:-/tmp}/bonfyre-web-address-compose.$$"
grep -q 'exec bonfyre-web-address' "${TMPDIR:-/tmp}/bonfyre-web-address-compose.$$"
grep -q 'host_ip: 127.0.0.1' "${TMPDIR:-/tmp}/bonfyre-web-address-compose.$$"
grep -q 'target: 8787' "${TMPDIR:-/tmp}/bonfyre-web-address-compose.$$"
echo 'web address compose service: PASS'
