#!/bin/sh
set -eu
cd "$(dirname "$0")"
out="$(mktemp "${TMPDIR:-/tmp}/bonfyre-dating-policy.XXXXXX")"
trap 'rm -f "$out"' EXIT HUP INT TERM
"${CC:-cc}" -std=c11 -Wall -Wextra -Werror -pedantic -O2 dating_policy_test.c -o "$out"
"$out"
