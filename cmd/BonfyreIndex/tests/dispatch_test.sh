#!/bin/sh
set -eu

index=${1:?index binary required}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-index-dispatch.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

cp "$script_dir/fixtures/artifact.json" "$tmp/artifact.json"
"$index" build "$tmp" --db "$tmp/index.db" >/dev/null
"$index" stats --db "$tmp/index.db" >/dev/null
"$index" search missing --db "$tmp/index.db" --limit 1 >/dev/null
"$index" collapse-preview --db "$tmp/index.db" --limit 1 --json >/dev/null

set +e
"$index" unknown-verb --db "$tmp/missing.db" >/dev/null 2>&1
unknown_status=$?
BONFYRE_INDEX_DB="$tmp/index.db" "$index" search >/dev/null 2>&1
incomplete_status=$?
set -e
if [ "$unknown_status" -ne 1 ] || [ "$incomplete_status" -ne 1 ] || [ -e "$tmp/missing.db" ]; then
    printf 'index dispatch mismatch: unknown=%s incomplete=%s store_created=%s\n' \
        "$unknown_status" "$incomplete_status" "$([ -e "$tmp/missing.db" ] && printf yes || printf no)" >&2
    exit 1
fi
printf '%s\n' 'index dispatch: PASS'
