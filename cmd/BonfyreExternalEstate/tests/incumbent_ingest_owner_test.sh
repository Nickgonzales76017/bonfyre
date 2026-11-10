#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-incumbent-ingest.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
printf '%s\n' '{"provider":"github","kind":"github_repository","source_ref":"source:github","receipt_ref":"receipt:github","observed_at":"2026-09-28T00:00:00Z","payload":{"id":"42","title":"org/repo","url":"https://github.test/org/repo","description":"Observed"}}' >"$tmp/input.json"
bun "$root/cmd/BonfyreExternalEstate/tools/ingest_incumbent_record.mjs" --input "$tmp/input.json" --cms "$root/cmd/BonfyreCMS/bonfyre-cms" --db "$tmp/cms.db" --schemas "$root/cmd/BonfyreCMS/content-types" | grep -q 'created:project'
bun "$root/cmd/BonfyreExternalEstate/tools/ingest_incumbent_record.mjs" --input "$tmp/input.json" --cms "$root/cmd/BonfyreCMS/bonfyre-cms" --db "$tmp/cms.db" --schemas "$root/cmd/BonfyreCMS/content-types" | grep -q 'updated:project'
sqlite3 "$tmp/cms.db" "SELECT title FROM project LIMIT 1" | grep -q '^org/repo$'
sqlite3 "$tmp/cms.db" 'SELECT count(*) FROM project' | grep -q '^1$'
echo 'incumbent ingest owner: PASS'
