#!/bin/sh
# foreign promote/twins/duplicates must address an existing Matrix without a
# declaration. On the Mac estate that is $HOME/.bonfyre/estate-local/matrix.db;
# before this, every undeclared promote refused with "cannot open matrix at
# /var/lib/bonfyre/matrix.db". A declared BONFYRE_MATRIX_DB still wins, an
# empty placeholder file is never promoted into, and nothing is created.
# Every store here is a throwaway copy under a temporary HOME.
set -eu

bin=${1:-./bonfyre-project}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-foreign-matrix.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

if [ -s /var/lib/bonfyre/matrix.db ]; then
    echo "SKIP: /var/lib/bonfyre/matrix.db exists here and outranks the home store"
    exit 0
fi

receipts="$tmp/receipts"; bodies="$tmp/bodies"
mkdir -p "$receipts" "$bodies"
url="https://fixture.example/README.md"
printf '# Fixture\n\nA retained body.\n' >"$bodies/fixture.body"
sha=$(shasum -a 256 "$bodies/fixture.body" | cut -d' ' -f1)
cat >"$receipts/fixture-20261030T000000Z-0000000000.json" <<JSON
{"schema":"bonfyre-foreign-observation.v1","twin_id":"fixture-readme","source_url":"$url","observed_at":"2026-10-30T00:00:00Z","observe_right":"public","policy_decision":"allow","fetch_attempted":true,"transport_exit":0,"http_status":200,"body_bytes":31,"content_sha256":"$sha","body_path":"$bodies/fixture.body","availability_claim":false,"observation_kind":"source_observed"}
JSON

subjects() { sqlite3 "$1" "SELECT count(*) FROM matrix_subjects"; }
fresh_copy() { # src dst: a schema-bearing Matrix with no subjects
    cp "$1" "$2"
    sqlite3 "$2" "DELETE FROM matrix_addresses; DELETE FROM matrix_subjects;"
}
run() { # HOME, then args
    h=$1; shift
    env -u BONFYRE_MATRIX_DB -u BONFYRE_STATE_ROOT HOME="$h" \
        BONFYRE_FOREIGN_RECEIPTS="$receipts" "$bin" "$@"
}

# 1. A declared store wins and is the one written.
mkdir -p "$tmp/declared"
declared="$tmp/declared/matrix.db"
BONFYRE_MATRIX_DB="$declared" BONFYRE_FOREIGN_RECEIPTS="$receipts" \
    "$bin" foreign promote "$url" >"$tmp/declared.json"
[ "$(subjects "$declared")" = 1 ] || { echo "FAIL: declared promote did not land one subject"; exit 1; }

# 2. No declaration: the home estate Matrix (real, non-empty) is used.
home_ok="$tmp/home-ok"; mkdir -p "$home_ok/.bonfyre/estate-local"
home_db="$home_ok/.bonfyre/estate-local/matrix.db"
fresh_copy "$declared" "$home_db"
run "$home_ok" foreign promote "$url" >"$tmp/home.json" 2>"$tmp/home.err" \
    || { echo "FAIL: undeclared promote refused"; cat "$tmp/home.err"; exit 1; }
[ "$(subjects "$home_db")" = 1 ] || { echo "FAIL: home estate Matrix did not receive the subject"; exit 1; }
grep -q '"promoted"' "$tmp/home.json" || { echo "FAIL: no promote report"; exit 1; }

# 3. BONFYRE_STATE_ROOT outranks the home store, which stays untouched.
mkdir -p "$tmp/root"
fresh_copy "$declared" "$tmp/root/matrix.db"
fresh_copy "$declared" "$home_db"
env -u BONFYRE_MATRIX_DB HOME="$home_ok" BONFYRE_STATE_ROOT="$tmp/root" \
    BONFYRE_FOREIGN_RECEIPTS="$receipts" "$bin" foreign promote "$url" >/dev/null
[ "$(subjects "$tmp/root/matrix.db")" = 1 ] || { echo "FAIL: STATE_ROOT matrix not used"; exit 1; }
[ "$(subjects "$home_db")" = 0 ] || { echo "FAIL: home store written while STATE_ROOT held one"; exit 1; }

# 4. An empty placeholder is not a Matrix: refuse, name /var/lib, create nothing.
home_empty="$tmp/home-empty"; mkdir -p "$home_empty/.bonfyre/estate-local"
: >"$home_empty/.bonfyre/estate-local/matrix.db"
if run "$home_empty" foreign promote "$url" >/dev/null 2>"$tmp/empty.err"; then
    echo "FAIL: promoted into an empty placeholder"; exit 1
fi
grep -q "cannot open matrix at /var/lib/bonfyre/matrix.db" "$tmp/empty.err" \
    || { echo "FAIL: refusal did not name where it looked"; cat "$tmp/empty.err"; exit 1; }
[ ! -s "$home_empty/.bonfyre/estate-local/matrix.db" ] || { echo "FAIL: placeholder was written"; exit 1; }

# 5. A dry run needs no Matrix at all.
run "$home_empty" foreign promote --dry-run "$url" >"$tmp/dry.json"
grep -q '"promoted"' "$tmp/dry.json" || { echo "FAIL: dry run did not report"; exit 1; }

echo "PASS foreign_matrix_default_test"
