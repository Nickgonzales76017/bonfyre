#!/bin/sh
# The one vendored asset too big to keep in history, fetched back on demand.
#
# Everything else this estate serves is source and lives in the tree. The
# DuckDB engine the reading room and the field run in a reader's tab is 32.7 MB
# of WebAssembly: regenerable, unchanging for a pinned version, and not
# something to carry in every clone of this repository forever.
#
# It is fetched by version and checked against the digest recorded here, so a
# clone either gets the exact bytes the estate was published with or gets
# nothing and is told why. A wrong engine is worse than a missing one.
#
#   scripts/fetch_web_assets.sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
into="$root/site/matrix"

DUCKDB_VERSION=1.32.0
DUCKDB_WASM_SHA256=4c221bfa59c11f24dbd750e70c90b9252eca6eec5633936e6a2ec766e55fd879
DUCKDB_WASM_URL="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@$DUCKDB_VERSION/dist/duckdb-eh.wasm"

digest() {
    if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
    else sha256sum "$1" | cut -d' ' -f1; fi
}

target="$into/duckdb-eh.wasm"
if [ -f "$target" ] && [ "$(digest "$target")" = "$DUCKDB_WASM_SHA256" ]; then
    echo "duckdb-eh.wasm: already here, digest matches"
    exit 0
fi

mkdir -p "$into"
tmp="$target.fetching.$$"
trap 'rm -f "$tmp"' EXIT INT TERM
echo "duckdb-eh.wasm: fetching $DUCKDB_VERSION"
curl -fsSL -o "$tmp" "$DUCKDB_WASM_URL"

got=$(digest "$tmp")
if [ "$got" != "$DUCKDB_WASM_SHA256" ]; then
    echo "duckdb-eh.wasm: REFUSED -- expected $DUCKDB_WASM_SHA256, got $got" >&2
    echo "Nothing has been written. The estate does not serve an engine it cannot identify." >&2
    exit 1
fi
mv "$tmp" "$target"
echo "duckdb-eh.wasm: $DUCKDB_VERSION in place, digest verified"
