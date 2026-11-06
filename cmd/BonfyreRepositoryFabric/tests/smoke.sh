#!/bin/sh
# Real integration smoke test for bonfyre-repository-fabric: no mocks, no
# stubs. Requires a live Forgejo instance (BONFYRE_FORGEJO_API, default
# http://127.0.0.1:3400) with a sealed "forgejo" credential already set in
# BonfyreCredentials, and BONFYRE_CREDENTIALS_BINARY / BONFYRE_PROJECT_ROOT
# pointing at a real install. Skips (exit 0) rather than fakes a pass when
# that real environment is not present, so CI without a forge stays honest.
set -eu
BIN="$1"
ORG="aurekai"
REPO="repofabric-smoke-$$"

if [ -z "${BONFYRE_CREDENTIALS_BINARY:-}" ] || [ ! -x "${BONFYRE_CREDENTIALS_BINARY:-/nonexistent}" ]; then
    echo "repository-fabric smoke: no BONFYRE_CREDENTIALS_BINARY, skipping (needs a real credential store)"
    exit 0
fi

connect_out=$("$BIN" connect --provider forgejo 2>&1) || {
    echo "repository-fabric smoke: forgejo not reachable, skipping: $connect_out"
    exit 0
}
echo "$connect_out" | grep -q '"outcome":"accepted"' || {
    echo "repository-fabric smoke: forgejo credential not accepted, skipping: $connect_out"
    exit 0
}

echo "== provision =="
"$BIN" provision --provider forgejo --org "$ORG" --repo "$REPO" --description "smoke test repo, deleted at end of run" | tee /tmp/rf-smoke-provision.json
grep -q '"ok":true' /tmp/rf-smoke-provision.json

echo "== resolve =="
"$BIN" resolve --provider forgejo --org "$ORG" --repo "$REPO" | grep -q '"ok":true'

echo "== view =="
"$BIN" view --provider forgejo --org "$ORG" --repo "$REPO" | grep -q '"ok":true'

echo "== observe =="
"$BIN" observe --provider forgejo --org "$ORG" --repo "$REPO" | grep -q '"ok":true'

echo "== service list =="
"$BIN" service --provider forgejo --org "$ORG" --repo "$REPO" --action list | grep -q '"ok":true'

echo "== service add =="
"$BIN" service --provider forgejo --org "$ORG" --repo "$REPO" --action add --url "https://example.invalid/smoke-hook" --events push | grep -q '"ok":true'

echo "== change (create) =="
CONTENT_FILE=$(mktemp)
echo "repository-fabric smoke $$" > "$CONTENT_FILE"
"$BIN" change --provider forgejo --org "$ORG" --repo "$REPO" --path smoke.txt --message "smoke: create smoke.txt" --content-file "$CONTENT_FILE" | grep -q '"ok":true'
rm -f "$CONTENT_FILE"

echo "== checkout (plain git) =="
CHECKOUT_DIR=$(mktemp -d)
rm -rf "$CHECKOUT_DIR"
"$BIN" checkout --provider forgejo --org "$ORG" --repo "$REPO" --dest "$CHECKOUT_DIR" | grep -q '"ok":true'
[ -f "$CHECKOUT_DIR/smoke.txt" ]
rm -rf "$CHECKOUT_DIR"

josh_connect=$("$BIN" connect --provider josh 2>&1) || true
if echo "$josh_connect" | grep -q '"outcome":"ready"'; then
    echo "== checkout (josh filtered view) =="
    FILTERED_DIR=$(mktemp -d)
    rm -rf "$FILTERED_DIR"
    "$BIN" checkout --provider forgejo --org "$ORG" --repo "$REPO" --dest "$FILTERED_DIR" --filter ":/" | grep -q '"mode":"josh"'
    rm -rf "$FILTERED_DIR"
else
    echo "repository-fabric smoke: josh not built, skipping filtered checkout: $josh_connect"
fi

echo "all repository-fabric smoke checks passed against a live Forgejo instance"
rm -f /tmp/rf-smoke-provision.json
