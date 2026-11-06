#!/bin/sh
# Two accounts at one provider keep two values.
#   - a store made before accounts existed opens, and its values still apply,
#     now under the estate's own account;
#   - the same provider and field hold a separate value per account, and
#     status, clear and apply touch only the named account;
#   - a sealed value copied into another account's slot does not open there;
#   - an unreadable providers file is named, never a garbled parser message.
# What this does not prove: which real person owns an account, or that a
# provider accepted any key. Only the store and its sealing are exercised.
set -eu
bin=${1:?credentials binary required}
old=${2:-}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-credentials-accounts.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
problems=0
check() { if eval "$1"; then :; else echo "credentials accounts: FAIL $2" >&2; problems=$((problems + 1)); fi; }

cat > "$tmp/providers.json" <<'JSON'
[{"provider":"stub","name":"Stub","purpose":"p","get_it":"https://example.invalid/",
  "fields":[{"field":"token","label":"Token","kind":"token","sensitive":true,"required":true,"generator":"","help":"h"}],
  "apply":{"headers":[{"name":"Authorization","value":"Bearer {token}"}],"query":[]},"cannot_infer":"c"}]
JSON
db="$tmp/credentials.db"; key="$tmp/credentials.key"; prov="$tmp/providers.json"
"$bin" init-key --key "$key" >/dev/null
base="--db $db --key $key --providers $prov"

if [ -n "$old" ] && [ -x "$old" ]; then
    # Seal with the binary that predates accounts, then open with this one.
    printf 'estate-token-0001' | "$old" set $base --provider stub --field token >/dev/null
    check '[ "$(sqlite3 "$db" "select count(*) from pragma_table_info(\"sealed_field\") where name=\"account\"")" = 0 ]' "the old store has no account column"
fi
[ -n "$old" ] || printf 'estate-token-0001' | "$bin" set $base --provider stub --field token >/dev/null

"$bin" status --db "$db" --providers "$prov" >/dev/null
check '[ "$(sqlite3 "$db" "select count(*) from sealed_field where account=\"\" and provider=\"stub\"")" = 1 ]' "the earlier value moved into the estate account"
check '"$bin" apply $base --provider stub --url https://example.invalid/x | grep -q "Bearer estate-token-0001"' "the earlier value still opens"

printf 'gmail-token-2222' | "$bin" set $base --provider stub --field token --account nick@gmail.example >/dev/null
printf 'proton-token-3333' | "$bin" set $base --provider stub --field token --account nick@proton.example >/dev/null
check '[ "$(sqlite3 "$db" "select count(*) from sealed_field where provider=\"stub\" and field=\"token\"")" = 3 ]' "three accounts keep three values"
check '"$bin" apply $base --provider stub --url https://example.invalid/x --account nick@gmail.example | grep -q "Bearer gmail-token-2222"' "gmail applies its own value"
check '"$bin" apply $base --provider stub --url https://example.invalid/x --account nick@proton.example | grep -q "Bearer proton-token-3333"' "proton applies its own value"
check '"$bin" apply $base --provider stub --url https://example.invalid/x | grep -q "Bearer estate-token-0001"' "the estate value is untouched"
check '"$bin" status --db "$db" --providers "$prov" --account nick@gmail.example | grep -q "\"account\":\"nick@gmail.example\".*\"shows\":\"2222\""' "status reads the named account"
check '! "$bin" status --db "$db" --providers "$prov" --account someone@else.example | grep -q "\"state\":\"set\""' "an account with nothing stored shows nothing set"

sqlite3 "$db" "update sealed_field set sealed=(select sealed from sealed_field where account='nick@gmail.example') where account='nick@proton.example'"
check '! "$bin" apply $base --provider stub --url https://example.invalid/x --account nick@proton.example 2>/dev/null | grep -q "gmail-token"' "a value moved to another account does not open"

"$bin" clear --db "$db" --providers "$prov" --provider stub --field token --account nick@proton.example >/dev/null
check '[ "$(sqlite3 "$db" "select count(*) from sealed_field where provider=\"stub\"")" = 2 ]' "clear removes only the named account"
check '[ "$(sqlite3 "$db" "select count(*) from audit where account=\"nick@proton.example\" and action=\"clear\"")" = 1 ]' "the audit names the account"
check '! "$bin" status --db "$db" --providers "$prov" --account "Nick Gmail" 2>/dev/null' "an account name outside the allowed letters is refused"

err=$("$bin" status --db "$db" --providers "$tmp/missing.json" 2>&1 || true)
check 'printf "%s" "$err" | grep -q "missing.json: No such file or directory"' "a missing providers file is named plainly"
printf '{not json' > "$tmp/bad.json"
err=$("$bin" status --db "$db" --providers "$tmp/bad.json" 2>&1 || true)
check 'printf "%s" "$err" | grep -q "bad.json: "' "a malformed providers file is named"

[ "$problems" = 0 ] || exit 1
echo 'credentials accounts: PASS'
