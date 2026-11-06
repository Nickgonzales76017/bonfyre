#!/bin/sh
# Witness for bonfyre-provider-estate: the shipped ledgers pass; a pasted
# credential, a stage claimed without its evidence, and an unknown provider are
# each refused; summary is one JSON object; next and matrix read the ledgers.
set -eu
bin=$(CDPATH= cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/provider-estate.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
fail() { echo "provider estate: FAIL: $1" >&2; exit 1; }
fresh() { rm -rf "$tmp/d"; mkdir "$tmp/d"; cp "$here/providers.tsv" "$here/acquisition.tsv" "$here/routes.tsv" "$here/prefill.tsv" "$tmp/d/"; }
# set_cell SLUG COLUMN VALUE  (1-based column in acquisition.tsv)
set_cell() { awk -F'\t' -v OFS='\t' -v s="$1" -v c="$2" -v v="$3" '$1==s{$c=v}1' "$tmp/d/acquisition.tsv" > "$tmp/x" && mv "$tmp/x" "$tmp/d/acquisition.tsv"; }
refused() { if "$bin" check --dir "$tmp/d" >/dev/null 2>"$tmp/err"; then fail "$1 was accepted"; fi; grep -q "$2" "$tmp/err" || fail "$1 refused without saying why: $(cat "$tmp/err")"; }

fresh
"$bin" check --dir "$tmp/d" | grep -q "102 providers" || fail "shipped ledgers did not pass"

fresh; set_cell twilio 10 'sk_live_51Habcdefghijklmnop'
refused "a Stripe-shaped key in secret_reference" "secret_reference must name where"
fresh; set_cell github 21 'token ghp_abcdefghijklmnopqrstuvwxyz0123456789'
refused "a GitHub token in notes" "reads like a credential"
fresh; set_cell aws 4 'Zx9Qp2Lm8Rt5Vb7Nk3Hs6Jd1Fg4Wc0Ye2Ua'
refused "a long mixed token in account_id" "reads like a credential"
fresh; set_cell vercel 10 'op://Aurekai/Vercel/token'
"$bin" check --dir "$tmp/d" >/dev/null || fail "a vault reference was refused"

fresh; set_cell cloudflare 2 secured; set_cell cloudflare 3 'nick@example.com'
refused "secured without two-factor" "secured needs two_factor yes"
fresh; set_cell cloudflare 2 account
refused "an account stage with no owner" "needs owner_email"
fresh; set_cell shopify 2 partner_submitted; set_cell shopify 3 'nick@example.com'; set_cell shopify 7 yes; set_cell shopify 8 'paper:safe'; set_cell shopify 4 '12345'
refused "partner_submitted with nothing submitted" "partner_submitted needs"
set_cell shopify 13 submitted
"$bin" check --dir "$tmp/d" >/dev/null || fail "a complete partner_submitted row was refused"

fresh; awk -F'\t' -v OFS='\t' '$2=="twilio"{$10=""}1' "$tmp/d/providers.tsv" > "$tmp/x" && mv "$tmp/x" "$tmp/d/providers.tsv"
refused "a tenancy primitive without a source" "needs evidence"
fresh; printf 'not-a-provider\tnone\t\t\t\tunknown\tunknown\t\t\t\tunknown\tunknown\tnone\t\tunknown\tunknown\tunknown\tunknown\tunknown\tunknown\t\n' >> "$tmp/d/acquisition.tsv"
refused "a tracker row for an unknown provider" "not in providers.tsv"

fresh
"$bin" summary --dir "$tmp/d" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["providers"]==102 and d["stages"]["none"]==102 and sum(d["waves"].values())==102' || fail "summary is not the expected JSON"
"$bin" next --dir "$tmp/d" --wave B --limit 2 | grep -q "Wave B: cloud hierarchy" || fail "next did not group by wave"
[ "$("$bin" matrix --dir "$tmp/d" | wc -l | tr -d ' ')" = 103 ] || fail "matrix is not one row per provider"
fresh; printf 'github\tsignup\thttp://github.com/join\tloads\t2026-10-26\n' >> "$tmp/d/routes.tsv"
refused "a non-https or duplicate route" "one https address"
fresh; "$bin" next --dir "$tmp/d" --limit 1 | grep -q "signup       https://" || fail "next did not print the step routes"
fresh; op="$tmp/operator.sqlite3"
sqlite3 "$op" "CREATE TABLE goals(id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);"
"$bin" publish --dir "$tmp/d" --operator-db "$op" >/dev/null || fail "publish failed"
[ "$(sqlite3 "$op" "select count(*) from goals")" = 1 ] || fail "publish wrote more than one checklist task"
sqlite3 "$op" "select title from goals" | grep -q "^Customer accounts: 8 of 102 providers usable by customers, 7 more connected, 87 to connect$" || fail "checklist title does not count customer-usable, connected and open providers"
details=$(sqlite3 "$op" "select details from goals")
printf "%s\n" "$details" | head -12 | grep -q "^GitHub: customers get .*https://aurekai.work/connect/github-app$" || fail "the provider customers can use was not first, with its connect route"
printf '%s\n' "$details" | grep -q "^AWS: build customers' " || fail "a held account did not name the customer side to build"
printf '%s\n' "$details" | grep -q "^Twilio (gives customers subaccount under a parent account): https://console.twilio.com/us1/account/keys-credentials/api-keys$" || fail "an open provider did not open where its key is made"
if printf '%s\n' "$details" | grep -q "try-twilio\|/signup\|sign-up\|/register"; then
    printf '%s\n' "$details" | grep "try-twilio\|/signup\|sign-up\|/register" | grep -vq "(gives customers" && fail "a sign-up page was offered for a held account"
fi
"$bin" publish --dir "$tmp/d" --operator-db "$op" >/dev/null; [ "$(sqlite3 "$op" "select count(*) from goals")" = 1 ] || fail "publishing twice duplicated the checklist"
set_cell github 2 recorded; set_cell github 3 'nick@example.com'; set_cell github 7 yes; set_cell github 8 'op://Aurekai/GitHub/recovery'; set_cell github 5 'aurekai'
"$bin" publish --dir "$tmp/d" --operator-db "$op" >/dev/null; sqlite3 "$op" "select title from goals" | grep -q "^Customer accounts: 7 of 102 " || fail "a recorded provider stayed on the checklist"
fresh; printf 'github\temail\t2026-10-26\n' >> "$tmp/d/prefill.tsv"; "$bin" check --dir "$tmp/d" >/dev/null || fail "a plain prefill row was refused"
fresh; printf 'nowhere\temail\t2026-10-26\n' >> "$tmp/d/prefill.tsv"
refused "prefill for a provider with no signup route" "prefill needs a signup route"
echo "provider estate: PASS"
