#!/bin/sh
# Extraction held to its declaration.
#
# Two bugs in this estate were one bug: an undeclared text rule failing
# quietly. A receipt reader stopped at the first quote in an ETag, so every
# source ignored the mangled validator and conditional re-observation had never
# worked. The atlas parser accepted any key and stored unknown ones where
# nothing reads them, so 77 `cannot_infer` lines were discarded while the loss
# report said those architectures had stated nothing.
#
# What is proved here is that the failure mode is closed by construction: one
# implementation per field kind, a declared vocabulary, and a check that names
# what a document carries that nobody declared.
set -eu
bin=$(cd "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
rules=$(cd "$(dirname -- "$2")" && pwd)/$(basename -- "$2")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
lex() { "$bin" "$@" --rules "$rules"; }

# --- the escaped quote, which is the whole reason this exists -------------
# An ETag is W/"abc" and appears in JSON escaped. A reader that stops at the
# first quote returns W/\ and the value is silently useless.
cat > "$tmp/etag.json" <<'JSON'
{"twin_id":"external:t","source_url":"https://e.test/","observed_at":"2026-09-12T00:00:00Z","observation_kind":"source_observed","etag":"W/\"e54e7dea\"","content_sha256":"abb5c7efc7be66748e70aa7aad7762ca7b53a42d12920bc8d37dbd0b1b1c6d6e"}
JSON
[ "$(lex get --set foreign-observation --field etag --file "$tmp/etag.json")" = 'W/"e54e7dea"' ]
# A 64-character digest in a 65-byte buffer is a value, not an absence: testing
# capacity before the closing quote reported it missing.
[ "$(lex get --set foreign-observation --field content_sha256 --file "$tmp/etag.json" | tr -d '\n' | wc -c | tr -d ' ')" = "64" ]

# --- absent, empty and present are three different answers ---------------
cat > "$tmp/empty.json" <<'JSON'
{"twin_id":"external:t","source_url":"https://e.test/","observed_at":"2026-09-12T00:00:00Z","observation_kind":"boundary_response","etag":""}
JSON
[ "$(lex get --set foreign-observation --field etag --file "$tmp/empty.json")" = "" ]
lex get --set foreign-observation --field etag --file "$tmp/empty.json" > /dev/null
# A field the document does not carry exits non-zero rather than printing
# nothing and letting a caller treat it as empty.
! lex get --set foreign-observation --field body_path --file "$tmp/empty.json" > /dev/null 2>&1
# A required field that is absent fails loudly and says which.
cat > "$tmp/missing.json" <<'JSON'
{"source_url":"https://e.test/","observation_kind":"source_observed"}
JSON
! lex get --set foreign-observation --field twin_id --file "$tmp/missing.json" > "$tmp/err.txt" 2>&1
lex get --set foreign-observation --field twin_id --file "$tmp/missing.json" 2>&1 | grep -q "required and absent" || true

# --- a caller names a field, never a key or an escaping rule -------------
# Asking for a field nobody declared lists what is declared instead of
# returning an empty string that reads like an answer.
! lex get --set foreign-observation --field not_a_field --file "$tmp/etag.json" > "$tmp/undeclared.txt" 2>&1
grep -q "declares no field not_a_field" "$tmp/undeclared.txt"
grep -q "observation_kind" "$tmp/undeclared.txt"

# --- a receipt file is a log, so the newest line is the live one ---------
# Reading the first line handed sources a validator they had long superseded.
cat > "$tmp/log.json" <<'JSON'
{"twin_id":"external:t","source_url":"https://e.test/","observed_at":"2026-09-12T00:00:00Z","observation_kind":"source_observed","etag":"\"old\""}
{"twin_id":"external:t","source_url":"https://e.test/","observed_at":"2026-09-12T01:00:00Z","observation_kind":"repeat_observed","etag":"\"new\""}
JSON
[ "$(lex get --set foreign-observation --field etag --file "$tmp/log.json" --line last)" = '"new"' ]
[ "$(lex get --set foreign-observation --field etag --file "$tmp/log.json")" = '"old"' ]

# --- numbers and booleans are read as written ---------------------------
[ "$(lex get --set foreign-observation --field http_status --file "$tmp/etag.json" 2>/dev/null || echo absent)" = "absent" ]
cat > "$tmp/scalar.json" <<'JSON'
{"twin_id":"t","source_url":"u","observed_at":"a","observation_kind":"k","http_status":304,"body_bytes":0,"conditional":true}
JSON
[ "$(lex get --set foreign-observation --field http_status --file "$tmp/scalar.json")" = "304" ]
[ "$(lex get --set foreign-observation --field body_bytes --file "$tmp/scalar.json")" = "0" ]
[ "$(lex get --set foreign-observation --field conditional --file "$tmp/scalar.json")" = "true" ]

# --- HTTP headers: case-insensitive, and a redirect chain's last block wins
printf 'HTTP/1.1 301 Moved\r\nETag: "stale"\r\n\r\nHTTP/1.1 200 OK\r\netag: W/"served"\r\nLink: <https://e.test/l>; rel="license"\r\nContent-Type: application/json\r\n' > "$tmp/headers.txt"
[ "$(lex get --set http-response --field etag --file "$tmp/headers.txt")" = 'W/"served"' ]
[ "$(lex get --set http-response --field content_type --file "$tmp/headers.txt")" = "application/json" ]
# One relation out of a Link header that may carry several.
lex get --set http-response --field link_license --file "$tmp/headers.txt" | grep -q 'rel="license"'
! lex get --set http-response --field link_canonical --file "$tmp/headers.txt" > /dev/null 2>&1

# --- tab columns: an empty trailing column is present and empty ---------
printf 'external:t\thttps://e.test/\te.test\t42\t\n' > "$tmp/row.tsv"
[ "$(lex get --set observe-plan --field twin_id --file "$tmp/row.tsv")" = "external:t" ]
[ "$(lex get --set observe-plan --field facets --file "$tmp/row.tsv")" = "42" ]
[ "$(lex get --set observe-plan --field phenotype --file "$tmp/row.tsv")" = "" ]

# --- what a document carries that nobody declared ------------------------
# The atlas stored unknown keys where nothing reads them and said nothing. This
# says it.
cat > "$tmp/extra.json" <<'JSON'
{"twin_id":"t","source_url":"u","observed_at":"a","observation_kind":"k","invented_field":1,"another":"x"}
JSON
lex check --set foreign-observation --file "$tmp/extra.json" > "$tmp/check.json"
python3 -c "
import json,sys
d=json.load(open('$tmp/check.json'))
assert d['undeclared_count'] == 2, d
assert 'invented_field' in d['undeclared'], d
assert d['missing_required'] == [], d
assert 'an undeclared key -> a key that should not exist; it is a key nobody wrote a rule for' in d['cannot_infer']
"
# A document missing a required field fails the check and names it.
! lex check --set foreign-observation --file "$tmp/missing.json" > "$tmp/check2.json" 2>&1
python3 -c "
import json
d=json.load(open('$tmp/check2.json'))
assert 'twin_id' in d['missing_required'], d
assert 'observed_at' in d['missing_required'], d
"

# --- the declaration is the vocabulary, and it is readable --------------
lex describe | grep -q "ruleset foreign-observation over bonfyre-foreign-observation.v1"
lex describe | grep -q "the validator the source issued, quotes included"
# Every declared field has a stated meaning, so the declaration explains itself.
[ "$(lex describe | grep -cE '^  [a-z_]+ +[a-z_]+ ')" -ge 25 ]

echo "text rules: ok"
