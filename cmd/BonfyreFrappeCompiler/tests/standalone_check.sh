#!/bin/sh
# What the binary can prove by itself, with no donor application, no estate
# and no network: it compiles a condition to a plan, evaluates it over a
# record, keeps the dialect's distinctions, refuses what it does not carry by
# name, and reads a metric from a store it opens read only. Needs sqlite3.
set -eu
bin=${1:?compiler binary}
fail() { echo "FAIL: $1" >&2; exit 1; }
field() { printf '%s' "$1" | sed -n "s/.*\"$2\":\\([^,}]*\\).*/\\1/p"; }
answer() { "$bin" --eval-condition "$1" --doc "$2"; }

"$bin" --help >/dev/null || fail "--help"
out=$(answer 'eval:doc.repeat_this_event && doc.repeat_on=="Weekly"' '{"repeat_this_event":1,"repeat_on":"Weekly"}')
[ "$(field "$out" status)" = '"compiled"' ] && [ "$(field "$out" result)" = true ] || fail "a two-field condition"
# Loose and strict equality differ, and so do an empty list bare and under eval.
[ "$(field "$(answer 'eval:doc.a==1' '{"a":"1"}')" result)" = true ] || fail "loose equality"
[ "$(field "$(answer 'eval:doc.a===1' '{"a":"1"}')" result)" = false ] || fail "strict equality"
[ "$(field "$(answer 'items' '{"items":[]}')" result)" = false ] || fail "a bare empty list"
[ "$(field "$(answer 'eval:doc.items' '{"items":[]}')" result)" = true ] || fail "an empty list under eval"
# Two lists are the same only when read from the same place.
[ "$(field "$(answer 'eval:doc.a != doc.b' '{"a":["x"],"b":["x"]}')" result)" = true ] || fail "two lists alike are not one list"
[ "$(field "$(answer 'eval:doc.a == doc.a' '{"a":["x"]}')" result)" = true ] || fail "a list is itself"
# Donor code is named, not guessed at.
out=$(answer 'eval:frappe.user.has_role("System Manager")' '{}' || true)   # a refusal exits non-zero
[ "$(field "$out" status)" = '"unsupported"' ] && [ "$(field "$out" missing_operation)" != '""' ] || fail "donor code was not refused by name"

if command -v sqlite3 >/dev/null 2>&1; then
  tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT INT TERM
  sqlite3 "$tmp/store.db" "CREATE TABLE applicant(id INTEGER PRIMARY KEY, country TEXT); INSERT INTO applicant(country) VALUES('Kenya'),('Kenya'),('Peru');"
  printf '%s' '{"doctype":"Dashboard Chart","name":"By Country","chart_name":"By Country","chart_type":"Group By","document_type":"Applicant","group_by_based_on":"country","group_by_type":"Count","filters_json":"[]"}' > "$tmp/chart.json"
  out=$("$bin" --read-metric "$tmp/chart.json" --store "$tmp/store.db" --table applicant)
  case "$out" in *'"status":"read"'*'{"label":"Kenya","value":2},{"label":"Peru","value":1}'*) ;; *) fail "a grouped chart read from a store: $out" ;; esac
  out=$("$bin" --read-metric - --store "$tmp/store.db" --table applicant < "$tmp/chart.json")
  case "$out" in *'"status":"read"'*) ;; *) fail "a definition on standard input" ;; esac
fi
echo "PASS standalone check"
