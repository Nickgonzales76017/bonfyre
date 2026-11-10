#!/bin/sh
# What this proves: the relationships view is a join and nothing more. Its
# totals equal counts taken directly from the stores it was handed, every
# opportunity appears once, and a store that is not named is reported as
# absent and not as empty.
# What this does not prove: that the stores are complete. An email that was
# sent and never recorded in Outreach is invisible here.
set -eu
finance=${1:?bonfyre-finance required}
fin=${2:?a finance store with opportunities is required}
outreach=${3:?an outreach store is required}
cms=${4:?a content store with CRM deals is required}
commerce=${5:?a commerce store is required}
case "$finance" in /*) ;; *) finance="$(pwd)/$finance";; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-relationships-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
ro() { sqlite3 "$1" "$2"; }   # read-only by what it asks; a WAL store cannot be opened mode=ro without its shm

"$finance" --db "$fin" relationships --outreach-db "$outreach" --cms-db "$cms" --commerce-db "$commerce" > "$tmp/all.json"
total() { sed -n "s/.*\"totals\":{[^}]*\"$1\":\\([0-9]*\\).*/\\1/p" "$tmp/all.json"; }

test "$(total relationships)" = "$(ro "$fin" 'select count(*) from opportunities')"
test "$(grep -c '^{"id":' "$tmp/all.json")" = "$(total relationships)"
test "$(total priced)" = "$(ro "$fin" 'select count(*) from opportunities where compensation_cents is not null')"
test "$(total price_offered_cents)" = "$(ro "$fin" 'select coalesce(sum(compensation_cents),0) from opportunities')"
deals=$(sqlite3 "$cms" "attach '$fin' as f; select count(distinct source) from frappe_crm_crm_deal where source in (select id from f.opportunities)")
test "$(total with_crm_deal)" = "$deals"
test "$(total in_commerce_chain)" = "$(ro "$commerce" "select count(distinct exchange) from commerce_progress where exchange like 'opportunity:%'")"
test "$(total replied)" -le "$(total sent)"
# No opportunity is called paid without a payment reference and revenue in finance.
test "$(total paid)" = "$(ro "$fin" "select count(*) from opportunities where payment_ref is not null and payment_ref<>'' and actual_revenue_cents>0")"

"$finance" --db "$fin" relationships > "$tmp/alone.json"
grep -q '"stores":{"outreach":false,"cms":false,"commerce":false}' "$tmp/alone.json"
grep -q '"outreach_entries":null' "$tmp/alone.json"

echo "relationships: PASS ($(total relationships) opportunities, $(total sent) sent, $(total replied) replied, $(total price_offered_cents) cents offered, $(total paid) paid)"
