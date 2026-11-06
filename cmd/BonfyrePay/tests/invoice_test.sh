#!/bin/sh
# What this proves: an invoice is the metered events for a key and period at the
# rates the meter admitted, and that without priced events there is no invoice.
set -eu
pay=${1:?bonfyre-pay required}
meter=${2:?bonfyre-meter required}
case "$pay" in /*) ;; *) pay="$(pwd)/$pay";; esac
case "$meter" in /*) ;; *) meter="$(pwd)/$meter";; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-pay-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
m="$tmp/meter.db" p="$tmp/pay.db"

"$meter" admit-rate --db "$m" --provider local --op transcribe --unit call --amount 0.5 \
  --evidence receipt:test:rate --effective-at 2026-10-01T00:00:00Z >/dev/null 2>&1
record() { "$meter" record --db "$m" --key "$1" --op "$2" --provider local --receipt "r:$3" \
  --attempt-ref "a:$3" --realization-ref "z:$3" --at "2026-10-0${3}T12:00:00Z" >/dev/null 2>&1; }
record k_billed transcribe 2
record k_billed transcribe 3
record k_billed brief 4
record k_unpriced brief 5

"$pay" --db "$p" invoice --user-id 7 --api-key k_billed --meter-db "$m" --period 2026-10 > "$tmp/out"
grep -q 'Amount: \$1.00 (100 cents)' "$tmp/out"
grep -q 'Metered events: 3 (1 unpriced, not billed)' "$tmp/out"
items=$(sqlite3 "$p" "select line_items_json from invoices where api_key='k_billed'")
printf '%s' "$items" | grep -q '"op":"transcribe","events":2,"priced_events":2,"unpriced_events":0,"amount_usd":1.000000'
printf '%s' "$items" | grep -q '"op":"brief","events":1,"priced_events":0,"unpriced_events":1,"amount_usd":null'

refused() { if "$pay" --db "$p" invoice "$@" >/dev/null 2>&1; then echo "wrote an invoice it had no basis for: $*" >&2; exit 1; fi; }
refused --user-id 7 --api-key nobody --meter-db "$m" --period 2026-10
refused --user-id 8 --api-key k_unpriced --meter-db "$m" --period 2026-10
refused --user-id 7 --api-key k_billed --meter-db "$m" --period 2026-09
refused --user-id 7 --api-key k_billed --meter-db "$tmp/absent.db" --period 2026-10
refused --user-id 7 --meter-db "$m" --period 2026-10
[ "$(sqlite3 "$p" 'select count(*) from invoices')" = 1 ]

# The reference is written by this test; no processor was asked about it.
"$pay" --db "$p" pay --invoice-id 1 --amount 100 --method wire --reference invoice-test-wire-1 | grep -q 'PAID'
printf '%s\n' 'pay invoice: PASS'
