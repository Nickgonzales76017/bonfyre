#!/bin/sh
# What this proves: a payment is recorded only against something that shows
# money arrived, the same reference is never counted twice, and an invoice is
# marked paid only when the recorded payments reach its amount.
# What this does not prove: that any money moved. The invoice is raised from a
# real meter store, but the references below are written by this test and no
# processor or bank was asked about them.
set -eu
pay=${1:?bonfyre-pay required}
meter_store=${2:?a meter store holding one key and one month of priced events is required}
case "$pay" in /*) ;; *) pay="$(pwd)/$pay";; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-payment-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/pay.db"

"$pay" --db "$db" invoice --user-id 1 --api-key auto --period auto --meter-db "$meter_store" > "$tmp/invoice.out"
cents=$(sed -n 's/.*(\([0-9]*\) cents).*/\1/p' "$tmp/invoice.out")
test "$cents" -gt 1

refused() { if "$pay" --db "$db" pay "$@" > "$tmp/out" 2> "$tmp/err"; then echo "accepted: $*" >&2; exit 1; fi; }
refused --invoice-id 1 --amount "$cents" --method bank
refused --invoice-id 1 --amount 0 --method bank --reference none
refused --invoice-id 9 --amount "$cents" --method bank --reference none
test "$(sqlite3 "$db" 'select count(*) from payments')" = 0
test "$(sqlite3 "$db" 'select status from invoices where id=1')" = pending

"$pay" --db "$db" pay --invoice-id 1 --amount 1 --method bank --reference test-a | grep -q 'partial payment'
refused --invoice-id 1 --amount $((cents - 1)) --method bank --reference test-a
test "$(sqlite3 "$db" 'select status from invoices where id=1')" = pending
"$pay" --db "$db" pay --invoice-id 1 --amount $((cents - 1)) --method bank --reference test-b | grep -q 'PAID'
test "$(sqlite3 "$db" 'select status from invoices where id=1')" = paid
test "$(sqlite3 "$db" 'select sum(amount_cents) from payments')" = "$cents"

echo "payment: PASS (invoice of $cents cents; no reference, zero, unknown invoice and repeated reference refused)"
