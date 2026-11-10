#!/bin/sh
# What this proves: the books are derived from the records and from nothing
# else. Debits equal credits, the model cost on the books is the sum of the
# cost rows, the receivable is invoices less payments, and every journal line
# names the row it came from.
# What this does not prove: that the records are complete or that money moved;
# the books say what the stores say.
set -eu
finance=${1:?bonfyre-finance required}
pay_store=${2:?a pay store with at least one invoice is required}
economy_store=${3:?an economy store with recorded costs is required}
case "$finance" in /*) ;; *) finance="$(pwd)/$finance";; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-books-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

"$finance" books --pay-db "$pay_store" --economy-db "$economy_store" > "$tmp/books.json"
grep -q '"balanced":true' "$tmp/books.json"
field() { sed -n "s/.*\"$1\":\\(-\\{0,1\\}[0-9]*\\).*/\\1/p" "$tmp/books.json"; }

costs=$(sqlite3 "file:$economy_store?mode=ro" "select count(*) from costs")
cost_micros=$(sqlite3 "file:$economy_store?mode=ro" "select cast(round(sum(round(usd*1000000))) as integer) from costs")
invoiced=$(sqlite3 "file:$pay_store?mode=ro" "select coalesce(sum(amount_cents),0)*10000 from invoices")
paid=$(sqlite3 "file:$pay_store?mode=ro" "select coalesce(sum(amount_cents),0)*10000 from payments")
credited=$(sqlite3 "file:$pay_store?mode=ro" "select coalesce(sum(amount_cents),0)*10000 from credits")
test "$(field model_cost_recorded)" = "$cost_micros"
test "$(field cash_evidenced)" = "$paid"
test "$(field receivable_outstanding)" = "$((invoiced - paid - credited))"
test "$(field total_debit)" = "$(field total_credit)"

"$finance" books --pay-db "$pay_store" --economy-db "$economy_store" --journal > "$tmp/journal.tsv"
rows=$(($(wc -l < "$tmp/journal.tsv") - 1))
sources=$(sqlite3 "file:$pay_store?mode=ro" "select (select count(*) from invoices)+(select count(*) from payments)+(select count(*) from credits)")
test "$rows" = "$((sources + costs))"
test "$(grep -c '^cost:' "$tmp/journal.tsv")" = "$costs"

# No stores, no books.
if "$finance" books > /dev/null 2>&1; then echo 'books without records' >&2; exit 1; fi
if "$finance" books --pay-db "$economy_store" > /dev/null 2>&1; then echo 'books from a store that is not a pay store' >&2; exit 1; fi

echo "books: PASS ($rows journal lines; model cost $cost_micros micro-USD; receivable $(field receivable_outstanding))"
