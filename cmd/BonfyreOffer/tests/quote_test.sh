#!/bin/sh
# What this proves: a review becomes a quoted offer only when every term is
# stated, the quote carries the digest of the review it was made about, it is
# made once, and the message another organ composes from it is the promise
# that was stated and nothing else.
# What this does not prove: that anyone was quoted. The terms below are
# written by this test; no recipient exists and nothing is sent.
set -eu
offer=${1:?bonfyre-offer required}
distribute=${2:?bonfyre-distribute required}
review=${3:?an offer.json written by bonfyre-offer generate is required}
for b in offer distribute; do eval "p=\$$b"; case "$p" in /*) ;; *) eval "$b=\"$(pwd)/$p\"";; esac; done
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-quote-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
grep -q '"commercialState":"not_quoted"' "$review"

quote() { "$offer" quote "$review" --out "$tmp/quoted.json" "$@"; }
refused() { if quote "$@" > /dev/null 2> "$tmp/err"; then echo "quoted without: $*" >&2; exit 1; fi; test ! -e "$tmp/quoted.json"; }
refused --recipient-need n --scope s --capacity c --price-cents 5000 --currency USD --turnaround t --promise p --approved-by a
refused --recipient-need n --scope s --capacity c --permissions r --currency USD --turnaround t --promise p --approved-by a
refused --recipient-need n --scope s --capacity c --permissions r --price-cents 0 --currency USD --turnaround t --promise p --approved-by a
refused --recipient-need n --scope s --capacity c --permissions r --price-cents 5000 --currency dollars --turnaround t --promise p --approved-by a
refused --recipient-need n --scope s --capacity c --permissions r --price-cents 5000 --currency USD --turnaround t --promise p

quote --recipient-need "asked for a checked brief of one meeting" --scope "one brief with its source" \
  --capacity "the brief and proof organs, free this week" --permissions "recording supplied by the recipient" \
  --price-cents 5000 --currency USD --turnaround "two working days" \
  --promise "One recorded meeting returned as a checked brief with its source." --approved-by "test approver" > /dev/null
grep -q '"commercialState":"quoted"' "$tmp/quoted.json"
grep -q '"priceCents":5000' "$tmp/quoted.json"
digest=$(shasum -a 256 "$review" | cut -d' ' -f1)
grep -q "\"reviewSha256\":\"$digest\"" "$tmp/quoted.json"

# A quote is made about a review, not about a quote.
if "$offer" quote "$tmp/quoted.json" --out "$tmp/again.json" --recipient-need n --scope s --capacity c --permissions r \
  --price-cents 1 --currency USD --turnaround t --promise p --approved-by a > /dev/null 2>&1; then
  echo 'a quote was quoted again' >&2; exit 1
fi

name=$(sed -n 's/.*"offerName":"\([^"]*\)".*/\1/p' "$tmp/quoted.json" | head -1)
"$distribute" message "$tmp/quoted.json" "$name" email > "$tmp/message.out"
grep -q '^Message: One recorded meeting returned as a checked brief with its source\. Price: 50\.00 USD\. Turnaround: two working days\.$' "$tmp/message.out"
if "$distribute" message "$review" "$name" email > /dev/null 2>&1; then echo 'a message was composed for an unquoted review' >&2; exit 1; fi

echo "quote: PASS (five incomplete quotes refused; quoted once; message is the stated promise, price and turnaround)"
