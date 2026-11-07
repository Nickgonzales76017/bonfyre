#!/bin/sh
# Render a prepared product-copy review fixture to HTML and to PDF through the
# real headless Chrome, and check the words, escaping, refusal and page count.
#   product-review-test.sh PRODUCT_REVIEW_BINARY
set -eu
binary=$(CDPATH='' cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
root=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-product-review.XXXXXX")
trap 'rm -rf "$root"' 0 1 2 15
fail() { echo "product review: FAIL $*" >&2; exit 1; }
# A 1x1 PNG stands in for the observed product photo.
printf 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==' | base64 -d > "$root/photo.png"
review() { # state
  cat > "$root/review.json" <<EOF
{"schema":"service-site.product-copy-review.v1","state":"$1","client":"RED3","title":"Easel \"studio\" review",
 "summary":"Copy for <the> easel & stand.","handoff":"Send notes back by Friday.",
 "proposed_copy":{"description":"A sturdy easel—built for studios.","features":["Folds flat","Holds 30 lb"],
  "seo_title":"Studio easel","seo_description":"An easel for working artists.","image_filename":"studio-easel.png",
  "image_alt":"Easel on a white floor","collection_proposals":["Easels","Studio"]},
 "source":{"url":"https://example.test/easel?a=1&b=2","image_url":"https://example.test/easel.png","image_path":"$root/photo.png",
  "price_observed":"\$149","price_note":"Seen on the legacy page","observed_at":"2026-10-08T03:30:00Z"},
 "questions":["Is the 30 lb rating current?","Which finish ships?"],
 "review_notes":["one","two","Supplier specifications are unconfirmed. 📏"]}
EOF
}
review draft
"$binary" "$root/review.json" "$root/refused.html" >/dev/null 2>"$root/err" && fail "a draft review must be refused"
grep -q 'Expected a prepared product-copy review.' "$root/err" || fail "refusal message"
[ ! -e "$root/refused.html" ] || fail "refusal wrote a page"
"$binary" "$root/review.json" "$root/out.txt" >/dev/null 2>&1 && fail "an unknown output kind must be refused"

review prepared_for_review
out=$("$binary" "$root/review.json" "$root/review.html")
printf '%s' "$out" | grep -q '"transmitted":false,"merchant_published":false' || fail "receipt: $out"
html=$root/review.html
grep -q 'Easel &quot;studio&quot; review' "$html" && grep -q 'Copy for &lt;the&gt; easel &amp; stand.' "$html" || fail "escaping"
grep -q 'A sturdy easel—built for studios.' "$html" && grep -q '📏' "$html" || fail "unicode escapes decoded"
grep -q 'href="https://example.test/easel?a=1&amp;b=2"' "$html" || fail "source link"
grep -q 'Read October 07, 2026, Central Time.' "$html" || fail "Central Time date"
grep -q '<th>Collection candidates</th><td>Easels; Studio</td>' "$html" && grep -q '<td>\$149. Seen on the legacy page</td>' "$html" || fail "table rows"

out=$("$binary" "$root/review.json" "$root/review.pdf")
printf '%s' "$out" | grep -q "\"output\":\"$(cd "$root" && pwd -P)/review.pdf\"" || fail "pdf receipt: $out"
[ "$(head -c 4 "$root/review.pdf")" = '%PDF' ] || fail "not a PDF"
if command -v pdftotext >/dev/null 2>&1; then
  pdftotext "$root/review.pdf" "$root/review.txt"
  for words in 'RED3 / PRODUCT COPY REVIEW' 'Proposed product copy' 'Before this goes into Shopify' 'Prepared by Nick / Aurekai'; do
    grep -q "$words" "$root/review.txt" || fail "PDF lacks: $words"
  done
  [ "$(pdfinfo "$root/review.pdf" | sed -n 's/^Pages: *//p')" = 1 ] || fail "one page"
fi
echo 'product review: PASS (refusal, escaping, unicode, Central date, HTML and Chrome PDF)'
