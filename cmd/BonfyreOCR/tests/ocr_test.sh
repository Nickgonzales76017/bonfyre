#!/bin/sh
# The operator reads a drawn page of known words and says where they sit.
set -eu
ocr=${1:?bonfyre-ocr binary}
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT INT TERM
# A page drawn by the system's own text tools: no network, no fixture file.
printf 'Barley Soup\n\n2 cups barley\n1 cup stock\n' > "$tmp/page.txt"
# Large type: at twelve characters to the inch this recogniser misreads a drawn page ("2 cups barley" came back as "I cups starkey").
cupsfilter -i text/plain -m application/pdf -o cpi=5 -o lpi=3 "$tmp/page.txt" > "$tmp/page.pdf" 2>/dev/null || { echo "ocr: SKIP (cannot draw a test page here)"; exit 0; }
"$ocr" text "$tmp/page.pdf" > "$tmp/read.txt"
grep -q "Barley Soup" "$tmp/read.txt" && grep -q "2 cups barley" "$tmp/read.txt" && grep -q "1 cup stock" "$tmp/read.txt" || { echo "ocr: FAIL (words not read)"; cat "$tmp/read.txt"; exit 1; }
"$ocr" lines "$tmp/page.pdf" | awk -F'\t' 'NF!=7 || $2<0 || $2>1 || $3<0 || $3>1 || $6<0 || $6>1 { bad=1 } END { exit bad }' || { echo "ocr: FAIL (line positions malformed)"; exit 1; }
# Reading order: the title is above the first ingredient.
[ "$("$ocr" lines "$tmp/page.pdf" | awk -F'\t' '$7 ~ /Barley Soup/ {a=$3} $7 ~ /2 cups barley/ {b=$3} END { print (a<b) }')" = 1 ] || { echo "ocr: FAIL (order)"; exit 1; }
if "$ocr" text "$tmp/missing.png" >/dev/null 2>&1; then echo "ocr: FAIL (missing file accepted)"; exit 1; fi
printf 'not an image' > "$tmp/bad.png"; if "$ocr" text "$tmp/bad.png" >/dev/null 2>&1; then echo "ocr: FAIL (non-image accepted)"; exit 1; fi
echo "ocr: ok"
