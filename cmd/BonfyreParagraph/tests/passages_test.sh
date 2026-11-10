#!/bin/sh
# Passages keep a text's own structure, never cut inside a number, and carry
# record ids through.  Runs the built organ on files it writes here.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
bin="${1:-$here/../bonfyre-paragraph}"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-paragraph-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
fail() { echo "passages_test: $1" >&2; exit 1; }

printf '# Pricing\n\nThe quote was 3.50 a unit at 10:22:39.738Z.\n\nSecond block.\n\n# Delivery\n\nShips Friday.\n\n# Delivery again\n\nShips Friday.\n' > "$tmp/in.md"
"$bin" --input "$tmp/in.md" --out "$tmp/out.txt" --passages "$tmp/out.jsonl" --source-id doc --with-headers >/dev/null
[ "$(grep -c . "$tmp/out.jsonl")" = 2 ] || fail "expected one passage per heading, repeated block dropped"
grep -q '"id":"doc:0","text":"Pricing\\nThe quote was 3.50 a unit at 10:22:39.738Z.\\nSecond block.","heading":"Pricing"' "$tmp/out.jsonl" || fail "structure or number was broken"
grep -q '"passagesRepeated": 1' "$tmp/status.json" || fail "repeated passage not counted"
grep -q '^## Delivery$' "$tmp/out.txt" || fail "heading lost in text output"

# A line of hashtags is text, not a heading.
printf '# Real\n\n#exports #globaltrade #dgft are tags on the post.\n' > "$tmp/tags.md"
"$bin" --input "$tmp/tags.md" --passages "$tmp/tags.jsonl" --source-id t >/dev/null
grep -q '"text":"Real\\n#exports #globaltrade #dgft are tags on the post.","heading":"Real"' "$tmp/tags.jsonl" || fail "hashtags were read as a heading"

# A break line ends the section a heading opened.
printf '# First\n\nUnder the first heading.\n\n---\n\nNot under any heading.\n' > "$tmp/brk.md"
"$bin" --input "$tmp/brk.md" --passages "$tmp/brk.jsonl" --source-id b >/dev/null
grep -q '"text":"Not under any heading.","heading":""' "$tmp/brk.jsonl" || fail "heading carried past a break"

# Unstructured speech is still cut on sentences, and nothing is dropped.
i=0; : > "$tmp/talk.txt"
while [ $i -lt 120 ]; do printf 'Sentence number %d costs 1.25 dollars. ' $i >> "$tmp/talk.txt"; i=$((i+1)); done
"$bin" --input "$tmp/talk.txt" --out "$tmp/talk.out" >/dev/null
[ "$(grep -c . "$tmp/talk.out")" -gt 5 ] || fail "long transcript was not cut"
[ "$(grep -o 'Sentence number' "$tmp/talk.out" | wc -l | tr -d ' ')" = 120 ] || fail "sentences were lost"
if grep -q '1\. 25' "$tmp/talk.out"; then fail "decimal was split"; fi
awk 'length($0) > 1300 { exit 1 }' "$tmp/talk.out" || fail "passage exceeded its bound"

# Records: ids carried, escapes decoded, a line with no id and text refused.
cat > "$tmp/rec.jsonl" <<'REC'
{"id":"note:1","text":"Short \"quoted\" café note."}
{"id":"note:2","text":"# One\n\nAlpha.\n\n# Two\n\nBeta."}
REC
"$bin" --input-jsonl "$tmp/rec.jsonl" --passages "$tmp/rec.out" >/dev/null
grep -q '"id":"note:1","text":"Short \\"quoted\\" café note."' "$tmp/rec.out" || fail "record text not decoded"
grep -q '"id":"note:2#1","text":"Two\\nBeta."' "$tmp/rec.out" || fail "record id not carried to later passages"
echo '{"text":"no id"}' > "$tmp/bad.jsonl"
if "$bin" --input-jsonl "$tmp/bad.jsonl" --passages "$tmp/bad.out" 2>/dev/null; then fail "record without id accepted"; fi
echo 'passages_test: PASS'
