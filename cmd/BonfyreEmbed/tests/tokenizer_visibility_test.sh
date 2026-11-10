#!/bin/sh
# What the model can see of a text, and what happens when it can see nothing.
#
# The pre-tokenizer used to walk bytes with ASCII predicates, so a typographic
# apostrophe, an accent or a capital Cyrillic letter turned a whole word into
# [UNK]. At the far end a headline none of whose words survived reached the
# model as [CLS] [UNK]xN [SEP] and embedded as its word count, sitting at
# cosine 1.000 from every unrelated headline of the same length.
#
# Pinned here: folded Latin is fully visible, punctuation does not count as a
# visible word, and a text with no visible content word is refused rather
# than stored.
set -eu

bin=${1:?binary required}
model_dir="$HOME/.cache/bonfyre/models/nomic-embed-text-onnx"
[ -f "$model_dir/vocab.txt" ] || { echo "SKIP: $model_dir has no vocab.txt"; exit 0; }
root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
fail() { echo "FAIL: $1"; exit 1; }

batch() { # jsonl-file -> summary json on stdout
  rm -f "$root/v.db"
  "$bin" --input-jsonl "$1" --model nomic-embed-text-onnx --insert-db "$root/v.db" 2>/dev/null
}

cat > "$root/latin.jsonl" <<'EOF'
{"id":"a","text":"The best rivalry in women’s tennis"}
{"id":"b","text":"Palmeiras bate o São Paulo e recupera a liderança"}
EOF
batch "$root/latin.jsonl" > "$root/latin.out" || fail "latin batch did not run"
bun - "$root/latin.out" <<'JS'
import { strict as assert } from 'node:assert';
import { readFileSync } from 'node:fs';
const d = JSON.parse(readFileSync(process.argv[2], 'utf8'));
assert.equal(d.stored, 2);
assert.equal(d.unknown_words, 0,
  `${d.unknown_words} unknown words in folded Latin; the apostrophe or accent is reaching the vocabulary unfolded`);
assert.equal(d.dims, 768, `store opened at ${d.dims} dims, not the model's 768`);
JS

# Every content word unknown, one known punctuation mark. Counting the comma as
# visible is exactly the rule that let collapsed rows through.
cat > "$root/invisible.jsonl" <<'EOF'
{"id":"c","text":"ডেঙ্গু প্রতিরোধে, ঢাবিতে ছাত্রদলের সচেতনতামূলক কর্মসূচি"}
EOF
batch "$root/invisible.jsonl" > "$root/invisible.out" || true
bun - "$root/invisible.out" <<'JS'
import { strict as assert } from 'node:assert';
import { readFileSync } from 'node:fs';
const d = JSON.parse(readFileSync(process.argv[2], 'utf8'));
assert.equal(d.invisible_to_vocabulary, 1);
assert.equal(d.stored, 0, 'a text the model cannot see was stored and will answer queries');
JS

# Mostly unknown with one incidental visible token. Mean pooling makes the
# shared [UNK] mass dominate, so two such headlines collapse onto "g20" at
# cosine 1.000 and read as corroboration.
printf '%s\n' '{"id":"d","text":"Зеленский готов встретиться с Путиным на саммите G20"}' > "$root/mostly.jsonl"
batch "$root/mostly.jsonl" > "$root/mostly.out" || true
bun -e '
import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
const d = JSON.parse(readFileSync(process.argv[1], "utf8"));
assert(d.unknown_words > d.content_words - d.unknown_words);
assert.equal(d.stored, 0, "a text whose unknown words outnumber its visible words was stored");
' "$root/mostly.out"

echo "PASS tokenizer: folded Latin fully visible, punctuation not counted, invisible and mostly-unknown text refused"
