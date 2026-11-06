#!/bin/sh
# A batch must report what the store holds, not what it tried to write.
#
# The vector writer checked no step and no commit and returned 0 always. With
# a reader holding the store, one COMMIT failed with SQLITE_BUSY, its
# transaction stayed open, every later insert rode it, and closing the store
# rolled all of it back -- while the batch summary counted every row as
# stored. On the publication corpus it reported 6,963 and kept 1,216.
#
# This holds a read transaction on the store while a batch runs and requires
# the reported count to equal the rows that actually landed. It does not
# require any write to fail: whether the reader's hold overlaps a commit is
# timing, and the invariant must hold either way.
set -eu

bin=${1:?binary required}
model_dir="$HOME/.cache/bonfyre/models/nomic-embed-text-onnx"
[ -f "$model_dir/vocab.txt" ] || { echo "SKIP: $model_dir has no vocab.txt"; exit 0; }
root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
db="$root/v.db"
fail() { echo "FAIL: $1"; exit 1; }

# Create the store with one row so a reader has something to hold.
printf '{"id":"seed","text":"seed headline about a harbour"}\n' > "$root/seed.jsonl"
"$bin" --input-jsonl "$root/seed.jsonl" --model nomic-embed-text-onnx --insert-db "$db" >/dev/null 2>&1 \
    || fail "could not create the store"

bun - "$root/batch.jsonl" <<'JS'
import { writeFileSync } from 'node:fs';
const topics = ['harbour','election','storm','market','bridge','festival','court','vaccine',
  'railway','drought','satellite','museum','strike','wildfire','budget','summit'];
writeFileSync(process.argv[2], Array.from({length: 48}, (_, i) => JSON.stringify({
  id: `r${i}`, text: `City council debates ${topics[i % topics.length]} plan number ${i}`,
})).join('\n') + '\n');
JS

bun - "$db" "$root/reader.ready" <<'JS' &
import { Database } from 'bun:sqlite';
import { writeFileSync } from 'node:fs';
const db = new Database(process.argv[2], { readonly: true });
try {
  db.exec('BEGIN');
  db.query('SELECT count(*) FROM artifacts').get();
  writeFileSync(process.argv[3], 'read transaction acquired\n');
  await Bun.sleep(8000);
  db.exec('COMMIT');
} finally { db.close(); }
JS
holder=$!
# Wait for the actual read lock, not an assumed interpreter startup duration.
tries=0
while [ ! -f "$root/reader.ready" ]; do
    kill -0 "$holder" 2>/dev/null || fail "reader exited before acquiring the store"
    tries=$((tries + 1))
    if [ "$tries" -ge 100 ]; then
        kill "$holder" 2>/dev/null || true
        wait "$holder" 2>/dev/null || true
        fail "reader did not acquire the store within five seconds"
    fi
    sleep 0.05
done

set +e
"$bin" --input-jsonl "$root/batch.jsonl" --model nomic-embed-text-onnx --insert-db "$db" \
    > "$root/out.json" 2> "$root/err.log"
rc=$?
set -e
wait "$holder" || fail "store reader failed"

bun - "$root/out.json" "$db" "$rc" <<'JS'
import { strict as assert } from 'node:assert';
import { readFileSync } from 'node:fs';
import { Database } from 'bun:sqlite';
const out = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const rc = Number(process.argv[4]);
const db = new Database(process.argv[3], { readonly: true });
try {
  const { landed } = db.query("SELECT count(*) AS landed FROM artifacts WHERE id LIKE 'r%'").get();
  assert.equal(out.stored, landed,
    `the batch reported ${out.stored} stored and the store holds ${landed}; a write that did not commit was counted`);
  assert.equal(out.stored + out.not_stored + out.invisible_to_vocabulary + out.failed, 48);
  if (out.not_stored) assert.notEqual(rc, 0, 'rows were not stored and the batch exited zero');
  console.log(`  reported ${out.stored}, landed ${landed}, not stored ${out.not_stored}, exit ${rc}`);
} finally { db.close(); }
JS

# Re-embedding an id must replace its vector, not just its text. sqlite-vec's
# vec0 refuses OR REPLACE on its primary key, so without an explicit delete the
# store would carry the new headline beside the old headline's vector.
printf '%s\n' '{"id":"swap","text":"Harbour authority approves new ferry terminal"}' > "$root/swap1.jsonl"
printf '%s\n' '{"id":"swap","text":"Wildfire forces evacuation of mountain villages"}' > "$root/swap2.jsonl"
printf 'Wildfire forces evacuation of mountain villages\n' > "$root/q.txt"
"$bin" --input-jsonl "$root/swap1.jsonl" --model nomic-embed-text-onnx --insert-db "$root/s.db" >/dev/null 2>&1 \
    || fail "first embedding of the swap row failed"
"$bin" --input-jsonl "$root/swap2.jsonl" --model nomic-embed-text-onnx --insert-db "$root/s.db" > "$root/swap2.out" 2>/dev/null \
    || fail "re-embedding an existing id failed; replacement is not being done"
"$bin" --text "$root/q.txt" --out "$root/q.json" --model nomic-embed-text-onnx >/dev/null 2>&1 \
    || fail "could not embed the query"
vec="$(dirname "$bin")/../BonfyreVec/bonfyre-vec"
if [ -x "$vec" ]; then
  "$vec" search "$root/s.db" "$root/q.json" --exact --top 1 2>/dev/null > "$root/swap.search"
  # These template expressions belong to JavaScript, not the invoking shell.
  # shellcheck disable=SC2016
  bun -e '
import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
const raw = readFileSync(process.argv[1], "utf8");
const r = JSON.parse(raw.slice(raw.indexOf("{"))).results[0];
assert(r.cosine_similarity > 0.99,
  `the stored vector for a re-embedded id is not the vector of the new text: cosine ${r.cosine_similarity}; the old vector survived the replacement`);
' "$root/swap.search" || fail "re-embedding kept the stale vector"
fi

echo "PASS store contention: reported stored equals rows landed while a reader held the store, and re-embedding replaces the vector"
