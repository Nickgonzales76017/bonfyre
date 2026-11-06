#!/bin/sh
# What this proves: recall proposes the right neighbour, using the embedding
# organ and the vector organ themselves. Scripts that printed a fixed vector
# and a fixed search result used to stand in for both, so this test passed
# whatever either organ did.
#   sh tests/semantic_recall_test.sh ./bonfyre-recall
# BONFYRE_EMBED_BINARY and BONFYRE_VEC_BINARY name the organs; the learned
# model they load must be installed.
set -eu

recall=${1:?bonfyre-recall required}
case "$recall" in /*) ;; *) recall="$(pwd)/$recall";; esac
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
embed=${BONFYRE_EMBED_BINARY:-$HOME/.bonfyre/bin/bonfyre-embed}
vec=${BONFYRE_VEC_BINARY:-$HOME/.bonfyre/bin/bonfyre-vec}
[ -x "$embed" ] && [ -x "$vec" ] || { echo "the embedding and vector organs are required" >&2; exit 1; }
export BONFYRE_EMBED_BINARY="$embed" BONFYRE_VEC_BINARY="$vec"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-recall-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
top() { sed -n 's/.*"results": *\[ *{"id":"\([^"]*\)".*/\1/p' | head -1; }

# Three sentences, embedded for real, each findable by a paraphrase.
indexed=$("$recall" index-jsonl --store "$tmp/store.db" --input-jsonl "$here/semantic_fixture.jsonl" --apply)
printf '%s' "$indexed" | grep -q '"vector_count":3'
printf '%s' "$indexed" | grep -q '"verified_complete":true'
"$vec" count "$tmp/store.db" | grep -q 'artifacts: 3, vectors: 3'

out=$("$recall" query --store "$tmp/store.db" --text 'a grand slam tennis match' --top 3)
printf '%s' "$out" | grep -q '"schema":"bonfyre.semantic-recall.v1"'
printf '%s' "$out" | grep -q '"truth":"candidate_only"'
[ "$(printf '%s' "$out" | tr -d '\n' | top)" = fixture-tennis ]
out=$("$recall" query --store "$tmp/store.db" --text 'somewhere to go dancing late at night' --top 3)
[ "$(printf '%s' "$out" | tr -d '\n' | top)" = fixture-nightlife ]
out=$("$recall" query --store "$tmp/store.db" --text 'inheriting a house with unpaid taxes' --top 3)
[ "$(printf '%s' "$out" | tr -d '\n' | top)" = fixture-estate ]

# With no embedding organ there is no geometry, and no candidate is offered.
if BONFYRE_EMBED_BINARY="$tmp/absent" "$recall" query --store "$tmp/store.db" --text 'must refuse' --top 3 \
  >"$tmp/refused.out" 2>"$tmp/refused.err"; then
  echo 'a query was answered with no embedding organ' >&2
  exit 1
fi
! grep -q '"candidate_only"' "$tmp/refused.out"

sqlite3 "$tmp/cms.db" <<'SQL'
CREATE TABLE site_fragment(
  id INTEGER PRIMARY KEY, namespace TEXT, coordinate TEXT, page TEXT, slot TEXT,
  audience TEXT, kind TEXT, status TEXT, heading TEXT, eyebrow TEXT, body TEXT,
  label TEXT, source_ref TEXT, source_digest TEXT, semantic_tags TEXT
);
INSERT INTO site_fragment VALUES
 (1,'aurekai','life:home:hero','home','hero','anonymous','copy','published',
  'Night Market','Night Market','Explore after dark','','source:night','sha:a',
  '["nightlife","migration","function:compiled"]'),
 (2,'aurekai','life:home:body','home','body','anonymous','copy','published',
  'Local worlds','','People, places, events','Open','source:world','sha:b',
  '["community","role:legacy","equivalence:old"]'),
 (3,'aurekai','life:home:empty','home','footer','anonymous','copy','published',
  '','','','','source:empty','sha:c','[]'),
 (4,'aurekai','life:draft','home','draft','anonymous','copy','draft',
  'Draft only','','Not published','','source:draft','sha:d','["draft"]');
SQL

preview=$("$recall" index-fragments --cms-db "$tmp/cms.db" --store "$tmp/fragments.db" \
  --work-dir "$tmp/work")
printf '%s' "$preview" | grep -q '"queried_count":3'
printf '%s' "$preview" | grep -q '"indexable_count":2'
printf '%s' "$preview" | grep -q '"skipped_empty":1'
[ "$(grep -c . "$tmp/work/fragments.jsonl")" = 2 ]
grep -q 'nightlife' "$tmp/work/fragments.jsonl"
grep -q 'community' "$tmp/work/fragments.jsonl"
! grep -q 'migration\|function:compiled\|role:legacy\|equivalence:old' "$tmp/work/fragments.jsonl"

applied=$("$recall" index-fragments --cms-db "$tmp/cms.db" --store "$tmp/fragments.db" \
  --work-dir "$tmp/work" --apply)
printf '%s' "$applied" | grep -q '"vector_count":2'
printf '%s' "$applied" | grep -q '"verified_complete":true'
printf '%s' "$applied" | grep -q '"publication_effect":false'
"$vec" count "$tmp/fragments.db" | grep -q 'vectors: 2'

echo 'semantic_recall_test: PASS'

# Passages come back with the hit, so a caller does not open the store by hand.
out=$("$recall" query --store "$tmp/store.db" --text 'a grand slam tennis match' --top 1 --passages)
printf '%s' "$out" | grep -q '"passages":\[{"id":"fixture-tennis","text":"US Open tennis'

# index-sources: a text file, an ingest artifact and any CMS record type land in
# one store; the cutting is BonfyreParagraph's, not Recall's.
paragraph="${BONFYRE_PARAGRAPH_BINARY:-$here/../../BonfyreParagraph/bonfyre-paragraph}"
[ -x "$paragraph" ] || { echo "bonfyre-paragraph not built" >&2; exit 1; }
export BONFYRE_PARAGRAPH_BINARY="$paragraph"
printf '# Courts\n\nThe tennis final is played on a hard court in New York.\n\n# Clubs\n\nDancing continues until four in the morning downtown.\n' > "$tmp/notes.md"
mkdir "$tmp/intake"
printf 'Probate transfers a house to heirs once delinquent taxes are settled.\n' > "$tmp/intake/normalized.txt"
printf '{\n  "normalized_path": "normalized.txt",\n  "normalized_media_type": "text/plain"\n}\n' > "$tmp/intake/intake-manifest.json"
sqlite3 "$tmp/cms.db" "CREATE TABLE field_note(id INTEGER PRIMARY KEY, namespace TEXT, created_at TEXT, subject TEXT, body TEXT, score INTEGER);
INSERT INTO field_note VALUES (7,'ops','2026-10-10','Supplier call','The freight forwarder quoted ocean shipping from Vietnam.',3),
 (8,'other','2026-10-10','Unrelated','Kept out by the namespace filter.',1),(9,'ops','2026-10-10','','',0);"
sources=$("$recall" index-sources --store "$tmp/sources.db" --work-dir "$tmp/sources" \
  --text "$tmp/notes.md=notes" --text "$tmp/intake=intake" --records "$tmp/cms.db:field_note:ops" --apply)
printf '%s' "$sources" | grep -q '"kind":"records","source":"field_note","records":1,"passages":1'
printf '%s' "$sources" | grep -q '"vector_count":4'
out=$("$recall" query --store "$tmp/sources.db" --text 'container shipping quote from an Asian supplier' --top 1 --passages)
printf '%s' "$out" | grep -q '"id":"field_note:7"'
printf '%s' "$out" | grep -q 'subject: Supplier call'
out=$("$recall" query --store "$tmp/sources.db" --text 'nightclub open late' --top 1 --passages)
printf '%s' "$out" | grep -q '"id":"notes:1"'
out=$("$recall" query --store "$tmp/sources.db" --text 'inherited property with back taxes' --top 1)
printf '%s' "$out" | grep -q '"id":"intake:0"'
# A directory that is not an ingest artifact, and a record type that does not
# exist, are refused rather than indexed as nothing.
mkdir "$tmp/empty"
if "$recall" index-sources --store "$tmp/x.db" --work-dir "$tmp/x" --text "$tmp/empty" 2>/dev/null; then
  echo 'a directory with no intake manifest was indexed' >&2; exit 1; fi
if "$recall" index-sources --store "$tmp/x.db" --work-dir "$tmp/x" --records "$tmp/cms.db:no_such_type" 2>/dev/null; then
  echo 'a record type that does not exist was indexed' >&2; exit 1; fi
# Words and meaning together: an exact token the embedding has no feel for is
# found by the word index, and the fused list says which list ranked it.
sqlite3 "$tmp/cms.db" "INSERT INTO field_note VALUES (11,'ops','2026-10-10','Reference','Purchase order ZX-48213 was acknowledged.',1);"
"$recall" index-sources --store "$tmp/sources.db" --work-dir "$tmp/sources" \
  --text "$tmp/notes.md=notes" --text "$tmp/intake=intake" --records "$tmp/cms.db:field_note:ops" --apply >/dev/null
out=$("$recall" query --store "$tmp/sources.db" --text 'ZX-48213' --top 1 --hybrid)
printf '%s' "$out" | grep -q '"fused":{"method":"reciprocal-rank"'
printf '%s' "$out" | grep -q '"results":\[{"id":"field_note:11"'
printf '%s' "$out" | grep -q '"word_rank":1'
# A store built before the word index existed says so, and gains it in place.
sqlite3 "$tmp/sources.db" "DROP TABLE passage_words;"
if "$recall" query --store "$tmp/sources.db" --text 'ZX-48213' --top 1 --hybrid >/dev/null 2>&1; then
  echo 'hybrid answered with no word index' >&2; exit 1; fi
"$recall" index-words --store "$tmp/sources.db" | grep -q '"word_index":true'
"$recall" query --store "$tmp/sources.db" --text 'ZX-48213' --top 1 --hybrid | grep -q '"id":"field_note:11"'
# Writing the embedding vocabulary cannot see is not a failed build: it is
# counted, kept out of the vectors, and still found by its words.
sqlite3 "$tmp/cms.db" "INSERT INTO field_note VALUES (12,'ops','2026-10-10','','广交会 采购商 办证',1);"
partial=$("$recall" index-sources --store "$tmp/sources.db" --work-dir "$tmp/sources" \
  --text "$tmp/notes.md=notes" --records "$tmp/cms.db:field_note:ops" --apply)
printf '%s' "$partial" | grep -q '"verified_complete":false,"not_embedded":1'
"$recall" query --store "$tmp/sources.db" --text '广交会' --top 2 --hybrid | grep -q '"id":"field_note:12","score":[0-9.]*,"vector_rank":0,"word_rank":1'
echo "index-sources ok"
