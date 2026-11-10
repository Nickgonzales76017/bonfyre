#!/bin/sh
# bonfyre-embed agree: a .bfse's neighbour overlap with its ONNX teacher.
# The teacher load must take a memory grant from the ledger and release it,
# the receipt must name the exact bytes compared, and texts the vocabulary
# cannot see are counted, not compared. Needs the declared local teacher and
# static model; without them this is an explicit skip, not a pass.
set -eu
bin=${1:-./bonfyre-embed}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
teacher="$HOME/.cache/bonfyre/models/nomic-embed-text-onnx/onnx/model.onnx"
bfse=${BONFYRE_TEST_BFSE:-"$HOME/BonfyreModels/static/nomic-static-128.bfse"}
if [ ! -f "$teacher" ] || [ ! -f "$bfse" ]; then echo "embed agree: SKIP (teacher or .bfse absent)"; exit 0; fi
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT INT TERM

# Refusals before anything loads.
! "$bin" agree --input-jsonl /dev/null --out "$tmp/x.json" >/dev/null 2>&1
! "$bin" agree --static "$bfse" --input-jsonl "$tmp/none.jsonl" --out "$tmp/x.json" >/dev/null 2>&1

i=0
for t in "City council approves new library budget" "Library budget passes council vote" \
         "Storm closes coastal highway overnight" "Highway reopens after coastal storm" \
         "Local team wins regional football final" "Football final ends in late winner" \
         "Bakery opens second shop downtown" "Downtown bakery expands with new shop" \
         "Researchers map river pollution sources" "River pollution traced by university team" \
         "Уроженка Москвы впервые выиграла турнир" "1 2 3 4"; do
  i=$((i + 1)); printf '{"id":"%d","text":"%s"}\n' "$i" "$t"
done >"$tmp/corpus.jsonl"

BONFYRE_ADMISSION_DB="$tmp/admission.db" "$bin" agree --static "$bfse" \
  --input-jsonl "$tmp/corpus.jsonl" --k 3 --out "$tmp/receipt.json" >"$tmp/stdout.json"
cmp -s "$tmp/receipt.json" "$tmp/stdout.json"
q() { sqlite3 :memory: "SELECT $1 FROM (SELECT readfile('$tmp/receipt.json') AS r)"; }
test "$(q "json_extract(r,'\$.schema')")" = bonfyre.embed.agreement.v1
test "$(q "json_extract(r,'\$.corpus.rows_read')")" = 12
test "$(q "json_extract(r,'\$.corpus.compared')")" = 10
test "$(q "json_extract(r,'\$.corpus.refused_invisible_to_teacher')")" = 2
test "$(q "json_extract(r,'\$.teacher.onnx_sha256')")" = "$(shasum -a 256 "$teacher" | cut -c1-64)"
test "$(q "json_extract(r,'\$.static.sha256')")" = "$(shasum -a 256 "$bfse" | cut -c1-64)"
test "$(q "json_extract(r,'\$.corpus.sha256')")" = "$(shasum -a 256 "$tmp/corpus.jsonl" | cut -c1-64)"
test "$(q "json_extract(r,'\$.neighbour_overlap_at_k') BETWEEN 0 AND 1 AND json_extract(r,'\$.top1_agreement') BETWEEN 0 AND 1")" = 1
test "$(q "json_extract(r,'\$.peak_footprint_bytes') > 0")" = 1
# exactly one memory grant, for the teacher, and it was released
test "$(sqlite3 "$tmp/admission.db" "SELECT count(*)||'|'||sum(released_at IS NOT NULL)||'|'||min(volume)||'|'||min(estimated_bytes) FROM resource_grants")" = \
  "1|1|memory|$(( $(stat -f %z "$teacher") * 2 ))"
echo "embed agree: PASS"
