#!/bin/sh
set -eu

bin=${1:?binary required}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-physical.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/physical.sqlite"
input="$tmp/passports.jsonl"

cat >"$input" <<'JSONL'
{"identity":"occurrence:packed:v1","subject_family":"occurrence","semantic_digest":"sha256:occurrence","representation_family":"packed-csr","representation_version":"1","exactness_class":"exact","quality_loss_budget":0,"measurement_state":"measured","shared_basis":"occurrence-spine:v1","binding_digest":"sha256:binding-a","exception_digest":"","physical_body_ref":"sqlite:occurrence","physical_digest":"sha256:exact-body","supports":["equality","filtering","adjacency","partial_decode","mutation","delta_application"],"lift_ref":"OccurrenceSpine","bytes":96,"decode_ns":4}
{"identity":"occurrence:int8:v1","subject_family":"occurrence","semantic_digest":"sha256:occurrence","representation_family":"int8-vector","representation_version":"1","exactness_class":"latent","quality_loss_budget":1,"measurement_state":"measured","shared_basis":"embedding-basis:v1","binding_digest":"sha256:binding-a","exception_digest":"sha256:residual-a","physical_body_ref":"lance:occurrence","physical_digest":"sha256:latent-body","supports":["top-k","filtering"],"lift_ref":"EmbeddingProjection","thaw_ref":"exact:occurrence","bytes":384,"decode_ns":8}
{"identity":"occurrence:sign:v1","subject_family":"occurrence","semantic_digest":"sha256:occurrence","representation_family":"sign-1bit","representation_version":"1","exactness_class":"latent","quality_loss_budget":9,"measurement_state":"declared","shared_basis":"embedding-basis:v1","binding_digest":"sha256:binding-a","exception_digest":"sha256:residual-b","physical_body_ref":"lance:occurrence","physical_digest":"sha256:latent-sign","supports":["top-k"],"lift_ref":"EmbeddingProjection","thaw_ref":"exact:occurrence","bytes":48}
{"identity":"occurrence:public-json:v1","subject_family":"occurrence","semantic_digest":"sha256:occurrence","representation_family":"public-json","representation_version":"1","exactness_class":"exact","quality_loss_budget":0,"measurement_state":"declared","shared_basis":"occurrence-spine:v1","binding_digest":"sha256:binding-b","exception_digest":"","physical_body_ref":"cid:shared","physical_digest":"sha256:exact-body","supports":["projection","partial_decode"],"lift_ref":"OccurrenceSpine","bytes":96}
JSONL

"$bin" compile-passports --db "$db" --input "$input" | grep -q '"compiled_passports":4'
"$bin" compile-passports --db "$db" --input "$input" | grep -q '"state":"unchanged"'
# A derived-schema change must not let digest no-op handling preserve an
# incomplete old registry. The source is recompiled once, then becomes a no-op.
sqlite3 "$db" 'DELETE FROM representation_body;'
"$bin" compile-passports --db "$db" --input "$input" | grep -q '"compiled_passports":4'
"$bin" ledger --db "$db" | grep -q '"unique_bodies":3'
"$bin" compile-passports --db "$db" --input "$input" | grep -q '"state":"unchanged"'
"$bin" plan --db "$db" --subject-family occurrence --operation top-k --loss-budget 1 >"$tmp/plan.json"
grep -q 'occurrence:int8:v1' "$tmp/plan.json"
! grep -q 'occurrence:sign:v1' "$tmp/plan.json"
"$bin" plan --db "$db" --subject-family occurrence --operation filtering --loss-budget 999 --requires-exact >"$tmp/exact.json"
grep -q 'occurrence:packed:v1' "$tmp/exact.json"
! grep -q 'occurrence:int8:v1' "$tmp/exact.json"
"$bin" record-receipt --db "$db" --passport occurrence:packed:v1 --receipt exact-read-1 --bytes-read 96 --bytes-borrowed 96 --bytes-copied 0 --bytes-decompressed 0 --bytes-reencoded 0 --bytes-hydrated 0 --allocations 0 --serialize-count 0 --deserialize-count 0 --materialization-count 0 --borrowed-boundaries 1 --fused-boundaries 1 --useful-bytes 96 | grep -q executed_runtime_counters
"$bin" ledger --db "$db" >"$tmp/ledger.json"
grep -q '"passports":4' "$tmp/ledger.json"
grep -q '"bases":2' "$tmp/ledger.json"
grep -q '"typed_bindings":4' "$tmp/ledger.json"
grep -q '"sparse_exceptions":2' "$tmp/ledger.json"
grep -q '"unique_bodies":3' "$tmp/ledger.json"
grep -q '"unique_body_declared_bytes":528' "$tmp/ledger.json"
grep -q '"declared_dedup_bytes_avoided":96' "$tmp/ledger.json"
grep -q '"copy_amplification":0' "$tmp/ledger.json"

printf '%s\n' '{"identity":"bad","subject_family":"occurrence","semantic_digest":"x","representation_family":"x","representation_version":"1","exactness_class":"exact","quality_loss_budget":0,"measurement_state":"measured","physical_body_ref":"x","physical_digest":"x","supports":["x"],"lift_ref":"x","unexpected":true}' >"$tmp/bad.jsonl"
! "$bin" compile-passports --db "$db" --input "$tmp/bad.jsonl" 2>/dev/null
"$bin" ledger --db "$db" | grep -q '"passports":4'
printf '%s\n' \
  '{"identity":"body-a","subject_family":"occurrence","semantic_digest":"sha256:a","representation_family":"packed","representation_version":"1","exactness_class":"exact","quality_loss_budget":0,"measurement_state":"measured","physical_body_ref":"cid:shared-a","physical_digest":"sha256:conflict","supports":["filtering"],"lift_ref":"OccurrenceSpine","bytes":96}' \
  '{"identity":"body-b","subject_family":"occurrence","semantic_digest":"sha256:b","representation_family":"packed","representation_version":"1","exactness_class":"exact","quality_loss_budget":0,"measurement_state":"measured","physical_body_ref":"cid:shared-b","physical_digest":"sha256:conflict","supports":["filtering"],"lift_ref":"OccurrenceSpine","bytes":95}' >"$tmp/body-conflict.jsonl"
! "$bin" compile-passports --db "$db" --input "$tmp/body-conflict.jsonl" 2>/dev/null
"$bin" ledger --db "$db" | grep -q '"unique_bodies":3'
# the structural measurement is of the registry just compiled, not of made-up rows
"$bin" benchmark-structural --db "$db" >"$tmp/structural.json"
grep -q '"cohort":"recorded_registry"' "$tmp/structural.json"
grep -q '"passports":4' "$tmp/structural.json"
! "$bin" benchmark-structural --records 10000 >/dev/null 2>&1

# A new passport generation must preserve the *old* CopyReceipt under its
# recorded generation identity, rather than silently dropping its counters.
old_digest=$(sqlite3 "$db" "SELECT source_digest FROM physical_generation WHERE name='passport'")
old_recorded_at=$(sqlite3 "$db" "SELECT recorded_at FROM compression_receipt WHERE receipt_id='exact-read-1'")
sed 's/"representation_version":"1"/"representation_version":"2"/g' "$input" >"$tmp/passports-v2.jsonl"
"$bin" compile-passports --db "$db" --input "$tmp/passports-v2.jsonl" | grep -q '"compiled_passports":4'
"$bin" ledger --db "$db" | grep -q '"runtime_receipts":0'
"$bin" ledger-history --db "$db" >"$tmp/historical.json"
grep -q '"archived_receipts":1' "$tmp/historical.json"
test "$(sqlite3 "$db" "SELECT generation_digest FROM compression_receipt_history WHERE receipt_id='exact-read-1'")" = "$old_digest"
test "$(sqlite3 "$db" "SELECT recorded_at FROM compression_receipt_history WHERE receipt_id='exact-read-1'")" = "$old_recorded_at"
test "$(sqlite3 "$db" "SELECT passport_identity || '|' || bytes_read || '|' || bytes_copied FROM compression_receipt_history WHERE receipt_id='exact-read-1'")" = 'occurrence:packed:v1|96|0'

# A metadata write failure *after* all new passports were imported must roll
# back the entire generation and leave the active receipt untouched.
"$bin" record-receipt --db "$db" --passport occurrence:packed:v1 --receipt exact-read-2 --bytes-read 96 --bytes-borrowed 96 --bytes-copied 0 --bytes-decompressed 0 --bytes-reencoded 0 --bytes-hydrated 0 --allocations 0 --serialize-count 0 --deserialize-count 0 --materialization-count 0 --borrowed-boundaries 1 --fused-boundaries 1 --useful-bytes 96 | grep -q executed_runtime_counters
second_digest=$(sqlite3 "$db" "SELECT source_digest FROM physical_generation WHERE name='passport'")
sed 's/"representation_version":"2"/"representation_version":"3"/g' "$tmp/passports-v2.jsonl" >"$tmp/passports-v3.jsonl"
sqlite3 "$db" "CREATE TRIGGER refuse_generation BEFORE UPDATE ON physical_generation BEGIN SELECT RAISE(ABORT,'injected metadata failure'); END;"
! "$bin" compile-passports --db "$db" --input "$tmp/passports-v3.jsonl" >"$tmp/refused-generation.json" 2>"$tmp/refused-generation.err"
test "$(sqlite3 "$db" "SELECT source_digest FROM physical_generation WHERE name='passport'")" = "$second_digest"
test "$(sqlite3 "$db" "SELECT count(*) FROM compression_receipt WHERE receipt_id='exact-read-2'")" = 1
test "$(sqlite3 "$db" 'SELECT count(*) FROM compression_receipt_history')" = 1
sqlite3 "$db" 'DROP TRIGGER refuse_generation;'

# Rollback on malformed material cannot erase either active receipts or
# immutable historical generations.
cat "$tmp/passports-v3.jsonl" "$tmp/passports-v3.jsonl" >"$tmp/invalid-repeated.jsonl"
! "$bin" compile-passports --db "$db" --input "$tmp/invalid-repeated.jsonl" >/dev/null 2>&1
test "$(sqlite3 "$db" "SELECT source_digest FROM physical_generation WHERE name='passport'")" = "$second_digest"
test "$(sqlite3 "$db" "SELECT count(*) FROM compression_receipt WHERE receipt_id='exact-read-2'")" = 1
test "$(sqlite3 "$db" 'SELECT count(*) FROM compression_receipt_history')" = 1

"$bin" compile-passports --db "$db" --input "$tmp/passports-v3.jsonl" | grep -q '"compiled_passports":4'
"$bin" ledger-history --db "$db" | grep -q '"archived_receipts":2'
test "$(sqlite3 "$db" 'SELECT count(*) FROM compression_receipt')" = 0
# All unchanged-source no-op paths must avoid archiving an existing generation.
"$bin" compile-passports --db "$db" --input "$tmp/passports-v3.jsonl" | grep -q '"state":"unchanged"'
test "$(sqlite3 "$db" 'SELECT count(*) FROM compression_receipt_history')" = 2
# A receipt ID reused in a later generation must not block a valid rebuild.
"$bin" record-receipt --db "$db" --passport occurrence:packed:v1 --receipt exact-read-1 --bytes-read 96 --bytes-borrowed 96 --bytes-copied 0 --bytes-decompressed 0 --bytes-reencoded 0 --bytes-hydrated 0 --allocations 0 --serialize-count 0 --deserialize-count 0 --materialization-count 0 --borrowed-boundaries 1 --fused-boundaries 1 --useful-bytes 96 | grep -q executed_runtime_counters
sed 's/"representation_version":"3"/"representation_version":"4"/g' "$tmp/passports-v3.jsonl" >"$tmp/passports-v4.jsonl"
"$bin" compile-passports --db "$db" --input "$tmp/passports-v4.jsonl" | grep -q '"compiled_passports":4'
test "$(sqlite3 "$db" "SELECT count(*) FROM compression_receipt_history WHERE receipt_id='exact-read-1'")" = 2
"$bin" ledger-history --db "$db" | grep -q '"archived_receipts":3'
"$bin" compile-passports --db "$db" --input "$tmp/passports-v4.jsonl" | grep -q '"state":"unchanged"'

# Reused receipt names must resolve by exact generation rather than default
# to the current passport. The command is read-only and emits at most one row.
current_digest=$(sqlite3 "$db" "SELECT source_digest FROM physical_generation WHERE name='passport'")
"$bin" record-receipt --db "$db" --passport occurrence:packed:v1 --receipt exact-read-1 --bytes-read 96 --bytes-borrowed 96 --bytes-copied 0 --bytes-decompressed 0 --bytes-reencoded 0 --bytes-hydrated 0 --allocations 0 --serialize-count 0 --deserialize-count 0 --materialization-count 0 --borrowed-boundaries 1 --fused-boundaries 1 --useful-bytes 96 | grep -q executed_runtime_counters
"$bin" read-receipt --db "$db" --generation "$current_digest" --receipt exact-read-1 >"$tmp/receipt-active.json"
"$bin" read-receipt --db "$db" --generation "$old_digest" --receipt exact-read-1 >"$tmp/receipt-archived.json"
grep -q '"origin":"active"' "$tmp/receipt-active.json"
grep -q '"origin":"archived"' "$tmp/receipt-archived.json"
grep -q '"schema":"bonfyre.copyreceipt.readback.v1"' "$tmp/receipt-archived.json"
grep -q '"no_effect":true' "$tmp/receipt-active.json"
grep -q '"useful_bytes":96' "$tmp/receipt-active.json"
! "$bin" read-receipt --db "$db" --generation "$current_digest" --receipt missing >"$tmp/missing.json"
test ! -s "$tmp/missing.json"
! "$bin" read-receipt --db "$db" --generation invalid --receipt exact-read-1 >"$tmp/invalid-digest.json"
test ! -s "$tmp/invalid-digest.json"
# Duplicate coordinates in one historical generation must be refused, not
# selected by the arbitrary SQLite row order.
sqlite3 "$db" "INSERT INTO compression_receipt_history(
 generation_digest,receipt_id,passport_identity,subject_family,semantic_digest,
 physical_digest,exactness_class,bytes_read,bytes_borrowed,bytes_copied,
 bytes_decompressed,bytes_reencoded,bytes_hydrated,allocations,serialize_count,
 deserialize_count,materialization_count,borrowed_boundaries,fused_boundaries,
 useful_bytes,recorded_at)
 SELECT generation_digest,receipt_id,passport_identity,subject_family,semantic_digest,
 physical_digest,exactness_class,bytes_read,bytes_borrowed,bytes_copied,
 bytes_decompressed,bytes_reencoded,bytes_hydrated,allocations,serialize_count,
 deserialize_count,materialization_count,borrowed_boundaries,fused_boundaries,
 useful_bytes,recorded_at FROM compression_receipt_history
 WHERE generation_digest='$old_digest' AND receipt_id='exact-read-1' LIMIT 1;"
! "$bin" read-receipt --db "$db" --generation "$old_digest" --receipt exact-read-1 >"$tmp/ambiguous.json"
test ! -s "$tmp/ambiguous.json"

printf '%s\n' 'physical compiler: PASS'
