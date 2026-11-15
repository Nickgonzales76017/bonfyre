/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRECOMPRESS_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRECOMPRESS_H
/* cmd/BonfyreCompress/src/physical.c */
#define BF_STORE_COMPRESS_REPRESENTATION_BASIS \
    "PRAGMA foreign_keys=ON;" \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS representation_basis(handle INTEGER PRIMARY KEY,identity TEXT NOT NULL UNIQUE) STRICT;" \
    "CREATE TABLE IF NOT EXISTS representation_body(handle INTEGER PRIMARY KEY,physical_digest TEXT NOT NULL UNIQUE,canonical_body_ref TEXT NOT NULL,declared_bytes INTEGER NOT NULL CHECK(declared_bytes>=0)) STRICT;" \
    "CREATE TABLE IF NOT EXISTS representation_passport(handle INTEGER PRIMARY KEY,identity TEXT NOT NULL UNIQUE,subject_family TEXT NOT NULL,semantic_digest TEXT NOT NULL,representation_family TEXT NOT NULL,representation_version TEXT NOT NULL,exactness_class TEXT NOT NULL CHECK(exactness_class IN('exact','latent')),quality_loss_budget REAL NOT NULL CHECK(quality_loss_budget>=0),measurement_state TEXT NOT NULL CHECK(measurement_state IN('measured','declared')),basis_handle INTEGER REFERENCES representation_basis(handle),binding_digest TEXT NOT NULL,exception_digest TEXT NOT NULL,physical_body_ref TEXT NOT NULL,physical_digest TEXT NOT NULL,lift_ref TEXT NOT NULL,thaw_ref TEXT NOT NULL,physical_bytes INTEGER NOT NULL CHECK(physical_bytes>=0),encode_ns INTEGER NOT NULL CHECK(encode_ns>=0),decode_ns INTEGER NOT NULL CHECK(decode_ns>=0),scan_ns INTEGER NOT NULL CHECK(scan_ns>=0),random_read_ns INTEGER NOT NULL CHECK(random_read_ns>=0),memory_bandwidth INTEGER NOT NULL CHECK(memory_bandwidth>=0),copy_amplification REAL NOT NULL CHECK(copy_amplification>=0),hydration_amplification REAL NOT NULL CHECK(hydration_amplification>=0),source_json TEXT NOT NULL) STRICT;" \
    "CREATE INDEX IF NOT EXISTS representation_family_ix ON representation_passport(subject_family,exactness_class,measurement_state);" \
    "CREATE TABLE IF NOT EXISTS representation_binding(passport_handle INTEGER PRIMARY KEY REFERENCES representation_passport(handle) ON DELETE CASCADE,basis_handle INTEGER NOT NULL REFERENCES representation_basis(handle),subject_family TEXT NOT NULL,binding_digest TEXT NOT NULL) STRICT;" \
    "CREATE INDEX IF NOT EXISTS representation_binding_basis_ix ON representation_binding(basis_handle,subject_family);" \
    "CREATE TABLE IF NOT EXISTS representation_exception(passport_handle INTEGER PRIMARY KEY REFERENCES representation_passport(handle) ON DELETE CASCADE,exception_digest TEXT NOT NULL) STRICT;" \
    "CREATE TABLE IF NOT EXISTS representation_support(passport_handle INTEGER NOT NULL REFERENCES representation_passport(handle) ON DELETE CASCADE,operation TEXT NOT NULL,PRIMARY KEY(passport_handle,operation)) WITHOUT ROWID;" \
    "CREATE INDEX IF NOT EXISTS representation_support_ix ON representation_support(operation,passport_handle);" \
    "CREATE TABLE IF NOT EXISTS physical_generation(name TEXT PRIMARY KEY,source_digest TEXT NOT NULL,compiled_at TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%fZ','now'))) STRICT;" \
    "CREATE TABLE IF NOT EXISTS compression_receipt(receipt_id TEXT PRIMARY KEY,passport_handle INTEGER NOT NULL REFERENCES representation_passport(handle),bytes_read INTEGER NOT NULL,bytes_borrowed INTEGER NOT NULL,bytes_copied INTEGER NOT NULL,bytes_decompressed INTEGER NOT NULL,bytes_reencoded INTEGER NOT NULL,bytes_hydrated INTEGER NOT NULL,allocations INTEGER NOT NULL,serialize_count INTEGER NOT NULL,deserialize_count INTEGER NOT NULL,materialization_count INTEGER NOT NULL,borrowed_boundaries INTEGER NOT NULL,fused_boundaries INTEGER NOT NULL,useful_bytes INTEGER NOT NULL,recorded_at TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%fZ','now'))) STRICT;" \
    "CREATE TABLE IF NOT EXISTS compression_receipt_history(archive_row INTEGER PRIMARY KEY,generation_digest TEXT NOT NULL,receipt_id TEXT NOT NULL,passport_identity TEXT NOT NULL,subject_family TEXT NOT NULL,semantic_digest TEXT NOT NULL,physical_digest TEXT NOT NULL,exactness_class TEXT NOT NULL,bytes_read INTEGER NOT NULL,bytes_borrowed INTEGER NOT NULL,bytes_copied INTEGER NOT NULL,bytes_decompressed INTEGER NOT NULL,bytes_reencoded INTEGER NOT NULL,bytes_hydrated INTEGER NOT NULL,allocations INTEGER NOT NULL,serialize_count INTEGER NOT NULL,deserialize_count INTEGER NOT NULL,materialization_count INTEGER NOT NULL,borrowed_boundaries INTEGER NOT NULL,fused_boundaries INTEGER NOT NULL,useful_bytes INTEGER NOT NULL,recorded_at TEXT NOT NULL,archived_at TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%fZ','now'))) STRICT;"
/* cmd/BonfyreCompress/src/record_factor.c */
#define BF_STORE_COMPRESS_RECORD_DICTIONARY \
    "PRAGMA application_id=1112688198;" \
    "PRAGMA user_version=2;" \
    "PRAGMA foreign_keys=ON;" \
    "PRAGMA journal_mode=DELETE;" \
    "CREATE TABLE record_dictionary(id INTEGER PRIMARY KEY,payload BLOB NOT NULL,digest TEXT NOT NULL);" \
    "CREATE TABLE record_atom(id INTEGER PRIMARY KEY,digest TEXT NOT NULL,raw_bytes INTEGER NOT NULL,codec INTEGER NOT NULL,payload BLOB NOT NULL);" \
    "CREATE UNIQUE INDEX build_atom_digest ON record_atom(digest);" \
    "CREATE TABLE record(seq INTEGER PRIMARY KEY,record_key TEXT NOT NULL UNIQUE,title TEXT NOT NULL,kind TEXT NOT NULL,source_url TEXT NOT NULL,keywords TEXT NOT NULL,digest TEXT NOT NULL,raw_bytes INTEGER NOT NULL,parts INTEGER NOT NULL);" \
    "CREATE TABLE record_part(record INTEGER NOT NULL REFERENCES record(seq),ordinal INTEGER NOT NULL,atom INTEGER NOT NULL REFERENCES record_atom(id),PRIMARY KEY(record,ordinal)) WITHOUT ROWID;" \
    "CREATE VIRTUAL TABLE record_search USING fts5(title,keywords,content='record',content_rowid='seq');" \
    "BEGIN IMMEDIATE;"
#endif
