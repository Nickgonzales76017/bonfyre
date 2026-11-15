/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRELATENTATLAS_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRELATENTATLAS_H
/* cmd/BonfyreLatentAtlas/src/main.c */
#define BF_STORE_LATENT_ATLAS_CHART \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS chart(chart_id TEXT PRIMARY KEY, family TEXT NOT NULL CHECK(family IN ('dense','hdc','hyperbolic','geo','circular','distribution','subspace','fingerprint','trajectory')), dims INTEGER NOT NULL, version TEXT NOT NULL, encoder TEXT NOT NULL, universe TEXT NOT NULL DEFAULT '');" \
    "CREATE TABLE IF NOT EXISTS projection(address TEXT NOT NULL, chart_id TEXT NOT NULL REFERENCES chart(chart_id), coordinate BLOB NOT NULL, source_digest TEXT NOT NULL, frontier TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL, PRIMARY KEY(address, chart_id));" \
    "CREATE TABLE IF NOT EXISTS label(address TEXT PRIMARY KEY, label TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS bridge(from_chart TEXT NOT NULL, to_chart TEXT NOT NULL, operator TEXT NOT NULL, calibration_error REAL NOT NULL, loss_envelope TEXT NOT NULL, validity_frontier TEXT NOT NULL, provenance TEXT NOT NULL, PRIMARY KEY(from_chart, to_chart));" \
    "CREATE TABLE IF NOT EXISTS relation(src TEXT NOT NULL, relation TEXT NOT NULL CHECK(relation IN ('same_subject_as','materializes','observed_as','performance_of', 'depicts','mentions','derived_from','fingerprints_to','projected_into','member_of','located_in','version_of','happens_at')), dst TEXT NOT NULL, evidence TEXT NOT NULL, PRIMARY KEY(src, relation, dst));" \
    "CREATE TABLE IF NOT EXISTS neighbourhood(logical TEXT PRIMARY KEY, lens TEXT NOT NULL, level INTEGER NOT NULL, chart_id TEXT NOT NULL, representation_type TEXT NOT NULL, current_snapshot TEXT, parent TEXT);" \
    "CREATE TABLE IF NOT EXISTS snapshot(snapshot_id TEXT PRIMARY KEY, logical TEXT NOT NULL, universe TEXT NOT NULL, lens TEXT NOT NULL, chart_id TEXT NOT NULL, member_closure TEXT NOT NULL, children TEXT NOT NULL DEFAULT '', representation TEXT NOT NULL, created_at TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS snapshot_member(snapshot_id TEXT NOT NULL, address TEXT NOT NULL, source_digest TEXT NOT NULL, PRIMARY KEY(snapshot_id, address));" \
    "CREATE TABLE IF NOT EXISTS latent_delta(address TEXT NOT NULL, chart_id TEXT NOT NULL, from_digest TEXT NOT NULL, to_digest TEXT NOT NULL, moved REAL NOT NULL, at TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS envelope_hold(parent TEXT NOT NULL, child TEXT NOT NULL, child_from TEXT NOT NULL, child_to TEXT NOT NULL, delta REAL NOT NULL, envelope REAL NOT NULL, at TEXT NOT NULL);"
/* cmd/BonfyreLatentAtlas/src/main.c */
#define BF_STORE_LATENT_ATLAS_LINKER_SOURCE_GENERATION \
    "CREATE TABLE IF NOT EXISTS _linker_source_generation(source TEXT PRIMARY KEY,input_digest TEXT NOT NULL,context_digest TEXT NOT NULL,relation_digest TEXT NOT NULL);"
/* cmd/BonfyreLatentAtlas/src/main.c */
#define BF_STORE_LATENT_ATLAS_RELATION \
    "BEGIN;" \
    "ALTER TABLE relation RENAME TO relation_before_happens_at;" \
    "CREATE TABLE relation(src TEXT NOT NULL, relation TEXT NOT NULL CHECK(relation IN ('same_subject_as','materializes','observed_as','performance_of', 'depicts','mentions','derived_from','fingerprints_to','projected_into','member_of','located_in','version_of','happens_at')), dst TEXT NOT NULL, evidence TEXT NOT NULL, PRIMARY KEY(src, relation, dst));" \
    "INSERT OR IGNORE INTO relation(src,relation,dst,evidence) SELECT src,relation,dst,evidence FROM relation_before_happens_at;" \
    "DROP TABLE relation_before_happens_at;" \
    "COMMIT;"
#endif
