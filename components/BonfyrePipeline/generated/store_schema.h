/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREPIPELINE_H
#define BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREPIPELINE_H
/* components/BonfyrePipeline/src/main.c */
#define BF_STORE_C_PIPELINE_FAMILIES \
    "CREATE TABLE IF NOT EXISTS families (  id INTEGER PRIMARY KEY, artifact_id TEXT UNIQUE, artifact_type TEXT,  source_system TEXT, created_at TEXT, root_hash TEXT, family_key TEXT, canonical_key TEXT, path TEXT,  n_atoms INTEGER, n_operators INTEGER, n_realizations INTEGER, component_total INTEGER, indexed_at TEXT);" \
    "CREATE INDEX IF NOT EXISTS idx_families_type ON families(artifact_type);" \
    "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);" \
    "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);"
/* components/BonfyrePipeline/src/main.c */
#define BF_STORE_C_PIPELINE_IDX_FAMILIES_FAMILY_KEY \
    "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);"
/* components/BonfyrePipeline/src/main.c */
#define BF_STORE_C_PIPELINE_IDX_FAMILIES_CANONICAL \
    "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);"
/* components/BonfyrePipeline/src/main.c */
#define BF_STORE_C_PIPELINE_EVENTS \
    "CREATE TABLE IF NOT EXISTS events (  id INTEGER PRIMARY KEY AUTOINCREMENT, key_id TEXT NOT NULL, op TEXT NOT NULL,  bytes INTEGER DEFAULT 0, duration_ms INTEGER DEFAULT 0,  timestamp TEXT NOT NULL, cost REAL DEFAULT 0.0);" \
    "CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id);"
#endif
