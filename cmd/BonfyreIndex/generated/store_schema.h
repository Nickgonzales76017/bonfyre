/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREINDEX_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREINDEX_H
/* cmd/BonfyreIndex/src/main.c */
#define BF_STORE_INDEX_FAMILIES \
    "CREATE TABLE IF NOT EXISTS families (  id INTEGER PRIMARY KEY,  artifact_id TEXT UNIQUE,  artifact_type TEXT,  source_system TEXT,  created_at TEXT,  root_hash TEXT,  family_key TEXT,  canonical_key TEXT,  path TEXT,  n_atoms INTEGER,  n_operators INTEGER,  n_realizations INTEGER,  component_total INTEGER,  indexed_at TEXT);" \
    "CREATE TABLE IF NOT EXISTS atoms (  id INTEGER PRIMARY KEY,  family_id INTEGER,  atom_id TEXT,  content_hash TEXT,  media_type TEXT,  byte_size INTEGER,  FOREIGN KEY(family_id) REFERENCES families(id));" \
    "CREATE TABLE IF NOT EXISTS operators (  id INTEGER PRIMARY KEY,  family_id INTEGER,  operator_id TEXT,  op TEXT,  node_hash TEXT,  version TEXT,  FOREIGN KEY(family_id) REFERENCES families(id));" \
    "CREATE INDEX IF NOT EXISTS idx_atoms_hash ON atoms(content_hash);" \
    "CREATE INDEX IF NOT EXISTS idx_ops_hash ON operators(node_hash);" \
    "CREATE INDEX IF NOT EXISTS idx_ops_type ON operators(op);" \
    "CREATE INDEX IF NOT EXISTS idx_families_type ON families(artifact_type);" \
    "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);" \
    "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);"
/* cmd/BonfyreIndex/src/main.c */
#define BF_STORE_INDEX_IDX_FAMILIES_FAMILY_KEY \
    "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);"
/* cmd/BonfyreIndex/src/main.c */
#define BF_STORE_INDEX_IDX_FAMILIES_CANONICAL \
    "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);"
#endif
