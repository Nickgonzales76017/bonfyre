/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESAE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESAE_H
/* cmd/BonfyreSAE/src/main.c */
#define BF_STORE_SAE_ACTIVATIONS \
    "PRAGMA journal_mode=WAL;" \
    "PRAGMA foreign_keys=ON;" \
    "CREATE TABLE IF NOT EXISTS activations (  id              INTEGER PRIMARY KEY AUTOINCREMENT,  artifact_id     TEXT,  dict_path       TEXT NOT NULL,  model_family    TEXT NOT NULL,  layer           INTEGER NOT NULL,  top_k           INTEGER NOT NULL,  feature_hash    TEXT NOT NULL,  danger_tripped  INTEGER NOT NULL DEFAULT 0,  elapsed_ms      REAL,  manifest_path   TEXT,  ts              TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS feature_events (  id              INTEGER PRIMARY KEY AUTOINCREMENT,  activation_id   INTEGER NOT NULL REFERENCES activations(id),  feature_id      INTEGER NOT NULL,  activation      REAL NOT NULL,  normalised      REAL NOT NULL,  tags            INTEGER NOT NULL,  label           TEXT,  rank            INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_fe_fid ON feature_events(feature_id);" \
    "CREATE INDEX IF NOT EXISTS idx_fe_act ON feature_events(activation);" \
    "CREATE INDEX IF NOT EXISTS idx_act_hash ON activations(feature_hash);" \
    "CREATE INDEX IF NOT EXISTS idx_act_model ON activations(model_family, layer);"
#endif
