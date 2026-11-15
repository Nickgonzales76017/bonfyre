/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRECOMPETE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRECOMPETE_H
/* cmd/BonfyreCompete/src/main.c */
#define BF_STORE_COMPETE_COMPETITIONS \
    "PRAGMA journal_mode=WAL;" \
    "PRAGMA foreign_keys=ON;" \
    "CREATE TABLE IF NOT EXISTS competitions(  id        TEXT PRIMARY KEY,  recipe    TEXT NOT NULL,  stage     TEXT NOT NULL,  status    TEXT NOT NULL DEFAULT 'active',  created   INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS variants(  id         TEXT PRIMARY KEY,  comp_id    TEXT NOT NULL REFERENCES competitions(id),  label      TEXT NOT NULL,  config     TEXT,  promoted   INTEGER NOT NULL DEFAULT 0,  wins       INTEGER NOT NULL DEFAULT 0,  losses     INTEGER NOT NULL DEFAULT 0,  created    INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS results(  id         INTEGER PRIMARY KEY AUTOINCREMENT,  comp_id    TEXT NOT NULL REFERENCES competitions(id),  variant_id TEXT NOT NULL REFERENCES variants(id),  input_hash TEXT,  score      REAL NOT NULL,  metric     TEXT NOT NULL DEFAULT 'composite',  latency_ms INTEGER,  ts         INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_results_comp ON results(comp_id,variant_id);"
#endif
