/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRETIER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRETIER_H
/* cmd/BonfyreTier/src/main.c */
#define BF_STORE_TIER_TIERS \
    "PRAGMA journal_mode=WAL;" \
    "PRAGMA foreign_keys=ON;" \
    "CREATE TABLE IF NOT EXISTS tiers(  name        TEXT PRIMARY KEY,  max_ms      INTEGER NOT NULL,  description TEXT NOT NULL,  color       TEXT);" \
    "CREATE TABLE IF NOT EXISTS assignments(  recipe      TEXT NOT NULL,  stage       TEXT NOT NULL,  tier        TEXT NOT NULL REFERENCES tiers(name),  updated     INTEGER NOT NULL,  PRIMARY KEY(recipe,stage));" \
    "CREATE TABLE IF NOT EXISTS observations(  id          INTEGER PRIMARY KEY AUTOINCREMENT,  recipe      TEXT NOT NULL,  stage       TEXT NOT NULL,  latency_ms  INTEGER NOT NULL,  violated    INTEGER NOT NULL DEFAULT 0,  tier        TEXT,  ts          INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_obs_recipe ON observations(recipe,stage);"
#endif
