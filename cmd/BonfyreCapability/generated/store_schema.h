/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRECAPABILITY_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRECAPABILITY_H
/* cmd/BonfyreCapability/src/main.c */
#define BF_STORE_CAPABILITY_CAPABILITIES \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS capabilities(  id            TEXT PRIMARY KEY,  name          TEXT NOT NULL,  description   TEXT NOT NULL,  tags          TEXT,  binary        TEXT NOT NULL,  command       TEXT,  model_id      TEXT,  hardware_tier TEXT NOT NULL DEFAULT 'cpu',  cost_estimate REAL NOT NULL DEFAULT 0.0,  latency_tier  TEXT NOT NULL DEFAULT 'fast',  stage_class   TEXT,  artifact_out  TEXT,  keywords      TEXT,  source        TEXT NOT NULL DEFAULT 'manual',  updated       INTEGER NOT NULL);" \
    "CREATE VIRTUAL TABLE IF NOT EXISTS cap_fts USING fts5(  id,name,description,tags,binary,  content='capabilities',content_rowid='rowid');"
/* cmd/BonfyreCapability/src/main.c */
#define BF_STORE_CAPABILITY_VERIFIED_REPAIRS \
    "CREATE TABLE IF NOT EXISTS verified_repairs(  id               TEXT PRIMARY KEY,  claim            TEXT NOT NULL,  vector_file      TEXT NOT NULL,  vector_sha256    TEXT NOT NULL,  case_text        TEXT NOT NULL,  runs             INTEGER NOT NULL,  before_sha256    TEXT NOT NULL,  before_source    TEXT,  after_binary     TEXT NOT NULL,  after_sha256     TEXT NOT NULL,  installed_sha256 TEXT,  record           TEXT NOT NULL,  admitted         INTEGER NOT NULL);"
#endif
