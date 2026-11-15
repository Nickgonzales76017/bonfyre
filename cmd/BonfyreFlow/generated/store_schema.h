/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREFLOW_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREFLOW_H
/* cmd/BonfyreFlow/src/main.c */
#define BF_STORE_FLOW_FLOWS \
    "PRAGMA journal_mode=WAL;" \
    "PRAGMA foreign_keys=ON;" \
    "CREATE TABLE IF NOT EXISTS flows(  id          TEXT PRIMARY KEY,  name        TEXT NOT NULL,  definition  TEXT NOT NULL,  entry_stage TEXT NOT NULL,  created     INTEGER NOT NULL,  updated     INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS edges(  flow_id     TEXT NOT NULL REFERENCES flows(id) ON DELETE CASCADE,  from_stage  TEXT NOT NULL,  to_stage    TEXT NOT NULL,  kind        TEXT NOT NULL DEFAULT 'seq',  condition   TEXT,  PRIMARY KEY(flow_id,from_stage,to_stage));" \
    "CREATE TABLE IF NOT EXISTS runs(  id            TEXT PRIMARY KEY,  flow_id       TEXT NOT NULL REFERENCES flows(id),  input         TEXT,  status        TEXT NOT NULL DEFAULT 'running',  current_stage TEXT,  started       INTEGER NOT NULL,  finished      INTEGER);" \
    "CREATE TABLE IF NOT EXISTS run_stages(  run_id  TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,  stage   TEXT NOT NULL,  status  TEXT NOT NULL DEFAULT 'pending',  started INTEGER,  done    INTEGER,  output  TEXT,  PRIMARY KEY(run_id,stage));" \
    "CREATE INDEX IF NOT EXISTS idx_runs_flow ON runs(flow_id);"
#endif
