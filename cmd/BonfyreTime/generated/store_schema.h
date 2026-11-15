/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRETIME_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRETIME_H
/* cmd/BonfyreTime/src/main.c */
#define BF_STORE_TIME_ARTIFACTS \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS artifacts (  id       TEXT PRIMARY KEY,  path     TEXT,  recipe   TEXT NOT NULL,  status   TEXT NOT NULL DEFAULT 'pending',  runs     INTEGER NOT NULL DEFAULT 0,  last_run INTEGER,  created  INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS runs (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  artifact   TEXT NOT NULL,  recipe     TEXT NOT NULL,  run_id     TEXT,  exit_code  INTEGER,  out_path   TEXT,  ts         INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS triggers (  id      INTEGER PRIMARY KEY AUTOINCREMENT,  event   TEXT NOT NULL,  recipe  TEXT NOT NULL,  active  INTEGER NOT NULL DEFAULT 1,  created INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_artifacts_status ON artifacts(status);" \
    "CREATE INDEX IF NOT EXISTS idx_runs_artifact    ON runs(artifact);"
#endif
