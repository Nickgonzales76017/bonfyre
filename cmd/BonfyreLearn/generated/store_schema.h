/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRELEARN_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRELEARN_H
/* cmd/BonfyreLearn/src/main.c */
#define BF_STORE_LEARN_FEEDBACK \
    "PRAGMA journal_mode=WAL;" \
    "PRAGMA foreign_keys=ON;" \
    "CREATE TABLE IF NOT EXISTS feedback(  id       INTEGER PRIMARY KEY AUTOINCREMENT,  run_id   TEXT NOT NULL,  stage    TEXT,  score    REAL NOT NULL,  label    TEXT,  ts       INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS thresholds(  stage    TEXT PRIMARY KEY,  quality  REAL NOT NULL DEFAULT 0.75,  latency_ms INTEGER NOT NULL DEFAULT 5000,  updated  INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS optimizations(  id       INTEGER PRIMARY KEY AUTOINCREMENT,  stage    TEXT NOT NULL,  metric   TEXT NOT NULL,  old_val  REAL NOT NULL,  new_val  REAL NOT NULL,  reason   TEXT,  ts       INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_feedback_run   ON feedback(run_id);" \
    "CREATE INDEX IF NOT EXISTS idx_feedback_stage ON feedback(stage);"
#endif
