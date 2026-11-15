/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRECONTROL_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRECONTROL_H
/* cmd/BonfyreControl/src/main.c */
#define BF_STORE_CONTROL_POLICIES \
    "PRAGMA journal_mode=WAL;" \
    "PRAGMA foreign_keys=ON;" \
    "CREATE TABLE IF NOT EXISTS policies (  id        TEXT PRIMARY KEY,  name      TEXT NOT NULL,  kind      TEXT NOT NULL CHECK(kind IN ('cost','latency','route','compete')),  recipe    TEXT,  rule_json TEXT NOT NULL,  active    INTEGER NOT NULL DEFAULT 1,  created   INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS decisions (  id        INTEGER PRIMARY KEY AUTOINCREMENT,  run_id    TEXT,  recipe    TEXT NOT NULL,  stage     TEXT,  decision  TEXT NOT NULL,  reason    TEXT,  score     REAL,  latency_ms INTEGER,  ts        INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS scores (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  artifact      TEXT NOT NULL,  relevance     REAL,  completeness  REAL,  coherence     REAL,  factuality    REAL,  latency_ms    INTEGER,  cost_usd      REAL,  composite     REAL,  entropy       REAL,  ts            INTEGER NOT NULL);" \
    "CREATE VIRTUAL TABLE IF NOT EXISTS policies_fts USING fts5(  id, name, kind, recipe, content='policies', content_rowid='rowid');" \
    "CREATE INDEX IF NOT EXISTS idx_decisions_recipe ON decisions(recipe);" \
    "CREATE INDEX IF NOT EXISTS idx_decisions_ts     ON decisions(ts);" \
    "CREATE INDEX IF NOT EXISTS idx_scores_artifact  ON scores(artifact);"
#endif
