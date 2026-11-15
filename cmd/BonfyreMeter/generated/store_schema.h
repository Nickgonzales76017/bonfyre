/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREMETER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREMETER_H
/* cmd/BonfyreMeter/src/main.c */
#define BF_STORE_METER_EVENTS \
    "CREATE TABLE IF NOT EXISTS events (  id INTEGER PRIMARY KEY AUTOINCREMENT,  key_id TEXT NOT NULL,  op TEXT NOT NULL,  bytes INTEGER DEFAULT 0,  duration_ms INTEGER DEFAULT 0,  timestamp TEXT NOT NULL,  cost REAL,  provider TEXT NOT NULL DEFAULT '',  receipt_ref TEXT NOT NULL DEFAULT '',  physical_attempt_ref TEXT NOT NULL DEFAULT '',  realization_ref TEXT NOT NULL DEFAULT '',  rate_id INTEGER,  work_subject TEXT NOT NULL DEFAULT '',  work_scope TEXT NOT NULL DEFAULT '',  work_id TEXT NOT NULL DEFAULT '',  work_run_id TEXT NOT NULL DEFAULT '');" \
    "CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id);" \
    "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);" \
    "CREATE TABLE IF NOT EXISTS rate (  id INTEGER PRIMARY KEY AUTOINCREMENT,  provider TEXT NOT NULL,  op TEXT NOT NULL,  unit TEXT NOT NULL,  amount REAL NOT NULL,  currency TEXT NOT NULL DEFAULT 'USD',  effective_at TEXT NOT NULL,  evidence_ref TEXT NOT NULL,  observed_at TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_rate_lookup ON rate(provider,op,effective_at);"
/* cmd/BonfyreMeter/src/main.c */
#define BF_STORE_METER_IDX_EVENTS_WORK \
    "CREATE INDEX IF NOT EXISTS idx_events_work ON events(work_subject,work_scope,work_id)"
/* cmd/BonfyreMeter/src/main.c */
#define BF_STORE_METER_IDX_EVENTS_PHYSICAL_ATTEMPT \
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_physical_attempt ON events(physical_attempt_ref) WHERE physical_attempt_ref <> ''"
/* store work_index: sqlite, declared by cmd/BonfyreMeter */
#define BF_STORE_METER_WORK_INDEX_KIND "sqlite"
#define BF_STORE_METER_WORK_INDEX_ATTACH "ATTACH DATABASE ? AS work"
#define BF_STORE_METER_WORK_INDEX_ATTACH_READONLY 1
#endif
