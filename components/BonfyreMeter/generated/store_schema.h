/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREMETER_H
#define BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREMETER_H
/* components/BonfyreMeter/src/main.c */
#define BF_STORE_C_METER_EVENTS \
    "CREATE TABLE IF NOT EXISTS events (  id INTEGER PRIMARY KEY AUTOINCREMENT,  key_id TEXT NOT NULL,  op TEXT NOT NULL,  bytes INTEGER DEFAULT 0,  duration_ms INTEGER DEFAULT 0,  timestamp TEXT NOT NULL,  cost REAL DEFAULT 0.0);" \
    "CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id);" \
    "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);"
#endif
