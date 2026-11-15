/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREATTENTION_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREATTENTION_H
/* cmd/BonfyreAttention/src/main.c */
#define BF_STORE_ATTENTION_WATCHES \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS watches(watch_id TEXT PRIMARY KEY,subject TEXT NOT NULL,temperature TEXT NOT NULL DEFAULT 'warm',reheat_at TEXT,reheat_on TEXT NOT NULL DEFAULT '',attempts INTEGER NOT NULL DEFAULT 0,last_checked_at TEXT,last_change_at TEXT,detail TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_watches_due ON watches(temperature,reheat_at);"
#endif
