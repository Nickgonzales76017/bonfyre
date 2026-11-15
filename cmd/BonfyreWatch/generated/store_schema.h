/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREWATCH_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREWATCH_H
/* cmd/BonfyreWatch/src/main.c */
#define BF_STORE_WATCH_WATCH_SESSIONS \
    "CREATE TABLE IF NOT EXISTS watch_sessions( session_id TEXT PRIMARY KEY, watch_dir TEXT, pipeline TEXT, out_dir TEXT, interval_sec INTEGER, dry_run INTEGER, recursive INTEGER, created_at TEXT);" \
    "CREATE TABLE IF NOT EXISTS watch_events( session_id TEXT, event_id TEXT PRIMARY KEY, path TEXT, size INTEGER, mtime INTEGER, status TEXT, output_dir TEXT, pipeline TEXT, run_ref TEXT, created_at TEXT);"
#endif
