/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREOBSIDIAN_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREOBSIDIAN_H
/* cmd/BonfyreObsidian/src/main.c */
#define BF_STORE_OBSIDIAN_BINDING \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS binding(account TEXT NOT NULL, name TEXT NOT NULL, vault_id TEXT NOT NULL, local_path TEXT NOT NULL UNIQUE, transport TEXT NOT NULL, sync_mode TEXT NOT NULL, conflict_strategy TEXT NOT NULL, excluded_folders TEXT NOT NULL, device_name TEXT NOT NULL, remote_vault TEXT NOT NULL, config_dir TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(account, name));" \
    "CREATE TABLE IF NOT EXISTS receipt(receipt_id TEXT PRIMARY KEY, at TEXT NOT NULL, account TEXT NOT NULL, actor TEXT NOT NULL, verb TEXT NOT NULL, binding TEXT NOT NULL, target TEXT NOT NULL, outcome TEXT NOT NULL, code TEXT NOT NULL, reason TEXT NOT NULL, via TEXT NOT NULL, sha_before TEXT NOT NULL, sha_after TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS receipt_account ON receipt(account, at);" \
    "CREATE TABLE IF NOT EXISTS sync_run(run_id INTEGER PRIMARY KEY, receipt_id TEXT NOT NULL, account TEXT NOT NULL, binding TEXT NOT NULL, kind TEXT NOT NULL, argv TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, exit_code INTEGER NOT NULL, outcome TEXT NOT NULL, log_path TEXT NOT NULL, pid INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS snapshot(snapshot_id INTEGER PRIMARY KEY, account TEXT NOT NULL, binding TEXT NOT NULL, taken_at TEXT NOT NULL, files INTEGER NOT NULL, bytes INTEGER NOT NULL, manifest_digest TEXT NOT NULL, rehashed INTEGER NOT NULL, reused INTEGER NOT NULL, include_config INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS snapshot_file(snapshot_id INTEGER NOT NULL, path TEXT NOT NULL, sha256 TEXT NOT NULL, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, PRIMARY KEY(snapshot_id, path));"
#endif
