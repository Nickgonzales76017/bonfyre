/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRECLI_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRECLI_H
/* cmd/BonfyreCLI/src/main.c */
#define BF_STORE_CLI_FRAGMENTS \
    "CREATE TABLE IF NOT EXISTS fragments ( fragment_id TEXT PRIMARY KEY, kind TEXT NOT NULL, key_text TEXT, value_json TEXT NOT NULL, score REAL DEFAULT 0, created_at TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS plans ( plan_id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL, score REAL DEFAULT 0, payload_json TEXT NOT NULL, created_at TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_fragments_kind ON fragments(kind);" \
    "CREATE INDEX IF NOT EXISTS idx_fragments_key ON fragments(key_text);" \
    "CREATE INDEX IF NOT EXISTS idx_plans_kind ON plans(kind);"
/* cmd/BonfyreCLI/src/main.c */
#define BF_STORE_CLI_NODES \
    "CREATE TABLE IF NOT EXISTS nodes ( node_id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload_json TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS edges ( src_id TEXT NOT NULL, rel TEXT NOT NULL, dst_id TEXT NOT NULL, meta_json TEXT, PRIMARY KEY(src_id, rel, dst_id));"
/* cmd/BonfyreCLI/src/main.c */
#define BF_STORE_CLI_KV_INDEX \
    "CREATE TABLE IF NOT EXISTS kv_index ( key_text TEXT PRIMARY KEY, kind TEXT NOT NULL, value_text TEXT);" \
    "CREATE TABLE IF NOT EXISTS family_counts ( family TEXT PRIMARY KEY, count_value INTEGER NOT NULL);"
#endif
