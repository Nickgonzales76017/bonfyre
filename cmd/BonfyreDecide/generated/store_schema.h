/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREDECIDE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREDECIDE_H
/* cmd/BonfyreDecide/src/dp_gguf.c */
#define BF_STORE_DECIDE_CHECKPOINT_DIGEST \
    "CREATE TABLE IF NOT EXISTS checkpoint_digest (path TEXT PRIMARY KEY, identity TEXT NOT NULL, sha256 TEXT NOT NULL, hashed_at TEXT NOT NULL)"
#endif
