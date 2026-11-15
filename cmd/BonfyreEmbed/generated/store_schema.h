/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREEMBED_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREEMBED_H
/* cmd/BonfyreEmbed/src/main.c */
#define BF_STORE_EMBED_VEC_ARTIFACTS \
    "CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifacts USING vec0(  id TEXT PRIMARY KEY, embedding float[%d])"
/* cmd/BonfyreEmbed/src/main.c */
#define BF_STORE_EMBED_ARTIFACTS \
    "CREATE TABLE IF NOT EXISTS artifacts(  id TEXT PRIMARY KEY, source TEXT, type TEXT, text TEXT,  metadata TEXT, created_at TEXT)"
#endif
