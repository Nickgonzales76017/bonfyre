/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREEMBED_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREEMBED_H
/* cmd/BonfyreEmbed/src/main.c */
#define BF_STORE_EMBED_ARTIFACTS \
    "CREATE TABLE IF NOT EXISTS artifacts(  id TEXT PRIMARY KEY, source TEXT, type TEXT, text TEXT,  metadata TEXT, created_at TEXT)"
/* store vec_artifacts: vector, declared by cmd/BonfyreEmbed */
#define BF_STORE_EMBED_INDEX_KIND "vector"
#define BF_STORE_EMBED_INDEX_VECTOR_TABLE "vec_artifacts"
#define BF_STORE_EMBED_INDEX_VECTOR_DIMENSION 0 /* runtime: the owner passes the width to _VECTOR_DDL */
#define BF_STORE_EMBED_INDEX_VECTOR_METRIC "l2"
#define BF_STORE_EMBED_INDEX_VECTOR_QUANTIZATION "float32"
#define BF_STORE_EMBED_INDEX_VECTOR_MODEL "the ONNX sentence model bonfyre-embed loads from ~/.cache/bonfyre/models; width is its hidden size"
#define BF_STORE_EMBED_INDEX_VECTOR_DDL "CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifacts USING vec0(id TEXT PRIMARY KEY, embedding float[%d])"
#endif
