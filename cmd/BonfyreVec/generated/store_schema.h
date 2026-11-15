/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREVEC_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREVEC_H
/* cmd/BonfyreVec/src/main.c */
#define BF_STORE_VEC_VECTOR_FIXTURE \
    "CREATE TABLE IF NOT EXISTS vector_fixture(id TEXT PRIMARY KEY,embedding BLOB NOT NULL,dimensions INTEGER NOT NULL,metadata TEXT NOT NULL,updated_at TEXT NOT NULL)"
/* cmd/BonfyreVec/src/main.c */
#define BF_STORE_VEC_ARTIFACTS \
    "CREATE TABLE IF NOT EXISTS artifacts(  id TEXT PRIMARY KEY,  source TEXT,  type TEXT,  text TEXT,  metadata TEXT,  created_at TEXT)"
/* store vec_artifacts: vector, declared by cmd/BonfyreVec */
#define BF_STORE_VEC_INDEX_KIND "vector"
#define BF_STORE_VEC_INDEX_VECTOR_TABLE "vec_artifacts"
#define BF_STORE_VEC_INDEX_VECTOR_DIMENSION 0 /* runtime: the owner passes the width to _VECTOR_DDL */
#define BF_STORE_VEC_INDEX_VECTOR_METRIC "l2"
#define BF_STORE_VEC_INDEX_VECTOR_QUANTIZATION "float32"
#define BF_STORE_VEC_INDEX_VECTOR_MODEL "caller-supplied vectors; bonfyre-vec init --dims N (default 384)"
#define BF_STORE_VEC_INDEX_VECTOR_DDL "CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifacts USING vec0(id TEXT PRIMARY KEY, embedding float[%d])"
#endif
