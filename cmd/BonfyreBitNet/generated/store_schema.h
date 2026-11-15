/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREBITNET_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREBITNET_H
/* cmd/BonfyreBitNet/src/main.c */
#define BF_STORE_BIT_NET_BITNET_RANK_RUN \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS bitnet_rank_run(run_id TEXT PRIMARY KEY,representation_id TEXT NOT NULL,scope TEXT NOT NULL,query_digest TEXT NOT NULL,gate_state TEXT NOT NULL,dimensions INTEGER NOT NULL,candidate_count INTEGER NOT NULL,created_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS embedding_projection(run_id TEXT NOT NULL,candidate_identity TEXT NOT NULL,representation_id TEXT NOT NULL,source_digest TEXT NOT NULL,vector_digest TEXT NOT NULL,vector BLOB NOT NULL,score REAL NOT NULL,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,causal_frontier TEXT NOT NULL,PRIMARY KEY(run_id,candidate_identity));" \
    "CREATE TABLE IF NOT EXISTS ranked_candidate_envelope(run_id TEXT NOT NULL,rank INTEGER NOT NULL,candidate_identity TEXT NOT NULL,candidate_kind TEXT NOT NULL,owner_ref TEXT NOT NULL,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,PRIMARY KEY(run_id,rank),UNIQUE(run_id,candidate_identity));" \
    "BEGIN IMMEDIATE;"
/* cmd/BonfyreBitNet/src/main.c */
#define BF_STORE_BIT_NET_Q \
    "CREATE TABLE q(identity TEXT PRIMARY KEY,observed INTEGER NOT NULL)"
#endif
