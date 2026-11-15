/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREPROVIDER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREPROVIDER_H
/* cmd/BonfyreProvider/src/main.c */
#define BF_STORE_PROVIDER_PROVIDER_OBSERVATIONS \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS provider_observations(id INTEGER PRIMARY KEY AUTOINCREMENT,provider TEXT NOT NULL,event_kind TEXT NOT NULL,observed_at TEXT NOT NULL,attempt_id TEXT,reset_at TEXT,confidence REAL NOT NULL DEFAULT 1.0,detail TEXT);" \
    "CREATE INDEX IF NOT EXISTS idx_provider_observations_provider ON provider_observations(provider,id);" \
    "CREATE TABLE IF NOT EXISTS provider_materializations(id INTEGER PRIMARY KEY AUTOINCREMENT,provider TEXT NOT NULL,artifact_sid TEXT NOT NULL,representation_digest TEXT NOT NULL,external_ref TEXT NOT NULL,kind TEXT NOT NULL,purpose TEXT NOT NULL,scope TEXT NOT NULL,capabilities TEXT NOT NULL DEFAULT '',authority_ref TEXT NOT NULL,created_at TEXT NOT NULL,expires_at TEXT,released_at TEXT,release_reason TEXT,UNIQUE(provider,scope,external_ref));" \
    "CREATE INDEX IF NOT EXISTS idx_provider_materializations_active ON provider_materializations(provider,artifact_sid,representation_digest,released_at,expires_at);" \
    "CREATE TABLE IF NOT EXISTS body_availability(derivation_key TEXT NOT NULL,representation_ref TEXT NOT NULL,body_digest TEXT NOT NULL,location TEXT NOT NULL,receipt_ref TEXT NOT NULL,validity TEXT NOT NULL CHECK(validity IN ('CURRENT','STALE')),observed_at TEXT NOT NULL,invalidated_at TEXT,invalidated_by TEXT,PRIMARY KEY(derivation_key,representation_ref));" \
    "CREATE INDEX IF NOT EXISTS idx_body_availability_current ON body_availability(derivation_key,representation_ref,validity);" \
    "CREATE TABLE IF NOT EXISTS provider_embedding_run(run_id TEXT PRIMARY KEY,provider TEXT NOT NULL,model TEXT NOT NULL,representation_id TEXT NOT NULL,input_digest TEXT NOT NULL,requested_count INTEGER NOT NULL,stored_count INTEGER NOT NULL DEFAULT 0,failed_count INTEGER NOT NULL DEFAULT 0,dimensions INTEGER NOT NULL DEFAULT 0,response_bytes INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL,started_at TEXT NOT NULL,completed_at TEXT,detail TEXT NOT NULL DEFAULT '');" \
    "CREATE TABLE IF NOT EXISTS provider_embedding_projection(run_id TEXT NOT NULL REFERENCES provider_embedding_run(run_id),subject_id TEXT NOT NULL,source_digest TEXT NOT NULL,vector_digest TEXT NOT NULL,dimensions INTEGER NOT NULL,vector BLOB NOT NULL,PRIMARY KEY(run_id,subject_id));" \
    "CREATE INDEX IF NOT EXISTS idx_provider_embedding_projection_subject ON provider_embedding_projection(subject_id,run_id);" \
    "CREATE TABLE IF NOT EXISTS provider_embedding_query(query_id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES provider_embedding_run(run_id),query_digest TEXT NOT NULL,vector_digest TEXT NOT NULL,dimensions INTEGER NOT NULL,candidate_count INTEGER NOT NULL,top_k INTEGER NOT NULL,response_bytes INTEGER NOT NULL,observed_at TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS provider_embedding_rank(query_id TEXT NOT NULL REFERENCES provider_embedding_query(query_id),rank INTEGER NOT NULL,subject_id TEXT NOT NULL,source_digest TEXT NOT NULL,score REAL NOT NULL,PRIMARY KEY(query_id,rank));" \
    "CREATE INDEX IF NOT EXISTS idx_provider_embedding_rank_subject ON provider_embedding_rank(subject_id,query_id);" \
    "CREATE TABLE IF NOT EXISTS provider_fact_delta(delta_id TEXT PRIMARY KEY,run_id TEXT NOT NULL UNIQUE REFERENCES provider_embedding_run(run_id),write_ref TEXT NOT NULL UNIQUE,state TEXT NOT NULL CHECK(state IN ('queued','dispatched')),queued_at TEXT NOT NULL,dispatched_at TEXT);"
#endif
