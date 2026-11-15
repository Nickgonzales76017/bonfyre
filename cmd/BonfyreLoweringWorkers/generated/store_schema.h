/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRELOWERINGWORKERS_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRELOWERINGWORKERS_H
/* cmd/BonfyreLoweringWorkers/src/main.c */
#define BF_STORE_LOWERING_WORKERS_MCP_PROJECTION_ARTIFACT \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS mcp_projection_artifact(identity TEXT PRIMARY KEY,closure_digest TEXT NOT NULL,projection_json TEXT NOT NULL,compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS protocol_compilation_artifact(identity TEXT PRIMARY KEY,program_digest TEXT NOT NULL,program_json TEXT NOT NULL,compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS provider_embedding_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,provider TEXT NOT NULL,model TEXT NOT NULL,representation_id TEXT NOT NULL,dimensions INTEGER NOT NULL,admission_state TEXT NOT NULL CHECK(admission_state='unrequested'),compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS material_observation_demand(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,topic TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'queued',requested_at INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0);" \
    "CREATE TABLE IF NOT EXISTS material_foreign_receipt_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,topic TEXT NOT NULL,receipt_path TEXT NOT NULL,receipt_digest TEXT NOT NULL,observation_kind TEXT NOT NULL,content_sha256 TEXT,body_path TEXT,compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS material_collection_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,subject_address TEXT NOT NULL,compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS material_pack_demand_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS media_library_surface_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,library_id TEXT NOT NULL,subject_ref TEXT NOT NULL,media_program_ref TEXT NOT NULL,grammar TEXT NOT NULL,compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS channel_surface_projection(identity TEXT PRIMARY KEY,projection_digest TEXT NOT NULL,projection_json TEXT NOT NULL,compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS interaction_surface_artifact(identity TEXT PRIMARY KEY,owner_ref TEXT NOT NULL,grammar_id TEXT NOT NULL,subject_ref TEXT NOT NULL,institution_ref TEXT NOT NULL,visibility TEXT NOT NULL,exact_verifier TEXT NOT NULL,target_lowerer_ref TEXT NOT NULL,state TEXT NOT NULL CHECK(state='pending_exact_owner'),compiled_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS interaction_surface_composition(identity TEXT PRIMARY KEY,composition_digest TEXT NOT NULL,composition_json TEXT NOT NULL,exact_verifier_state TEXT NOT NULL CHECK(exact_verifier_state='unqueried'),compiled_at INTEGER NOT NULL);"
/* cmd/BonfyreLoweringWorkers/src/main.c */
#define BF_STORE_LOWERING_WORKERS_P \
    "CREATE TABLE p(j TEXT NOT NULL);"
#endif
