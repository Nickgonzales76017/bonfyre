/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREORCHESTRATE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREORCHESTRATE_H
/* cmd/BonfyreOrchestrate/src/main.c */
#define BF_STORE_ORCHESTRATE_ORCHESTRATION_POLICY \
    "CREATE TABLE IF NOT EXISTS orchestration_policy (signature TEXT PRIMARY KEY,state_key TEXT NOT NULL DEFAULT '',family TEXT NOT NULL DEFAULT 'general',input_type TEXT NOT NULL DEFAULT '',latency_class TEXT NOT NULL DEFAULT '',surface TEXT NOT NULL DEFAULT '',booster_csv TEXT NOT NULL,predicted_confidence REAL NOT NULL,predicted_information_gain REAL NOT NULL,avg_quality_gain REAL NOT NULL DEFAULT 0,avg_latency_delta REAL NOT NULL DEFAULT 0,avg_regret REAL NOT NULL DEFAULT 0,exec_score REAL NOT NULL DEFAULT 0,artifact_score REAL NOT NULL DEFAULT 0,tensor_score REAL NOT NULL DEFAULT 0,cms_score REAL NOT NULL DEFAULT 0,retrieval_score REAL NOT NULL DEFAULT 0,value_score REAL NOT NULL DEFAULT 0,policy_score REAL NOT NULL DEFAULT 0,samples INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL);"
#endif
