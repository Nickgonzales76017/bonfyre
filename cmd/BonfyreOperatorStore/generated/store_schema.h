/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREOPERATORSTORE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREOPERATORSTORE_H
/* cmd/BonfyreOperatorStore/src/main.c */
#define BF_STORE_OPERATOR_STORE_GOALS \
    "CREATE TABLE IF NOT EXISTS goals (id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY,goal_id TEXT NOT NULL,started_at REAL NOT NULL,completed_at REAL,state TEXT NOT NULL,response_id TEXT,summary TEXT NOT NULL DEFAULT '',usage_json TEXT NOT NULL DEFAULT '{}');" \
    "CREATE TABLE IF NOT EXISTS actions (seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,tool TEXT NOT NULL,args_json TEXT NOT NULL,state TEXT NOT NULL,result_json TEXT NOT NULL,started_at REAL NOT NULL,duration_ms INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS budgets (day TEXT PRIMARY KEY,cycles INTEGER NOT NULL DEFAULT 0,tool_calls INTEGER NOT NULL DEFAULT 0,input_tokens INTEGER NOT NULL DEFAULT 0,output_tokens INTEGER NOT NULL DEFAULT 0);" \
    "CREATE TABLE IF NOT EXISTS operator_response_usage (response_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,usage_json TEXT NOT NULL,recorded_at REAL NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS operator_run_owners (run_id TEXT PRIMARY KEY,pid INTEGER NOT NULL,owner_host TEXT NOT NULL,owner_nonce TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS operator_provider_health (provider TEXT PRIMARY KEY,state TEXT NOT NULL,cause TEXT NOT NULL,observed_at REAL NOT NULL,retry_at REAL NOT NULL DEFAULT 0);" \
    "CREATE INDEX IF NOT EXISTS operator_goals_due ON goals(state,priority DESC,updated_at,retry_at);" \
    "CREATE INDEX IF NOT EXISTS operator_runs_goal_open ON runs(goal_id,completed_at,started_at);" \
    "CREATE INDEX IF NOT EXISTS operator_runs_started ON runs(started_at);" \
    "CREATE INDEX IF NOT EXISTS operator_actions_run ON actions(run_id,seq);"
#endif
