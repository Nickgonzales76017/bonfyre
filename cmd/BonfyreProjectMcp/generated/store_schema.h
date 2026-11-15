/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREPROJECTMCP_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREPROJECTMCP_H
/* cmd/BonfyreProjectMcp/src/main.c */
#define BF_STORE_PROJECT_MCP_OPERATOR_RUN_CONTEXT \
    "CREATE TABLE IF NOT EXISTS operator_run_context(run_id TEXT PRIMARY KEY,agent TEXT NOT NULL,role TEXT NOT NULL,session_id TEXT NOT NULL,interactive INTEGER NOT NULL DEFAULT 1);" \
    "CREATE UNIQUE INDEX IF NOT EXISTS operator_run_context_session ON operator_run_context(session_id);"
#endif
