package stores

// cmd/BonfyreProjectMcp: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreProjectMcp/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreProjectMcp": {
	fragments: [
		{
			macro:  "BF_STORE_PROJECT_MCP_OPERATOR_RUN_CONTEXT"
			source: "cmd/BonfyreProjectMcp/src/main.c"
			statements: [
				{kind: "table", name: "operator_run_context", sql: "CREATE TABLE IF NOT EXISTS operator_run_context(run_id TEXT PRIMARY KEY,agent TEXT NOT NULL,role TEXT NOT NULL,session_id TEXT NOT NULL,interactive INTEGER NOT NULL DEFAULT 1);"},
				{kind: "index", name: "operator_run_context_session", sql: "CREATE UNIQUE INDEX IF NOT EXISTS operator_run_context_session ON operator_run_context(session_id);"},
			]
		},
	]
}
