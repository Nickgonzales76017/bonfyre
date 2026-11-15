package stores

// cmd/BonfyreOperatorStore: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreOperatorStore/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreOperatorStore": {
	fragments: [
		{
			macro:  "BF_STORE_OPERATOR_STORE_GOALS"
			source: "cmd/BonfyreOperatorStore/src/main.c"
			statements: [
				{kind: "table", name: "goals", sql: "CREATE TABLE IF NOT EXISTS goals (id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);"},
				{kind: "table", name: "runs", sql: "CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY,goal_id TEXT NOT NULL,started_at REAL NOT NULL,completed_at REAL,state TEXT NOT NULL,response_id TEXT,summary TEXT NOT NULL DEFAULT '',usage_json TEXT NOT NULL DEFAULT '{}');"},
				{kind: "table", name: "actions", sql: "CREATE TABLE IF NOT EXISTS actions (seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,tool TEXT NOT NULL,args_json TEXT NOT NULL,state TEXT NOT NULL,result_json TEXT NOT NULL,started_at REAL NOT NULL,duration_ms INTEGER NOT NULL);"},
				{kind: "table", name: "meta", sql: "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);"},
				{kind: "table", name: "budgets", sql: "CREATE TABLE IF NOT EXISTS budgets (day TEXT PRIMARY KEY,cycles INTEGER NOT NULL DEFAULT 0,tool_calls INTEGER NOT NULL DEFAULT 0,input_tokens INTEGER NOT NULL DEFAULT 0,output_tokens INTEGER NOT NULL DEFAULT 0);"},
				{kind: "table", name: "operator_response_usage", sql: "CREATE TABLE IF NOT EXISTS operator_response_usage (response_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,usage_json TEXT NOT NULL,recorded_at REAL NOT NULL);"},
				{kind: "table", name: "operator_run_owners", sql: "CREATE TABLE IF NOT EXISTS operator_run_owners (run_id TEXT PRIMARY KEY,pid INTEGER NOT NULL,owner_host TEXT NOT NULL,owner_nonce TEXT NOT NULL);"},
				{kind: "table", name: "operator_provider_health", sql: "CREATE TABLE IF NOT EXISTS operator_provider_health (provider TEXT PRIMARY KEY,state TEXT NOT NULL,cause TEXT NOT NULL,observed_at REAL NOT NULL,retry_at REAL NOT NULL DEFAULT 0);"},
				{kind: "index", name: "operator_goals_due", sql: "CREATE INDEX IF NOT EXISTS operator_goals_due ON goals(state,priority DESC,updated_at,retry_at);"},
				{kind: "index", name: "operator_runs_goal_open", sql: "CREATE INDEX IF NOT EXISTS operator_runs_goal_open ON runs(goal_id,completed_at,started_at);"},
				{kind: "index", name: "operator_runs_started", sql: "CREATE INDEX IF NOT EXISTS operator_runs_started ON runs(started_at);"},
				{kind: "index", name: "operator_actions_run", sql: "CREATE INDEX IF NOT EXISTS operator_actions_run ON actions(run_id,seq);"},
			]
		},
	]
}
