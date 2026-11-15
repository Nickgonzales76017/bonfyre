package stores

// cmd/BonfyreFlow: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreFlow/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreFlow": {
	fragments: [
		{
			macro:  "BF_STORE_FLOW_FLOWS"
			source: "cmd/BonfyreFlow/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "flows", sql: "CREATE TABLE IF NOT EXISTS flows(  id          TEXT PRIMARY KEY,  name        TEXT NOT NULL,  definition  TEXT NOT NULL,  entry_stage TEXT NOT NULL,  created     INTEGER NOT NULL,  updated     INTEGER NOT NULL);"},
				{kind: "table", name: "edges", sql: "CREATE TABLE IF NOT EXISTS edges(  flow_id     TEXT NOT NULL REFERENCES flows(id) ON DELETE CASCADE,  from_stage  TEXT NOT NULL,  to_stage    TEXT NOT NULL,  kind        TEXT NOT NULL DEFAULT 'seq',  condition   TEXT,  PRIMARY KEY(flow_id,from_stage,to_stage));"},
				{kind: "table", name: "runs", sql: "CREATE TABLE IF NOT EXISTS runs(  id            TEXT PRIMARY KEY,  flow_id       TEXT NOT NULL REFERENCES flows(id),  input         TEXT,  status        TEXT NOT NULL DEFAULT 'running',  current_stage TEXT,  started       INTEGER NOT NULL,  finished      INTEGER);"},
				{kind: "table", name: "run_stages", sql: "CREATE TABLE IF NOT EXISTS run_stages(  run_id  TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,  stage   TEXT NOT NULL,  status  TEXT NOT NULL DEFAULT 'pending',  started INTEGER,  done    INTEGER,  output  TEXT,  PRIMARY KEY(run_id,stage));"},
				{kind: "index", name: "idx_runs_flow", sql: "CREATE INDEX IF NOT EXISTS idx_runs_flow ON runs(flow_id);"},
			]
		},
	]
}
