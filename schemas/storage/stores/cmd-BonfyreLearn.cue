package stores

// cmd/BonfyreLearn: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreLearn/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreLearn": {
	fragments: [
		{
			macro:  "BF_STORE_LEARN_FEEDBACK"
			source: "cmd/BonfyreLearn/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "feedback", sql: "CREATE TABLE IF NOT EXISTS feedback(  id       INTEGER PRIMARY KEY AUTOINCREMENT,  run_id   TEXT NOT NULL,  stage    TEXT,  score    REAL NOT NULL,  label    TEXT,  ts       INTEGER NOT NULL);"},
				{kind: "table", name: "thresholds", sql: "CREATE TABLE IF NOT EXISTS thresholds(  stage    TEXT PRIMARY KEY,  quality  REAL NOT NULL DEFAULT 0.75,  latency_ms INTEGER NOT NULL DEFAULT 5000,  updated  INTEGER NOT NULL);"},
				{kind: "table", name: "optimizations", sql: "CREATE TABLE IF NOT EXISTS optimizations(  id       INTEGER PRIMARY KEY AUTOINCREMENT,  stage    TEXT NOT NULL,  metric   TEXT NOT NULL,  old_val  REAL NOT NULL,  new_val  REAL NOT NULL,  reason   TEXT,  ts       INTEGER NOT NULL);"},
				{kind: "index", name: "idx_feedback_run", sql: "CREATE INDEX IF NOT EXISTS idx_feedback_run   ON feedback(run_id);"},
				{kind: "index", name: "idx_feedback_stage", sql: "CREATE INDEX IF NOT EXISTS idx_feedback_stage ON feedback(stage);"},
			]
		},
	]
}
