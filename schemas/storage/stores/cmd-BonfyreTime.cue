package stores

// cmd/BonfyreTime: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreTime/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreTime": {
	fragments: [
		{
			macro:  "BF_STORE_TIME_ARTIFACTS"
			source: "cmd/BonfyreTime/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "artifacts", sql: "CREATE TABLE IF NOT EXISTS artifacts (  id       TEXT PRIMARY KEY,  path     TEXT,  recipe   TEXT NOT NULL,  status   TEXT NOT NULL DEFAULT 'pending',  runs     INTEGER NOT NULL DEFAULT 0,  last_run INTEGER,  created  INTEGER NOT NULL);"},
				{kind: "table", name: "runs", sql: "CREATE TABLE IF NOT EXISTS runs (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  artifact   TEXT NOT NULL,  recipe     TEXT NOT NULL,  run_id     TEXT,  exit_code  INTEGER,  out_path   TEXT,  ts         INTEGER NOT NULL);"},
				{kind: "table", name: "triggers", sql: "CREATE TABLE IF NOT EXISTS triggers (  id      INTEGER PRIMARY KEY AUTOINCREMENT,  event   TEXT NOT NULL,  recipe  TEXT NOT NULL,  active  INTEGER NOT NULL DEFAULT 1,  created INTEGER NOT NULL);"},
				{kind: "index", name: "idx_artifacts_status", sql: "CREATE INDEX IF NOT EXISTS idx_artifacts_status ON artifacts(status);"},
				{kind: "index", name: "idx_runs_artifact", sql: "CREATE INDEX IF NOT EXISTS idx_runs_artifact    ON runs(artifact);"},
			]
		},
	]
}
