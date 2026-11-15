package stores

// cmd/BonfyreControl: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreControl/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreControl": {
	fragments: [
		{
			macro:  "BF_STORE_CONTROL_POLICIES"
			source: "cmd/BonfyreControl/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "policies", sql: "CREATE TABLE IF NOT EXISTS policies (  id        TEXT PRIMARY KEY,  name      TEXT NOT NULL,  kind      TEXT NOT NULL CHECK(kind IN ('cost','latency','route','compete')),  recipe    TEXT,  rule_json TEXT NOT NULL,  active    INTEGER NOT NULL DEFAULT 1,  created   INTEGER NOT NULL);"},
				{kind: "table", name: "decisions", sql: "CREATE TABLE IF NOT EXISTS decisions (  id        INTEGER PRIMARY KEY AUTOINCREMENT,  run_id    TEXT,  recipe    TEXT NOT NULL,  stage     TEXT,  decision  TEXT NOT NULL,  reason    TEXT,  score     REAL,  latency_ms INTEGER,  ts        INTEGER NOT NULL);"},
				{kind: "table", name: "scores", sql: "CREATE TABLE IF NOT EXISTS scores (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  artifact      TEXT NOT NULL,  relevance     REAL,  completeness  REAL,  coherence     REAL,  factuality    REAL,  latency_ms    INTEGER,  cost_usd      REAL,  composite     REAL,  entropy       REAL,  ts            INTEGER NOT NULL);"},
				{kind: "virtual_table", name: "policies_fts", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS policies_fts USING fts5(  id, name, kind, recipe, content='policies', content_rowid='rowid');"},
				{kind: "index", name: "idx_decisions_recipe", sql: "CREATE INDEX IF NOT EXISTS idx_decisions_recipe ON decisions(recipe);"},
				{kind: "index", name: "idx_decisions_ts", sql: "CREATE INDEX IF NOT EXISTS idx_decisions_ts     ON decisions(ts);"},
				{kind: "index", name: "idx_scores_artifact", sql: "CREATE INDEX IF NOT EXISTS idx_scores_artifact  ON scores(artifact);"},
			]
		},
	]
}
