package stores

// cmd/BonfyreCompete: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreCompete/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreCompete": {
	fragments: [
		{
			macro:  "BF_STORE_COMPETE_COMPETITIONS"
			source: "cmd/BonfyreCompete/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "competitions", sql: "CREATE TABLE IF NOT EXISTS competitions(  id        TEXT PRIMARY KEY,  recipe    TEXT NOT NULL,  stage     TEXT NOT NULL,  status    TEXT NOT NULL DEFAULT 'active',  created   INTEGER NOT NULL);"},
				{kind: "table", name: "variants", sql: "CREATE TABLE IF NOT EXISTS variants(  id         TEXT PRIMARY KEY,  comp_id    TEXT NOT NULL REFERENCES competitions(id),  label      TEXT NOT NULL,  config     TEXT,  promoted   INTEGER NOT NULL DEFAULT 0,  wins       INTEGER NOT NULL DEFAULT 0,  losses     INTEGER NOT NULL DEFAULT 0,  created    INTEGER NOT NULL);"},
				{kind: "table", name: "results", sql: "CREATE TABLE IF NOT EXISTS results(  id         INTEGER PRIMARY KEY AUTOINCREMENT,  comp_id    TEXT NOT NULL REFERENCES competitions(id),  variant_id TEXT NOT NULL REFERENCES variants(id),  input_hash TEXT,  score      REAL NOT NULL,  metric     TEXT NOT NULL DEFAULT 'composite',  latency_ms INTEGER,  ts         INTEGER NOT NULL);"},
				{kind: "index", name: "idx_results_comp", sql: "CREATE INDEX IF NOT EXISTS idx_results_comp ON results(comp_id,variant_id);"},
			]
		},
	]
}
