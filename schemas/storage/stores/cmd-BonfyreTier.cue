package stores

// cmd/BonfyreTier: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreTier/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreTier": {
	fragments: [
		{
			macro:  "BF_STORE_TIER_TIERS"
			source: "cmd/BonfyreTier/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "tiers", sql: "CREATE TABLE IF NOT EXISTS tiers(  name        TEXT PRIMARY KEY,  max_ms      INTEGER NOT NULL,  description TEXT NOT NULL,  color       TEXT);"},
				{kind: "table", name: "assignments", sql: "CREATE TABLE IF NOT EXISTS assignments(  recipe      TEXT NOT NULL,  stage       TEXT NOT NULL,  tier        TEXT NOT NULL REFERENCES tiers(name),  updated     INTEGER NOT NULL,  PRIMARY KEY(recipe,stage));"},
				{kind: "table", name: "observations", sql: "CREATE TABLE IF NOT EXISTS observations(  id          INTEGER PRIMARY KEY AUTOINCREMENT,  recipe      TEXT NOT NULL,  stage       TEXT NOT NULL,  latency_ms  INTEGER NOT NULL,  violated    INTEGER NOT NULL DEFAULT 0,  tier        TEXT,  ts          INTEGER NOT NULL);"},
				{kind: "index", name: "idx_obs_recipe", sql: "CREATE INDEX IF NOT EXISTS idx_obs_recipe ON observations(recipe,stage);"},
			]
		},
	]
}
