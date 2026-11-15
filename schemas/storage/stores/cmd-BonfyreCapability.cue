package stores

// cmd/BonfyreCapability: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreCapability/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreCapability": {
	fragments: [
		{
			macro:  "BF_STORE_CAPABILITY_CAPABILITIES"
			source: "cmd/BonfyreCapability/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "capabilities", sql: "CREATE TABLE IF NOT EXISTS capabilities(  id            TEXT PRIMARY KEY,  name          TEXT NOT NULL,  description   TEXT NOT NULL,  tags          TEXT,  binary        TEXT NOT NULL,  command       TEXT,  model_id      TEXT,  hardware_tier TEXT NOT NULL DEFAULT 'cpu',  cost_estimate REAL NOT NULL DEFAULT 0.0,  latency_tier  TEXT NOT NULL DEFAULT 'fast',  stage_class   TEXT,  artifact_out  TEXT,  keywords      TEXT,  source        TEXT NOT NULL DEFAULT 'manual',  updated       INTEGER NOT NULL);"},
				{kind: "virtual_table", name: "cap_fts", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS cap_fts USING fts5(  id,name,description,tags,binary,  content='capabilities',content_rowid='rowid');"},
			]
		},
		{
			macro:  "BF_STORE_CAPABILITY_VERIFIED_REPAIRS"
			source: "cmd/BonfyreCapability/src/main.c"
			statements: [
				{kind: "table", name: "verified_repairs", sql: "CREATE TABLE IF NOT EXISTS verified_repairs(  id               TEXT PRIMARY KEY,  claim            TEXT NOT NULL,  vector_file      TEXT NOT NULL,  vector_sha256    TEXT NOT NULL,  case_text        TEXT NOT NULL,  runs             INTEGER NOT NULL,  before_sha256    TEXT NOT NULL,  before_source    TEXT,  after_binary     TEXT NOT NULL,  after_sha256     TEXT NOT NULL,  installed_sha256 TEXT,  record           TEXT NOT NULL,  admitted         INTEGER NOT NULL);"},
			]
		},
	]
}
