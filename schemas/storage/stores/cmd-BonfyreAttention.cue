package stores

// cmd/BonfyreAttention: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreAttention/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreAttention": {
	fragments: [
		{
			macro:  "BF_STORE_ATTENTION_WATCHES"
			source: "cmd/BonfyreAttention/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "watches", sql: "CREATE TABLE IF NOT EXISTS watches(watch_id TEXT PRIMARY KEY,subject TEXT NOT NULL,temperature TEXT NOT NULL DEFAULT 'warm',reheat_at TEXT,reheat_on TEXT NOT NULL DEFAULT '',attempts INTEGER NOT NULL DEFAULT 0,last_checked_at TEXT,last_change_at TEXT,detail TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL);"},
				{kind: "index", name: "idx_watches_due", sql: "CREATE INDEX IF NOT EXISTS idx_watches_due ON watches(temperature,reheat_at);"},
			]
		},
	]
}
