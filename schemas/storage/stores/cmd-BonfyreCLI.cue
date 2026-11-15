package stores

// cmd/BonfyreCLI: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreCLI/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreCLI": {
	fragments: [
		{
			macro:  "BF_STORE_CLI_FRAGMENTS"
			source: "cmd/BonfyreCLI/src/main.c"
			statements: [
				{kind: "table", name: "fragments", sql: "CREATE TABLE IF NOT EXISTS fragments ( fragment_id TEXT PRIMARY KEY, kind TEXT NOT NULL, key_text TEXT, value_json TEXT NOT NULL, score REAL DEFAULT 0, created_at TEXT NOT NULL);"},
				{kind: "table", name: "plans", sql: "CREATE TABLE IF NOT EXISTS plans ( plan_id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL, score REAL DEFAULT 0, payload_json TEXT NOT NULL, created_at TEXT NOT NULL);"},
				{kind: "index", name: "idx_fragments_kind", sql: "CREATE INDEX IF NOT EXISTS idx_fragments_kind ON fragments(kind);"},
				{kind: "index", name: "idx_fragments_key", sql: "CREATE INDEX IF NOT EXISTS idx_fragments_key ON fragments(key_text);"},
				{kind: "index", name: "idx_plans_kind", sql: "CREATE INDEX IF NOT EXISTS idx_plans_kind ON plans(kind);"},
			]
		},
		{
			macro:  "BF_STORE_CLI_NODES"
			source: "cmd/BonfyreCLI/src/main.c"
			statements: [
				{kind: "table", name: "nodes", sql: "CREATE TABLE IF NOT EXISTS nodes ( node_id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload_json TEXT NOT NULL);"},
				{kind: "table", name: "edges", sql: "CREATE TABLE IF NOT EXISTS edges ( src_id TEXT NOT NULL, rel TEXT NOT NULL, dst_id TEXT NOT NULL, meta_json TEXT, PRIMARY KEY(src_id, rel, dst_id));"},
			]
		},
		{
			macro:  "BF_STORE_CLI_KV_INDEX"
			source: "cmd/BonfyreCLI/src/main.c"
			statements: [
				{kind: "table", name: "kv_index", sql: "CREATE TABLE IF NOT EXISTS kv_index ( key_text TEXT PRIMARY KEY, kind TEXT NOT NULL, value_text TEXT);"},
				{kind: "table", name: "family_counts", sql: "CREATE TABLE IF NOT EXISTS family_counts ( family TEXT PRIMARY KEY, count_value INTEGER NOT NULL);"},
			]
		},
	]
}
