package stores

// cmd/BonfyreDecide: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreDecide/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreDecide": {
	fragments: [
		{
			macro:  "BF_STORE_DECIDE_CHECKPOINT_DIGEST"
			source: "cmd/BonfyreDecide/src/dp_gguf.c"
			statements: [
				{kind: "table", name: "checkpoint_digest", sql: "CREATE TABLE IF NOT EXISTS checkpoint_digest (path TEXT PRIMARY KEY, identity TEXT NOT NULL, sha256 TEXT NOT NULL, hashed_at TEXT NOT NULL)"},
			]
		},
	]
}
