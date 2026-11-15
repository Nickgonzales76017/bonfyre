package stores

// cmd/BonfyreEmbed: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreEmbed/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreEmbed": {
	fragments: [
		{
			macro:  "BF_STORE_EMBED_VEC_ARTIFACTS"
			source: "cmd/BonfyreEmbed/src/main.c"
			statements: [
				{kind: "virtual_table", name: "vec_artifacts", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifacts USING vec0(  id TEXT PRIMARY KEY, embedding float[%d])"},
			]
		},
		{
			macro:  "BF_STORE_EMBED_ARTIFACTS"
			source: "cmd/BonfyreEmbed/src/main.c"
			statements: [
				{kind: "table", name: "artifacts", sql: "CREATE TABLE IF NOT EXISTS artifacts(  id TEXT PRIMARY KEY, source TEXT, type TEXT, text TEXT,  metadata TEXT, created_at TEXT)"},
			]
		},
	]
}
