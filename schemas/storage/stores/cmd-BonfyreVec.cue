package stores

// cmd/BonfyreVec: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreVec/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreVec": {
	fragments: [
		{
			macro:  "BF_STORE_VEC_VECTOR_FIXTURE"
			source: "cmd/BonfyreVec/src/main.c"
			statements: [
				{kind: "table", name: "vector_fixture", sql: "CREATE TABLE IF NOT EXISTS vector_fixture(id TEXT PRIMARY KEY,embedding BLOB NOT NULL,dimensions INTEGER NOT NULL,metadata TEXT NOT NULL,updated_at TEXT NOT NULL)"},
			]
		},
		{
			macro:  "BF_STORE_VEC_ARTIFACTS"
			source: "cmd/BonfyreVec/src/main.c"
			statements: [
				{kind: "table", name: "artifacts", sql: "CREATE TABLE IF NOT EXISTS artifacts(  id TEXT PRIMARY KEY,  source TEXT,  type TEXT,  text TEXT,  metadata TEXT,  created_at TEXT)"},
			]
		},
		{
			macro:  "BF_STORE_VEC_VEC_ARTIFACTS"
			source: "cmd/BonfyreVec/src/main.c"
			statements: [
				{kind: "virtual_table", name: "vec_artifacts", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifacts USING vec0(  id TEXT PRIMARY KEY,  embedding float[%d])"},
			]
		},
	]
}
