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
	]
}

// The vector index: a sqlite-vec vec0 table whose width the owner learns at
// open. Declared as a vector store; the owner formats _VECTOR_DDL with it.
store_schemas: "cmd/BonfyreVec": stores: vec_artifacts: {
	kind:  "vector"
	macro: "VEC_INDEX"
	vector: {
		table:        "vec_artifacts"
		dimension:    "runtime"
		metric:       "l2"
		quantization: "float32"
		model:        "caller-supplied vectors; bonfyre-vec init --dims N (default 384)"
	}
}
