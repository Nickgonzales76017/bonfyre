package stores

// cmd/BonfyreEmbed: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreEmbed/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreEmbed": {
	fragments: [
		{
			macro:  "BF_STORE_EMBED_ARTIFACTS"
			source: "cmd/BonfyreEmbed/src/main.c"
			statements: [
				{kind: "table", name: "artifacts", sql: "CREATE TABLE IF NOT EXISTS artifacts(  id TEXT PRIMARY KEY, source TEXT, type TEXT, text TEXT,  metadata TEXT, created_at TEXT)"},
			]
		},
	]
}

// The vector index: a sqlite-vec vec0 table whose width the owner learns at
// open. Declared as a vector store; the owner formats _VECTOR_DDL with it.
store_schemas: "cmd/BonfyreEmbed": stores: vec_artifacts: {
	kind:  "vector"
	macro: "EMBED_INDEX"
	vector: {
		table:        "vec_artifacts"
		dimension:    "runtime"
		metric:       "l2"
		quantization: "float32"
		model:        "the ONNX sentence model bonfyre-embed loads from ~/.cache/bonfyre/models; width is its hidden size"
	}
}
