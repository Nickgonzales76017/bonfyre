package stores

// cmd/BonfyreLayer: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreLayer/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreLayer": {
	fragments: [
		{
			macro:  "BF_STORE_LAYER_LAYERS"
			source: "cmd/BonfyreLayer/src/main.c"
			statements: [
				{kind: "table", name: "layers", sql: "CREATE TABLE IF NOT EXISTS layers (id TEXT PRIMARY KEY,name TEXT,artifact_type TEXT NOT NULL,source_model TEXT,layer_spec_type TEXT,layer_spec_value TEXT,node_start INTEGER,node_end INTEGER,n_nodes INTEGER,n_params INTEGER,sha256 TEXT,format TEXT,artifact_path TEXT,onnx_path TEXT,created_at TEXT NOT NULL);"},
			]
		},
		{
			macro:  "BF_STORE_LAYER_IDX_LAYERS_KIND"
			source: "cmd/BonfyreLayer/src/main.c"
			statements: [
				{kind: "index", name: "idx_layers_kind", sql: "CREATE INDEX IF NOT EXISTS idx_layers_kind ON layers(artifact_kind);"},
			]
		},
	]
}
