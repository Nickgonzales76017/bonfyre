/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRELAYER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRELAYER_H
/* cmd/BonfyreLayer/src/main.c */
#define BF_STORE_LAYER_LAYERS \
    "CREATE TABLE IF NOT EXISTS layers (id TEXT PRIMARY KEY,name TEXT,artifact_type TEXT NOT NULL,source_model TEXT,layer_spec_type TEXT,layer_spec_value TEXT,node_start INTEGER,node_end INTEGER,n_nodes INTEGER,n_params INTEGER,sha256 TEXT,format TEXT,artifact_path TEXT,onnx_path TEXT,created_at TEXT NOT NULL);"
/* cmd/BonfyreLayer/src/main.c */
#define BF_STORE_LAYER_IDX_LAYERS_KIND \
    "CREATE INDEX IF NOT EXISTS idx_layers_kind ON layers(artifact_kind);"
#endif
