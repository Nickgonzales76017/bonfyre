package stores

// cmd/BonfyreSAE: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreSAE/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreSAE": {
	fragments: [
		{
			macro:  "BF_STORE_SAE_ACTIVATIONS"
			source: "cmd/BonfyreSAE/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "activations", sql: "CREATE TABLE IF NOT EXISTS activations (  id              INTEGER PRIMARY KEY AUTOINCREMENT,  artifact_id     TEXT,  dict_path       TEXT NOT NULL,  model_family    TEXT NOT NULL,  layer           INTEGER NOT NULL,  top_k           INTEGER NOT NULL,  feature_hash    TEXT NOT NULL,  danger_tripped  INTEGER NOT NULL DEFAULT 0,  elapsed_ms      REAL,  manifest_path   TEXT,  ts              TEXT NOT NULL);"},
				{kind: "table", name: "feature_events", sql: "CREATE TABLE IF NOT EXISTS feature_events (  id              INTEGER PRIMARY KEY AUTOINCREMENT,  activation_id   INTEGER NOT NULL REFERENCES activations(id),  feature_id      INTEGER NOT NULL,  activation      REAL NOT NULL,  normalised      REAL NOT NULL,  tags            INTEGER NOT NULL,  label           TEXT,  rank            INTEGER NOT NULL);"},
				{kind: "index", name: "idx_fe_fid", sql: "CREATE INDEX IF NOT EXISTS idx_fe_fid ON feature_events(feature_id);"},
				{kind: "index", name: "idx_fe_act", sql: "CREATE INDEX IF NOT EXISTS idx_fe_act ON feature_events(activation);"},
				{kind: "index", name: "idx_act_hash", sql: "CREATE INDEX IF NOT EXISTS idx_act_hash ON activations(feature_hash);"},
				{kind: "index", name: "idx_act_model", sql: "CREATE INDEX IF NOT EXISTS idx_act_model ON activations(model_family, layer);"},
			]
		},
	]
}
