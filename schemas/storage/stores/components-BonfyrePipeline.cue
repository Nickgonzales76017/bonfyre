package stores

// components/BonfyrePipeline: store DDL, byte-for-byte what the owner executes.
// Compiled to components/BonfyrePipeline/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "components/BonfyrePipeline": {
	fragments: [
		{
			macro:  "BF_STORE_C_PIPELINE_FAMILIES"
			source: "components/BonfyrePipeline/src/main.c"
			statements: [
				{kind: "table", name: "families", sql: "CREATE TABLE IF NOT EXISTS families (  id INTEGER PRIMARY KEY, artifact_id TEXT UNIQUE, artifact_type TEXT,  source_system TEXT, created_at TEXT, root_hash TEXT, family_key TEXT, canonical_key TEXT, path TEXT,  n_atoms INTEGER, n_operators INTEGER, n_realizations INTEGER, component_total INTEGER, indexed_at TEXT);"},
				{kind: "index", name: "idx_families_type", sql: "CREATE INDEX IF NOT EXISTS idx_families_type ON families(artifact_type);"},
				{kind: "index", name: "idx_families_family_key", sql: "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);"},
				{kind: "index", name: "idx_families_canonical", sql: "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);"},
			]
		},
		{
			macro:  "BF_STORE_C_PIPELINE_IDX_FAMILIES_FAMILY_KEY"
			source: "components/BonfyrePipeline/src/main.c"
			statements: [
				{kind: "index", name: "idx_families_family_key", sql: "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);"},
			]
		},
		{
			macro:  "BF_STORE_C_PIPELINE_IDX_FAMILIES_CANONICAL"
			source: "components/BonfyrePipeline/src/main.c"
			statements: [
				{kind: "index", name: "idx_families_canonical", sql: "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);"},
			]
		},
		{
			macro:  "BF_STORE_C_PIPELINE_EVENTS"
			source: "components/BonfyrePipeline/src/main.c"
			statements: [
				{kind: "table", name: "events", sql: "CREATE TABLE IF NOT EXISTS events (  id INTEGER PRIMARY KEY AUTOINCREMENT, key_id TEXT NOT NULL, op TEXT NOT NULL,  bytes INTEGER DEFAULT 0, duration_ms INTEGER DEFAULT 0,  timestamp TEXT NOT NULL, cost REAL DEFAULT 0.0);"},
				{kind: "index", name: "idx_events_key", sql: "CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id);"},
			]
		},
	]
}
