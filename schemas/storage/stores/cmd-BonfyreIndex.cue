package stores

// cmd/BonfyreIndex: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreIndex/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreIndex": {
	fragments: [
		{
			macro:  "BF_STORE_INDEX_FAMILIES"
			source: "cmd/BonfyreIndex/src/main.c"
			statements: [
				{kind: "table", name: "families", sql: "CREATE TABLE IF NOT EXISTS families (  id INTEGER PRIMARY KEY,  artifact_id TEXT UNIQUE,  artifact_type TEXT,  source_system TEXT,  created_at TEXT,  root_hash TEXT,  family_key TEXT,  canonical_key TEXT,  path TEXT,  n_atoms INTEGER,  n_operators INTEGER,  n_realizations INTEGER,  component_total INTEGER,  indexed_at TEXT);"},
				{kind: "table", name: "atoms", sql: "CREATE TABLE IF NOT EXISTS atoms (  id INTEGER PRIMARY KEY,  family_id INTEGER,  atom_id TEXT,  content_hash TEXT,  media_type TEXT,  byte_size INTEGER,  FOREIGN KEY(family_id) REFERENCES families(id));"},
				{kind: "table", name: "operators", sql: "CREATE TABLE IF NOT EXISTS operators (  id INTEGER PRIMARY KEY,  family_id INTEGER,  operator_id TEXT,  op TEXT,  node_hash TEXT,  version TEXT,  FOREIGN KEY(family_id) REFERENCES families(id));"},
				{kind: "index", name: "idx_atoms_hash", sql: "CREATE INDEX IF NOT EXISTS idx_atoms_hash ON atoms(content_hash);"},
				{kind: "index", name: "idx_ops_hash", sql: "CREATE INDEX IF NOT EXISTS idx_ops_hash ON operators(node_hash);"},
				{kind: "index", name: "idx_ops_type", sql: "CREATE INDEX IF NOT EXISTS idx_ops_type ON operators(op);"},
				{kind: "index", name: "idx_families_type", sql: "CREATE INDEX IF NOT EXISTS idx_families_type ON families(artifact_type);"},
				{kind: "index", name: "idx_families_family_key", sql: "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);"},
				{kind: "index", name: "idx_families_canonical", sql: "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);"},
			]
		},
		{
			macro:  "BF_STORE_INDEX_IDX_FAMILIES_FAMILY_KEY"
			source: "cmd/BonfyreIndex/src/main.c"
			statements: [
				{kind: "index", name: "idx_families_family_key", sql: "CREATE INDEX IF NOT EXISTS idx_families_family_key ON families(family_key);"},
			]
		},
		{
			macro:  "BF_STORE_INDEX_IDX_FAMILIES_CANONICAL"
			source: "cmd/BonfyreIndex/src/main.c"
			statements: [
				{kind: "index", name: "idx_families_canonical", sql: "CREATE INDEX IF NOT EXISTS idx_families_canonical ON families(canonical_key);"},
			]
		},
	]
}
