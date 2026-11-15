package stores

// cmd/BonfyreGraph: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreGraph/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreGraph": {
	fragments: [
		{
			macro:  "BF_STORE_GRAPH_ATOMS"
			source: "cmd/BonfyreGraph/src/main.c"
			statements: [
				{kind: "table", name: "atoms", sql: "CREATE TABLE IF NOT EXISTS atoms (  atom_id TEXT PRIMARY KEY,  content_hash TEXT NOT NULL,  media_type TEXT NOT NULL,  path TEXT,  byte_size INTEGER,  label TEXT);"},
				{kind: "table", name: "operators", sql: "CREATE TABLE IF NOT EXISTS operators (  operator_id TEXT PRIMARY KEY,  op TEXT NOT NULL,  inputs TEXT NOT NULL,  output TEXT NOT NULL,  params TEXT DEFAULT '{}',  node_hash TEXT,  version TEXT DEFAULT '1.0.0',  deterministic INTEGER DEFAULT 1);"},
				{kind: "table", name: "realizations", sql: "CREATE TABLE IF NOT EXISTS realizations (  realization_id TEXT PRIMARY KEY,  media_type TEXT NOT NULL,  path TEXT,  content_hash TEXT,  byte_size INTEGER,  pinned INTEGER DEFAULT 0,  produced_by TEXT,  label TEXT);"},
				{kind: "table", name: "meta", sql: "CREATE TABLE IF NOT EXISTS meta (  key TEXT PRIMARY KEY,  value TEXT);"},
			]
		},
	]
}
