package stores

// cmd/BonfyreScopedFrontier: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreScopedFrontier/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreScopedFrontier": {
	fragments: [
		{
			macro:  "BF_STORE_SCOPED_FRONTIER_CLOSURE_FRONTIER"
			source: "cmd/BonfyreScopedFrontier/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "closure_frontier", sql: "CREATE TABLE IF NOT EXISTS closure_frontier(subject TEXT NOT NULL,scope TEXT NOT NULL,tracked_at INTEGER NOT NULL,PRIMARY KEY(subject,scope));"},
				{kind: "table", name: "closure_dependency", sql: "CREATE TABLE IF NOT EXISTS closure_dependency(subject TEXT NOT NULL,scope TEXT NOT NULL,dependency_ref TEXT NOT NULL,basis TEXT NOT NULL,PRIMARY KEY(subject,scope,dependency_ref,basis));"},
				{kind: "index", name: "closure_dependency_lookup", sql: "CREATE INDEX IF NOT EXISTS closure_dependency_lookup ON closure_dependency(dependency_ref,subject,scope);"},
			]
		},
	]
}
