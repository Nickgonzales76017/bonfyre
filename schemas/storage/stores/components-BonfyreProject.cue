package stores

// components/BonfyreProject: store DDL, byte-for-byte what the owner executes.
// Compiled to components/BonfyreProject/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "components/BonfyreProject": {
	fragments: [
		{
			macro:  "BF_STORE_C_PROJECT_META"
			source: "components/BonfyreProject/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA page_size=8192; "},
				{kind: "other", name: "", sql: "PRAGMA journal_mode=OFF; "},
				{kind: "other", name: "", sql: "PRAGMA synchronous=OFF;"},
				{kind: "table", name: "meta", sql: "CREATE TABLE meta(key TEXT PRIMARY KEY, value BLOB);"},
				{kind: "table", name: "receipt", sql: "CREATE TABLE receipt(name TEXT PRIMARY KEY, bytes INTEGER NOT NULL, z BLOB NOT NULL) WITHOUT ROWID;"},
				{kind: "other", name: "", sql: "BEGIN"},
			]
		},
		{
			macro:  "BF_STORE_C_PROJECT_BODY"
			source: "components/BonfyreProject/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA page_size=65536; "},
				{kind: "other", name: "", sql: "PRAGMA journal_mode=OFF; "},
				{kind: "other", name: "", sql: "PRAGMA synchronous=OFF;"},
				{kind: "table", name: "body", sql: "CREATE TABLE body(name TEXT PRIMARY KEY, sha256 TEXT NOT NULL) WITHOUT ROWID;"},
				{kind: "table", name: "blob", sql: "CREATE TABLE blob(sha256 TEXT PRIMARY KEY, bytes INTEGER NOT NULL, z BLOB NOT NULL);"},
				{kind: "other", name: "", sql: "BEGIN"},
			]
		},
		{
			macro:  "BF_STORE_C_PROJECT_FOREIGN_RELATIONS"
			source: "components/BonfyreProject/src/main.c"
			statements: [
				{kind: "table", name: "foreign_relations", sql: "CREATE TABLE IF NOT EXISTS foreign_relations ( id INTEGER PRIMARY KEY, subject_from TEXT NOT NULL, predicate TEXT NOT NULL, subject_to TEXT NOT NULL, source_federation TEXT NOT NULL, carrier TEXT NOT NULL, observed_at TEXT NOT NULL, evidence TEXT NOT NULL, scope TEXT NOT NULL DEFAULT 'public', claim_state TEXT NOT NULL DEFAULT 'observed', admitted_as TEXT, UNIQUE(subject_from, predicate, subject_to, source_federation));"},
				{kind: "index", name: "foreign_relations_scope", sql: "CREATE INDEX IF NOT EXISTS foreign_relations_scope  ON foreign_relations(scope);"},
			]
		},
	]
}
