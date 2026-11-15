package stores

// cmd/BonfyreScopedRespecialize: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreScopedRespecialize/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreScopedRespecialize": {
	fragments: [
		{
			macro:  "BF_STORE_SCOPED_RESPECIALIZE_SCOPED_RESPECIALIZE_RECEIPT"
			source: "cmd/BonfyreScopedRespecialize/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "scoped_respecialize_receipt", sql: "CREATE TABLE IF NOT EXISTS scoped_respecialize_receipt(delta_id TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,closure_path TEXT NOT NULL,new_external_candidates INTEGER NOT NULL,processed_at INTEGER NOT NULL,PRIMARY KEY(delta_id,subject,scope));"},
			]
		},
	]
}
