package stores

// cmd/BonfyreScopedLeaseReheat: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreScopedLeaseReheat/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreScopedLeaseReheat": {
	fragments: [
		{
			macro:  "BF_STORE_SCOPED_LEASE_REHEAT_SCOPED_LEASE_REHEAT_RECEIPT"
			source: "cmd/BonfyreScopedLeaseReheat/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "scoped_lease_reheat_receipt", sql: "CREATE TABLE IF NOT EXISTS scoped_lease_reheat_receipt(delta_id TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,closure_path TEXT NOT NULL,processed_at INTEGER NOT NULL,PRIMARY KEY(delta_id,subject,scope));"},
			]
		},
	]
}
