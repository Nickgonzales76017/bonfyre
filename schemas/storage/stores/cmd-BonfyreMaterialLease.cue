package stores

// cmd/BonfyreMaterialLease: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreMaterialLease/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreMaterialLease": {
	fragments: [
		{
			macro:  "BF_STORE_MATERIAL_LEASE_MATERIAL_LEASE"
			source: "cmd/BonfyreMaterialLease/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "material_lease", sql: "CREATE TABLE IF NOT EXISTS material_lease(subject TEXT NOT NULL,scope TEXT NOT NULL,identity TEXT NOT NULL,owner_ref TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(subject,scope,identity));"},
				{kind: "index", name: "material_lease_identity", sql: "CREATE INDEX IF NOT EXISTS material_lease_identity ON material_lease(identity);"},
				{kind: "table", name: "material_transition", sql: "CREATE TABLE IF NOT EXISTS material_transition(id INTEGER PRIMARY KEY AUTOINCREMENT,action TEXT NOT NULL CHECK(action IN ('acquire','release')),identity TEXT NOT NULL,owner_ref TEXT NOT NULL,refcount_before INTEGER NOT NULL,refcount_after INTEGER NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,created_at INTEGER NOT NULL,emitted INTEGER NOT NULL DEFAULT 0 CHECK(emitted IN (0,1)));"},
				{kind: "index", name: "material_transition_pending", sql: "CREATE INDEX IF NOT EXISTS material_transition_pending ON material_transition(emitted,id);"},
			]
		},
	]
}
