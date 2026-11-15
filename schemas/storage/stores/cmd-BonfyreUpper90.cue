package stores

// cmd/BonfyreUpper90: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreUpper90/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreUpper90": {
	fragments: [
		{
			macro:  "BF_STORE_UPPER90_SCHEMA_MIGRATIONS"
			source: "cmd/BonfyreUpper90/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "schema_migrations", sql: "CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY,applied_at TEXT NOT NULL);"},
				{kind: "table", name: "vertical_role_assignments", sql: "CREATE TABLE IF NOT EXISTS vertical_role_assignments( actor TEXT NOT NULL,estate TEXT NOT NULL,role TEXT NOT NULL,scope_ref TEXT NOT NULL, evidence_ref TEXT NOT NULL,authority_edge_id TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1, granted_at TEXT NOT NULL,revoked_at TEXT, PRIMARY KEY(actor,estate,role,scope_ref));"},
				{kind: "index", name: "vertical_role_assignments_actor", sql: "CREATE INDEX IF NOT EXISTS vertical_role_assignments_actor  ON vertical_role_assignments(actor,estate,active);"},
				{kind: "table", name: "vertical_private_requests", sql: "CREATE TABLE IF NOT EXISTS vertical_private_requests( request_id TEXT PRIMARY KEY,estate TEXT NOT NULL,scope_ref TEXT NOT NULL,request_kind TEXT NOT NULL, reference_token TEXT NOT NULL,requested_by TEXT NOT NULL,mission_id TEXT NOT NULL UNIQUE,node_id TEXT NOT NULL, status TEXT NOT NULL,created_at TEXT NOT NULL);"},
				{kind: "index", name: "vertical_private_requests_actor", sql: "CREATE INDEX IF NOT EXISTS vertical_private_requests_actor  ON vertical_private_requests(requested_by,estate,scope_ref,created_at DESC);"},
			]
		},
		{
			macro:  "BF_STORE_UPPER90_UPPER90_ROLE_ASSIGNMENTS"
			source: "cmd/BonfyreUpper90/src/main.c"
			statements: [
				{kind: "view", name: "upper90_role_assignments", sql: "CREATE VIEW IF NOT EXISTS upper90_role_assignments AS  SELECT * FROM vertical_role_assignments;"},
				{kind: "view", name: "upper90_private_requests", sql: "CREATE VIEW IF NOT EXISTS upper90_private_requests AS  SELECT * FROM vertical_private_requests;"},
			]
		},
	]
}
