package stores

// cmd/BonfyrePackCondition: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyrePackCondition/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyrePackCondition": {
	fragments: [
		{
			macro:  "BF_STORE_PACK_CONDITION_PACK_CONDITION_STATE"
			source: "cmd/BonfyrePackCondition/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "pack_condition_state", sql: "CREATE TABLE IF NOT EXISTS pack_condition_state(condition_ref TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('satisfied','unsatisfied')),generation INTEGER NOT NULL CHECK(generation>=1),basis TEXT NOT NULL CHECK(length(basis)>0),updated_at INTEGER NOT NULL,PRIMARY KEY(condition_ref,subject,scope));"},
				{kind: "index", name: "pack_condition_subject_scope", sql: "CREATE INDEX IF NOT EXISTS pack_condition_subject_scope ON pack_condition_state(subject,scope,condition_ref);"},
				{kind: "table", name: "pack_condition_outbox", sql: "CREATE TABLE IF NOT EXISTS pack_condition_outbox(condition_ref TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('satisfied','unsatisfied')),generation INTEGER NOT NULL CHECK(generation>=1),basis TEXT NOT NULL CHECK(length(basis)>0),emitted INTEGER NOT NULL DEFAULT 0 CHECK(emitted IN (0,1)),created_at INTEGER NOT NULL,PRIMARY KEY(condition_ref,subject,scope,generation));"},
				{kind: "index", name: "pack_condition_outbox_pending", sql: "CREATE INDEX IF NOT EXISTS pack_condition_outbox_pending ON pack_condition_outbox(emitted,created_at);"},
			]
		},
	]
}
