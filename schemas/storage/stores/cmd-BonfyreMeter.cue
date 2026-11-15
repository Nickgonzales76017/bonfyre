package stores

// cmd/BonfyreMeter: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreMeter/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreMeter": {
	fragments: [
		{
			macro:  "BF_STORE_METER_EVENTS"
			source: "cmd/BonfyreMeter/src/main.c"
			statements: [
				{kind: "table", name: "events", sql: "CREATE TABLE IF NOT EXISTS events (  id INTEGER PRIMARY KEY AUTOINCREMENT,  key_id TEXT NOT NULL,  op TEXT NOT NULL,  bytes INTEGER DEFAULT 0,  duration_ms INTEGER DEFAULT 0,  timestamp TEXT NOT NULL,  cost REAL,  provider TEXT NOT NULL DEFAULT '',  receipt_ref TEXT NOT NULL DEFAULT '',  physical_attempt_ref TEXT NOT NULL DEFAULT '',  realization_ref TEXT NOT NULL DEFAULT '',  rate_id INTEGER,  work_subject TEXT NOT NULL DEFAULT '',  work_scope TEXT NOT NULL DEFAULT '',  work_id TEXT NOT NULL DEFAULT '',  work_run_id TEXT NOT NULL DEFAULT '');"},
				{kind: "index", name: "idx_events_key", sql: "CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id);"},
				{kind: "index", name: "idx_events_ts", sql: "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);"},
				{kind: "table", name: "rate", sql: "CREATE TABLE IF NOT EXISTS rate (  id INTEGER PRIMARY KEY AUTOINCREMENT,  provider TEXT NOT NULL,  op TEXT NOT NULL,  unit TEXT NOT NULL,  amount REAL NOT NULL,  currency TEXT NOT NULL DEFAULT 'USD',  effective_at TEXT NOT NULL,  evidence_ref TEXT NOT NULL,  observed_at TEXT NOT NULL);"},
				{kind: "index", name: "idx_rate_lookup", sql: "CREATE INDEX IF NOT EXISTS idx_rate_lookup ON rate(provider,op,effective_at);"},
			]
		},
		{
			macro:  "BF_STORE_METER_IDX_EVENTS_WORK"
			source: "cmd/BonfyreMeter/src/main.c"
			statements: [
				{kind: "index", name: "idx_events_work", sql: "CREATE INDEX IF NOT EXISTS idx_events_work ON events(work_subject,work_scope,work_id)"},
			]
		},
		{
			macro:  "BF_STORE_METER_IDX_EVENTS_PHYSICAL_ATTEMPT"
			source: "cmd/BonfyreMeter/src/main.c"
			statements: [
				{kind: "index", name: "idx_events_physical_attempt", sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_physical_attempt ON events(physical_attempt_ref) WHERE physical_attempt_ref <> ''"},
			]
		},
	]
}
