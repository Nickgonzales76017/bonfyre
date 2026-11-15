package stores

// components/BonfyreMeter: store DDL, byte-for-byte what the owner executes.
// Compiled to components/BonfyreMeter/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "components/BonfyreMeter": {
	fragments: [
		{
			macro:  "BF_STORE_C_METER_EVENTS"
			source: "components/BonfyreMeter/src/main.c"
			statements: [
				{kind: "table", name: "events", sql: "CREATE TABLE IF NOT EXISTS events (  id INTEGER PRIMARY KEY AUTOINCREMENT,  key_id TEXT NOT NULL,  op TEXT NOT NULL,  bytes INTEGER DEFAULT 0,  duration_ms INTEGER DEFAULT 0,  timestamp TEXT NOT NULL,  cost REAL DEFAULT 0.0);"},
				{kind: "index", name: "idx_events_key", sql: "CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id);"},
				{kind: "index", name: "idx_events_ts", sql: "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);"},
			]
		},
	]
}
