package stores

// cmd/BonfyreWatch: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreWatch/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreWatch": {
	fragments: [
		{
			macro:  "BF_STORE_WATCH_WATCH_SESSIONS"
			source: "cmd/BonfyreWatch/src/main.c"
			statements: [
				{kind: "table", name: "watch_sessions", sql: "CREATE TABLE IF NOT EXISTS watch_sessions( session_id TEXT PRIMARY KEY, watch_dir TEXT, pipeline TEXT, out_dir TEXT, interval_sec INTEGER, dry_run INTEGER, recursive INTEGER, created_at TEXT);"},
				{kind: "table", name: "watch_events", sql: "CREATE TABLE IF NOT EXISTS watch_events( session_id TEXT, event_id TEXT PRIMARY KEY, path TEXT, size INTEGER, mtime INTEGER, status TEXT, output_dir TEXT, pipeline TEXT, run_ref TEXT, created_at TEXT);"},
			]
		},
	]
}
