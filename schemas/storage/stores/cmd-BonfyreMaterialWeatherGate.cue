package stores

// cmd/BonfyreMaterialWeatherGate: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreMaterialWeatherGate/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreMaterialWeatherGate": {
	fragments: [
		{
			macro:  "BF_STORE_MATERIAL_WEATHER_GATE_WEATHER_PENDING"
			source: "cmd/BonfyreMaterialWeatherGate/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "weather_pending", sql: "CREATE TABLE IF NOT EXISTS weather_pending(acquire_id TEXT PRIMARY KEY,identity TEXT NOT NULL,owner_ref TEXT NOT NULL,handoff_json TEXT NOT NULL,source_ref TEXT,state TEXT NOT NULL CHECK(state IN ('pending','admitted','cancelled')),tier TEXT NOT NULL,refresh_delta_id TEXT NOT NULL DEFAULT '',updated_at INTEGER NOT NULL);"},
				{kind: "index", name: "weather_pending_identity", sql: "CREATE INDEX IF NOT EXISTS weather_pending_identity ON weather_pending(identity,state);"},
			]
		},
		{
			macro:  "BF_STORE_MATERIAL_WEATHER_GATE_WEATHER_PENDING_SOURCE_STATE"
			source: "cmd/BonfyreMaterialWeatherGate/src/main.c"
			statements: [
				{kind: "index", name: "weather_pending_source_state", sql: "CREATE INDEX IF NOT EXISTS weather_pending_source_state ON weather_pending(source_ref,state)"},
			]
		},
	]
}
