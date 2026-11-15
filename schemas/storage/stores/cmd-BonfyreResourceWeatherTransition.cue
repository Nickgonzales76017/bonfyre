package stores

// cmd/BonfyreResourceWeatherTransition: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreResourceWeatherTransition/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreResourceWeatherTransition": {
	fragments: [
		{
			macro:  "BF_STORE_RESOURCE_WEATHER_TRANSITION_RESOURCE_WEATHER_SNAPSHOT"
			source: "cmd/BonfyreResourceWeatherTransition/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "resource_weather_snapshot", sql: "CREATE TABLE IF NOT EXISTS resource_weather_snapshot(singleton INTEGER PRIMARY KEY CHECK(singleton=1),digest TEXT NOT NULL,body TEXT NOT NULL,updated_at INTEGER NOT NULL);"},
			]
		},
	]
}
