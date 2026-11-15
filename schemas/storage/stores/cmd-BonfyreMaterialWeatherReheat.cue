package stores

// cmd/BonfyreMaterialWeatherReheat: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreMaterialWeatherReheat/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreMaterialWeatherReheat": {
	fragments: [
		{
			macro:  "BF_STORE_MATERIAL_WEATHER_REHEAT_MATERIAL_WEATHER_REHEAT_RECEIPT"
			source: "cmd/BonfyreMaterialWeatherReheat/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "material_weather_reheat_receipt", sql: "CREATE TABLE IF NOT EXISTS material_weather_reheat_receipt(delta_id TEXT NOT NULL,source_ref TEXT NOT NULL,processed_at INTEGER NOT NULL,PRIMARY KEY(delta_id,source_ref));"},
			]
		},
	]
}
