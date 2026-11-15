/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREMATERIALWEATHERGATE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREMATERIALWEATHERGATE_H
/* cmd/BonfyreMaterialWeatherGate/src/main.c */
#define BF_STORE_MATERIAL_WEATHER_GATE_WEATHER_PENDING \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS weather_pending(acquire_id TEXT PRIMARY KEY,identity TEXT NOT NULL,owner_ref TEXT NOT NULL,handoff_json TEXT NOT NULL,source_ref TEXT,state TEXT NOT NULL CHECK(state IN ('pending','admitted','cancelled')),tier TEXT NOT NULL,refresh_delta_id TEXT NOT NULL DEFAULT '',updated_at INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS weather_pending_identity ON weather_pending(identity,state);"
/* cmd/BonfyreMaterialWeatherGate/src/main.c */
#define BF_STORE_MATERIAL_WEATHER_GATE_WEATHER_PENDING_SOURCE_STATE \
    "CREATE INDEX IF NOT EXISTS weather_pending_source_state ON weather_pending(source_ref,state)"
#endif
