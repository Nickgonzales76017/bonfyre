/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREMATERIALWEATHERREHEAT_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREMATERIALWEATHERREHEAT_H
/* cmd/BonfyreMaterialWeatherReheat/src/main.c */
#define BF_STORE_MATERIAL_WEATHER_REHEAT_MATERIAL_WEATHER_REHEAT_RECEIPT \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS material_weather_reheat_receipt(delta_id TEXT NOT NULL,source_ref TEXT NOT NULL,processed_at INTEGER NOT NULL,PRIMARY KEY(delta_id,source_ref));"
#endif
