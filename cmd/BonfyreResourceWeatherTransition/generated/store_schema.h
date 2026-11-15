/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRERESOURCEWEATHERTRANSITION_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRERESOURCEWEATHERTRANSITION_H
/* cmd/BonfyreResourceWeatherTransition/src/main.c */
#define BF_STORE_RESOURCE_WEATHER_TRANSITION_RESOURCE_WEATHER_SNAPSHOT \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS resource_weather_snapshot(singleton INTEGER PRIMARY KEY CHECK(singleton=1),digest TEXT NOT NULL,body TEXT NOT NULL,updated_at INTEGER NOT NULL);"
#endif
