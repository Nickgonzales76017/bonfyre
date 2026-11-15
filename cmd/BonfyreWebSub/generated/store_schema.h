/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREWEBSUB_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREWEBSUB_H
/* cmd/BonfyreWebSub/src/main.c */
#define BF_STORE_WEB_SUB_SUBSCRIPTIONS \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS subscriptions(token TEXT PRIMARY KEY,topic TEXT NOT NULL,hub TEXT NOT NULL,secret TEXT NOT NULL,authority_receipt TEXT NOT NULL DEFAULT '',state TEXT NOT NULL DEFAULT 'pending',lease_expires INTEGER DEFAULT 0,created_at TEXT,verified_at TEXT,last_delivery_at TEXT,last_fetch_at INTEGER DEFAULT 0,deliveries INTEGER DEFAULT 0,rejected INTEGER DEFAULT 0);" \
    "CREATE TABLE IF NOT EXISTS source_dirty(token TEXT PRIMARY KEY,topic TEXT NOT NULL,frontier TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',dirty_at INTEGER NOT NULL,claimed_at INTEGER DEFAULT 0,attempts INTEGER DEFAULT 0,last_error TEXT DEFAULT '',demand_count INTEGER NOT NULL DEFAULT 0,last_demand_identity TEXT NOT NULL DEFAULT '');" \
    "CREATE TABLE IF NOT EXISTS source_dirty_demand(topic TEXT NOT NULL,identity TEXT NOT NULL,first_requested_at INTEGER NOT NULL,last_requested_at INTEGER NOT NULL,PRIMARY KEY(topic,identity));" \
    "CREATE TABLE IF NOT EXISTS foreign_delta(token TEXT PRIMARY KEY,topic TEXT NOT NULL,observed_at INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '');"
#endif
