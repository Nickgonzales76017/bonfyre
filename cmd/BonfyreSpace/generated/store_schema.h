/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESPACE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESPACE_H
/* cmd/BonfyreSpace/src/main.c */
#define BF_STORE_SPACE_SPACES \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS spaces(  name      TEXT PRIMARY KEY,  created   INTEGER NOT NULL,  accessed  INTEGER NOT NULL,  mode      TEXT NOT NULL DEFAULT 'rw');" \
    "CREATE TABLE IF NOT EXISTS entries(  space     TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,  key       TEXT NOT NULL,  value     BLOB NOT NULL,  written   INTEGER NOT NULL,  ttl_s     INTEGER,  PRIMARY KEY(space,key));" \
    "CREATE TABLE IF NOT EXISTS attachments(  space     TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,  stage     TEXT NOT NULL,  attached  INTEGER NOT NULL,  detached  INTEGER,  PRIMARY KEY(space,stage));" \
    "CREATE INDEX IF NOT EXISTS idx_entries_space ON entries(space);"
#endif
