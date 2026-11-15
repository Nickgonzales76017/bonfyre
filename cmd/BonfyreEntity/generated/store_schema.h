/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREENTITY_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREENTITY_H
/* cmd/BonfyreEntity/src/main.c */
#define BF_STORE_ENTITY_ENTITIES \
    "PRAGMA journal_mode=WAL;" \
    "PRAGMA foreign_keys=ON;" \
    "CREATE TABLE IF NOT EXISTS entities (  id          TEXT PRIMARY KEY,  display     TEXT NOT NULL,  kind        TEXT NOT NULL DEFAULT 'person',  confidence  REAL NOT NULL DEFAULT 1.0,  obs_count   INTEGER NOT NULL DEFAULT 0,  first_seen  INTEGER NOT NULL,  last_seen   INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS observations (  id        INTEGER PRIMARY KEY AUTOINCREMENT,  entity    TEXT NOT NULL REFERENCES entities(id),  modal     TEXT NOT NULL,  value     TEXT NOT NULL,  source    TEXT,  confidence REAL NOT NULL DEFAULT 1.0,  ts        INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS links (  a           TEXT NOT NULL REFERENCES entities(id),  b           TEXT NOT NULL REFERENCES entities(id),  confidence  REAL NOT NULL DEFAULT 1.0,  reason      TEXT,  created     INTEGER NOT NULL,  PRIMARY KEY(a,b));" \
    "CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(  id, display, kind, content='entities', content_rowid='rowid');" \
    "CREATE INDEX IF NOT EXISTS idx_obs_entity ON observations(entity);" \
    "CREATE INDEX IF NOT EXISTS idx_obs_modal  ON observations(modal,value);"
#endif
