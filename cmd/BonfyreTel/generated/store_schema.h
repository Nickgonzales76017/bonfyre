/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRETEL_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRETEL_H
/* cmd/BonfyreTel/src/main.c */
#define BF_STORE_TEL_CALLS \
    "CREATE TABLE IF NOT EXISTS calls (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  uuid       TEXT NOT NULL,  direction  TEXT NOT NULL,  caller     TEXT,  callee     TEXT,  started_at TEXT NOT NULL,  ended_at   TEXT,  duration_s REAL,  recording  TEXT,  pipeline   TEXT,  status     TEXT DEFAULT 'active');" \
    "CREATE TABLE IF NOT EXISTS messages (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  direction   TEXT NOT NULL,  from_number TEXT,  to_number   TEXT,  body        TEXT,  media_url   TEXT,  sent_at     TEXT NOT NULL,  status      TEXT DEFAULT 'sent');" \
    "CREATE TABLE IF NOT EXISTS verify_codes (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  phone      TEXT NOT NULL,  code       TEXT NOT NULL,  created_at TEXT NOT NULL,  expires_at TEXT NOT NULL,  verified   INTEGER DEFAULT 0,  attempts   INTEGER DEFAULT 0);"
#endif
