/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREOUTREACH_H
#define BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREOUTREACH_H
/* components/BonfyreOutreach/src/main.c */
#define BF_STORE_C_OUTREACH_SENDS \
    "CREATE TABLE IF NOT EXISTS sends (  id INTEGER PRIMARY KEY AUTOINCREMENT,  channel TEXT NOT NULL,  target TEXT NOT NULL,  offer_name TEXT,  template TEXT,  message_preview TEXT,  sent_at TEXT NOT NULL,  reply_type TEXT,  reply_notes TEXT,  replied_at TEXT);"
#endif
