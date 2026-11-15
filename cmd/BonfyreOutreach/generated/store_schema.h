/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREOUTREACH_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREOUTREACH_H
/* cmd/BonfyreOutreach/src/main.c */
#define BF_STORE_OUTREACH_SENDS \
    "CREATE TABLE IF NOT EXISTS sends (  id INTEGER PRIMARY KEY AUTOINCREMENT,  channel TEXT NOT NULL,  target TEXT NOT NULL,  offer_name TEXT,  template TEXT,  message_preview TEXT,  sent_at TEXT NOT NULL,  reply_type TEXT,  reply_notes TEXT,  replied_at TEXT,  source_message_id TEXT,  source_sent_at TEXT,  reply_message_id TEXT);"
/* cmd/BonfyreOutreach/src/main.c */
#define BF_STORE_OUTREACH_OUTREACH_RESPONSES \
    "CREATE TABLE IF NOT EXISTS outreach_responses (id INTEGER PRIMARY KEY AUTOINCREMENT,send_id INTEGER NOT NULL REFERENCES sends(id),source_message_id TEXT NOT NULL UNIQUE,received_at TEXT NOT NULL,classification TEXT NOT NULL,notes TEXT);" \
    "CREATE INDEX IF NOT EXISTS ix_outreach_responses_send ON outreach_responses(send_id,received_at);" \
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_source ON sends(source_message_id) WHERE source_message_id IS NOT NULL;" \
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_reply ON sends(reply_message_id) WHERE reply_message_id IS NOT NULL;"
#endif
