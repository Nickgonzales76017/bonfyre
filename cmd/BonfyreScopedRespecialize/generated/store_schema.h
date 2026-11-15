/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESCOPEDRESPECIALIZE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESCOPEDRESPECIALIZE_H
/* cmd/BonfyreScopedRespecialize/src/main.c */
#define BF_STORE_SCOPED_RESPECIALIZE_SCOPED_RESPECIALIZE_RECEIPT \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS scoped_respecialize_receipt(delta_id TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,closure_path TEXT NOT NULL,new_external_candidates INTEGER NOT NULL,processed_at INTEGER NOT NULL,PRIMARY KEY(delta_id,subject,scope));"
#endif
