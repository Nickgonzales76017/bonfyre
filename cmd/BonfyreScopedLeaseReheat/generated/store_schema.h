/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESCOPEDLEASEREHEAT_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESCOPEDLEASEREHEAT_H
/* cmd/BonfyreScopedLeaseReheat/src/main.c */
#define BF_STORE_SCOPED_LEASE_REHEAT_SCOPED_LEASE_REHEAT_RECEIPT \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS scoped_lease_reheat_receipt(delta_id TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,closure_path TEXT NOT NULL,processed_at INTEGER NOT NULL,PRIMARY KEY(delta_id,subject,scope));"
#endif
