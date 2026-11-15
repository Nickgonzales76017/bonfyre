/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREMATERIALLEASE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREMATERIALLEASE_H
/* cmd/BonfyreMaterialLease/src/main.c */
#define BF_STORE_MATERIAL_LEASE_MATERIAL_LEASE \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS material_lease(subject TEXT NOT NULL,scope TEXT NOT NULL,identity TEXT NOT NULL,owner_ref TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(subject,scope,identity));" \
    "CREATE INDEX IF NOT EXISTS material_lease_identity ON material_lease(identity);" \
    "CREATE TABLE IF NOT EXISTS material_transition(id INTEGER PRIMARY KEY AUTOINCREMENT,action TEXT NOT NULL CHECK(action IN ('acquire','release')),identity TEXT NOT NULL,owner_ref TEXT NOT NULL,refcount_before INTEGER NOT NULL,refcount_after INTEGER NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,created_at INTEGER NOT NULL,emitted INTEGER NOT NULL DEFAULT 0 CHECK(emitted IN (0,1)));" \
    "CREATE INDEX IF NOT EXISTS material_transition_pending ON material_transition(emitted,id);"
#endif
