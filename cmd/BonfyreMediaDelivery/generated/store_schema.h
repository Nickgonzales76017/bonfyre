/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREMEDIADELIVERY_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREMEDIADELIVERY_H
/* cmd/BonfyreMediaDelivery/src/main.c */
#define BF_STORE_MEDIA_DELIVERY_MEDIA_DELIVERY_RIGHT \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS media_delivery_right(right_id TEXT PRIMARY KEY,library_id TEXT NOT NULL,subject_ref TEXT NOT NULL,representation_id TEXT NOT NULL,actor_subject TEXT NOT NULL,protocol TEXT NOT NULL CHECK(protocol IN('hls','dash','moqt')),starts_at INTEGER NOT NULL,ends_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('active','revoked')),evidence_ref TEXT NOT NULL,granted_by TEXT NOT NULL,created_at INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS media_delivery_right_lookup ON media_delivery_right(library_id,subject_ref,representation_id,actor_subject,protocol,state,starts_at,ends_at);" \
    "CREATE TABLE IF NOT EXISTS media_transport_lease(lease_id TEXT PRIMARY KEY,right_id TEXT NOT NULL,library_id TEXT NOT NULL,subject_ref TEXT NOT NULL,representation_id TEXT NOT NULL,actor_subject TEXT NOT NULL,protocol TEXT NOT NULL,issued_at INTEGER NOT NULL,expires_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('active','revoked','expired')),created_at INTEGER NOT NULL);"
#endif
