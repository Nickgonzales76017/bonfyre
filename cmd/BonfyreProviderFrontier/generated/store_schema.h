/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREPROVIDERFRONTIER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREPROVIDERFRONTIER_H
/* cmd/BonfyreProviderFrontier/src/main.c */
#define BF_STORE_PROVIDER_FRONTIER_EXACT_OWNER \
    "CREATE TABLE exact_owner(rank INTEGER PRIMARY KEY,identity TEXT UNIQUE NOT NULL,source_digest TEXT NOT NULL,owner_ref TEXT NOT NULL,lowerer_ref TEXT NOT NULL,receipt_id TEXT,outcome TEXT NOT NULL CHECK(outcome IN ('observed','unobserved')),evidence_digest TEXT NOT NULL);" \
    "CREATE TABLE receipt(receipt_id TEXT PRIMARY KEY,identity TEXT NOT NULL,observed_at TEXT NOT NULL,content_sha256 TEXT NOT NULL,body_path TEXT NOT NULL);"
#endif
