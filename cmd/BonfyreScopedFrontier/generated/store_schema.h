/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESCOPEDFRONTIER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESCOPEDFRONTIER_H
/* cmd/BonfyreScopedFrontier/src/main.c */
#define BF_STORE_SCOPED_FRONTIER_CLOSURE_FRONTIER \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS closure_frontier(subject TEXT NOT NULL,scope TEXT NOT NULL,tracked_at INTEGER NOT NULL,PRIMARY KEY(subject,scope));" \
    "CREATE TABLE IF NOT EXISTS closure_dependency(subject TEXT NOT NULL,scope TEXT NOT NULL,dependency_ref TEXT NOT NULL,basis TEXT NOT NULL,PRIMARY KEY(subject,scope,dependency_ref,basis));" \
    "CREATE INDEX IF NOT EXISTS closure_dependency_lookup ON closure_dependency(dependency_ref,subject,scope);"
#endif
