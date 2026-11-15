/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREFRAGMENTMIGRATION_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREFRAGMENTMIGRATION_H
/* cmd/BonfyreFragmentMigration/src/compile.c */
#define BF_STORE_FRAGMENT_MIGRATION_EQUIVALENCE \
    "CREATE TABLE equivalence(hash TEXT PRIMARY KEY,n INTEGER NOT NULL)"
/* cmd/BonfyreFragmentMigration/src/admit.c */
#define BF_STORE_FRAGMENT_MIGRATION_SEEN \
    "CREATE TABLE seen(coordinate TEXT PRIMARY KEY)"
#endif
