/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREDISCIPL_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREDISCIPL_H
/* cmd/BonfyreDiscipl/src/main.c */
#define BF_STORE_DISCIPL_DISCIPL_INVOCATION \
    "CREATE TABLE IF NOT EXISTS discipl_invocation( family TEXT PRIMARY KEY, binary TEXT, subcommand TEXT, wants_outdir INTEGER, verified_at TEXT, fixture TEXT)"
#endif
