/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREADMISSION_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREADMISSION_H
/* store admission: sqlite, declared by cmd/BonfyreAdmission; written by bonfyre-admission */
#define BF_STORE_ADMISSION_KIND "sqlite"
#define BF_STORE_ADMISSION_ENV "BONFYRE_ADMISSION_DB"
#define BF_STORE_ADMISSION_LEASE { "admission", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_admission = { "admission", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
#endif
