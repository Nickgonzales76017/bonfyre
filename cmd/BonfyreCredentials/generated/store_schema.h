/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRECREDENTIALS_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRECREDENTIALS_H
/* cmd/BonfyreCredentials/src/main.c */
#define BF_STORE_CREDENTIALS_IMPORTED_PROVIDER \
    "PRAGMA journal_mode=DELETE;" \
    "CREATE TABLE IF NOT EXISTS imported_provider(provider TEXT PRIMARY KEY, declaration TEXT NOT NULL, spec_source TEXT NOT NULL, imported_at TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS sealed_field(provider TEXT NOT NULL, field TEXT NOT NULL, sealed BLOB NOT NULL, last4 TEXT NOT NULL, generated INTEGER NOT NULL, set_by TEXT NOT NULL, set_at TEXT NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account, field));" \
    "CREATE TABLE IF NOT EXISTS verification(provider TEXT NOT NULL, at TEXT NOT NULL, http_status INTEGER NOT NULL, outcome TEXT NOT NULL, account TEXT NOT NULL DEFAULT '');" \
    "CREATE TABLE IF NOT EXISTS token_cache(provider TEXT NOT NULL, sealed BLOB NOT NULL, expires_at INTEGER NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account));" \
    "CREATE TABLE IF NOT EXISTS audit(at TEXT NOT NULL, actor TEXT NOT NULL, provider TEXT NOT NULL, field TEXT NOT NULL, action TEXT NOT NULL, outcome TEXT NOT NULL, account TEXT NOT NULL DEFAULT '');"
/* cmd/BonfyreCredentials/src/main.c */
#define BF_STORE_CREDENTIALS_SEALED_FIELD_ACCOUNTS \
    "BEGIN IMMEDIATE;" \
    "CREATE TABLE sealed_field_accounts(provider TEXT NOT NULL, field TEXT NOT NULL, sealed BLOB NOT NULL, last4 TEXT NOT NULL, generated INTEGER NOT NULL, set_by TEXT NOT NULL, set_at TEXT NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account, field));" \
    "INSERT INTO sealed_field_accounts SELECT provider, field, sealed, last4, generated, set_by, set_at, '' FROM sealed_field;" \
    "DROP TABLE sealed_field; " \
    "ALTER TABLE sealed_field_accounts RENAME TO sealed_field;" \
    "CREATE TABLE token_cache_accounts(provider TEXT NOT NULL, sealed BLOB NOT NULL, expires_at INTEGER NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account));" \
    "INSERT INTO token_cache_accounts SELECT provider, sealed, expires_at, '' FROM token_cache;" \
    "DROP TABLE token_cache; " \
    "ALTER TABLE token_cache_accounts RENAME TO token_cache;" \
    "ALTER TABLE verification ADD COLUMN account TEXT NOT NULL DEFAULT '';" \
    "ALTER TABLE audit ADD COLUMN account TEXT NOT NULL DEFAULT '';" \
    "COMMIT;"
#endif
