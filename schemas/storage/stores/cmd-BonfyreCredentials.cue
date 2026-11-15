package stores

// cmd/BonfyreCredentials: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreCredentials/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreCredentials": {
	fragments: [
		{
			macro:  "BF_STORE_CREDENTIALS_IMPORTED_PROVIDER"
			source: "cmd/BonfyreCredentials/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=DELETE;"},
				{kind: "table", name: "imported_provider", sql: "CREATE TABLE IF NOT EXISTS imported_provider(provider TEXT PRIMARY KEY, declaration TEXT NOT NULL, spec_source TEXT NOT NULL, imported_at TEXT NOT NULL);"},
				{kind: "table", name: "sealed_field", sql: "CREATE TABLE IF NOT EXISTS sealed_field(provider TEXT NOT NULL, field TEXT NOT NULL, sealed BLOB NOT NULL, last4 TEXT NOT NULL, generated INTEGER NOT NULL, set_by TEXT NOT NULL, set_at TEXT NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account, field));"},
				{kind: "table", name: "verification", sql: "CREATE TABLE IF NOT EXISTS verification(provider TEXT NOT NULL, at TEXT NOT NULL, http_status INTEGER NOT NULL, outcome TEXT NOT NULL, account TEXT NOT NULL DEFAULT '');"},
				{kind: "table", name: "token_cache", sql: "CREATE TABLE IF NOT EXISTS token_cache(provider TEXT NOT NULL, sealed BLOB NOT NULL, expires_at INTEGER NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account));"},
				{kind: "table", name: "audit", sql: "CREATE TABLE IF NOT EXISTS audit(at TEXT NOT NULL, actor TEXT NOT NULL, provider TEXT NOT NULL, field TEXT NOT NULL, action TEXT NOT NULL, outcome TEXT NOT NULL, account TEXT NOT NULL DEFAULT '');"},
			]
		},
		{
			macro:  "BF_STORE_CREDENTIALS_SEALED_FIELD_ACCOUNTS"
			source: "cmd/BonfyreCredentials/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "BEGIN IMMEDIATE;"},
				{kind: "table", name: "sealed_field_accounts", sql: "CREATE TABLE sealed_field_accounts(provider TEXT NOT NULL, field TEXT NOT NULL, sealed BLOB NOT NULL, last4 TEXT NOT NULL, generated INTEGER NOT NULL, set_by TEXT NOT NULL, set_at TEXT NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account, field));"},
				{kind: "other", name: "", sql: "INSERT INTO sealed_field_accounts SELECT provider, field, sealed, last4, generated, set_by, set_at, '' FROM sealed_field;"},
				{kind: "other", name: "", sql: "DROP TABLE sealed_field; "},
				{kind: "other", name: "", sql: "ALTER TABLE sealed_field_accounts RENAME TO sealed_field;"},
				{kind: "table", name: "token_cache_accounts", sql: "CREATE TABLE token_cache_accounts(provider TEXT NOT NULL, sealed BLOB NOT NULL, expires_at INTEGER NOT NULL, account TEXT NOT NULL DEFAULT '', PRIMARY KEY(provider, account));"},
				{kind: "other", name: "", sql: "INSERT INTO token_cache_accounts SELECT provider, sealed, expires_at, '' FROM token_cache;"},
				{kind: "other", name: "", sql: "DROP TABLE token_cache; "},
				{kind: "other", name: "", sql: "ALTER TABLE token_cache_accounts RENAME TO token_cache;"},
				{kind: "other", name: "", sql: "ALTER TABLE verification ADD COLUMN account TEXT NOT NULL DEFAULT '';"},
				{kind: "other", name: "", sql: "ALTER TABLE audit ADD COLUMN account TEXT NOT NULL DEFAULT '';"},
				{kind: "other", name: "", sql: "COMMIT;"},
			]
		},
	]
}
