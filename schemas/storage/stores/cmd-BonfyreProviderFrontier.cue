package stores

// cmd/BonfyreProviderFrontier: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreProviderFrontier/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreProviderFrontier": {
	fragments: [
		{
			macro:  "BF_STORE_PROVIDER_FRONTIER_EXACT_OWNER"
			source: "cmd/BonfyreProviderFrontier/src/main.c"
			statements: [
				{kind: "table", name: "exact_owner", sql: "CREATE TABLE exact_owner(rank INTEGER PRIMARY KEY,identity TEXT UNIQUE NOT NULL,source_digest TEXT NOT NULL,owner_ref TEXT NOT NULL,lowerer_ref TEXT NOT NULL,receipt_id TEXT,outcome TEXT NOT NULL CHECK(outcome IN ('observed','unobserved')),evidence_digest TEXT NOT NULL);"},
				{kind: "table", name: "receipt", sql: "CREATE TABLE receipt(receipt_id TEXT PRIMARY KEY,identity TEXT NOT NULL,observed_at TEXT NOT NULL,content_sha256 TEXT NOT NULL,body_path TEXT NOT NULL);"},
			]
		},
	]
}
