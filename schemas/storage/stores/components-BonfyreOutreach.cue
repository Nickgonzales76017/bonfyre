package stores

// components/BonfyreOutreach: store DDL, byte-for-byte what the owner executes.
// Compiled to components/BonfyreOutreach/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "components/BonfyreOutreach": {
	fragments: [
		{
			macro:  "BF_STORE_C_OUTREACH_SENDS"
			source: "components/BonfyreOutreach/src/main.c"
			statements: [
				{kind: "table", name: "sends", sql: "CREATE TABLE IF NOT EXISTS sends (  id INTEGER PRIMARY KEY AUTOINCREMENT,  channel TEXT NOT NULL,  target TEXT NOT NULL,  offer_name TEXT,  template TEXT,  message_preview TEXT,  sent_at TEXT NOT NULL,  reply_type TEXT,  reply_notes TEXT,  replied_at TEXT);"},
			]
		},
	]
}
