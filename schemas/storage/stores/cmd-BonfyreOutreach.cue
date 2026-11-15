package stores

// cmd/BonfyreOutreach: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreOutreach/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreOutreach": {
	fragments: [
		{
			macro:  "BF_STORE_OUTREACH_SENDS"
			source: "cmd/BonfyreOutreach/src/main.c"
			statements: [
				{kind: "table", name: "sends", sql: "CREATE TABLE IF NOT EXISTS sends (  id INTEGER PRIMARY KEY AUTOINCREMENT,  channel TEXT NOT NULL,  target TEXT NOT NULL,  offer_name TEXT,  template TEXT,  message_preview TEXT,  sent_at TEXT NOT NULL,  reply_type TEXT,  reply_notes TEXT,  replied_at TEXT,  source_message_id TEXT,  source_sent_at TEXT,  reply_message_id TEXT);"},
			]
		},
		{
			macro:  "BF_STORE_OUTREACH_OUTREACH_RESPONSES"
			source: "cmd/BonfyreOutreach/src/main.c"
			statements: [
				{kind: "table", name: "outreach_responses", sql: "CREATE TABLE IF NOT EXISTS outreach_responses (id INTEGER PRIMARY KEY AUTOINCREMENT,send_id INTEGER NOT NULL REFERENCES sends(id),source_message_id TEXT NOT NULL UNIQUE,received_at TEXT NOT NULL,classification TEXT NOT NULL,notes TEXT);"},
				{kind: "index", name: "ix_outreach_responses_send", sql: "CREATE INDEX IF NOT EXISTS ix_outreach_responses_send ON outreach_responses(send_id,received_at);"},
				{kind: "index", name: "ux_outreach_source", sql: "CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_source ON sends(source_message_id) WHERE source_message_id IS NOT NULL;"},
				{kind: "index", name: "ux_outreach_reply", sql: "CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_reply ON sends(reply_message_id) WHERE reply_message_id IS NOT NULL;"},
			]
		},
	]
}
