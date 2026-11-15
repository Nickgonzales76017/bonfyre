package stores

// cmd/BonfyreTel: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreTel/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreTel": {
	fragments: [
		{
			macro:  "BF_STORE_TEL_CALLS"
			source: "cmd/BonfyreTel/src/main.c"
			statements: [
				{kind: "table", name: "calls", sql: "CREATE TABLE IF NOT EXISTS calls (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  uuid       TEXT NOT NULL,  direction  TEXT NOT NULL,  caller     TEXT,  callee     TEXT,  started_at TEXT NOT NULL,  ended_at   TEXT,  duration_s REAL,  recording  TEXT,  pipeline   TEXT,  status     TEXT DEFAULT 'active');"},
				{kind: "table", name: "messages", sql: "CREATE TABLE IF NOT EXISTS messages (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  direction   TEXT NOT NULL,  from_number TEXT,  to_number   TEXT,  body        TEXT,  media_url   TEXT,  sent_at     TEXT NOT NULL,  status      TEXT DEFAULT 'sent');"},
				{kind: "table", name: "verify_codes", sql: "CREATE TABLE IF NOT EXISTS verify_codes (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  phone      TEXT NOT NULL,  code       TEXT NOT NULL,  created_at TEXT NOT NULL,  expires_at TEXT NOT NULL,  verified   INTEGER DEFAULT 0,  attempts   INTEGER DEFAULT 0);"},
			]
		},
	]
}
