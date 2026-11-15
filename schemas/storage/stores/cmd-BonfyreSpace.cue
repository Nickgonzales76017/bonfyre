package stores

// cmd/BonfyreSpace: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreSpace/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreSpace": {
	fragments: [
		{
			macro:  "BF_STORE_SPACE_SPACES"
			source: "cmd/BonfyreSpace/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "spaces", sql: "CREATE TABLE IF NOT EXISTS spaces(  name      TEXT PRIMARY KEY,  created   INTEGER NOT NULL,  accessed  INTEGER NOT NULL,  mode      TEXT NOT NULL DEFAULT 'rw');"},
				{kind: "table", name: "entries", sql: "CREATE TABLE IF NOT EXISTS entries(  space     TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,  key       TEXT NOT NULL,  value     BLOB NOT NULL,  written   INTEGER NOT NULL,  ttl_s     INTEGER,  PRIMARY KEY(space,key));"},
				{kind: "table", name: "attachments", sql: "CREATE TABLE IF NOT EXISTS attachments(  space     TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,  stage     TEXT NOT NULL,  attached  INTEGER NOT NULL,  detached  INTEGER,  PRIMARY KEY(space,stage));"},
				{kind: "index", name: "idx_entries_space", sql: "CREATE INDEX IF NOT EXISTS idx_entries_space ON entries(space);"},
			]
		},
	]
}
