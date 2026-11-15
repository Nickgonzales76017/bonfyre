package stores

// cmd/BonfyreEntity: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreEntity/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreEntity": {
	fragments: [
		{
			macro:  "BF_STORE_ENTITY_ENTITIES"
			source: "cmd/BonfyreEntity/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "entities", sql: "CREATE TABLE IF NOT EXISTS entities (  id          TEXT PRIMARY KEY,  display     TEXT NOT NULL,  kind        TEXT NOT NULL DEFAULT 'person',  confidence  REAL NOT NULL DEFAULT 1.0,  obs_count   INTEGER NOT NULL DEFAULT 0,  first_seen  INTEGER NOT NULL,  last_seen   INTEGER NOT NULL);"},
				{kind: "table", name: "observations", sql: "CREATE TABLE IF NOT EXISTS observations (  id        INTEGER PRIMARY KEY AUTOINCREMENT,  entity    TEXT NOT NULL REFERENCES entities(id),  modal     TEXT NOT NULL,  value     TEXT NOT NULL,  source    TEXT,  confidence REAL NOT NULL DEFAULT 1.0,  ts        INTEGER NOT NULL);"},
				{kind: "table", name: "links", sql: "CREATE TABLE IF NOT EXISTS links (  a           TEXT NOT NULL REFERENCES entities(id),  b           TEXT NOT NULL REFERENCES entities(id),  confidence  REAL NOT NULL DEFAULT 1.0,  reason      TEXT,  created     INTEGER NOT NULL,  PRIMARY KEY(a,b));"},
				{kind: "virtual_table", name: "entities_fts", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(  id, display, kind, content='entities', content_rowid='rowid');"},
				{kind: "index", name: "idx_obs_entity", sql: "CREATE INDEX IF NOT EXISTS idx_obs_entity ON observations(entity);"},
				{kind: "index", name: "idx_obs_modal", sql: "CREATE INDEX IF NOT EXISTS idx_obs_modal  ON observations(modal,value);"},
			]
		},
	]
}
