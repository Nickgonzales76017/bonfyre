package stores

// cmd/BonfyreRecall: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreRecall/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreRecall": {
	fragments: [
		{
			macro:  "BF_STORE_RECALL_PASSAGE_WORDS"
			source: "cmd/BonfyreRecall/src/index.c"
			statements: [
				{kind: "virtual_table", name: "passage_words", sql: "CREATE VIRTUAL TABLE passage_words USING fts5(id UNINDEXED, text, tokenize='porter unicode61');"},
				{kind: "other", name: "", sql: "INSERT INTO passage_words(id, text) SELECT id, text FROM passage_text;"},
				{kind: "other", name: "", sql: "COMMIT;"},
			]
		},
		{
			macro:  "BF_STORE_RECALL_PASSAGE_TEXT"
			source: "cmd/BonfyreRecall/src/index.c"
			statements: [
				{kind: "other", name: "", sql: "BEGIN;"},
				{kind: "other", name: "", sql: "DROP TABLE IF EXISTS passage_words;"},
				{kind: "other", name: "", sql: "DROP TABLE IF EXISTS passage_text;"},
				{kind: "table", name: "passage_text", sql: "CREATE TABLE passage_text(id TEXT PRIMARY KEY, text TEXT NOT NULL);"},
			]
		},
	]
}
