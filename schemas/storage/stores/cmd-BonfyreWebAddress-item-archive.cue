package stores

// The item archive's shape, executed by its writer (page_archive_open in
// cmd/BonfyreWebAddress/src/page_routes_and_emitters.h) byte for byte.
//
// item_text is a full-text index over what each kept item says and who made
// it. It holds no copy of the text (external content over item) and the
// three triggers keep it in step with every insert, re-file and removal, so
// the index is maintained where the archive is written rather than rebuilt
// by a reader. Search reads it to reach every source the estate has ever
// read, not only the few whose feeds are current: an item found here is what
// was read at a time, never a claim that its source still publishes it.
store_schemas: "cmd/BonfyreWebAddress": fragments: [
	{
		macro:  "BF_STORE_WEB_ITEM_ARCHIVE"
		source: "cmd/BonfyreWebAddress/src/page_routes_and_emitters.h"
		statements: [
			{kind: "other", name: "", sql: "PRAGMA journal_mode=DELETE;"},
			{kind: "table", name: "item", sql: "CREATE TABLE IF NOT EXISTS item(source TEXT NOT NULL, key TEXT NOT NULL, kind TEXT NOT NULL, family TEXT DEFAULT '', title TEXT NOT NULL, body TEXT DEFAULT '', href TEXT DEFAULT '', image TEXT DEFAULT '', by TEXT DEFAULT '', publisher TEXT DEFAULT '', ident TEXT DEFAULT '', at INTEGER DEFAULT 0, first_read TEXT NOT NULL, last_read TEXT NOT NULL, PRIMARY KEY(source, key));"},
			{kind: "index", name: "item_by_source", sql: "CREATE INDEX IF NOT EXISTS item_by_source ON item(source, at DESC);"},
			{kind: "index", name: "idx_item_credit_by_source_at", sql: "CREATE INDEX IF NOT EXISTS idx_item_credit_by_source_at ON item(by, source, at DESC);"},
			{kind: "index", name: "item_credit_scan", sql: "CREATE INDEX IF NOT EXISTS item_credit_scan ON item(family,by,source,at);"},
			{kind: "index", name: "item_publisher_read", sql: "CREATE INDEX IF NOT EXISTS item_publisher_read ON item(publisher);"},
			{kind: "index", name: "item_by_ident", sql: "CREATE INDEX IF NOT EXISTS item_by_ident ON item(ident);"},
			{kind: "virtual_table", name: "item_text", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS item_text USING fts5(title, body, by, publisher, content='item', content_rowid='rowid', tokenize='porter unicode61');"},
			{kind: "trigger", name: "item_text_insert", sql: "CREATE TRIGGER IF NOT EXISTS item_text_insert AFTER INSERT ON item BEGIN INSERT INTO item_text(rowid,title,body,by,publisher) VALUES(new.rowid,new.title,new.body,new.by,new.publisher); END;"},
			{kind: "trigger", name: "item_text_delete", sql: "CREATE TRIGGER IF NOT EXISTS item_text_delete AFTER DELETE ON item BEGIN INSERT INTO item_text(item_text,rowid,title,body,by,publisher) VALUES('delete',old.rowid,old.title,old.body,old.by,old.publisher); END;"},
			{kind: "trigger", name: "item_text_update", sql: "CREATE TRIGGER IF NOT EXISTS item_text_update AFTER UPDATE ON item BEGIN INSERT INTO item_text(item_text,rowid,title,body,by,publisher) VALUES('delete',old.rowid,old.title,old.body,old.by,old.publisher); INSERT INTO item_text(rowid,title,body,by,publisher) VALUES(new.rowid,new.title,new.body,new.by,new.publisher); END;"},
		]
	},
]
