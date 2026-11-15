package stores

// cmd/BonfyreInteractionGrammar: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreInteractionGrammar/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreInteractionGrammar": {
	fragments: [
		{
			macro:  "BF_STORE_INTERACTION_GRAMMAR_INTERACTION_GRAMMAR_CATALOG"
			source: "cmd/BonfyreInteractionGrammar/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "interaction_grammar_catalog", sql: "CREATE TABLE IF NOT EXISTS interaction_grammar_catalog(grammar_id TEXT PRIMARY KEY,donor_reference TEXT NOT NULL,family TEXT NOT NULL,application TEXT NOT NULL,operation TEXT NOT NULL,extent TEXT NOT NULL,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,owner_family TEXT NOT NULL);"},
				{kind: "table", name: "interaction_grammar_binding", sql: "CREATE TABLE IF NOT EXISTS interaction_grammar_binding(binding_id TEXT PRIMARY KEY,grammar_id TEXT NOT NULL REFERENCES interaction_grammar_catalog(grammar_id),subject_ref TEXT NOT NULL,institution_ref TEXT NOT NULL DEFAULT '',owner_ref TEXT NOT NULL,visibility TEXT NOT NULL CHECK(visibility IN('public','member','private')),state TEXT NOT NULL CHECK(state IN('active','retired')),updated_at INTEGER NOT NULL,source_kind TEXT NOT NULL DEFAULT 'manual',source_ref TEXT NOT NULL DEFAULT '');"},
				{kind: "index", name: "interaction_grammar_active", sql: "CREATE INDEX IF NOT EXISTS interaction_grammar_active ON interaction_grammar_binding(grammar_id,state,subject_ref);"},
			]
		},
	]
}
