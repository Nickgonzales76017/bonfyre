/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREINTERACTIONGRAMMAR_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREINTERACTIONGRAMMAR_H
/* cmd/BonfyreInteractionGrammar/src/main.c */
#define BF_STORE_INTERACTION_GRAMMAR_INTERACTION_GRAMMAR_CATALOG \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS interaction_grammar_catalog(grammar_id TEXT PRIMARY KEY,donor_reference TEXT NOT NULL,family TEXT NOT NULL,application TEXT NOT NULL,operation TEXT NOT NULL,extent TEXT NOT NULL,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,owner_family TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS interaction_grammar_binding(binding_id TEXT PRIMARY KEY,grammar_id TEXT NOT NULL REFERENCES interaction_grammar_catalog(grammar_id),subject_ref TEXT NOT NULL,institution_ref TEXT NOT NULL DEFAULT '',owner_ref TEXT NOT NULL,visibility TEXT NOT NULL CHECK(visibility IN('public','member','private')),state TEXT NOT NULL CHECK(state IN('active','retired')),updated_at INTEGER NOT NULL,source_kind TEXT NOT NULL DEFAULT 'manual',source_ref TEXT NOT NULL DEFAULT '');" \
    "CREATE INDEX IF NOT EXISTS interaction_grammar_active ON interaction_grammar_binding(grammar_id,state,subject_ref);"
#endif
