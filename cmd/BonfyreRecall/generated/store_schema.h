/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRERECALL_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRERECALL_H
/* cmd/BonfyreRecall/src/index.c */
#define BF_STORE_RECALL_PASSAGE_WORDS \
    "CREATE VIRTUAL TABLE passage_words USING fts5(id UNINDEXED, text, tokenize='porter unicode61');" \
    "INSERT INTO passage_words(id, text) SELECT id, text FROM passage_text;" \
    "COMMIT;"
/* cmd/BonfyreRecall/src/index.c */
#define BF_STORE_RECALL_PASSAGE_TEXT \
    "BEGIN;" \
    "DROP TABLE IF EXISTS passage_words;" \
    "DROP TABLE IF EXISTS passage_text;" \
    "CREATE TABLE passage_text(id TEXT PRIMARY KEY, text TEXT NOT NULL);"
#endif
