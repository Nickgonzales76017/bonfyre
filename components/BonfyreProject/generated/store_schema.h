/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREPROJECT_H
#define BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREPROJECT_H
/* components/BonfyreProject/src/main.c */
#define BF_STORE_C_PROJECT_META \
    "PRAGMA page_size=8192; " \
    "PRAGMA journal_mode=OFF; " \
    "PRAGMA synchronous=OFF;" \
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value BLOB);" \
    "CREATE TABLE receipt(name TEXT PRIMARY KEY, bytes INTEGER NOT NULL, z BLOB NOT NULL) WITHOUT ROWID;" \
    "BEGIN"
/* components/BonfyreProject/src/main.c */
#define BF_STORE_C_PROJECT_BODY \
    "PRAGMA page_size=65536; " \
    "PRAGMA journal_mode=OFF; " \
    "PRAGMA synchronous=OFF;" \
    "CREATE TABLE body(name TEXT PRIMARY KEY, sha256 TEXT NOT NULL) WITHOUT ROWID;" \
    "CREATE TABLE blob(sha256 TEXT PRIMARY KEY, bytes INTEGER NOT NULL, z BLOB NOT NULL);" \
    "BEGIN"
/* components/BonfyreProject/src/main.c */
#define BF_STORE_C_PROJECT_FOREIGN_RELATIONS \
    "CREATE TABLE IF NOT EXISTS foreign_relations ( id INTEGER PRIMARY KEY, subject_from TEXT NOT NULL, predicate TEXT NOT NULL, subject_to TEXT NOT NULL, source_federation TEXT NOT NULL, carrier TEXT NOT NULL, observed_at TEXT NOT NULL, evidence TEXT NOT NULL, scope TEXT NOT NULL DEFAULT 'public', claim_state TEXT NOT NULL DEFAULT 'observed', admitted_as TEXT, UNIQUE(subject_from, predicate, subject_to, source_federation));" \
    "CREATE INDEX IF NOT EXISTS foreign_relations_scope  ON foreign_relations(scope);"
#endif
