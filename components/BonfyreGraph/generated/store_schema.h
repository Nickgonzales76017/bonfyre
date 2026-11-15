/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREGRAPH_H
#define BONFYRE_STORE_SCHEMA_COMPONENTS_BONFYREGRAPH_H
/* components/BonfyreGraph/src/main.c */
#define BF_STORE_C_GRAPH_ATOMS \
    "CREATE TABLE IF NOT EXISTS atoms (  atom_id TEXT PRIMARY KEY,  content_hash TEXT NOT NULL,  media_type TEXT NOT NULL,  path TEXT,  byte_size INTEGER,  label TEXT);" \
    "CREATE TABLE IF NOT EXISTS operators (  operator_id TEXT PRIMARY KEY,  op TEXT NOT NULL,  inputs TEXT NOT NULL,  output TEXT NOT NULL,  params TEXT DEFAULT '{}',  node_hash TEXT,  version TEXT DEFAULT '1.0.0',  deterministic INTEGER DEFAULT 1);" \
    "CREATE TABLE IF NOT EXISTS realizations (  realization_id TEXT PRIMARY KEY,  media_type TEXT NOT NULL,  path TEXT,  content_hash TEXT,  byte_size INTEGER,  pinned INTEGER DEFAULT 0,  produced_by TEXT,  label TEXT);" \
    "CREATE TABLE IF NOT EXISTS meta (  key TEXT PRIMARY KEY,  value TEXT);"
#endif
