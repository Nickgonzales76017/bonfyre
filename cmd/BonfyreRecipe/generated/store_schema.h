/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRERECIPE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRERECIPE_H
/* cmd/BonfyreRecipe/src/main.c */
#define BF_STORE_RECIPE_RECIPES \
    "CREATE TABLE IF NOT EXISTS recipes (  recipe_id TEXT PRIMARY KEY,  name TEXT NOT NULL,  version TEXT NOT NULL,  hash TEXT UNIQUE NOT NULL,  json_data TEXT NOT NULL,  created_at INTEGER NOT NULL,  category TEXT,  UNIQUE(recipe_id, version));" \
    "CREATE TABLE IF NOT EXISTS recipe_tags (  recipe_id TEXT,  tag TEXT,  FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id),  PRIMARY KEY(recipe_id, tag));" \
    "CREATE TABLE IF NOT EXISTS models (  model_name TEXT PRIMARY KEY,  hash TEXT NOT NULL,  size_mb INTEGER,  source_url TEXT);" \
    "CREATE TABLE IF NOT EXISTS recipe_models (  recipe_id TEXT,  model_name TEXT,  FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id),  FOREIGN KEY(model_name) REFERENCES models(model_name),  PRIMARY KEY(recipe_id, model_name));" \
    "CREATE VIRTUAL TABLE IF NOT EXISTS recipe_fts USING fts5(  recipe_id UNINDEXED, name, description, tags);"
#endif
