package stores

// cmd/BonfyreRecipe: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreRecipe/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreRecipe": {
	fragments: [
		{
			macro:  "BF_STORE_RECIPE_RECIPES"
			source: "cmd/BonfyreRecipe/src/main.c"
			statements: [
				{kind: "table", name: "recipes", sql: "CREATE TABLE IF NOT EXISTS recipes (  recipe_id TEXT PRIMARY KEY,  name TEXT NOT NULL,  version TEXT NOT NULL,  hash TEXT UNIQUE NOT NULL,  json_data TEXT NOT NULL,  created_at INTEGER NOT NULL,  category TEXT,  UNIQUE(recipe_id, version));"},
				{kind: "table", name: "recipe_tags", sql: "CREATE TABLE IF NOT EXISTS recipe_tags (  recipe_id TEXT,  tag TEXT,  FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id),  PRIMARY KEY(recipe_id, tag));"},
				{kind: "table", name: "models", sql: "CREATE TABLE IF NOT EXISTS models (  model_name TEXT PRIMARY KEY,  hash TEXT NOT NULL,  size_mb INTEGER,  source_url TEXT);"},
				{kind: "table", name: "recipe_models", sql: "CREATE TABLE IF NOT EXISTS recipe_models (  recipe_id TEXT,  model_name TEXT,  FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id),  FOREIGN KEY(model_name) REFERENCES models(model_name),  PRIMARY KEY(recipe_id, model_name));"},
				{kind: "virtual_table", name: "recipe_fts", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS recipe_fts USING fts5(  recipe_id UNINDEXED, name, description, tags);"},
			]
		},
	]
}
