package stores

// cmd/BonfyreModel: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreModel/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreModel": {
	fragments: [
		{
			macro:  "BF_STORE_MODEL_MODELS"
			source: "cmd/BonfyreModel/src/main.c"
			statements: [
				{kind: "table", name: "models", sql: "CREATE TABLE IF NOT EXISTS models (  id                  TEXT PRIMARY KEY,  name                TEXT NOT NULL,  description         TEXT,  format              TEXT NOT NULL,  sha256              TEXT UNIQUE,  size_mb             REAL,  fpq_sha256          TEXT,  fpq_size_mb         REAL,  transform_family    TEXT,  geometry            TEXT,  geometry_condition  TEXT,  layer_frag_spec     TEXT,  mean_f1             REAL,  added_at            INTEGER NOT NULL);"},
				{kind: "table", name: "sources", sql: "CREATE TABLE IF NOT EXISTS sources (  model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,  url      TEXT NOT NULL,  priority INTEGER NOT NULL DEFAULT 50,  PRIMARY KEY(model_id, url));"},
				{kind: "table", name: "recipe_models", sql: "CREATE TABLE IF NOT EXISTS recipe_models (  recipe_code TEXT NOT NULL,  model_id    TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,  role        TEXT,  PRIMARY KEY(recipe_code, model_id));"},
				{kind: "table", name: "model_representation_passport", sql: "CREATE TABLE IF NOT EXISTS model_representation_passport (  representation_id TEXT PRIMARY KEY,  model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,  representation_family TEXT NOT NULL,  representation_version TEXT NOT NULL,  exactness_class TEXT NOT NULL CHECK(exactness_class IN('exact','latent')),  measurement_state TEXT NOT NULL CHECK(measurement_state IN('measured','declared')),  admission_state TEXT NOT NULL CHECK(admission_state IN('ADMIT','DEFER','REJECT')),  admission_scope TEXT NOT NULL CHECK(admission_scope IN('diagnostic','embedding','rerank','generation')),  quality_envelope TEXT NOT NULL,  receipt_ref TEXT NOT NULL,  receipt_digest TEXT NOT NULL,  physical_body_ref TEXT NOT NULL,  physical_digest TEXT NOT NULL,  physical_bytes INTEGER NOT NULL CHECK(physical_bytes>=0),  supports_json TEXT NOT NULL,  recorded_at TEXT NOT NULL);"},
				{kind: "index", name: "model_representation_by_model", sql: "CREATE INDEX IF NOT EXISTS model_representation_by_model ON model_representation_passport(model_id,admission_scope,admission_state);"},
				{kind: "virtual_table", name: "models_fts", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS models_fts USING fts5(  id, name, description,  content=models, content_rowid=rowid);"},
			]
		},
	]
}
