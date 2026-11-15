package stores

// cmd/BonfyreSpatial: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreSpatial/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreSpatial": {
	fragments: [
		{
			macro:  "BF_STORE_SPATIAL_SPATIAL_FACT"
			source: "cmd/BonfyreSpatial/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "spatial_fact", sql: "CREATE TABLE IF NOT EXISTS spatial_fact(  fact_id INTEGER PRIMARY KEY AUTOINCREMENT,  observation_id TEXT NOT NULL,  subject TEXT NOT NULL,  geometry_kind TEXT NOT NULL,  lon REAL, lat REAL,  geometry TEXT NOT NULL,  crs TEXT NOT NULL,  observed_at TEXT NOT NULL,  effective_at TEXT NOT NULL,  valid_until TEXT,  recorded_at TEXT NOT NULL,  observation_mode TEXT NOT NULL,  source_occurrence TEXT NOT NULL,  derived_from TEXT NOT NULL DEFAULT '[]',  retracts INTEGER,  retraction_reason TEXT,  accuracy_h REAL,  provenance TEXT NOT NULL);"},
				{kind: "index", name: "spatial_fact_subject", sql: "CREATE INDEX IF NOT EXISTS spatial_fact_subject  ON spatial_fact(subject, effective_at);"},
				{kind: "index", name: "spatial_fact_knowledge", sql: "CREATE INDEX IF NOT EXISTS spatial_fact_knowledge  ON spatial_fact(recorded_at);"},
				{kind: "index", name: "spatial_fact_retraction", sql: "CREATE INDEX IF NOT EXISTS spatial_fact_retraction  ON spatial_fact(retracts, recorded_at) WHERE retracts IS NOT NULL;"},
				{kind: "index", name: "spatial_fact_observation", sql: "CREATE INDEX IF NOT EXISTS spatial_fact_observation  ON spatial_fact(observation_id);"},
				{kind: "index", name: "spatial_fact_derived", sql: "CREATE INDEX IF NOT EXISTS spatial_fact_derived  ON spatial_fact(fact_id) WHERE derived_from <> '[]';"},
				{kind: "table", name: "current_spatial_state", sql: "CREATE TABLE IF NOT EXISTS current_spatial_state(  subject TEXT PRIMARY KEY,  fact_id INTEGER NOT NULL,  effective_at TEXT NOT NULL,  maintained_through INTEGER NOT NULL);"},
				{kind: "table", name: "subject_alias", sql: "CREATE TABLE IF NOT EXISTS subject_alias(  alias TEXT PRIMARY KEY,  subject TEXT NOT NULL,  first_seen TEXT NOT NULL,  last_seen TEXT NOT NULL,  declared_by TEXT NOT NULL);"},
				{kind: "index", name: "subject_alias_subject", sql: "CREATE INDEX IF NOT EXISTS subject_alias_subject ON subject_alias(subject);"},
				{kind: "table", name: "maintenance_watermark", sql: "CREATE TABLE IF NOT EXISTS maintenance_watermark(  name TEXT PRIMARY KEY,  last_fact_id INTEGER NOT NULL);"},
				{kind: "trigger", name: "spatial_fact_no_update", sql: "CREATE TRIGGER IF NOT EXISTS spatial_fact_no_update BEFORE UPDATE ON spatial_fact  BEGIN SELECT RAISE(ABORT,'spatial facts are append-only: retract, do not update'); END;"},
				{kind: "trigger", name: "spatial_fact_no_delete", sql: "CREATE TRIGGER IF NOT EXISTS spatial_fact_no_delete BEFORE DELETE ON spatial_fact  BEGIN SELECT RAISE(ABORT,'spatial facts are append-only: retract, do not delete'); END;"},
			]
		},
		{
			macro:  "BF_STORE_SPATIAL_ORPHAN"
			source: "cmd/BonfyreSpatial/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "DROP TABLE IF EXISTS temp.orphan;"},
				{kind: "table", name: "orphan", sql: "CREATE TEMP TABLE orphan(fact_id INTEGER PRIMARY KEY);"},
			]
		},
		{
			macro:  "BF_STORE_SPATIAL_FRONTIER_LATEST"
			source: "cmd/BonfyreSpatial/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "DROP TABLE IF EXISTS temp.frontier_latest;"},
				{kind: "table", name: "frontier_latest", sql: "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);"},
			]
		},
		{
			macro:  "BF_STORE_SPATIAL_THAWED_SPATIAL_FACT"
			source: "cmd/BonfyreSpatial/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "INSTALL sqlite; "},
				{kind: "other", name: "", sql: "LOAD sqlite;"},
				{kind: "other", name: "", sql: "ATTACH '%s' AS thawed (TYPE sqlite);"},
				{kind: "table", name: "thawed.spatial_fact", sql: "CREATE TABLE thawed.spatial_fact AS SELECT * FROM read_parquet('%s');"},
			]
		},
		{
			macro:  "BF_STORE_SPATIAL_FRONTIER_LATEST_2"
			source: "cmd/BonfyreSpatial/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "DROP TABLE IF EXISTS temp.frontier_latest;"},
				{kind: "table", name: "frontier_latest", sql: "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);"},
				{kind: "other", name: "", sql: "INSERT OR IGNORE INTO temp.frontier_latest(fact_id) SELECT fact_id FROM current_spatial_state;"},
			]
		},
		{
			macro:  "BF_STORE_SPATIAL_FRONTIER_LATEST_3"
			source: "cmd/BonfyreSpatial/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "DROP TABLE IF EXISTS temp.frontier_latest;"},
				{kind: "table", name: "frontier_latest", sql: "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);"},
			]
		},
	]
}
