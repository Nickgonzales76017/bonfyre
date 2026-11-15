/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESPATIAL_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESPATIAL_H
/* cmd/BonfyreSpatial/src/main.c */
#define BF_STORE_SPATIAL_SPATIAL_FACT \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS spatial_fact(  fact_id INTEGER PRIMARY KEY AUTOINCREMENT,  observation_id TEXT NOT NULL,  subject TEXT NOT NULL,  geometry_kind TEXT NOT NULL,  lon REAL, lat REAL,  geometry TEXT NOT NULL,  crs TEXT NOT NULL,  observed_at TEXT NOT NULL,  effective_at TEXT NOT NULL,  valid_until TEXT,  recorded_at TEXT NOT NULL,  observation_mode TEXT NOT NULL,  source_occurrence TEXT NOT NULL,  derived_from TEXT NOT NULL DEFAULT '[]',  retracts INTEGER,  retraction_reason TEXT,  accuracy_h REAL,  provenance TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS spatial_fact_subject  ON spatial_fact(subject, effective_at);" \
    "CREATE INDEX IF NOT EXISTS spatial_fact_knowledge  ON spatial_fact(recorded_at);" \
    "CREATE INDEX IF NOT EXISTS spatial_fact_retraction  ON spatial_fact(retracts, recorded_at) WHERE retracts IS NOT NULL;" \
    "CREATE INDEX IF NOT EXISTS spatial_fact_observation  ON spatial_fact(observation_id);" \
    "CREATE INDEX IF NOT EXISTS spatial_fact_derived  ON spatial_fact(fact_id) WHERE derived_from <> '[]';" \
    "CREATE TABLE IF NOT EXISTS current_spatial_state(  subject TEXT PRIMARY KEY,  fact_id INTEGER NOT NULL,  effective_at TEXT NOT NULL,  maintained_through INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS subject_alias(  alias TEXT PRIMARY KEY,  subject TEXT NOT NULL,  first_seen TEXT NOT NULL,  last_seen TEXT NOT NULL,  declared_by TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS subject_alias_subject ON subject_alias(subject);" \
    "CREATE TABLE IF NOT EXISTS maintenance_watermark(  name TEXT PRIMARY KEY,  last_fact_id INTEGER NOT NULL);" \
    "CREATE TRIGGER IF NOT EXISTS spatial_fact_no_update BEFORE UPDATE ON spatial_fact  BEGIN SELECT RAISE(ABORT,'spatial facts are append-only: retract, do not update'); END;" \
    "CREATE TRIGGER IF NOT EXISTS spatial_fact_no_delete BEFORE DELETE ON spatial_fact  BEGIN SELECT RAISE(ABORT,'spatial facts are append-only: retract, do not delete'); END;"
/* cmd/BonfyreSpatial/src/main.c */
#define BF_STORE_SPATIAL_ORPHAN \
    "DROP TABLE IF EXISTS temp.orphan;" \
    "CREATE TEMP TABLE orphan(fact_id INTEGER PRIMARY KEY);"
/* cmd/BonfyreSpatial/src/main.c */
#define BF_STORE_SPATIAL_FRONTIER_LATEST \
    "DROP TABLE IF EXISTS temp.frontier_latest;" \
    "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);"
/* cmd/BonfyreSpatial/src/main.c */
#define BF_STORE_SPATIAL_THAWED_SPATIAL_FACT \
    "INSTALL sqlite; " \
    "LOAD sqlite;" \
    "ATTACH '%s' AS thawed (TYPE sqlite);" \
    "CREATE TABLE thawed.spatial_fact AS SELECT * FROM read_parquet('%s');"
/* cmd/BonfyreSpatial/src/main.c */
#define BF_STORE_SPATIAL_FRONTIER_LATEST_2 \
    "DROP TABLE IF EXISTS temp.frontier_latest;" \
    "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);" \
    "INSERT OR IGNORE INTO temp.frontier_latest(fact_id) SELECT fact_id FROM current_spatial_state;"
/* cmd/BonfyreSpatial/src/main.c */
#define BF_STORE_SPATIAL_FRONTIER_LATEST_3 \
    "DROP TABLE IF EXISTS temp.frontier_latest;" \
    "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);"
#endif
