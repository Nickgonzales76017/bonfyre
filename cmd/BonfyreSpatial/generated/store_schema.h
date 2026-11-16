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
/* store address_plane: sqlite, declared by cmd/BonfyreSpatial; written by bonfyre-address-plane (--index) */
#define BF_STORE_SPATIAL_ADDRESS_PLANE_KIND "sqlite"
#define BF_STORE_SPATIAL_ADDRESS_PLANE_ATTACH "ATTACH DATABASE ? AS plane"
#define BF_STORE_SPATIAL_ADDRESS_PLANE_ATTACH_READONLY 1
/* store spatial_fact_cold: columnar, declared by cmd/BonfyreSpatial */
#define BF_STORE_SPATIAL_FACT_COLD_KIND "columnar"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_FORMAT "parquet"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_COLD_GLOB "argument: durable --out <file.parquet>"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_PROMOTE "thaw only: durable --thaw restores a cold file into a new SQLite file (BF_STORE_SPATIAL_THAWED_SPATIAL_FACT), never into the live hot store; durable --verify must report identical first"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_COLUMNS "fact_id:int64,observation_id:utf8,subject:utf8,geometry_kind:utf8,lon:float64,lat:float64,geometry:utf8,crs:utf8,observed_at:utf8,effective_at:utf8,valid_until:utf8,recorded_at:utf8,observation_mode:utf8,source_occurrence:utf8,derived_from:utf8,retracts:int64,retraction_reason:utf8,accuracy_h:float64,provenance:utf8"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_COLUMN_COUNT 19
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_COLD_SCAN "read_parquet('%s')"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_COLD_SCAN_BOUND "read_parquet(?)"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_MISSING_COLUMNS "SELECT d.name FROM (VALUES ('fact_id'),('observation_id'),('subject'),('geometry_kind'),('lon'),('lat'),('geometry'),('crs'),('observed_at'),('effective_at'),('valid_until'),('recorded_at'),('observation_mode'),('source_occurrence'),('derived_from'),('retracts'),('retraction_reason'),('accuracy_h'),('provenance')) d(name) WHERE d.name NOT IN (SELECT name FROM parquet_schema(?)) ORDER BY d.name"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_HOT_TABLE "spatial_fact"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_HOT_ATTACH "ATTACH '%s' AS hot (TYPE sqlite, READ_ONLY);"
#define BF_STORE_SPATIAL_FACT_COLD_COLUMNAR_EXPORT "COPY (SELECT * FROM hot.spatial_fact ORDER BY fact_id) TO '%s' (FORMAT parquet, COMPRESSION zstd);"
#endif
