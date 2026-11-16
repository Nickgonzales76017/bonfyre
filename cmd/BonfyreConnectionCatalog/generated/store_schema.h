/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRECONNECTIONCATALOG_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRECONNECTIONCATALOG_H
/* store connection_entries: columnar, declared by cmd/BonfyreConnectionCatalog; written by the connection studio hunt (workboard-source-entries-*.parquet) */
#define BF_STORE_CONNECTION_ENTRIES_KIND "columnar"
#define BF_STORE_CONNECTION_ENTRIES_COLUMNAR_FORMAT "parquet"
#define BF_STORE_CONNECTION_ENTRIES_COLUMNAR_COLD_GLOB "argument: list --entries <file.parquet>"
#define BF_STORE_CONNECTION_ENTRIES_COLUMNAR_PROMOTE "never: the cold files are the catalogue; nothing is promoted to a hot table"
#define BF_STORE_CONNECTION_ENTRIES_COLUMNAR_COLUMNS "source_goal_id:utf8,source_goal_key:utf8,original_ordinal:int32,source_entry:utf8,source_url_candidate:utf8,row_sha256:utf8,raw_line:utf8,goal_source:utf8"
#define BF_STORE_CONNECTION_ENTRIES_COLUMNAR_COLUMN_COUNT 8
#define BF_STORE_CONNECTION_ENTRIES_COLUMNAR_COLD_SCAN "read_parquet('%s')"
#define BF_STORE_CONNECTION_ENTRIES_COLUMNAR_COLD_SCAN_BOUND "read_parquet(?)"
#define BF_STORE_CONNECTION_ENTRIES_COLUMNAR_MISSING_COLUMNS "SELECT d.name FROM (VALUES ('source_goal_id'),('source_goal_key'),('original_ordinal'),('source_entry'),('source_url_candidate'),('row_sha256'),('raw_line'),('goal_source')) d(name) WHERE d.name NOT IN (SELECT name FROM parquet_schema(?)) ORDER BY d.name"
/* store connection_facets: columnar, declared by cmd/BonfyreConnectionCatalog; written by the connection studio hunt (workboard-connector-facets-*.parquet) */
#define BF_STORE_CONNECTION_FACETS_KIND "columnar"
#define BF_STORE_CONNECTION_FACETS_COLUMNAR_FORMAT "parquet"
#define BF_STORE_CONNECTION_FACETS_COLUMNAR_COLD_GLOB "argument: list --facets <file.parquet>"
#define BF_STORE_CONNECTION_FACETS_COLUMNAR_PROMOTE "never: the cold files are the catalogue; nothing is promoted to a hot table"
#define BF_STORE_CONNECTION_FACETS_COLUMNAR_COLUMNS "catalogue:utf8,raw_key:utf8,source_url_candidate:utf8,source_goal_id:utf8,source_goal_key:utf8,original_ordinal:int32,row_sha256:utf8,source_entry:utf8,status:utf8"
#define BF_STORE_CONNECTION_FACETS_COLUMNAR_COLUMN_COUNT 9
#define BF_STORE_CONNECTION_FACETS_COLUMNAR_COLD_SCAN "read_parquet('%s')"
#define BF_STORE_CONNECTION_FACETS_COLUMNAR_COLD_SCAN_BOUND "read_parquet(?)"
#define BF_STORE_CONNECTION_FACETS_COLUMNAR_MISSING_COLUMNS "SELECT d.name FROM (VALUES ('catalogue'),('raw_key'),('source_url_candidate'),('source_goal_id'),('source_goal_key'),('original_ordinal'),('row_sha256'),('source_entry'),('status')) d(name) WHERE d.name NOT IN (SELECT name FROM parquet_schema(?)) ORDER BY d.name"
#endif
