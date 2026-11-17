/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREWEBADDRESS_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREWEBADDRESS_H
/* cmd/BonfyreWebAddress/src/page_routes_and_emitters.h */
#define BF_STORE_WEB_ITEM_ARCHIVE \
    "PRAGMA journal_mode=DELETE;" \
    "CREATE TABLE IF NOT EXISTS item(source TEXT NOT NULL, key TEXT NOT NULL, kind TEXT NOT NULL, family TEXT DEFAULT '', title TEXT NOT NULL, body TEXT DEFAULT '', href TEXT DEFAULT '', image TEXT DEFAULT '', by TEXT DEFAULT '', publisher TEXT DEFAULT '', ident TEXT DEFAULT '', at INTEGER DEFAULT 0, first_read TEXT NOT NULL, last_read TEXT NOT NULL, PRIMARY KEY(source, key));" \
    "CREATE INDEX IF NOT EXISTS item_by_source ON item(source, at DESC);" \
    "CREATE INDEX IF NOT EXISTS idx_item_credit_by_source_at ON item(by, source, at DESC);" \
    "CREATE INDEX IF NOT EXISTS item_credit_scan ON item(family,by,source,at);" \
    "CREATE INDEX IF NOT EXISTS item_publisher_read ON item(publisher);" \
    "CREATE INDEX IF NOT EXISTS item_by_ident ON item(ident);" \
    "CREATE VIRTUAL TABLE IF NOT EXISTS item_text USING fts5(title, body, by, publisher, content='item', content_rowid='rowid', tokenize='porter unicode61');" \
    "CREATE TRIGGER IF NOT EXISTS item_text_insert AFTER INSERT ON item BEGIN INSERT INTO item_text(rowid,title,body,by,publisher) VALUES(new.rowid,new.title,new.body,new.by,new.publisher); END;" \
    "CREATE TRIGGER IF NOT EXISTS item_text_delete AFTER DELETE ON item BEGIN INSERT INTO item_text(item_text,rowid,title,body,by,publisher) VALUES('delete',old.rowid,old.title,old.body,old.by,old.publisher); END;" \
    "CREATE TRIGGER IF NOT EXISTS item_text_update AFTER UPDATE ON item BEGIN INSERT INTO item_text(item_text,rowid,title,body,by,publisher) VALUES('delete',old.rowid,old.title,old.body,old.by,old.publisher); INSERT INTO item_text(rowid,title,body,by,publisher) VALUES(new.rowid,new.title,new.body,new.by,new.publisher); END;"
/* store admission: sqlite, declared by cmd/BonfyreAdmission; written by bonfyre-admission */
#define BF_STORE_ADMISSION_KIND "sqlite"
#define BF_STORE_ADMISSION_ENV "BONFYRE_ADMISSION_DB"
#define BF_STORE_ADMISSION_LEASE { "admission", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_admission = { "admission", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store cms: sqlite, declared by cmd/BonfyreCMS */
#define BF_STORE_CMS_DB_KIND "sqlite"
#define BF_STORE_CMS_DB_ENV "BONFYRE_CMS_DB"
#define BF_STORE_CMS_DB_DURABILITY { "cms", "WAL", "NORMAL", 1000, 67108864LL, 16384, 5000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_cms = { "cms", "WAL", "NORMAL", 1000, 67108864LL, 16384, 5000, 120000 };
#endif
/* store account_work: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-queue account work */
#define BF_STORE_WEB_ACCOUNT_WORK_KIND "sqlite"
#define BF_STORE_WEB_ACCOUNT_WORK_ENV "BONFYRE_ACCOUNT_WORK_DB"
#define BF_STORE_WEB_ACCOUNT_WORK_LEASE { "account_work", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_account_work = { "account_work", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store auth: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-auth */
#define BF_STORE_WEB_AUTH_KIND "sqlite"
#define BF_STORE_WEB_AUTH_ENV "BONFYRE_AUTH_DB"
#define BF_STORE_WEB_AUTH_LEASE { "auth", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_auth = { "auth", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store browser_queue: sqlite, declared by cmd/BonfyreWebAddress; written by the browser page queue */
#define BF_STORE_WEB_BROWSER_QUEUE_KIND "sqlite"
#define BF_STORE_WEB_BROWSER_QUEUE_ENV "BONFYRE_BROWSER_QUEUE_DB"
#define BF_STORE_WEB_BROWSER_QUEUE_LEASE { "browser_queue", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_browser_queue = { "browser_queue", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store commerce: sqlite, declared by cmd/BonfyreWebAddress; written by the commerce owner (engine/core) */
#define BF_STORE_WEB_COMMERCE_KIND "sqlite"
#define BF_STORE_WEB_COMMERCE_ENV "BONFYRE_COMMERCE_DB"
#define BF_STORE_WEB_COMMERCE_LEASE { "commerce", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_commerce = { "commerce", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store commitment: sqlite, declared by cmd/BonfyreWebAddress; written by the commitment owner (engine/core) */
#define BF_STORE_WEB_COMMITMENT_KIND "sqlite"
#define BF_STORE_WEB_COMMITMENT_ENV "BONFYRE_COMMITMENT_DB"
#define BF_STORE_WEB_COMMITMENT_LEASE { "commitment", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_commitment = { "commitment", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store commitments_home: sqlite, declared by cmd/BonfyreWebAddress; written by the commitment owner (<home>/commitments.db) */
#define BF_STORE_WEB_COMMITMENTS_HOME_KIND "sqlite"
#define BF_STORE_WEB_COMMITMENTS_HOME_LEASE { "commitments_home", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_commitments_home = { "commitments_home", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store detail_depth: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-web-address detail-depth build */
#define BF_STORE_WEB_DETAIL_DEPTH_KIND "sqlite"
#define BF_STORE_WEB_DETAIL_DEPTH_ENV "BONFYRE_DETAIL_DEPTH_DB"
#define BF_STORE_WEB_DETAIL_DEPTH_LEASE { "detail_depth", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_detail_depth = { "detail_depth", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store economy_membrane: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-economy */
#define BF_STORE_WEB_ECONOMY_SCAN_KIND "sqlite"
#define BF_STORE_WEB_ECONOMY_SCAN_ENV "BONFYRE_ECONOMY_DB"
#define BF_STORE_WEB_ECONOMY_SCAN_DUCKDB_ATTACH "ATTACH '%s' AS ec (TYPE sqlite, READ_ONLY); "
/* store entity: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-entity */
#define BF_STORE_WEB_ENTITY_KIND "sqlite"
#define BF_STORE_WEB_ENTITY_ENV "BONFYRE_ENTITY_DB"
#define BF_STORE_WEB_ENTITY_LEASE { "entity", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_entity = { "entity", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store estate_scan: sqlite, declared by cmd/BonfyreWebAddress; written by whichever owner the caller names */
#define BF_STORE_WEB_ESTATE_SCAN_KIND "sqlite"
#define BF_STORE_WEB_ESTATE_SCAN_DUCKDB_ATTACH "ATTACH '%s' AS e (TYPE sqlite, READ_ONLY); "
/* store finance: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-finance */
#define BF_STORE_WEB_FINANCE_KIND "sqlite"
#define BF_STORE_WEB_FINANCE_ENV "BONFYRE_FINANCE_DB"
#define BF_STORE_WEB_FINANCE_LEASE { "finance", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_finance = { "finance", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store finance_membrane: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-finance */
#define BF_STORE_WEB_FINANCE_SCAN_KIND "sqlite"
#define BF_STORE_WEB_FINANCE_SCAN_ENV "BONFYRE_FINANCE_DB"
#define BF_STORE_WEB_FINANCE_SCAN_DUCKDB_ATTACH "ATTACH '%s' AS fi (TYPE sqlite, READ_ONLY); "
/* store fleming_work: sqlite, declared by cmd/BonfyreWebAddress; written by Fleming (<fleming root>/fleming-work.db) */
#define BF_STORE_WEB_FLEMING_WORK_KIND "sqlite"
#define BF_STORE_WEB_FLEMING_WORK_LEASE { "fleming_work", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_fleming_work = { "fleming_work", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
#define BF_STORE_WEB_FLEMING_WORK_DUCKDB_ATTACH "ATTACH '%s' AS w (TYPE sqlite, READ_ONLY); "
/* store foreign_observations: columnar, declared by cmd/BonfyreWebAddress; written by the ForeignTwin observation exporter */
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_KIND "columnar"
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_COLUMNAR_FORMAT "parquet"
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_COLUMNAR_COLD_GLOB "argument: the observation Parquet file the source page is given"
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_COLUMNAR_PROMOTE "never: observations stay cold; the page counts them in place"
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_COLUMNAR_COLUMNS "src_id:utf8,page_sha256:utf8,candidate_id:utf8,observed_at:utf8"
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_COLUMNAR_COLUMN_COUNT 4
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_COLUMNAR_COLD_SCAN "read_parquet('%s')"
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_COLUMNAR_COLD_SCAN_BOUND "read_parquet(?)"
#define BF_STORE_WEB_FOREIGN_OBSERVATIONS_COLUMNAR_MISSING_COLUMNS "SELECT d.name FROM (VALUES ('src_id'),('page_sha256'),('candidate_id'),('observed_at')) d(name) WHERE d.name NOT IN (SELECT name FROM parquet_schema(?)) ORDER BY d.name"
/* store frappe_surface: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-frappe-compiler */
#define BF_STORE_WEB_FRAPPE_SURFACE_KIND "sqlite"
#define BF_STORE_WEB_FRAPPE_SURFACE_ENV "BONFYRE_FRAPPE_SURFACE_DB"
#define BF_STORE_WEB_FRAPPE_SURFACE_LEASE { "frappe_surface", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_frappe_surface = { "frappe_surface", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store held_message: sqlite, declared by cmd/BonfyreWebAddress; written by the outbound message hold */
#define BF_STORE_WEB_HELD_MESSAGE_KIND "sqlite"
#define BF_STORE_WEB_HELD_MESSAGE_ENV "BONFYRE_HELD_MESSAGE_DB"
#define BF_STORE_WEB_HELD_MESSAGE_LEASE { "held_message", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_held_message = { "held_message", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store internet: sqlite, declared by cmd/BonfyreWebAddress; written by WebAddress internet observatory */
#define BF_STORE_WEB_INTERNET_KIND "sqlite"
#define BF_STORE_WEB_INTERNET_ENV "BONFYRE_INTERNET_DB"
#define BF_STORE_WEB_INTERNET_LEASE { "internet", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_internet = { "internet", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store item_archive: sqlite, declared by cmd/BonfyreWebAddress; written by the item archive builder */
#define BF_STORE_WEB_ITEM_ARCHIVE_KIND "sqlite"
#define BF_STORE_WEB_ITEM_ARCHIVE_ENV "BONFYRE_ITEM_ARCHIVE_DB"
#define BF_STORE_WEB_ITEM_ARCHIVE_LEASE { "item_archive", NULL, NULL, 0, -1LL, 0, 1500, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_item_archive = { "item_archive", NULL, NULL, 0, -1LL, 0, 1500, 120000 };
#endif
/* store item_depth: dynamic, declared by cmd/BonfyreWebAddress; written by the item archive builder */
#define BF_STORE_WEB_ITEM_DEPTH_KIND "dynamic"
#define BF_STORE_WEB_ITEM_DEPTH_ENV "BONFYRE_ITEM_ARCHIVE_DB"
#define BF_STORE_WEB_ITEM_DEPTH_LEASE { "item_depth", NULL, NULL, 0, -1LL, 0, 1500, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_item_depth = { "item_depth", NULL, NULL, 0, -1LL, 0, 1500, 120000 };
#endif
#define BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_SHARD "named"
#define BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_ATTACH "ATTACH DATABASE ?1 AS ?2"
#define BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_COUNT 4
#define BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOINS { { "fr", "BONFYRE_SOURCE_FRONTIER_DB", "channel", 0 }, { "fp", "BONFYRE_FRAPPE_SURFACE_DB", "frappe_program", 0 }, { "dd", "BONFYRE_DETAIL_DEPTH_DB", "item_facet", 0 }, { "jt", "BONFYRE_CMS_DB", "internet_source", 0 } }
#define BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_FR 0
#define BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_FP 1
#define BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_DD 2
#define BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_JT 3
/* store latent_atlas: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-latent-atlas */
#define BF_STORE_WEB_LATENT_ATLAS_KIND "sqlite"
#define BF_STORE_WEB_LATENT_ATLAS_ENV "BONFYRE_LATENT_ATLAS_DB"
#define BF_STORE_WEB_LATENT_ATLAS_LEASE { "latent_atlas", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_latent_atlas = { "latent_atlas", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store matrix: sqlite, declared by cmd/BonfyreWebAddress; written by the matrix closure compiler (engine/core) */
#define BF_STORE_WEB_MATRIX_KIND "sqlite"
#define BF_STORE_WEB_MATRIX_ENV "BONFYRE_MATRIX_DB"
#define BF_STORE_WEB_MATRIX_LEASE { "matrix", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_matrix = { "matrix", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store membrane: sqlite, declared by cmd/BonfyreWebAddress; written by the financial membrane */
#define BF_STORE_WEB_MEMBRANE_KIND "sqlite"
#define BF_STORE_WEB_MEMBRANE_ENV "BONFYRE_MEMBRANE_DB"
#define BF_STORE_WEB_MEMBRANE_LEASE { "membrane", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_membrane = { "membrane", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store meter: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-meter */
#define BF_STORE_WEB_METER_KIND "sqlite"
#define BF_STORE_WEB_METER_ENV "BONFYRE_METER_DB"
#define BF_STORE_WEB_METER_LEASE { "meter", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_meter = { "meter", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store model_catalog: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-model */
#define BF_STORE_WEB_MODEL_CATALOG_KIND "sqlite"
#define BF_STORE_WEB_MODEL_CATALOG_ENV "BONFYRE_MODEL_CATALOG"
#define BF_STORE_WEB_MODEL_CATALOG_LEASE { "model_catalog", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_model_catalog = { "model_catalog", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store operator: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-operator-store (beside the account work index) */
#define BF_STORE_WEB_OPERATOR_KIND "sqlite"
#define BF_STORE_WEB_OPERATOR_LEASE { "operator", NULL, NULL, 0, -1LL, 0, 3000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_operator = { "operator", NULL, NULL, 0, -1LL, 0, 3000, 120000 };
#endif
/* store participation: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-participation (BONFYRE_DATING_DB names the same file on older hosts) */
#define BF_STORE_WEB_PARTICIPATION_KIND "sqlite"
#define BF_STORE_WEB_PARTICIPATION_ENV "BONFYRE_PARTICIPATION_DB"
#define BF_STORE_WEB_PARTICIPATION_LEASE { "participation", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_participation = { "participation", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store pay: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-pay (<home>/pay.db) */
#define BF_STORE_WEB_PAY_KIND "sqlite"
#define BF_STORE_WEB_PAY_LEASE { "pay", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_pay = { "pay", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store people_cms: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-cms */
#define BF_STORE_WEB_PEOPLE_CMS_KIND "sqlite"
#define BF_STORE_WEB_PEOPLE_CMS_ENV "BONFYRE_CMS_DB"
#define BF_STORE_WEB_PEOPLE_CMS_ATTACH "ATTACH DATABASE ? AS cms_source"
#define BF_STORE_WEB_PEOPLE_CMS_ATTACH_READONLY 1
/* store prices: sqlite, declared by cmd/BonfyreWebAddress; written by the price observation index */
#define BF_STORE_WEB_PRICES_KIND "sqlite"
#define BF_STORE_WEB_PRICES_ENV "BONFYRE_PRICES_DB"
#define BF_STORE_WEB_PRICES_LEASE { "prices", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_prices = { "prices", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store public_projection: sqlite, declared by cmd/BonfyreWebAddress; written by WebAddress public projection cache */
#define BF_STORE_WEB_PUBLIC_PROJECTION_KIND "sqlite"
#define BF_STORE_WEB_PUBLIC_PROJECTION_LEASE { "public_projection", NULL, NULL, 0, -1LL, 0, 2000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_public_projection = { "public_projection", NULL, NULL, 0, -1LL, 0, 2000, 120000 };
#endif
/* store receipt_pack: sqlite, declared by cmd/BonfyreWebAddress; written by the ForeignTwin receipt packer */
#define BF_STORE_WEB_RECEIPT_PACK_KIND "sqlite"
#define BF_STORE_WEB_RECEIPT_PACK_ENV "BONFYRE_FOREIGN_RECEIPT_PACK"
#define BF_STORE_WEB_RECEIPT_PACK_LEASE { "receipt_pack", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_receipt_pack = { "receipt_pack", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store source_frontier: sqlite, declared by cmd/BonfyreWebAddress; written by bonfyre-source-frontier */
#define BF_STORE_WEB_SOURCE_FRONTIER_KIND "sqlite"
#define BF_STORE_WEB_SOURCE_FRONTIER_ENV "BONFYRE_SOURCE_FRONTIER_DB"
#define BF_STORE_WEB_SOURCE_FRONTIER_LEASE { "source_frontier", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_source_frontier = { "source_frontier", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store subject_fabric: sqlite, declared by cmd/BonfyreWebAddress; written by the subject fabric compiler */
#define BF_STORE_WEB_SUBJECT_FABRIC_KIND "sqlite"
#define BF_STORE_WEB_SUBJECT_FABRIC_ENV "BONFYRE_SUBJECT_FABRIC_DB"
#define BF_STORE_WEB_SUBJECT_FABRIC_LEASE { "subject_fabric", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_subject_fabric = { "subject_fabric", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
/* store surfaceir: sqlite, declared by cmd/BonfyreWebAddress; written by WebAddress room composer */
#define BF_STORE_WEB_SURFACEIR_KIND "sqlite"
#define BF_STORE_WEB_SURFACEIR_ENV "BONFYRE_SURFACEIR_DB"
#define BF_STORE_WEB_SURFACEIR_LEASE { "surfaceir", NULL, NULL, 0, -1LL, 0, 2000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_surfaceir = { "surfaceir", NULL, NULL, 0, -1LL, 0, 2000, 120000 };
#endif
/* store websub: sqlite, declared by cmd/BonfyreWebAddress; written by WebAddress WebSub subscriber */
#define BF_STORE_WEB_WEBSUB_KIND "sqlite"
#define BF_STORE_WEB_WEBSUB_ENV "BONFYRE_WEBSUB_DB"
#define BF_STORE_WEB_WEBSUB_LEASE { "websub", NULL, NULL, 0, -1LL, 0, 1000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_websub = { "websub", NULL, NULL, 0, -1LL, 0, 1000, 120000 };
#endif
#endif
