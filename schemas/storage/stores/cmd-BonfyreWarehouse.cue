package stores

// cmd/BonfyreWarehouse: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreWarehouse/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreWarehouse": {
	fragments: [
		{
			macro:  "BF_STORE_WAREHOUSE_WAREHOUSE_SNAPSHOT_HEAD"
			source: "cmd/BonfyreWarehouse/src/warehouse_seed.h"
			statements: [
				{kind: "table", name: "warehouse_snapshot_head", sql: "CREATE TABLE IF NOT EXISTS warehouse_snapshot_head(semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),representation_id TEXT NOT NULL,digest TEXT NOT NULL,frontier TEXT NOT NULL,created_at TEXT NOT NULL,changed_at TEXT NOT NULL,PRIMARY KEY(semantic_kind,representation_kind))"},
			]
		},
		{
			macro:  "BF_STORE_WAREHOUSE_WAREHOUSE_SNAPSHOT_STATS"
			source: "cmd/BonfyreWarehouse/src/warehouse_seed.h"
			statements: [
				{kind: "table", name: "warehouse_snapshot_stats", sql: "CREATE TABLE IF NOT EXISTS warehouse_snapshot_stats(semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,generation INTEGER NOT NULL,representation_id TEXT NOT NULL,digest TEXT NOT NULL,realization_count INTEGER NOT NULL,max_realization_bytes INTEGER,frontier_rows INTEGER,updated_at TEXT NOT NULL,PRIMARY KEY(semantic_kind,representation_kind))"},
			]
		},
		{
			macro:  "BF_STORE_WAREHOUSE_IDX_WAREHOUSE_RI_LATEST"
			source: "cmd/BonfyreWarehouse/src/warehouse_seed.h"
			statements: [
				{kind: "index", name: "idx_warehouse_ri_latest", sql: "CREATE INDEX IF NOT EXISTS idx_warehouse_ri_latest ON representation_instance(schema_ref,kind,created_at DESC,id DESC)"},
			]
		},
		{
			macro:  "BF_STORE_WAREHOUSE_IDX_WAREHOUSE_PR_LIVE"
			source: "cmd/BonfyreWarehouse/src/warehouse_seed.h"
			statements: [
				{kind: "index", name: "idx_warehouse_pr_live", sql: "CREATE INDEX IF NOT EXISTS idx_warehouse_pr_live ON physical_realization(representation_id,available,digest)"},
			]
		},
		{
			macro:  "BF_STORE_WAREHOUSE_IDX_WAREHOUSE_SB_KIND"
			source: "cmd/BonfyreWarehouse/src/warehouse_seed.h"
			statements: [
				{kind: "index", name: "idx_warehouse_sb_kind", sql: "CREATE INDEX IF NOT EXISTS idx_warehouse_sb_kind ON source_binding(semantic_kind)"},
			]
		},
	]
}
