/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREWAREHOUSE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREWAREHOUSE_H
/* cmd/BonfyreWarehouse/src/warehouse_seed.h */
#define BF_STORE_WAREHOUSE_WAREHOUSE_SNAPSHOT_HEAD \
    "CREATE TABLE IF NOT EXISTS warehouse_snapshot_head(semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),representation_id TEXT NOT NULL,digest TEXT NOT NULL,frontier TEXT NOT NULL,created_at TEXT NOT NULL,changed_at TEXT NOT NULL,PRIMARY KEY(semantic_kind,representation_kind))"
/* cmd/BonfyreWarehouse/src/warehouse_seed.h */
#define BF_STORE_WAREHOUSE_WAREHOUSE_SNAPSHOT_STATS \
    "CREATE TABLE IF NOT EXISTS warehouse_snapshot_stats(semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,generation INTEGER NOT NULL,representation_id TEXT NOT NULL,digest TEXT NOT NULL,realization_count INTEGER NOT NULL,max_realization_bytes INTEGER,frontier_rows INTEGER,updated_at TEXT NOT NULL,PRIMARY KEY(semantic_kind,representation_kind))"
/* cmd/BonfyreWarehouse/src/warehouse_seed.h */
#define BF_STORE_WAREHOUSE_IDX_WAREHOUSE_RI_LATEST \
    "CREATE INDEX IF NOT EXISTS idx_warehouse_ri_latest ON representation_instance(schema_ref,kind,created_at DESC,id DESC)"
/* cmd/BonfyreWarehouse/src/warehouse_seed.h */
#define BF_STORE_WAREHOUSE_IDX_WAREHOUSE_PR_LIVE \
    "CREATE INDEX IF NOT EXISTS idx_warehouse_pr_live ON physical_realization(representation_id,available,digest)"
/* cmd/BonfyreWarehouse/src/warehouse_seed.h */
#define BF_STORE_WAREHOUSE_IDX_WAREHOUSE_SB_KIND \
    "CREATE INDEX IF NOT EXISTS idx_warehouse_sb_kind ON source_binding(semantic_kind)"
#endif
