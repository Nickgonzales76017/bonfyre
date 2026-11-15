/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESEMANTICOPTIMIZER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESEMANTICOPTIMIZER_H
/* cmd/BonfyreSemanticOptimizer/src/main.c */
#define BF_STORE_SEMANTIC_OPTIMIZER_INVERSE_TARGET_RECEIPTS \
    "CREATE TABLE IF NOT EXISTS inverse_target_receipts(target_id TEXT PRIMARY KEY,state TEXT NOT NULL,checked_at TEXT NOT NULL,residual_count INTEGER NOT NULL,selection_policy TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS inverse_predicate_receipts(target_id TEXT NOT NULL,predicate_id TEXT NOT NULL,owner TEXT NOT NULL,state TEXT NOT NULL,reason TEXT NOT NULL,checked_at TEXT NOT NULL,PRIMARY KEY(target_id,predicate_id));" \
    "CREATE TABLE IF NOT EXISTS inverse_closure_residuals(target_id TEXT NOT NULL,predicate_id TEXT NOT NULL,transformation_id TEXT NOT NULL,eclass INTEGER NOT NULL,pareto_state TEXT NOT NULL,checked_at TEXT NOT NULL,PRIMARY KEY(target_id,predicate_id,transformation_id));" \
    "CREATE TABLE IF NOT EXISTS inverse_cross_owner_residuals(target_id TEXT NOT NULL,predicate_id TEXT NOT NULL,transformation_id TEXT NOT NULL,capability_binding TEXT NOT NULL,authority_requirement TEXT NOT NULL,protocol_contract TEXT NOT NULL,commitment_pattern TEXT NOT NULL,resource_pattern TEXT NOT NULL,effect_kind TEXT NOT NULL,compensation_pattern TEXT NOT NULL,state TEXT NOT NULL,checked_at TEXT NOT NULL,PRIMARY KEY(target_id,predicate_id,transformation_id));" \
    "CREATE TABLE IF NOT EXISTS inverse_effect_admissions(target_id TEXT NOT NULL,predicate_id TEXT NOT NULL,transformation_id TEXT NOT NULL,state TEXT NOT NULL,reason TEXT NOT NULL,checked_at TEXT NOT NULL,PRIMARY KEY(target_id,predicate_id,transformation_id));" \
    "CREATE TABLE IF NOT EXISTS inverse_target_reconciliations(target_id TEXT PRIMARY KEY,state TEXT NOT NULL,proof_invariant TEXT NOT NULL DEFAULT '',checked_at TEXT NOT NULL,detail TEXT NOT NULL);"
/* cmd/BonfyreSemanticOptimizer/src/estate_evidence.c */
#define BF_STORE_SEMANTIC_OPTIMIZER_ESTATE_EVIDENCE_SYNC_RECEIPTS \
    "CREATE TABLE IF NOT EXISTS estate_evidence_sync_receipts(sync_id TEXT PRIMARY KEY,cms_path TEXT NOT NULL,publication_count INTEGER NOT NULL,publication_watermark TEXT NOT NULL,considered INTEGER NOT NULL,exact INTEGER NOT NULL,unresolved INTEGER NOT NULL,evidence_relations INTEGER NOT NULL,foreign_observations INTEGER NOT NULL,measured_invariants INTEGER NOT NULL,proven_invariants INTEGER NOT NULL,generation TEXT NOT NULL DEFAULT '',synced_at TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS estate_evidence_sync_latest ON estate_evidence_sync_receipts(synced_at DESC,sync_id DESC);"
#endif
