/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREPACKCONDITION_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREPACKCONDITION_H
/* cmd/BonfyrePackCondition/src/main.c */
#define BF_STORE_PACK_CONDITION_PACK_CONDITION_STATE \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS pack_condition_state(condition_ref TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('satisfied','unsatisfied')),generation INTEGER NOT NULL CHECK(generation>=1),basis TEXT NOT NULL CHECK(length(basis)>0),updated_at INTEGER NOT NULL,PRIMARY KEY(condition_ref,subject,scope));" \
    "CREATE INDEX IF NOT EXISTS pack_condition_subject_scope ON pack_condition_state(subject,scope,condition_ref);" \
    "CREATE TABLE IF NOT EXISTS pack_condition_outbox(condition_ref TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('satisfied','unsatisfied')),generation INTEGER NOT NULL CHECK(generation>=1),basis TEXT NOT NULL CHECK(length(basis)>0),emitted INTEGER NOT NULL DEFAULT 0 CHECK(emitted IN (0,1)),created_at INTEGER NOT NULL,PRIMARY KEY(condition_ref,subject,scope,generation));" \
    "CREATE INDEX IF NOT EXISTS pack_condition_outbox_pending ON pack_condition_outbox(emitted,created_at);"
#endif
