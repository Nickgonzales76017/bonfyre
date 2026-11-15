/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREQUEUE_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREQUEUE_H
/* cmd/BonfyreQueue/src/account_work.h */
#define BF_STORE_QUEUE_ACCOUNT_WORK \
    "CREATE TABLE IF NOT EXISTS account_work(seq INTEGER PRIMARY KEY,id TEXT NOT NULL UNIQUE,subject TEXT NOT NULL,scope TEXT NOT NULL,source TEXT NOT NULL,title TEXT NOT NULL,state TEXT NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,attention TEXT NOT NULL DEFAULT '');" \
    "CREATE INDEX IF NOT EXISTS account_work_page ON account_work(subject,scope,seq DESC);" \
    "CREATE TABLE IF NOT EXISTS account_work_run(work_id TEXT NOT NULL,run_id TEXT NOT NULL,worker_id TEXT NOT NULL,state TEXT NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,agent TEXT NOT NULL DEFAULT '',role TEXT NOT NULL DEFAULT '',session_kind TEXT NOT NULL DEFAULT '',duration_ms INTEGER,limit_signal INTEGER NOT NULL DEFAULT 0,provider_session_ref TEXT NOT NULL DEFAULT '',handoff_ref TEXT NOT NULL DEFAULT '');" \
    "CREATE INDEX IF NOT EXISTS account_work_run_work ON account_work_run(work_id,updated_ms DESC);" \
    "CREATE TABLE IF NOT EXISTS account_work_usage(work_id TEXT NOT NULL,run_id TEXT NOT NULL,input_tokens INTEGER,output_tokens INTEGER,estimated_micro_usd INTEGER,cache_read_tokens INTEGER,cache_write_tokens INTEGER,PRIMARY KEY(work_id,run_id));" \
    "CREATE INDEX IF NOT EXISTS account_work_usage_work ON account_work_usage(work_id);" \
    "CREATE TABLE IF NOT EXISTS account_work_estimate(work_id TEXT NOT NULL,run_id TEXT NOT NULL,input_tokens INTEGER NOT NULL,output_tokens INTEGER NOT NULL,estimated_micro_usd INTEGER NOT NULL,source_digest TEXT NOT NULL,PRIMARY KEY(work_id,run_id));" \
    "CREATE TABLE IF NOT EXISTS account_work_step(work_id TEXT NOT NULL,node_id TEXT NOT NULL,operator_id TEXT NOT NULL,state TEXT NOT NULL,attempt INTEGER NOT NULL,updated_ms INTEGER NOT NULL,output_ref TEXT NOT NULL DEFAULT '');" \
    "CREATE INDEX IF NOT EXISTS account_work_step_work ON account_work_step(work_id,node_id);" \
    "CREATE TABLE IF NOT EXISTS account_work_meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL);"
/* cmd/BonfyreQueue/src/account_work.h */
#define BF_STORE_QUEUE_W \
    "CREATE TEMP TABLE w(id,source,title,state,created_ms,updated_ms,attention);" \
    "CREATE TEMP TABLE r(work_id,run_id,worker_id,state,created_ms,updated_ms,agent DEFAULT '',role DEFAULT '',session_kind DEFAULT '',duration_ms DEFAULT NULL,limit_signal DEFAULT 0,provider_session_ref DEFAULT '',handoff_ref DEFAULT '');" \
    "CREATE TEMP TABLE s(work_id,node_id,operator_id,state,attempt,updated_ms,output_ref TEXT NOT NULL DEFAULT '');" \
    "CREATE TEMP TABLE u(work_id,run_id,input_tokens,output_tokens,estimated_micro_usd,cache_read_tokens DEFAULT NULL,cache_write_tokens DEFAULT NULL);" \
    "CREATE INDEX temp.r_work ON r(work_id);" \
    "CREATE INDEX temp.s_work ON s(work_id);"
/* cmd/BonfyreQueue/src/account_work.h */
#define BF_STORE_QUEUE_TEMP_R_SESSION \
    "CREATE INDEX temp.r_session ON r(provider_session_ref);" \
    "CREATE INDEX temp.u_run ON u(work_id,run_id);" \
    "CREATE TEMP TABLE meter_foreign(ref TEXT PRIMARY KEY) WITHOUT ROWID;"
/* cmd/BonfyreQueue/src/account_activity.h */
#define BF_STORE_QUEUE_QUEUE_ACTIVITY_SCOPES \
    "CREATE TABLE IF NOT EXISTS queue_activity_scopes(sequence INTEGER PRIMARY KEY AUTOINCREMENT,mission_id TEXT NOT NULL UNIQUE,subject TEXT NOT NULL,scope TEXT NOT NULL,title TEXT NOT NULL,continuation TEXT NOT NULL,root_sha256 TEXT NOT NULL,created_at_ms INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS queue_activity_scope_page ON queue_activity_scopes(subject,scope,sequence DESC);" \
    "CREATE TRIGGER IF NOT EXISTS queue_activity_scope_immutable_update BEFORE UPDATE ON queue_activity_scopes BEGIN SELECT RAISE(ABORT,'queue activity admission is immutable'); END;" \
    "CREATE TRIGGER IF NOT EXISTS queue_activity_scope_immutable_delete BEFORE DELETE ON queue_activity_scopes BEGIN SELECT RAISE(ABORT,'queue activity admission is immutable'); END;"
/* cmd/BonfyreQueue/src/account_inbox.h */
#define BF_STORE_QUEUE_QUEUE_INBOX_STATES \
    "CREATE TABLE IF NOT EXISTS queue_inbox_states(subject TEXT NOT NULL,scope TEXT NOT NULL,actor TEXT NOT NULL,mission_id TEXT NOT NULL,source_version TEXT NOT NULL,read INTEGER NOT NULL CHECK(read IN(0,1)),dismissed INTEGER NOT NULL CHECK(dismissed IN(0,1)),revision INTEGER NOT NULL CHECK(revision>0),PRIMARY KEY(subject,scope,actor,mission_id));" \
    "CREATE TABLE IF NOT EXISTS queue_inbox_operations(subject TEXT NOT NULL,scope TEXT NOT NULL,actor TEXT NOT NULL,request_id TEXT NOT NULL,mission_id TEXT NOT NULL,source_version TEXT NOT NULL,expected_revision INTEGER NOT NULL,action TEXT NOT NULL,result_revision INTEGER NOT NULL,created_at_ms INTEGER NOT NULL,PRIMARY KEY(subject,scope,actor,request_id));"
/* store account_work_source: sqlite, declared by cmd/BonfyreQueue */
#define BF_STORE_QUEUE_WORK_SOURCE_KIND "sqlite"
#define BF_STORE_QUEUE_WORK_SOURCE_ATTACH "ATTACH DATABASE ? AS src"
#define BF_STORE_QUEUE_WORK_SOURCE_ATTACH_READONLY 1
#endif
