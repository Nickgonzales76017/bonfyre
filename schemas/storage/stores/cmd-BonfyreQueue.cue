package stores

// cmd/BonfyreQueue: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreQueue/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreQueue": {
	fragments: [
		{
			macro:  "BF_STORE_QUEUE_ACCOUNT_WORK"
			source: "cmd/BonfyreQueue/src/account_work.h"
			statements: [
				{kind: "table", name: "account_work", sql: "CREATE TABLE IF NOT EXISTS account_work(seq INTEGER PRIMARY KEY,id TEXT NOT NULL UNIQUE,subject TEXT NOT NULL,scope TEXT NOT NULL,source TEXT NOT NULL,title TEXT NOT NULL,state TEXT NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,attention TEXT NOT NULL DEFAULT '');"},
				{kind: "index", name: "account_work_page", sql: "CREATE INDEX IF NOT EXISTS account_work_page ON account_work(subject,scope,seq DESC);"},
				{kind: "table", name: "account_work_run", sql: "CREATE TABLE IF NOT EXISTS account_work_run(work_id TEXT NOT NULL,run_id TEXT NOT NULL,worker_id TEXT NOT NULL,state TEXT NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,agent TEXT NOT NULL DEFAULT '',role TEXT NOT NULL DEFAULT '',session_kind TEXT NOT NULL DEFAULT '',duration_ms INTEGER,limit_signal INTEGER NOT NULL DEFAULT 0,provider_session_ref TEXT NOT NULL DEFAULT '',handoff_ref TEXT NOT NULL DEFAULT '');"},
				{kind: "index", name: "account_work_run_work", sql: "CREATE INDEX IF NOT EXISTS account_work_run_work ON account_work_run(work_id,updated_ms DESC);"},
				{kind: "table", name: "account_work_usage", sql: "CREATE TABLE IF NOT EXISTS account_work_usage(work_id TEXT NOT NULL,run_id TEXT NOT NULL,input_tokens INTEGER,output_tokens INTEGER,estimated_micro_usd INTEGER,cache_read_tokens INTEGER,cache_write_tokens INTEGER,PRIMARY KEY(work_id,run_id));"},
				{kind: "index", name: "account_work_usage_work", sql: "CREATE INDEX IF NOT EXISTS account_work_usage_work ON account_work_usage(work_id);"},
				{kind: "table", name: "account_work_estimate", sql: "CREATE TABLE IF NOT EXISTS account_work_estimate(work_id TEXT NOT NULL,run_id TEXT NOT NULL,input_tokens INTEGER NOT NULL,output_tokens INTEGER NOT NULL,estimated_micro_usd INTEGER NOT NULL,source_digest TEXT NOT NULL,PRIMARY KEY(work_id,run_id));"},
				{kind: "table", name: "account_work_step", sql: "CREATE TABLE IF NOT EXISTS account_work_step(work_id TEXT NOT NULL,node_id TEXT NOT NULL,operator_id TEXT NOT NULL,state TEXT NOT NULL,attempt INTEGER NOT NULL,updated_ms INTEGER NOT NULL,output_ref TEXT NOT NULL DEFAULT '');"},
				{kind: "index", name: "account_work_step_work", sql: "CREATE INDEX IF NOT EXISTS account_work_step_work ON account_work_step(work_id,node_id);"},
				{kind: "table", name: "account_work_meta", sql: "CREATE TABLE IF NOT EXISTS account_work_meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL);"},
			]
		},
		{
			macro:  "BF_STORE_QUEUE_W"
			source: "cmd/BonfyreQueue/src/account_work.h"
			statements: [
				{kind: "table", name: "w", sql: "CREATE TEMP TABLE w(id,source,title,state,created_ms,updated_ms,attention);"},
				{kind: "table", name: "r", sql: "CREATE TEMP TABLE r(work_id,run_id,worker_id,state,created_ms,updated_ms,agent DEFAULT '',role DEFAULT '',session_kind DEFAULT '',duration_ms DEFAULT NULL,limit_signal DEFAULT 0,provider_session_ref DEFAULT '',handoff_ref DEFAULT '');"},
				{kind: "table", name: "s", sql: "CREATE TEMP TABLE s(work_id,node_id,operator_id,state,attempt,updated_ms,output_ref TEXT NOT NULL DEFAULT '');"},
				{kind: "table", name: "u", sql: "CREATE TEMP TABLE u(work_id,run_id,input_tokens,output_tokens,estimated_micro_usd,cache_read_tokens DEFAULT NULL,cache_write_tokens DEFAULT NULL);"},
				{kind: "index", name: "temp.r_work", sql: "CREATE INDEX temp.r_work ON r(work_id);"},
				{kind: "index", name: "temp.s_work", sql: "CREATE INDEX temp.s_work ON s(work_id);"},
			]
		},
		{
			macro:  "BF_STORE_QUEUE_TEMP_R_SESSION"
			source: "cmd/BonfyreQueue/src/account_work.h"
			statements: [
				{kind: "index", name: "temp.r_session", sql: "CREATE INDEX temp.r_session ON r(provider_session_ref);"},
				{kind: "index", name: "temp.u_run", sql: "CREATE INDEX temp.u_run ON u(work_id,run_id);"},
				{kind: "table", name: "meter_foreign", sql: "CREATE TEMP TABLE meter_foreign(ref TEXT PRIMARY KEY) WITHOUT ROWID;"},
			]
		},
		{
			macro:  "BF_STORE_QUEUE_QUEUE_ACTIVITY_SCOPES"
			source: "cmd/BonfyreQueue/src/account_activity.h"
			statements: [
				{kind: "table", name: "queue_activity_scopes", sql: "CREATE TABLE IF NOT EXISTS queue_activity_scopes(sequence INTEGER PRIMARY KEY AUTOINCREMENT,mission_id TEXT NOT NULL UNIQUE,subject TEXT NOT NULL,scope TEXT NOT NULL,title TEXT NOT NULL,continuation TEXT NOT NULL,root_sha256 TEXT NOT NULL,created_at_ms INTEGER NOT NULL);"},
				{kind: "index", name: "queue_activity_scope_page", sql: "CREATE INDEX IF NOT EXISTS queue_activity_scope_page ON queue_activity_scopes(subject,scope,sequence DESC);"},
				{kind: "trigger", name: "queue_activity_scope_immutable_update", sql: "CREATE TRIGGER IF NOT EXISTS queue_activity_scope_immutable_update BEFORE UPDATE ON queue_activity_scopes BEGIN SELECT RAISE(ABORT,'queue activity admission is immutable'); END;"},
				{kind: "trigger", name: "queue_activity_scope_immutable_delete", sql: "CREATE TRIGGER IF NOT EXISTS queue_activity_scope_immutable_delete BEFORE DELETE ON queue_activity_scopes BEGIN SELECT RAISE(ABORT,'queue activity admission is immutable'); END;"},
			]
		},
		{
			macro:  "BF_STORE_QUEUE_QUEUE_INBOX_STATES"
			source: "cmd/BonfyreQueue/src/account_inbox.h"
			statements: [
				{kind: "table", name: "queue_inbox_states", sql: "CREATE TABLE IF NOT EXISTS queue_inbox_states(subject TEXT NOT NULL,scope TEXT NOT NULL,actor TEXT NOT NULL,mission_id TEXT NOT NULL,source_version TEXT NOT NULL,read INTEGER NOT NULL CHECK(read IN(0,1)),dismissed INTEGER NOT NULL CHECK(dismissed IN(0,1)),revision INTEGER NOT NULL CHECK(revision>0),PRIMARY KEY(subject,scope,actor,mission_id));"},
				{kind: "table", name: "queue_inbox_operations", sql: "CREATE TABLE IF NOT EXISTS queue_inbox_operations(subject TEXT NOT NULL,scope TEXT NOT NULL,actor TEXT NOT NULL,request_id TEXT NOT NULL,mission_id TEXT NOT NULL,source_version TEXT NOT NULL,expected_revision INTEGER NOT NULL,action TEXT NOT NULL,result_revision INTEGER NOT NULL,created_at_ms INTEGER NOT NULL,PRIMARY KEY(subject,scope,actor,request_id));"},
			]
		},
	]
}
