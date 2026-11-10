PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY,kind TEXT NOT NULL,title TEXT NOT NULL,prompt TEXT NOT NULL,state TEXT NOT NULL,
 worktree TEXT,branch TEXT,apply_to_project INTEGER NOT NULL DEFAULT 0,created_at REAL NOT NULL,
 started_at REAL,completed_at REAL,result_json TEXT NOT NULL DEFAULT '{}',error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS tasks_state_idx ON tasks(state,created_at DESC);
CREATE TABLE IF NOT EXISTS runs (
 id TEXT PRIMARY KEY,task_id TEXT NOT NULL,agent TEXT NOT NULL,role TEXT NOT NULL,state TEXT NOT NULL,
 session_id TEXT,command_json TEXT NOT NULL,cwd TEXT NOT NULL,stdout_path TEXT NOT NULL,stderr_path TEXT NOT NULL,
 final_path TEXT NOT NULL,exit_code INTEGER,started_at REAL NOT NULL,completed_at REAL,duration_ms INTEGER,result_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS runs_task_idx ON runs(task_id,started_at);
CREATE TABLE IF NOT EXISTS events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,ts REAL NOT NULL,source TEXT NOT NULL,kind TEXT NOT NULL,session_id TEXT,payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_ts_idx ON events(ts DESC);
CREATE TABLE IF NOT EXISTS locks (name TEXT PRIMARY KEY,task_id TEXT NOT NULL,acquired_at REAL NOT NULL,expires_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sessions (
 id TEXT PRIMARY KEY,agent TEXT NOT NULL,task_id TEXT,cwd TEXT NOT NULL,state TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL,metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS receipts (
 id TEXT PRIMARY KEY,task_id TEXT NOT NULL,kind TEXT NOT NULL,path TEXT NOT NULL,digest TEXT,payload_json TEXT NOT NULL,created_at REAL NOT NULL
);
-- Additive disclosure admission. Legacy tasks have no row and remain operator work.
CREATE TABLE IF NOT EXISTS task_scopes (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
 actor TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL CHECK(scope IN ('self','institution','household')),
 request_id TEXT NOT NULL,request_digest TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),created_at REAL NOT NULL,
 UNIQUE(actor,subject,scope,request_id),CHECK(scope!='self' OR actor=subject)
);
CREATE INDEX IF NOT EXISTS task_scopes_owner ON task_scopes(subject,scope,seq DESC);
CREATE TRIGGER IF NOT EXISTS task_scopes_immutable BEFORE UPDATE OF task_id,actor,subject,scope,request_id,request_digest ON task_scopes
 BEGIN SELECT RAISE(ABORT,'task disclosure admission is immutable'); END;
CREATE TRIGGER IF NOT EXISTS task_scope_task_revision AFTER UPDATE ON tasks
 BEGIN UPDATE task_scopes SET revision=revision+1 WHERE task_id=NEW.id; END;
CREATE TRIGGER IF NOT EXISTS task_scope_run_insert AFTER INSERT ON runs
 BEGIN UPDATE task_scopes SET revision=revision+1 WHERE task_id=NEW.task_id; END;
CREATE TRIGGER IF NOT EXISTS task_scope_run_revision AFTER UPDATE ON runs
 BEGIN UPDATE task_scopes SET revision=revision+1 WHERE task_id=NEW.task_id; END;
CREATE TRIGGER IF NOT EXISTS task_scope_receipt_revision AFTER INSERT ON receipts
 BEGIN UPDATE task_scopes SET revision=revision+1 WHERE task_id=NEW.task_id; END;
-- Recipient lifecycle belongs to this task owner, not to a browser cache or a queue cancel.
CREATE TABLE IF NOT EXISTS task_notifications (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,task_id TEXT NOT NULL REFERENCES task_scopes(task_id),
 recipient TEXT NOT NULL,event_key TEXT NOT NULL,kind TEXT NOT NULL,title TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('unread','read','dismissed')),revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
 created_at REAL NOT NULL,updated_at REAL NOT NULL,UNIQUE(task_id,recipient,event_key)
);
CREATE INDEX IF NOT EXISTS task_notifications_recipient ON task_notifications(recipient,state,seq DESC);
CREATE TABLE IF NOT EXISTS notification_actions (
 recipient TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,request_id TEXT NOT NULL,request_digest TEXT NOT NULL,
 notification_id TEXT NOT NULL REFERENCES task_notifications(id),receipt BLOB NOT NULL,created_at REAL NOT NULL,
 PRIMARY KEY(recipient,subject,scope,request_id)
);
-- Browser delivery extends the recipient owner. These rows are never task or
-- execution grants, and browser acknowledgements never mark an inbox item read.
CREATE TABLE IF NOT EXISTS push_subscriptions (
 id TEXT PRIMARY KEY,recipient TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,
 endpoint_hash TEXT NOT NULL,endpoint TEXT NOT NULL,p256dh TEXT NOT NULL,auth TEXT NOT NULL,key_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('active','revoked','expired')),reason TEXT NOT NULL DEFAULT '',
 after_seq INTEGER NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL,expires_at REAL NOT NULL,
 UNIQUE(recipient,subject,scope,endpoint_hash),CHECK(scope IN ('self','institution','household')),
 CHECK(scope!='self' OR recipient=subject)
);
CREATE INDEX IF NOT EXISTS push_subscriptions_recipient ON push_subscriptions(recipient,subject,scope,state);
CREATE INDEX IF NOT EXISTS push_subscriptions_endpoint ON push_subscriptions(endpoint_hash,state);
CREATE TABLE IF NOT EXISTS push_deliveries (
 id TEXT PRIMARY KEY,notification_id TEXT NOT NULL REFERENCES task_notifications(id),
 subscription_id TEXT NOT NULL REFERENCES push_subscriptions(id),notification_revision INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('queued','claimed','sending','retry','accepted','uncertain','revoked','expired','suppressed','denied','failed')),
 created_at REAL NOT NULL,updated_at REAL NOT NULL,expires_at REAL NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL,lease_until REAL NOT NULL DEFAULT 0,lease_token TEXT NOT NULL DEFAULT '',
 accepted_at REAL,shown_at REAL,opened_at REAL,last_status INTEGER NOT NULL DEFAULT 0,reason TEXT NOT NULL DEFAULT '',
 UNIQUE(notification_id,subscription_id)
);
CREATE INDEX IF NOT EXISTS push_deliveries_due ON push_deliveries(state,next_attempt);
CREATE TRIGGER IF NOT EXISTS push_subscription_owner_immutable
 BEFORE UPDATE OF recipient,subject,scope,endpoint_hash,endpoint,p256dh,auth,key_id ON push_subscriptions
 BEGIN SELECT RAISE(ABORT,'push recipient and device binding is immutable'); END;
