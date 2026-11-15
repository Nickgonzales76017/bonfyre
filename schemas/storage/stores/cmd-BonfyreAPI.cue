package stores

// cmd/BonfyreAPI: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreAPI/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreAPI": {
	fragments: [
		{
			macro:  "BF_STORE_API_JOBS"
			source: "cmd/BonfyreAPI/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA busy_timeout=5000;"},
				{kind: "other", name: "", sql: "PRAGMA synchronous=NORMAL;"},
				{kind: "table", name: "jobs", sql: "CREATE TABLE IF NOT EXISTS jobs (  id INTEGER PRIMARY KEY AUTOINCREMENT,  type TEXT NOT NULL,  status TEXT NOT NULL DEFAULT 'queued',  priority INTEGER NOT NULL DEFAULT 100,  input_path TEXT,  output_path TEXT,  source TEXT DEFAULT 'api',  api_key TEXT,  worker TEXT,  attempts INTEGER DEFAULT 0,  max_retries INTEGER DEFAULT 3,  created_at TEXT NOT NULL,  claimed_at TEXT,  completed_at TEXT,  error TEXT,  result_json TEXT,  wall_ms REAL,  exit_code INTEGER);"},
				{kind: "index", name: "idx_jobs_status", sql: "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);"},
				{kind: "index", name: "idx_jobs_priority", sql: "CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority, id);"},
				{kind: "index", name: "idx_jobs_type", sql: "CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);"},
				{kind: "table", name: "events", sql: "CREATE TABLE IF NOT EXISTS events (  id INTEGER PRIMARY KEY AUTOINCREMENT,  job_id INTEGER NOT NULL,  event TEXT NOT NULL,  data TEXT,  created_at TEXT NOT NULL,  FOREIGN KEY(job_id) REFERENCES jobs(id));"},
				{kind: "index", name: "idx_events_job", sql: "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);"},
				{kind: "table", name: "webhooks", sql: "CREATE TABLE IF NOT EXISTS webhooks (  id INTEGER PRIMARY KEY AUTOINCREMENT,  url TEXT NOT NULL,  events TEXT DEFAULT 'completed,failed',  active INTEGER DEFAULT 1,  created_at TEXT NOT NULL,  api_key TEXT,  actor TEXT,  subject TEXT,  pack_digest TEXT);"},
				{kind: "virtual_table", name: "jobs_fts", sql: "CREATE VIRTUAL TABLE IF NOT EXISTS jobs_fts USING fts5(  type, source, result_json, error,  content=jobs, content_rowid=id);"},
				{kind: "table", name: "uploads", sql: "CREATE TABLE IF NOT EXISTS uploads (  id INTEGER PRIMARY KEY AUTOINCREMENT,  filename TEXT NOT NULL,  path TEXT NOT NULL,  size_bytes INTEGER,  content_type TEXT,  uploaded_at TEXT NOT NULL,  api_key TEXT,  actor TEXT,  subject TEXT,  pack_digest TEXT);"},
				{kind: "table", name: "api_keys", sql: "CREATE TABLE IF NOT EXISTS api_keys (  id INTEGER PRIMARY KEY AUTOINCREMENT,  key TEXT NOT NULL UNIQUE,  label TEXT,  scopes TEXT DEFAULT '*',  auth_user_id INTEGER,  active INTEGER DEFAULT 1,  created_at TEXT NOT NULL);"},
				{kind: "index", name: "idx_api_keys_key", sql: "CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys(key);"},
				{kind: "table", name: "request_log", sql: "CREATE TABLE IF NOT EXISTS request_log (  id INTEGER PRIMARY KEY AUTOINCREMENT,  method TEXT,  path TEXT,  api_key TEXT,  status INTEGER,  wall_ms REAL,  created_at TEXT NOT NULL);"},
			]
		},
	]
}
