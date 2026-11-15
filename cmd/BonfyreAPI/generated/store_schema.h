/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREAPI_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREAPI_H
/* cmd/BonfyreAPI/src/main.c */
#define BF_STORE_API_JOBS \
    "PRAGMA journal_mode=WAL;" \
    "PRAGMA busy_timeout=5000;" \
    "PRAGMA synchronous=NORMAL;" \
    "CREATE TABLE IF NOT EXISTS jobs (  id INTEGER PRIMARY KEY AUTOINCREMENT,  type TEXT NOT NULL,  status TEXT NOT NULL DEFAULT 'queued',  priority INTEGER NOT NULL DEFAULT 100,  input_path TEXT,  output_path TEXT,  source TEXT DEFAULT 'api',  api_key TEXT,  worker TEXT,  attempts INTEGER DEFAULT 0,  max_retries INTEGER DEFAULT 3,  created_at TEXT NOT NULL,  claimed_at TEXT,  completed_at TEXT,  error TEXT,  result_json TEXT,  wall_ms REAL,  exit_code INTEGER);" \
    "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);" \
    "CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority, id);" \
    "CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);" \
    "CREATE TABLE IF NOT EXISTS events (  id INTEGER PRIMARY KEY AUTOINCREMENT,  job_id INTEGER NOT NULL,  event TEXT NOT NULL,  data TEXT,  created_at TEXT NOT NULL,  FOREIGN KEY(job_id) REFERENCES jobs(id));" \
    "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);" \
    "CREATE TABLE IF NOT EXISTS webhooks (  id INTEGER PRIMARY KEY AUTOINCREMENT,  url TEXT NOT NULL,  events TEXT DEFAULT 'completed,failed',  active INTEGER DEFAULT 1,  created_at TEXT NOT NULL,  api_key TEXT,  actor TEXT,  subject TEXT,  pack_digest TEXT);" \
    "CREATE VIRTUAL TABLE IF NOT EXISTS jobs_fts USING fts5(  type, source, result_json, error,  content=jobs, content_rowid=id);" \
    "CREATE TABLE IF NOT EXISTS uploads (  id INTEGER PRIMARY KEY AUTOINCREMENT,  filename TEXT NOT NULL,  path TEXT NOT NULL,  size_bytes INTEGER,  content_type TEXT,  uploaded_at TEXT NOT NULL,  api_key TEXT,  actor TEXT,  subject TEXT,  pack_digest TEXT);" \
    "CREATE TABLE IF NOT EXISTS api_keys (  id INTEGER PRIMARY KEY AUTOINCREMENT,  key TEXT NOT NULL UNIQUE,  label TEXT,  scopes TEXT DEFAULT '*',  auth_user_id INTEGER,  active INTEGER DEFAULT 1,  created_at TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys(key);" \
    "CREATE TABLE IF NOT EXISTS request_log (  id INTEGER PRIMARY KEY AUTOINCREMENT,  method TEXT,  path TEXT,  api_key TEXT,  status INTEGER,  wall_ms REAL,  created_at TEXT NOT NULL);"
#endif
